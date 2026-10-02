#!/usr/bin/env python3
"""
NameNode - the metadata service of the mini HDFS.

Responsibilities
  * namespace: filename -> ordered list of blocks (with size + SHA-256)
  * block map: block -> set of DataNodes holding a replica (rebuilt from block
    reports, never trusted blindly from disk)
  * placement: decides which DataNodes receive each new block
  * liveness: heartbeat tracking, dead-node detection
  * fault tolerance: detects under-/over-replicated blocks and orders DataNodes
    to copy / delete replicas

What it deliberately does NOT do: it never touches file data and never opens
outgoing connections. Orders to DataNodes (replicate / delete) are returned in
heartbeat responses, exactly like HDFS. Clients stream data straight to and
from DataNodes, so the NameNode is never a data-path bottleneck.
"""
import json
import os
import random
import signal
import threading
import time
import uuid
from collections import defaultdict

from common import Server, env_float, env_int, env_str, setup_logging

log = setup_logging("namenode")

# ---------------------------------------------------------------- config ---
HOST = env_str("NAMENODE_BIND", "0.0.0.0")
PORT = env_int("NAMENODE_PORT", 5000)
DATA_DIR = env_str("NAMENODE_DATA_DIR", "./nn_data")
CHUNK_SIZE = env_int("CHUNK_SIZE", 4 * 1024 * 1024)
REPLICATION = env_int("REPLICATION_FACTOR", 2)
HEARTBEAT_INTERVAL = env_float("HEARTBEAT_INTERVAL", 3.0)       # told to DataNodes
HEARTBEAT_TIMEOUT = env_float("HEARTBEAT_TIMEOUT", 15.0)
BLOCK_REPORT_INTERVAL = env_float("BLOCK_REPORT_INTERVAL", 60.0)  # told to DataNodes
MONITOR_INTERVAL = env_float("MONITOR_INTERVAL", 3.0)
SAFE_MODE_SECONDS = env_float("SAFE_MODE_SECONDS", 15.0)
REPLICATION_TIMEOUT = env_float("REPLICATION_TIMEOUT", 60.0)
PENDING_FILE_TTL = env_float("PENDING_FILE_TTL", 600.0)
MAX_REPL_COMMANDS_PER_CYCLE = env_int("MAX_REPL_COMMANDS_PER_CYCLE", 50)
MIN_FREE_BYTES = env_int("MIN_FREE_BYTES", 64 * 1024 * 1024)
REPORT_GRACE = env_float("REPORT_GRACE", 10.0)   # fresh replicas are not dropped by a stale report
DELETE_RETRY = env_float("DELETE_RETRY", 30.0)   # re-issue a delete order that was never carried out


class NNError(Exception):
    """Error that is reported back to the caller as {'status': 'error'}."""


class NameNode:
    def __init__(self):
        self.lock = threading.RLock()          # guards ALL state below
        self.started = time.time()
        self.cluster_id = None
        self.files = {}        # filename -> {file_id,size,blocks[],replication,created}
        self.blocks = {}       # block_id -> {file_id,idx,size,sha256,replication,state,replicas{node:ts}}
        self.pending = {}      # file_id -> {filename,replication,created,touched}
        self.datanodes = {}    # node_id -> {host,port,alive,last_hb,stats,registered}
        self.node_blocks = defaultdict(set)
        self.commands = defaultdict(list)       # node_id -> [cmd] delivered via heartbeat
        self.pending_repl = {}                  # block_id -> {target_id: ts}
        self.pending_delete = defaultdict(dict) # node_id -> {block_id: ts} delete ordered, not yet confirmed
        self.save_lock = threading.Lock()      # serializes metadata file writes
        self.stop = threading.Event()
        self._load()
        self.safe_mode_until = self.started + (SAFE_MODE_SECONDS if self.blocks else 0)

    # ----------------------------------------------------- persistence ------
    @property
    def _meta_path(self):
        return os.path.join(DATA_DIR, "metadata.json")

    def _load(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        if not os.path.isfile(self._meta_path):
            self.cluster_id = uuid.uuid4().hex
            log.info("new namespace created, cluster_id=%s", self.cluster_id)
            self.save()
            return
        with open(self._meta_path, "r") as f:
            data = json.load(f)           # corrupt file => fail loudly, never start "empty"
        self.cluster_id = data["cluster_id"]
        self.files = data.get("files", {})
        for bid, b in data.get("blocks", {}).items():
            b["replicas"] = {}
            b["state"] = "complete"
            self.blocks[bid] = b
        log.info("loaded namespace: %d files, %d blocks (replica locations come from block reports)",
                 len(self.files), len(self.blocks))

    def _snapshot(self):
        with self.lock:
            return {
                "version": 1,
                "cluster_id": self.cluster_id,
                "files": self.files,
                "blocks": {
                    bid: {k: v for k, v in b.items() if k not in ("replicas", "state")}
                    for bid, b in self.blocks.items() if b["state"] == "complete"
                },
            }

    def save(self):
        """Durable atomic write: temp file + fsync + rename (+ directory fsync).
        Called synchronously *before* a mutating request is acknowledged, so an
        acknowledged upload/delete survives a NameNode crash. Never called under
        self.lock (disk I/O must not block heartbeats)."""
        with self.save_lock:
            data = json.dumps(self._snapshot())
            tmp = self._meta_path + ".tmp"
            with open(tmp, "w") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._meta_path)
            dfd = os.open(DATA_DIR, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)

    # ------------------------------------------------ internal helpers ------
    def _node_info(self, nid):
        n = self.datanodes[nid]
        return {"id": nid, "host": n["host"], "port": n["port"]}

    def _add_replica(self, bid, nid):
        b = self.blocks.get(bid)
        if b is not None:
            b["replicas"][nid] = time.time()
            self.node_blocks[nid].add(bid)

    def _remove_replica(self, bid, nid):
        b = self.blocks.get(bid)
        if b is not None:
            b["replicas"].pop(nid, None)
        self.node_blocks[nid].discard(bid)

    def _queue_delete(self, nid, bid):
        n = self.datanodes.get(nid)
        if n and n["alive"]:      # dead nodes are reconciled by their next block report
            self.commands[nid].append({"type": "delete", "block_id": bid})
            # Remember the order: until the node confirms (by no longer listing the block) a
            # block report that still lists it must NOT bring the replica back, otherwise the
            # replica would be counted again and trimmed a second time (undershooting R).
            self.pending_delete[nid][bid] = time.time()

    def _drop_block(self, bid):
        b = self.blocks.pop(bid, None)
        if not b:
            return
        for nid in list(b["replicas"]):
            self.node_blocks[nid].discard(bid)
            self._queue_delete(nid, bid)
        self.pending_repl.pop(bid, None)

    def _live(self):
        return {i: n for i, n in self.datanodes.items() if n["alive"]}

    def _pick_targets(self, count, exclude=()):
        """Least-loaded live nodes (by block count) with enough free disk."""
        cands = []
        for nid, n in self.datanodes.items():
            if not n["alive"] or nid in exclude:
                continue
            if n["stats"].get("disk_free", MIN_FREE_BYTES) < MIN_FREE_BYTES:
                continue
            cands.append((len(self.node_blocks[nid]), random.random(), nid))
        cands.sort()
        return [nid for _, _, nid in cands[:count]]

    @property
    def safe_mode(self):
        return time.time() < self.safe_mode_until

    # --------------------------------------------- DataNode-facing RPCs ------
    def rpc_register(self, h, addr):
        nid = h["node_id"]
        cid = h.get("cluster_id")
        with self.lock:
            if cid and cid != self.cluster_id:
                raise NNError(f"cluster_id mismatch (node has {cid}, namenode is {self.cluster_id}); "
                              "refusing so that its blocks are not deleted")
            host = h.get("host") or addr[0]
            old = self.datanodes.get(nid)
            self.datanodes[nid] = {
                "host": host, "port": int(h["port"]), "alive": True,
                "last_hb": time.time(), "stats": h.get("stats", {}),
                "registered": old["registered"] if old else time.time(),
            }
            self._process_block_report(nid, h.get("blocks", []), full_replace=True)
            log.info("registered %s at %s:%s (%d blocks reported)", nid, host, h["port"],
                     len(h.get("blocks", [])))
        return {"status": "ok", "cluster_id": self.cluster_id,
                "heartbeat_interval": HEARTBEAT_INTERVAL,
                "block_report_interval": BLOCK_REPORT_INTERVAL}

    def rpc_heartbeat(self, h, addr):
        nid = h["node_id"]
        with self.lock:
            n = self.datanodes.get(nid)
            if n is None or not n["alive"]:
                return {"status": "reregister"}     # unknown or declared dead -> full re-register
            n["last_hb"] = time.time()
            n["stats"] = h.get("stats", {})
            cmds, self.commands[nid] = self.commands[nid], []
        return {"status": "ok", "commands": cmds}

    def rpc_block_report(self, h, addr):
        nid = h["node_id"]
        with self.lock:
            n = self.datanodes.get(nid)
            if n is None or not n["alive"]:
                return {"status": "reregister"}
            self._process_block_report(nid, h.get("blocks", []), full_replace=False)
        return {"status": "ok"}

    def _process_block_report(self, nid, reported, full_replace):
        """Reconcile what the node really holds with what we believe it holds."""
        now = time.time()
        if full_replace:
            for bid in list(self.node_blocks[nid]):
                self._remove_replica(bid, nid)
        seen = set()
        doomed = self.pending_delete[nid]
        for r in reported:
            bid = r["id"]
            seen.add(bid)
            if bid in doomed:
                if now - doomed[bid] > DELETE_RETRY:          # order was lost: send it again
                    self._queue_delete(nid, bid)
                continue                                      # being deleted: not a usable replica
            b = self.blocks.get(bid)
            if b is None:
                self._queue_delete(nid, bid)        # block of a deleted/unknown file
                continue
            if b["state"] != "complete":
                continue                            # upload in progress; commit will register it
            if r.get("sha256") != b["sha256"] or r.get("size") != b["size"]:
                log.warning("node %s holds a wrong copy of %s -> deleting it", nid, bid)
                self._remove_replica(bid, nid)
                self._queue_delete(nid, bid)
                continue
            if nid not in b["replicas"]:
                self._add_replica(bid, nid)
        for bid in [d for d in doomed if d not in seen]:
            del doomed[bid]                                   # node no longer holds it: delete confirmed
        if not full_replace:
            for bid in list(self.node_blocks[nid]):
                b = self.blocks.get(bid)
                if bid not in seen and b and now - b["replicas"].get(nid, 0) > REPORT_GRACE:
                    log.warning("node %s no longer holds %s -> replica removed", nid, bid)
                    self._remove_replica(bid, nid)

    def rpc_block_received(self, h, addr):
        nid, bid = h["node_id"], h["block_id"]
        with self.lock:
            b = self.blocks.get(bid)
            if b is None or b["state"] != "complete":
                self._queue_delete(nid, bid)
                return {"status": "ok"}
            if bid in self.pending_delete[nid]:
                return {"status": "ok"}                       # replica is being deleted; ignore
            if nid in self.datanodes and self.datanodes[nid]["alive"]:
                self._add_replica(bid, nid)
            tgts = self.pending_repl.get(bid)
            if tgts:
                tgts.pop(nid, None)
                if not tgts:
                    self.pending_repl.pop(bid, None)
        return {"status": "ok"}

    def rpc_block_deleted(self, h, addr):
        """DataNode confirms a delete order was carried out; the block may be placed there again."""
        with self.lock:
            self.pending_delete[h["node_id"]].pop(h["block_id"], None)
        return {"status": "ok"}

    def rpc_report_corrupt(self, h, addr):
        nid, bid = h["node_id"], h["block_id"]
        with self.lock:
            if bid in self.blocks:
                log.warning("corrupt replica of %s on %s reported -> will re-replicate", bid, nid)
                self._remove_replica(bid, nid)
                self._queue_delete(nid, bid)
        return {"status": "ok"}

    # ------------------------------------------------- client-facing RPCs ----
    def rpc_create_file(self, h, addr):
        name = h["filename"]
        with self.lock:
            if not self._live():
                raise NNError("no live datanodes")
            fid = uuid.uuid4().hex
            now = time.time()
            self.pending[fid] = {"filename": name, "replication": REPLICATION,
                                 "created": now, "touched": now}
        return {"status": "ok", "file_id": fid, "chunk_size": CHUNK_SIZE,
                "replication": REPLICATION}

    def rpc_allocate_block(self, h, addr):
        fid, idx = h["file_id"], int(h["idx"])
        with self.lock:
            p = self.pending.get(fid)
            if p is None:
                raise NNError("unknown or expired upload")
            p["touched"] = time.time()
            targets = self._pick_targets(p["replication"], exclude=set(h.get("exclude", [])))
            if not targets:
                raise NNError("no datanode available for this block")
            bid = f"{fid}_{idx}"
            self.blocks[bid] = {"file_id": fid, "idx": idx, "size": None, "sha256": None,
                                "replication": p["replication"], "state": "pending",
                                "replicas": {}}
            return {"status": "ok", "block_id": bid,
                    "targets": [self._node_info(t) for t in targets]}

    def rpc_commit_block(self, h, addr):
        fid, bid = h["file_id"], h["block_id"]
        with self.lock:
            p = self.pending.get(fid)
            b = self.blocks.get(bid)
            if p is None or b is None or b["file_id"] != fid:
                raise NNError("unknown or expired upload")
            stored = [n for n in h.get("stored_on", []) if n in self.datanodes]
            if not stored:
                raise NNError("block was not stored on any known datanode")
            p["touched"] = time.time()
            b["size"], b["sha256"] = int(h["size"]), h["sha256"]
            for nid in list(b["replicas"]):
                self._remove_replica(bid, nid)
            for nid in stored:
                self._add_replica(bid, nid)
        return {"status": "ok"}

    def rpc_complete_file(self, h, addr):
        fid, nblocks, size = h["file_id"], int(h["num_blocks"]), int(h["size"])
        with self.lock:
            p = self.pending.get(fid)
            if p is None:
                raise NNError("unknown or expired upload")
            bids = [f"{fid}_{i}" for i in range(nblocks)]
            total = 0
            for bid in bids:
                b = self.blocks.get(bid)
                if b is None or b["sha256"] is None or not b["replicas"]:
                    raise NNError(f"block {bid} is missing or was never committed")
                total += b["size"]
            if total != size:
                raise NNError(f"size mismatch: blocks hold {total} bytes, client says {size}")
            old = self.files.get(p["filename"])
            if old:                                  # re-upload replaces the old version
                for obid in old["blocks"]:
                    self._drop_block(obid)
            for bid in bids:
                self.blocks[bid]["state"] = "complete"
            self.files[p["filename"]] = {"file_id": fid, "size": size, "blocks": bids,
                                         "replication": p["replication"],
                                         "created": time.time()}
            del self.pending[fid]
            name = p["filename"]
        self.save()                                  # durable before we acknowledge
        log.info("file complete: %s (%d bytes, %d blocks)", name, size, nblocks)
        return {"status": "ok"}

    def rpc_abort_file(self, h, addr):
        with self.lock:
            self._abort_pending(h["file_id"])
        return {"status": "ok"}

    def _abort_pending(self, fid):
        self.pending.pop(fid, None)
        for bid in [b for b, v in self.blocks.items()
                    if v["file_id"] == fid and v["state"] == "pending"]:
            self._drop_block(bid)

    def rpc_delete_file(self, h, addr):
        with self.lock:
            f = self.files.pop(h["filename"], None)
            if f is None:
                raise NNError("file not found")
            for bid in f["blocks"]:
                self._drop_block(bid)
        self.save()
        log.info("file deleted: %s", h["filename"])
        return {"status": "ok"}

    def rpc_get_locations(self, h, addr):
        with self.lock:
            f = self.files.get(h["filename"])
            if f is None:
                raise NNError("file not found")
            out = []
            for bid in f["blocks"]:
                b = self.blocks[bid]
                locs = [self._node_info(n) for n in b["replicas"]
                        if n in self.datanodes and self.datanodes[n]["alive"]]
                random.shuffle(locs)               # spread read load across replicas
                out.append({"block_id": bid, "size": b["size"], "sha256": b["sha256"],
                            "locations": locs})
        return {"status": "ok", "size": f["size"], "blocks": out}

    def rpc_status(self, h, addr):
        now = time.time()
        with self.lock:
            nodes, under, missing = [], 0, 0
            files = []
            for fname, f in sorted(self.files.items()):
                u = m = 0
                for bid in f["blocks"]:
                    b = self.blocks[bid]
                    live = sum(1 for n in b["replicas"] if self.datanodes.get(n, {}).get("alive"))
                    if live == 0:
                        m += 1
                    elif live < b["replication"]:
                        u += 1
                under += u
                missing += m
                files.append({"name": fname, "size": f["size"], "blocks": len(f["blocks"]),
                              "replication": f["replication"], "created": f["created"],
                              "under_replicated": u, "missing": m})
            file_index = {fname: i for i, fname in enumerate(sorted(self.files))}
            by_fid = {f["file_id"]: file_index[name] for name, f in self.files.items()}
            for nid, n in sorted(self.datanodes.items()):
                cells = []
                for bid in sorted(self.node_blocks[nid])[:600]:
                    b = self.blocks.get(bid)
                    if b and b["state"] == "complete" and b["file_id"] in by_fid:
                        cells.append([by_fid[b["file_id"]], b["idx"]])
                nodes.append({"id": nid, "host": n["host"], "port": n["port"], "alive": n["alive"],
                              "age": round(now - n["last_hb"], 1), "stats": n["stats"],
                              "num_blocks": len(self.node_blocks[nid]), "cells": cells})
            return {"status": "ok", "cluster_id": self.cluster_id,
                    "uptime": round(now - self.started),
                    "safe_mode": self.safe_mode, "replication": REPLICATION,
                    "chunk_size": CHUNK_SIZE,
                    "summary": {"live_nodes": sum(1 for n in nodes if n["alive"]),
                                "dead_nodes": sum(1 for n in nodes if not n["alive"]),
                                "files": len(files), "blocks": len(self.blocks),
                                "under_replicated": under, "missing": missing},
                    "nodes": nodes, "files": files}

    def rpc_block_map(self, h, addr):
        with self.lock:
            f = self.files.get(h["filename"])
            if f is None:
                raise NNError("file not found")
            return {"status": "ok", "blocks": [
                {"block_id": bid, "size": self.blocks[bid]["size"],
                 "replicas": [{"node": n, "alive": bool(self.datanodes.get(n, {}).get("alive"))}
                              for n in self.blocks[bid]["replicas"]]}
                for bid in f["blocks"]]}

    # ----------------------------------------------------------- dispatch ---
    def dispatch(self, header, payload, addr):
        action = header.get("action", "")
        fn = getattr(self, f"rpc_{action}", None)
        if fn is None:
            return {"status": "error", "error": f"unknown action '{action}'"}
        try:
            return fn(header, addr)
        except NNError as e:
            return {"status": "error", "error": str(e)}
        except KeyError as e:
            return {"status": "error", "error": f"missing field {e}"}

    # ----------------------------------------------------- background -------
    def monitor_loop(self):
        while not self.stop.wait(MONITOR_INTERVAL):
            try:
                with self.lock:
                    self._detect_dead_nodes()
                    self._expire_pending_files()
                    if not self.safe_mode:
                        self._schedule_replication()
            except Exception:
                log.exception("monitor loop error")

    def _detect_dead_nodes(self):
        now = time.time()
        for nid, n in self.datanodes.items():
            if n["alive"] and now - n["last_hb"] > HEARTBEAT_TIMEOUT:
                n["alive"] = False
                lost = list(self.node_blocks[nid])
                for bid in lost:
                    self._remove_replica(bid, nid)
                self.commands.pop(nid, None)
                self.pending_delete.pop(nid, None)
                for tg in self.pending_repl.values():
                    tg.pop(nid, None)
                log.warning("datanode %s declared DEAD (no heartbeat for %.0fs); %d replicas lost",
                            nid, now - n["last_hb"], len(lost))

    def _expire_pending_files(self):
        now = time.time()
        for fid in [f for f, p in self.pending.items() if now - p["touched"] > PENDING_FILE_TTL]:
            log.warning("abandoned upload %s expired", fid)
            self._abort_pending(fid)

    def _schedule_replication(self):
        now = time.time()
        for bid in list(self.pending_repl):
            tg = self.pending_repl[bid]
            for t in [t for t, ts in tg.items() if now - ts > REPLICATION_TIMEOUT]:
                tg.pop(t)
            if not tg:
                del self.pending_repl[bid]
        live = self._live()
        budget = MAX_REPL_COMMANDS_PER_CYCLE
        for bid, b in self.blocks.items():
            if b["state"] != "complete":
                continue
            want = b["replication"]
            have = [n for n in b["replicas"] if n in live]
            inflight = self.pending_repl.get(bid, {})
            if not have:
                continue                                    # nothing to copy from: "missing"
            if len(have) < want and budget > 0:
                need = want - len(have) - len(inflight)
                if need <= 0:
                    continue
                doomed_on = {n for n, d in self.pending_delete.items() if bid in d}
                targets = self._pick_targets(need, exclude=set(b["replicas"]) | set(inflight) | doomed_on)
                if not targets:
                    continue
                source = min(have, key=lambda n: len(self.commands[n]))
                self.commands[source].append({
                    "type": "replicate", "block_id": bid, "size": b["size"],
                    "sha256": b["sha256"], "targets": [self._node_info(t) for t in targets]})
                for t in targets:
                    self.pending_repl.setdefault(bid, {})[t] = now
                budget -= 1
                log.info("re-replicating %s: %s -> %s (%d/%d live replicas)", bid, source,
                         targets, len(have), want)
            elif len(have) > want and not inflight:
                extras = sorted(have, key=lambda n: -len(self.node_blocks[n]))[: len(have) - want]
                for n in extras:
                    self._remove_replica(bid, n)
                    self._queue_delete(n, bid)
                log.info("over-replicated %s: removing replicas on %s", bid, extras)

    # ------------------------------------------------------------ run -------
    def run(self):
        threading.Thread(target=self.monitor_loop, daemon=True).start()
        server = Server((HOST, PORT), self.dispatch)

        def shutdown(*_):
            log.info("shutting down, flushing metadata")
            self.stop.set()
            try:
                self.save()
            finally:
                threading.Thread(target=server.shutdown, daemon=True).start()

        signal.signal(signal.SIGTERM, shutdown)
        signal.signal(signal.SIGINT, shutdown)
        log.info("listening on %s:%d  (replication=%d, chunk=%d bytes, safe mode %s)",
                 HOST, PORT, REPLICATION, CHUNK_SIZE,
                 f"{SAFE_MODE_SECONDS:.0f}s" if self.blocks else "off")
        server.serve_forever()


if __name__ == "__main__":
    NameNode().run()
