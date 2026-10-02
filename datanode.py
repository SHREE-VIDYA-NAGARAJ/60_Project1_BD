#!/usr/bin/env python3
"""
DataNode - stores blocks on local disk and serves them to clients and peers.

  * registers with the NameNode (retrying forever; it never exits because the
    NameNode happens to be down) and sends its full block report
  * heartbeats every few seconds with health stats; executes the commands the
    NameNode returns (replicate a block to peers, delete a block, re-register)
  * write pipeline: a block received from a client is forwarded to the next
    DataNode in the pipeline, so replicas are created node-to-node
  * integrity: every block is stored with its SHA-256; verified on write, on
    every read, and by a background scrubber. A corrupt replica is quarantined
    and reported so the NameNode re-replicates from a healthy copy.

One image / one script runs any number of DataNodes - identity and port come
from environment variables.
"""
import json
import os
import re
import signal
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import psutil

from common import (Server, env_float, env_int, env_str, rpc, setup_logging,
                    sha256_hex)

log = setup_logging("datanode")

NAMENODE_HOST = env_str("NAMENODE_HOST", "127.0.0.1")
NAMENODE_PORT = env_int("NAMENODE_PORT", 5000)
PORT = env_int("DATANODE_PORT", 6000)
BIND = env_str("DATANODE_BIND", "0.0.0.0")
ADVERTISE_HOST = env_str("ADVERTISE_HOST", "")       # empty => NameNode uses our source IP
DATA_DIR = env_str("DATA_DIR", "./dn_data")
SCRUB_INTERVAL = env_float("SCRUB_INTERVAL", 600.0)
RESERVED_FREE_BYTES = env_int("RESERVED_FREE_BYTES", 32 * 1024 * 1024)
TRANSFER_TIMEOUT = env_float("TRANSFER_TIMEOUT", 60.0)

BLOCK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,199}$")


class CorruptBlock(Exception):
    pass


class BlockStore:
    """Blocks live in DATA_DIR/blocks/<id> with a <id>.meta sidecar (size + sha256)."""

    def __init__(self, root):
        self.blocks_dir = os.path.join(root, "blocks")
        self.quarantine_dir = os.path.join(root, "quarantine")
        os.makedirs(self.blocks_dir, exist_ok=True)
        os.makedirs(self.quarantine_dir, exist_ok=True)
        self.lock = threading.Lock()
        self.total_bytes = 0
        self.count = 0
        self._recover()

    def _path(self, bid):
        if not BLOCK_ID_RE.match(bid) or bid.endswith(".meta"):
            raise ValueError(f"invalid block id: {bid!r}")      # blocks path traversal
        return os.path.join(self.blocks_dir, bid)

    def _recover(self):
        """Drop half-written files left by a crash; recompute stored bytes."""
        for name in os.listdir(self.blocks_dir):
            p = os.path.join(self.blocks_dir, name)
            if ".tmp." in name:
                os.remove(p)
            elif not name.endswith(".meta") and not os.path.exists(p + ".meta"):
                log.warning("removing block without metadata (interrupted write): %s", name)
                os.remove(p)
            elif name.endswith(".meta") and not os.path.exists(p[:-5]):
                os.remove(p)
        blocks = self.list_blocks()
        self.total_bytes = sum(b["size"] for b in blocks)
        self.count = len(blocks)

    @staticmethod
    def _atomic_write(path, data):
        tmp = f"{path}.tmp.{uuid.uuid4().hex[:8]}"
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def write(self, bid, data, sha):
        path = self._path(bid)
        with self.lock:
            existed = os.path.exists(path + ".meta")
            old = self._meta(bid)["size"] if existed else 0
        self._atomic_write(path, data)                          # data first ...
        self._atomic_write(path + ".meta", json.dumps({"size": len(data), "sha256": sha}).encode())
        with self.lock:                                         # ... meta last = commit point
            self.total_bytes += len(data) - old
            self.count += 0 if existed else 1

    def _meta(self, bid):
        with open(self._path(bid) + ".meta", "r") as f:
            return json.load(f)

    def read(self, bid):
        path = self._path(bid)
        meta = self._meta(bid)                                  # FileNotFoundError if absent
        with open(path, "rb") as f:
            data = f.read()
        if len(data) != meta["size"] or sha256_hex(data) != meta["sha256"]:
            raise CorruptBlock(bid)
        return data, meta

    def delete(self, bid):
        path = self._path(bid)
        with self.lock:
            try:
                size, had = self._meta(bid)["size"], True
            except (FileNotFoundError, ValueError):
                size, had = 0, False
            if had:
                self.count = max(0, self.count - 1)
            for p in (path, path + ".meta"):
                try:
                    os.remove(p)
                except FileNotFoundError:
                    pass
            self.total_bytes = max(0, self.total_bytes - size)

    def quarantine(self, bid):
        path = self._path(bid)
        dest = os.path.join(self.quarantine_dir, f"{bid}.{int(time.time())}.corrupt")
        try:
            os.replace(path, dest)
        except FileNotFoundError:
            pass
        self.delete(bid)

    def list_blocks(self):
        out = []
        for name in os.listdir(self.blocks_dir):
            if name.endswith(".meta"):
                try:
                    with open(os.path.join(self.blocks_dir, name), "r") as f:
                        m = json.load(f)
                    out.append({"id": name[:-5], "size": m["size"], "sha256": m["sha256"]})
                except (OSError, ValueError, KeyError):
                    continue
        return out


class DataNode:
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.store = BlockStore(DATA_DIR)
        self.node_id = self._load_or_create("node_id", env_str("DATANODE_ID", ""))
        self.cluster_id = self._load_or_create("cluster_id", "", create=False)
        self.hb_interval = 3.0
        self.report_interval = 60.0
        self.stop = threading.Event()
        self.pool = ThreadPoolExecutor(max_workers=4)
        self.counters = {"stored": 0, "served": 0, "replicated": 0, "corrupt": 0}
        psutil.cpu_percent(interval=None)                       # prime the counter

    # ------------------------------------------------------ identity --------
    def _load_or_create(self, name, preset, create=True):
        path = os.path.join(DATA_DIR, name)
        if preset:
            with open(path, "w") as f:
                f.write(preset)
            return preset
        if os.path.exists(path):
            with open(path) as f:
                return f.read().strip() or None
        if not create:
            return None
        value = f"dn-{uuid.uuid4().hex[:8]}"
        with open(path, "w") as f:
            f.write(value)
        return value

    def _save_cluster_id(self, cid):
        self.cluster_id = cid
        with open(os.path.join(DATA_DIR, "cluster_id"), "w") as f:
            f.write(cid)

    # ------------------------------------------------------ NameNode RPC ----
    def _nn(self, header, timeout=10.0):
        resp, _ = rpc(NAMENODE_HOST, NAMENODE_PORT, header, timeout=timeout)
        return resp

    def stats(self):
        try:
            disk = psutil.disk_usage(DATA_DIR)
            return {"cpu_percent": psutil.cpu_percent(interval=None),
                    "memory_percent": psutil.virtual_memory().percent,
                    "disk_total": disk.total, "disk_free": disk.free,
                    "block_bytes": self.store.total_bytes,
                    "num_blocks": self.store.count,
                    **{f"n_{k}": v for k, v in self.counters.items()}}
        except Exception as e:                                  # never let stats kill a heartbeat
            return {"error": str(e)}

    def register(self):
        msg = {"action": "register", "node_id": self.node_id, "port": PORT,
               "cluster_id": self.cluster_id, "stats": self.stats(),
               "blocks": self.store.list_blocks()}
        if ADVERTISE_HOST:
            msg["host"] = ADVERTISE_HOST
        resp = self._nn(msg, timeout=30.0)
        if resp.get("status") != "ok":
            raise RuntimeError(resp.get("error", "registration rejected"))
        if self.cluster_id != resp["cluster_id"]:
            self._save_cluster_id(resp["cluster_id"])
        self.hb_interval = resp.get("heartbeat_interval", self.hb_interval)
        self.report_interval = resp.get("block_report_interval", self.report_interval)
        log.info("registered with namenode as %s (cluster %s)", self.node_id, self.cluster_id)

    def register_forever(self):
        delay = 1.0
        while not self.stop.is_set():
            try:
                self.register()
                return
            except Exception as e:
                log.warning("registration failed (%s); retrying in %.0fs", e, delay)
                self.stop.wait(delay)
                delay = min(delay * 2, 15.0)

    def heartbeat_loop(self):
        fails = 0
        while not self.stop.wait(self.hb_interval):
            try:
                resp = self._nn({"action": "heartbeat", "node_id": self.node_id,
                                 "stats": self.stats()}, timeout=5.0)
                if resp.get("status") == "reregister":
                    log.info("namenode asked us to re-register")
                    self.register()
                else:
                    for cmd in resp.get("commands", []):
                        self.pool.submit(self._run_command, cmd)
                if fails:
                    log.info("connection to namenode restored after %d failed heartbeats", fails)
                fails = 0
            except Exception as e:
                fails += 1
                if fails == 1 or fails % 10 == 0:
                    log.warning("heartbeat failed (%d in a row): %s", fails, e)

    def block_report_loop(self):
        while not self.stop.wait(self.report_interval):
            try:
                resp = self._nn({"action": "block_report", "node_id": self.node_id,
                                 "blocks": self.store.list_blocks()}, timeout=30.0)
                if resp.get("status") == "reregister":
                    self.register()
            except Exception as e:
                log.warning("block report failed: %s", e)

    def scrub_loop(self):
        """Background integrity check of every stored block."""
        while not self.stop.wait(SCRUB_INTERVAL):
            for b in self.store.list_blocks():
                if self.stop.is_set():
                    return
                try:
                    self.store.read(b["id"])
                except CorruptBlock:
                    self._handle_corrupt(b["id"], "scrubber")
                except FileNotFoundError:
                    pass
                time.sleep(0.05)                                # keep disk I/O gentle

    def _handle_corrupt(self, bid, where):
        self.counters["corrupt"] += 1
        log.error("CORRUPT block %s detected by %s -> quarantined and reported", bid, where)
        self.store.quarantine(bid)
        try:
            self._nn({"action": "report_corrupt", "node_id": self.node_id, "block_id": bid})
        except Exception as e:
            log.warning("could not report corrupt block (block report will fix it): %s", e)

    # ------------------------------------------------ NameNode commands -----
    def _run_command(self, cmd):
        try:
            if cmd["type"] == "delete":
                self.store.delete(cmd["block_id"])
                log.info("deleted block %s (namenode order)", cmd["block_id"])
                try:
                    self._nn({"action": "block_deleted", "node_id": self.node_id,
                              "block_id": cmd["block_id"]})
                except Exception as e:      # the next block report confirms it anyway
                    log.warning("could not confirm delete of %s: %s", cmd["block_id"], e)
            elif cmd["type"] == "replicate":
                self._replicate(cmd)
        except Exception:
            log.exception("command failed: %s", cmd)

    def _replicate(self, cmd):
        bid = cmd["block_id"]
        try:
            data, meta = self.store.read(bid)
        except CorruptBlock:
            self._handle_corrupt(bid, "replication read")
            return
        except FileNotFoundError:
            log.warning("cannot replicate %s: not on this node", bid)
            return
        for t in cmd["targets"]:
            try:
                resp, _ = rpc(t["host"], t["port"],
                              {"action": "store_block", "block_id": bid, "size": meta["size"],
                               "sha256": meta["sha256"], "pipeline": [], "notify": True},
                              data, timeout=TRANSFER_TIMEOUT)
                if resp.get("status") == "ok":
                    self.counters["replicated"] += 1
                    log.info("replicated %s -> %s", bid, t["id"])
                else:
                    log.warning("replica of %s rejected by %s: %s", bid, t["id"], resp.get("error"))
            except Exception as e:
                log.warning("replicating %s -> %s failed: %s", bid, t["id"], e)

    # -------------------------------------------------- block service -------
    def dispatch(self, header, payload, addr):
        action = header.get("action")
        try:
            if action == "store_block":
                return self._store_block(header, payload)
            if action == "get_block":
                return self._get_block(header)
            if action == "ping":
                return {"status": "ok", "node_id": self.node_id}
            return {"status": "error", "error": f"unknown action '{action}'"}
        except ValueError as e:
            return {"status": "error", "error": str(e)}

    def _store_block(self, h, payload):
        bid, sha, size = h["block_id"], h["sha256"], int(h["size"])
        if len(payload) != size or sha256_hex(payload) != sha:
            return {"status": "error", "error": "checksum_mismatch"}
        if psutil.disk_usage(DATA_DIR).free < size + RESERVED_FREE_BYTES:
            return {"status": "error", "error": "no_space"}
        self.store.write(bid, payload, sha)
        self.counters["stored"] += 1
        stored_on = [self.node_id]
        pipeline = h.get("pipeline", [])
        if pipeline:                                    # forward down the replication pipeline
            nxt = pipeline[0]
            try:
                resp, _ = rpc(nxt["host"], nxt["port"],
                              {"action": "store_block", "block_id": bid, "size": size,
                               "sha256": sha, "pipeline": pipeline[1:]},
                              payload, timeout=TRANSFER_TIMEOUT)
                if resp.get("status") == "ok":
                    stored_on += resp.get("stored_on", [])
                else:
                    log.warning("downstream %s rejected %s: %s", nxt["id"], bid, resp.get("error"))
            except Exception as e:
                log.warning("pipeline forward of %s to %s failed: %s", bid, nxt["id"], e)
        if h.get("notify"):                             # replica created on NameNode's order
            try:
                self._nn({"action": "block_received", "node_id": self.node_id, "block_id": bid})
            except Exception as e:
                log.warning("block_received notification failed: %s", e)
        return {"status": "ok", "stored_on": stored_on}

    def _get_block(self, h):
        bid = h["block_id"]
        try:
            data, meta = self.store.read(bid)
        except FileNotFoundError:
            return {"status": "error", "error": "block_not_found"}
        except CorruptBlock:
            self._handle_corrupt(bid, "client read")
            return {"status": "error", "error": "block_corrupt"}
        self.counters["served"] += 1
        return {"status": "ok", "sha256": meta["sha256"], "size": meta["size"]}, data

    # ------------------------------------------------------------ run -------
    def run(self):
        server = Server((BIND, PORT), self.dispatch)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        log.info("datanode %s serving blocks on %s:%d, data dir %s",
                 self.node_id, BIND, PORT, os.path.abspath(DATA_DIR))
        self.register_forever()
        for fn in (self.heartbeat_loop, self.block_report_loop, self.scrub_loop):
            threading.Thread(target=fn, daemon=True).start()

        def shutdown(*_):
            log.info("shutting down")
            self.stop.set()

        signal.signal(signal.SIGTERM, shutdown)
        signal.signal(signal.SIGINT, shutdown)
        self.stop.wait()
        server.shutdown()


if __name__ == "__main__":
    DataNode().run()
