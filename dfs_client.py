"""
Client library for the mini HDFS (used by the Flask web app and the tests).

Upload  : NameNode hands out block targets; the client streams each block to the
          first DataNode of the pipeline, which forwards it to the next one.
          Memory use is bounded by one block, whatever the file size.
Download: NameNode returns block locations; the client reads each block from a
          replica, verifies its SHA-256, and silently fails over to another
          replica on error or corruption.
"""
import logging

from common import env_int, env_str, rpc, sha256_hex

log = logging.getLogger("dfs_client")
MAX_WRITE_ATTEMPTS = 3


class DFSError(Exception):
    pass


class DFSClient:
    def __init__(self, nn_host=None, nn_port=None, timeout=15.0):
        self.nn_host = nn_host or env_str("NAMENODE_HOST", "127.0.0.1")
        self.nn_port = nn_port or env_int("NAMENODE_PORT", 5000)
        self.timeout = timeout

    # ---------------------------------------------------------------- rpc ---
    def _nn(self, action, **fields):
        try:
            resp, _ = rpc(self.nn_host, self.nn_port, {"action": action, **fields},
                          timeout=self.timeout)
        except OSError as e:
            raise DFSError(f"cannot reach namenode at {self.nn_host}:{self.nn_port}: {e}")
        if resp.get("status") != "ok":
            raise DFSError(resp.get("error", "namenode error"))
        return resp

    # ------------------------------------------------------------- upload ---
    @staticmethod
    def _read_full(stream, n):
        parts, got = [], 0
        while got < n:
            piece = stream.read(n - got)
            if not piece:
                break
            parts.append(piece)
            got += len(piece)
        return b"".join(parts)

    def _write_block(self, file_id, idx, data):
        sha = sha256_hex(data)
        exclude = []
        for attempt in range(1, MAX_WRITE_ATTEMPTS + 1):
            alloc = self._nn("allocate_block", file_id=file_id, idx=idx, exclude=exclude)
            targets = alloc["targets"]
            for i, head in enumerate(targets):
                try:
                    resp, _ = rpc(head["host"], head["port"],
                                  {"action": "store_block", "block_id": alloc["block_id"],
                                   "size": len(data), "sha256": sha,
                                   "pipeline": targets[i + 1:]},
                                  data, timeout=60.0)
                    if resp.get("status") == "ok" and resp.get("stored_on"):
                        self._nn("commit_block", file_id=file_id, block_id=alloc["block_id"],
                                 size=len(data), sha256=sha, stored_on=resp["stored_on"])
                        return
                    log.warning("datanode %s refused block %d: %s", head["id"], idx, resp.get("error"))
                except OSError as e:
                    log.warning("write of block %d to %s failed: %s", idx, head["id"], e)
                exclude.append(head["id"])
            log.warning("block %d: attempt %d/%d failed, asking namenode for new targets",
                        idx, attempt, MAX_WRITE_ATTEMPTS)
        raise DFSError(f"could not store block {idx} after {MAX_WRITE_ATTEMPTS} attempts")

    def upload(self, filename, stream):
        """Stream a file-like object into the cluster. Returns a summary dict."""
        info = self._nn("create_file", filename=filename)
        fid, chunk = info["file_id"], info["chunk_size"]
        idx = size = 0
        try:
            while True:
                data = self._read_full(stream, chunk)
                if not data:
                    break
                self._write_block(fid, idx, data)
                size += len(data)
                idx += 1
            self._nn("complete_file", file_id=fid, num_blocks=idx, size=size)
        except Exception:
            try:
                self._nn("abort_file", file_id=fid)
            except DFSError:
                pass
            raise
        return {"filename": filename, "size": size, "blocks": idx,
                "replication": info["replication"]}

    # ----------------------------------------------------------- download ---
    def _read_block(self, blk):
        last = "no live replica"
        for loc in blk["locations"]:
            try:
                resp, data = rpc(loc["host"], loc["port"],
                                 {"action": "get_block", "block_id": blk["block_id"]},
                                 timeout=60.0)
            except OSError as e:
                last = f"{loc['id']}: {e}"
                continue
            if resp.get("status") != "ok":
                last = f"{loc['id']}: {resp.get('error')}"
                continue
            if sha256_hex(data) != blk["sha256"] or len(data) != blk["size"]:
                last = f"{loc['id']}: checksum mismatch"
                try:
                    self._nn("report_corrupt", node_id=loc["id"], block_id=blk["block_id"])
                except DFSError:
                    pass
                continue
            return data
        raise DFSError(f"block {blk['block_id']} unreadable ({last})")

    def open_download(self, filename):
        """Returns (size, generator of bytes). Raises DFSError *before* streaming
        starts if any block has no live replica."""
        meta = self._nn("get_locations", filename=filename)
        for blk in meta["blocks"]:
            if not blk["locations"]:
                raise DFSError(f"block {blk['block_id']} has no live replica right now")

        def gen():
            for blk in meta["blocks"]:
                yield self._read_block(blk)
        return meta["size"], gen()

    # ------------------------------------------------------------- admin ---
    def delete(self, filename):
        self._nn("delete_file", filename=filename)

    def status(self):
        return self._nn("status")

    def block_map(self, filename):
        return self._nn("block_map", filename=filename)
