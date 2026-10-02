"""
End-to-end tests. Each test boots a REAL cluster (NameNode + N DataNodes as
separate OS processes talking over TCP), then injects faults: kills nodes,
corrupts blocks on disk, restarts the NameNode, adds nodes at runtime.

Run:  python -m unittest discover -s tests -v
"""
import hashlib
import io
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common import rpc                      # noqa: E402
from dfs_client import DFSClient, DFSError  # noqa: E402

CHUNK = 64 * 1024


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(cond, timeout=25.0, step=0.2, what="condition"):
    end = time.time() + timeout
    last = None
    while time.time() < end:
        try:
            last = cond()
            if last:
                return last
        except Exception as e:      # noqa: BLE001 - keep polling while cluster settles
            last = e
        time.sleep(step)
    raise AssertionError(f"timed out waiting for {what} (last={last!r})")


class Cluster:
    def __init__(self, tmp, replication=2, safe_mode=0, extra_env=None):
        self.tmp = tmp
        self.nn_port = free_port()
        self.procs = {}
        self.dn_ports = {}
        self.env = dict(os.environ, PYTHONPATH=ROOT, PYTHONUNBUFFERED="1",
                        NAMENODE_HOST="127.0.0.1", NAMENODE_PORT=str(self.nn_port),
                        NAMENODE_DATA_DIR=os.path.join(tmp, "nn"),
                        CHUNK_SIZE=str(CHUNK), REPLICATION_FACTOR=str(replication),
                        HEARTBEAT_INTERVAL="0.5", HEARTBEAT_TIMEOUT="3",
                        MONITOR_INTERVAL="0.5", SAFE_MODE_SECONDS=str(safe_mode),
                        BLOCK_REPORT_INTERVAL="3", REPORT_GRACE="1",
                        MIN_FREE_BYTES="1000000", RESERVED_FREE_BYTES="1000000",
                        SCRUB_INTERVAL="3600", LOG_LEVEL="INFO")
        self.env.update(extra_env or {})
        self.client = DFSClient("127.0.0.1", self.nn_port, timeout=5)

    def _spawn(self, name, script, env):
        with open(os.path.join(self.tmp, f"{name}.log"), "ab") as log:
            self.procs[name] = subprocess.Popen([sys.executable, os.path.join(ROOT, script)],
                                                env=env, stdout=log, stderr=subprocess.STDOUT)

    def start_namenode(self):
        self._spawn("namenode", "namenode.py", self.env)
        wait_for(lambda: socket.create_connection(("127.0.0.1", self.nn_port), 1).close() or True,
                 what="namenode port")

    def start_datanode(self, name):
        port = self.dn_ports.setdefault(name, free_port())
        env = dict(self.env, DATANODE_PORT=str(port), DATANODE_ID=name,
                   DATA_DIR=os.path.join(self.tmp, name), ADVERTISE_HOST="127.0.0.1")
        self._spawn(name, "datanode.py", env)

    def start(self, n):
        self.start_namenode()
        for i in range(n):
            self.start_datanode(f"dn{i}")
        wait_for(lambda: self.client.status()["summary"]["live_nodes"] == n, what=f"{n} nodes live")

    def crash(self, name):
        p = self.procs.pop(name)
        p.send_signal(signal.SIGKILL)
        p.wait()

    def stop_all(self):
        for p in self.procs.values():
            p.terminate()
        for p in self.procs.values():
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

    # ---- introspection helpers
    def live_replicas(self, filename):
        bm = self.client.block_map(filename)["blocks"]
        return {b["block_id"]: sorted(r["node"] for r in b["replicas"] if r["alive"]) for b in bm}

    def on_disk(self, name):
        d = os.path.join(self.tmp, name, "blocks")
        return sorted(f for f in os.listdir(d) if not f.endswith(".meta")) if os.path.isdir(d) else []

    def wait_dead(self, name):
        wait_for(lambda: not [n for n in self.client.status()["nodes"] if n["id"] == name][0]["alive"],
                 what=f"{name} declared dead")

    def healthy(self, filename, want):
        reps = self.live_replicas(filename)
        return bool(reps) and all(len(v) == want for v in reps.values())


def blob(n, seed=0):
    out, i = bytearray(), 0
    while len(out) < n:
        out += hashlib.sha256(f"{seed}-{i}".encode()).digest()
        i += 1
    return bytes(out[:n])


def read_all(client, name):
    size, gen = client.open_download(name)
    data = b"".join(gen)
    assert len(data) == size
    return data


class ClusterCase(unittest.TestCase):
    NODES = 4
    REPLICATION = 2
    SAFE_MODE = 0
    EXTRA_ENV = None

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="minihdfs-test-")
        self.cluster = Cluster(self.tmp, self.REPLICATION, self.SAFE_MODE, self.EXTRA_ENV)
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.addCleanup(self.cluster.stop_all)
        if self.NODES:
            self.cluster.start(self.NODES)
        self.c = self.cluster.client


class TestCore(ClusterCase):
    def test_roundtrip_all_boundary_sizes(self):
        for size in (0, 1, CHUNK - 1, CHUNK, CHUNK + 1, 5 * CHUNK + 7):
            data = blob(size, size)
            self.c.upload(f"f{size}.bin", io.BytesIO(data))
            self.assertEqual(read_all(self.c, f"f{size}.bin"), data, f"size {size}")

    def test_every_block_has_replicas_on_distinct_nodes(self):
        self.c.upload("big.bin", io.BytesIO(blob(12 * CHUNK)))
        reps = self.cluster.live_replicas("big.bin")
        self.assertEqual(len(reps), 12)
        for bid, nodes in reps.items():
            self.assertEqual(len(nodes), 2, bid)
            self.assertEqual(len(set(nodes)), 2, bid)
        used = {n for v in reps.values() for n in v}
        self.assertGreaterEqual(len(used), 3, "blocks should spread over the nodes")

    def test_reupload_replaces_and_old_blocks_are_removed(self):
        self.c.upload("doc.bin", io.BytesIO(blob(3 * CHUNK, 1)))
        old_ids = set(self.cluster.live_replicas("doc.bin"))
        self.c.upload("doc.bin", io.BytesIO(blob(2 * CHUNK, 2)))
        self.assertEqual(read_all(self.c, "doc.bin"), blob(2 * CHUNK, 2))
        wait_for(lambda: not (old_ids & {b for n in range(4) for b in self.cluster.on_disk(f"dn{n}")}),
                 what="old blocks deleted from disks")

    def test_delete_removes_blocks_from_every_datanode(self):
        self.c.upload("gone.bin", io.BytesIO(blob(6 * CHUNK)))
        self.assertTrue(any(self.cluster.on_disk(f"dn{i}") for i in range(4)))
        self.c.delete("gone.bin")
        wait_for(lambda: not any(self.cluster.on_disk(f"dn{i}") for i in range(4)),
                 what="blocks removed from disks")
        with self.assertRaises(DFSError):
            self.c.open_download("gone.bin")

    def test_unknown_file_and_bad_requests(self):
        with self.assertRaises(DFSError):
            self.c.open_download("nope")
        resp, _ = rpc("127.0.0.1", self.cluster.nn_port, {"action": "does_not_exist"})
        self.assertEqual(resp["status"], "error")
        dn_port = self.cluster.dn_ports["dn0"]
        resp, _ = rpc("127.0.0.1", dn_port, {"action": "get_block", "block_id": "../../etc/passwd"})
        self.assertEqual(resp["status"], "error")                # path traversal is rejected
        data = b"x" * 100
        resp, _ = rpc("127.0.0.1", dn_port, {"action": "store_block", "block_id": "b1", "size": 100,
                                             "sha256": "0" * 64, "pipeline": []}, data)
        self.assertEqual(resp["error"], "checksum_mismatch")     # bad data is never stored

    def test_failed_upload_leaves_nothing_behind(self):
        class Boom(io.BytesIO):
            calls = 0

            def read(self, n=-1):
                Boom.calls += 1
                if Boom.calls > 3:
                    raise IOError("client died mid-upload")
                return super().read(n)

        with self.assertRaises(IOError):
            self.c.upload("half.bin", Boom(blob(10 * CHUNK)))
        self.assertEqual(self.c.status()["files"], [])
        wait_for(lambda: not any(self.cluster.on_disk(f"dn{i}") for i in range(4)),
                 what="aborted upload cleaned from disks")


class TestFaultTolerance(ClusterCase):
    def test_dead_node_triggers_automatic_rereplication(self):
        data = blob(10 * CHUNK, 7)
        self.c.upload("a.bin", io.BytesIO(data))
        victim = max(("dn0", "dn1", "dn2", "dn3"), key=lambda n: len(self.cluster.on_disk(n)))
        held = len(self.cluster.on_disk(victim))
        self.assertGreater(held, 0)
        self.cluster.crash(victim)                               # SIGKILL: no goodbye
        wait_for(lambda: not any(n["alive"] for n in self.c.status()["nodes"] if n["id"] == victim),
                 what="victim declared dead")
        wait_for(lambda: self.cluster.healthy("a.bin", 2), timeout=30,
                 what="all blocks back to 2 live replicas")
        self.assertNotIn(victim, {n for v in self.cluster.live_replicas("a.bin").values() for n in v})
        self.assertEqual(read_all(self.c, "a.bin"), data)

    def test_survives_loss_of_all_but_one_node_one_by_one(self):
        data = blob(8 * CHUNK, 9)
        self.c.upload("a.bin", io.BytesIO(data))
        # kill nodes one at a time, letting the cluster heal between failures
        for victim, expect_replicas in (("dn0", 2), ("dn1", 2), ("dn2", 1)):
            self.cluster.crash(victim)
            self.cluster.wait_dead(victim)           # failure must be detected before we judge healing
            wait_for(lambda: self.cluster.healthy("a.bin", expect_replicas), timeout=30,
                     what=f"healed after losing {victim}")
            self.assertEqual(read_all(self.c, "a.bin"), data)

    def test_returning_node_rejoins_and_extra_replicas_are_trimmed(self):
        data = blob(8 * CHUNK, 3)
        self.c.upload("a.bin", io.BytesIO(data))
        self.cluster.crash("dn1")
        self.cluster.wait_dead("dn1")
        wait_for(lambda: self.cluster.healthy("a.bin", 2), timeout=30, what="healed")
        self.cluster.start_datanode("dn1")                       # same id + same disk comes back
        wait_for(lambda: all(n["alive"] for n in self.c.status()["nodes"]), what="dn1 rejoined")
        wait_for(lambda: self.cluster.healthy("a.bin", 2), timeout=30,
                 what="exactly 2 replicas per block after rejoin")
        self.assertEqual(read_all(self.c, "a.bin"), data)

    def test_node_dead_longer_than_timeout_is_not_orphaned(self):
        """Regression for the original bug: a node removed after a missed heartbeat
        window could never come back. Simulate a long pause with SIGSTOP."""
        p = self.cluster.procs["dn2"]
        p.send_signal(signal.SIGSTOP)
        wait_for(lambda: not [n for n in self.c.status()["nodes"] if n["id"] == "dn2"][0]["alive"],
                 what="dn2 declared dead")
        p.send_signal(signal.SIGCONT)
        wait_for(lambda: [n for n in self.c.status()["nodes"] if n["id"] == "dn2"][0]["alive"],
                 what="dn2 alive again")

    def test_corrupt_replica_is_detected_failed_over_and_healed(self):
        data = blob(6 * CHUNK, 5)
        self.c.upload("a.bin", io.BytesIO(data))
        reps = self.cluster.live_replicas("a.bin")
        bid, nodes = next(iter(reps.items()))
        bad = nodes[0]
        path = os.path.join(self.tmp, bad, "blocks", bid)
        with open(path, "r+b") as f:
            f.seek(10)
            f.write(b"\xde\xad\xbe\xef")                         # silent bit rot
        # reads still return correct data (client verifies hash, fails over)
        for _ in range(12):
            self.assertEqual(read_all(self.c, "a.bin"), data)
        resp, _ = rpc("127.0.0.1", self.cluster.dn_ports[bad], {"action": "get_block", "block_id": bid})
        self.assertIn(resp["error"], ("block_corrupt", "block_not_found"))
        wait_for(lambda: self.cluster.healthy("a.bin", 2), timeout=30, what="corrupt replica replaced")
        for n in self.cluster.live_replicas("a.bin")[bid]:       # every remaining copy is genuinely good
            p = os.path.join(self.tmp, n, "blocks", bid)
            with open(p, "rb") as f:
                chunk = f.read()
            self.assertEqual(hashlib.sha256(chunk).hexdigest(),
                             hashlib.sha256(data[int(bid.split("_")[1]) * CHUNK:][:CHUNK]).hexdigest())

    def test_write_survives_a_dead_target_node(self):
        self.cluster.crash("dn0")                                # dead but not yet detected
        data = blob(8 * CHUNK, 11)
        self.c.upload("a.bin", io.BytesIO(data))                 # client retries other targets
        self.assertEqual(read_all(self.c, "a.bin"), data)
        wait_for(lambda: self.cluster.healthy("a.bin", 2), timeout=30, what="full replication")


class TestTrimRace(ClusterCase):
    """Regression (found while testing under Docker): trimming surplus replicas must never
    drop a block below its replication factor, even when a block report arrives while a
    delete order is still in flight (the DataNode still lists the block)."""
    NODES = 3
    EXTRA_ENV = {"BLOCK_REPORT_INTERVAL": "0.25", "HEARTBEAT_INTERVAL": "1.0"}

    def test_trimming_never_undershoots(self):
        data = blob(30 * CHUNK, 77)
        self.c.upload("a.bin", io.BytesIO(data))
        self.cluster.crash("dn1")
        self.cluster.wait_dead("dn1")
        wait_for(lambda: self.cluster.healthy("a.bin", 2), timeout=30, what="healed")
        self.cluster.start_datanode("dn1")                       # old replicas return -> 3 copies
        lowest, end = 99, time.time() + 12
        while time.time() < end:
            reps = self.cluster.live_replicas("a.bin")
            if reps:
                lowest = min(lowest, min(len(v) for v in reps.values()))
            time.sleep(0.05)
        self.assertGreaterEqual(lowest, 2, "a block briefly had fewer than 2 live replicas")
        wait_for(lambda: self.cluster.healthy("a.bin", 2), what="exactly 2 copies")
        self.assertEqual(read_all(self.c, "a.bin"), data)


class TestSmallClusterCorruption(ClusterCase):
    """With exactly R nodes, the replacement for a corrupt replica MUST be created on the same
    node that held the corrupt copy. That fresh copy must be accepted and must never be
    deleted by a stale 'pending delete' order."""
    NODES = 2
    EXTRA_ENV = {"DELETE_RETRY": "3", "BLOCK_REPORT_INTERVAL": "1"}

    def test_replacement_on_same_node_is_kept(self):
        data = blob(4 * CHUNK, 88)
        self.c.upload("a.bin", io.BytesIO(data))
        bid = sorted(self.cluster.live_replicas("a.bin"))[0]
        path = os.path.join(self.tmp, "dn0", "blocks", bid)
        with open(path, "r+b") as f:
            f.seek(5)
            f.write(b"\xff\xff\xff\xff")
        resp, _ = rpc("127.0.0.1", self.cluster.dn_ports["dn0"], {"action": "get_block", "block_id": bid})
        self.assertEqual(resp["error"], "block_corrupt")          # detected, quarantined, reported
        wait_for(lambda: self.cluster.healthy("a.bin", 2), timeout=30, what="replacement created")
        time.sleep(8)                                             # > DELETE_RETRY: a stale order must not bite
        self.assertTrue(self.cluster.healthy("a.bin", 2), "good replacement was lost")
        with open(path, "rb") as f:
            self.assertEqual(hashlib.sha256(f.read()).hexdigest(),
                             hashlib.sha256(data[int(bid.split("_")[1]) * CHUNK:][:CHUNK]).hexdigest())
        self.assertEqual(read_all(self.c, "a.bin"), data)


class TestNameNodeRestart(ClusterCase):
    SAFE_MODE = 2

    def test_metadata_persists_and_cluster_reassembles(self):
        data = blob(7 * CHUNK, 13)
        self.c.upload("keep.bin", io.BytesIO(data))
        self.cluster.crash("namenode")
        self.cluster.start_namenode()
        # datanodes get "reregister" on their next heartbeat and resend block reports
        wait_for(lambda: self.c.status()["summary"]["live_nodes"] == 4, what="nodes re-registered")
        self.assertEqual([f["name"] for f in self.c.status()["files"]], ["keep.bin"])
        wait_for(lambda: self.cluster.healthy("keep.bin", 2), what="locations rebuilt from block reports")
        self.assertEqual(read_all(self.c, "keep.bin"), data)

    def test_no_replication_storm_during_safe_mode(self):
        self.c.upload("keep.bin", io.BytesIO(blob(7 * CHUNK, 1)))
        before = sum(len(self.cluster.on_disk(f"dn{i}")) for i in range(4))
        self.cluster.crash("namenode")
        self.cluster.start_namenode()
        time.sleep(1.5)                                          # still inside safe mode
        wait_for(lambda: self.c.status()["summary"]["live_nodes"] == 4, what="re-registered")
        wait_for(lambda: self.cluster.healthy("keep.bin", 2), what="reports processed")
        after = sum(len(self.cluster.on_disk(f"dn{i}")) for i in range(4))
        self.assertEqual(before, after, "no spurious copies or deletions")


class TestScaling(ClusterCase):
    NODES = 1

    def test_single_node_then_scale_out_at_runtime(self):
        data = blob(8 * CHUNK, 21)
        self.c.upload("a.bin", io.BytesIO(data))                 # only 1 node: 1 copy, still works
        self.assertTrue(all(len(v) == 1 for v in self.cluster.live_replicas("a.bin").values()))
        self.cluster.start_datanode("dn1")                       # scale out: NameNode tops up replicas
        wait_for(lambda: self.cluster.healthy("a.bin", 2), timeout=30, what="replicated onto new node")
        for i in range(2, 8):
            self.cluster.start_datanode(f"dn{i}")
        wait_for(lambda: self.c.status()["summary"]["live_nodes"] == 8, what="8 nodes")
        big = blob(60 * CHUNK, 22)
        self.c.upload("big.bin", io.BytesIO(big))
        self.assertEqual(read_all(self.c, "big.bin"), big)
        counts = [len(self.cluster.on_disk(f"dn{i}")) for i in range(8)]
        self.assertTrue(all(c > 0 for c in counts), f"every node should get blocks: {counts}")
        self.assertLessEqual(max(counts) - min(counts), 20, f"reasonably balanced: {counts}")


class TestReplicationFactor3(ClusterCase):
    NODES = 6
    REPLICATION = 3

    def test_three_copies_and_two_failures(self):
        data = blob(9 * CHUNK, 31)
        self.c.upload("a.bin", io.BytesIO(data))
        self.assertTrue(self.cluster.healthy("a.bin", 3))
        self.cluster.crash("dn0")
        self.cluster.crash("dn1")                                # two simultaneous failures
        # R=3 means two simultaneous crashes can never lose a block, even before detection
        self.assertEqual(read_all(self.c, "a.bin"), data)        # still readable immediately
        wait_for(lambda: self.cluster.healthy("a.bin", 3), timeout=40, what="back to 3 copies")


class TestStartupOrderAndSecurity(unittest.TestCase):
    def test_datanode_started_before_namenode_keeps_retrying(self):
        tmp = tempfile.mkdtemp(prefix="minihdfs-test-")
        self.addCleanup(shutil.rmtree, tmp, True)
        cl = Cluster(tmp)
        self.addCleanup(cl.stop_all)
        cl.start_datanode("dn0")                                 # NameNode not running yet
        time.sleep(2)
        self.assertIsNone(cl.procs["dn0"].poll(), "datanode must not exit")
        cl.start_namenode()
        wait_for(lambda: cl.client.status()["summary"]["live_nodes"] == 1, timeout=30,
                 what="datanode registers once namenode is up")

    def test_cluster_token_rejects_unauthenticated_callers(self):
        tmp = tempfile.mkdtemp(prefix="minihdfs-test-")
        self.addCleanup(shutil.rmtree, tmp, True)
        cl = Cluster(tmp, extra_env={"CLUSTER_TOKEN": "s3cret"})
        self.addCleanup(cl.stop_all)
        cl.start_namenode()
        resp, _ = rpc("127.0.0.1", cl.nn_port, {"action": "status"})
        self.assertEqual(resp["status"], "error")
        self.assertIn("unauthorized", resp["error"])

    def test_datanode_with_foreign_cluster_id_is_refused(self):
        tmp = tempfile.mkdtemp(prefix="minihdfs-test-")
        self.addCleanup(shutil.rmtree, tmp, True)
        cl = Cluster(tmp)
        self.addCleanup(cl.stop_all)
        cl.start_namenode()
        resp, _ = rpc("127.0.0.1", cl.nn_port, {"action": "register", "node_id": "evil",
                                                "port": 1, "cluster_id": "some-other-cluster",
                                                "blocks": [{"id": "x", "size": 1, "sha256": "0"}]})
        self.assertEqual(resp["status"], "error")
        self.assertIn("cluster_id mismatch", resp["error"])
        self.assertEqual(cl.client.status()["nodes"], [])


class TestWebApp(ClusterCase):
    NODES = 3

    def setUp(self):
        super().setUp()
        import client as webclient
        webclient.dfs = DFSClient("127.0.0.1", self.cluster.nn_port, timeout=5)
        self.web = webclient.app.test_client()

    def test_dashboard_upload_download_delete(self):
        self.assertEqual(self.web.get("/").status_code, 200)
        data = blob(5 * CHUNK + 5, 41)
        r = self.web.post("/upload", data={"file": (io.BytesIO(data), "my report.bin")},
                          content_type="multipart/form-data")
        self.assertEqual(r.status_code, 200, r.data)
        self.assertEqual(r.get_json()["blocks"], 6)
        st = self.web.get("/api/status").get_json()
        self.assertEqual(st["files"][0]["name"], "my report.bin")
        d = self.web.get("/download/my%20report.bin")
        self.assertEqual(d.status_code, 200)
        self.assertEqual(d.data, data)
        self.assertEqual(int(d.headers["Content-Length"]), len(data))
        self.assertEqual(self.web.post("/delete/my%20report.bin").status_code, 200)
        self.assertEqual(self.web.get("/download/my%20report.bin").status_code, 404)

    def test_hostile_filename_is_reduced_to_a_base_name(self):
        r = self.web.post("/upload", data={"file": (io.BytesIO(b"hi"), "../../etc/passwd")},
                          content_type="multipart/form-data")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()["filename"], "passwd")
        r = self.web.post("/upload", data={"file": (io.BytesIO(b"hi"), "..")},
                          content_type="multipart/form-data")
        self.assertEqual(r.status_code, 400)

    def test_api_reports_node_failure_and_recovery(self):
        self.web.post("/upload", data={"file": (io.BytesIO(blob(6 * CHUNK)), "a.bin")},
                      content_type="multipart/form-data")
        self.cluster.crash("dn0")
        wait_for(lambda: self.web.get("/api/status").get_json()["summary"]["dead_nodes"] == 1,
                 what="dashboard shows dead node")
        wait_for(lambda: self.web.get("/api/status").get_json()["summary"]["under_replicated"] == 0,
                 timeout=30, what="dashboard shows healed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
