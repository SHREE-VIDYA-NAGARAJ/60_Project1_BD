# Mini HDFS

A small distributed file system modelled on HDFS: one **NameNode** (metadata), any number of **DataNodes**
(block storage), and a Flask web client with a live dashboard. Everything talks over raw TCP sockets with a
custom length-prefixed protocol. Written in Python, no frameworks in the cluster itself.

* Files are split into 4 MB blocks, each block is stored on `R` different DataNodes (default `R = 2`).
* DataNodes heartbeat; a silent node is declared dead and the NameNode **automatically re-replicates**
  every block that dropped below `R` copies.
* Works with **any number of DataNodes** (1, 3, 50 ...). Add nodes at runtime; they register themselves.
* Data never flows through the NameNode. Clients stream blocks directly to and from DataNodes.
* Every block carries a SHA-256, verified on write, on every read, and by a background scrubber.

```
                          metadata only                  heartbeats, block reports
              +------------------------------+     (commands come back in the response)
  browser --> |  Web client (Flask+waitress) | ------------> +-------------+ <-------------+
              |  DFSClient library           |   allocate /  |  NameNode   |               |
              +--------------+---------------+   commit /    |  namespace  |               |
                             |                   locate      |  block map  |               |
              block data     |                               |  placement  |               |
              (raw bytes)    |                               |  replication|               |
                             v                               +-------------+               |
                      +-------------+  pipeline   +-------------+  pipeline   +-------------+
                      | DataNode A  | ----------> | DataNode B  | ----------> | DataNode C  |  ... N nodes
                      | blocks +    |             |             |             |             |
                      | checksums   |             |             |             |             |
                      +-------------+             +-------------+             +-------------+
```

## Quick start

### With Docker (3 DataNodes by default)

Requires Docker with the Compose plugin (`docker compose version` should work).

```bash
docker compose up --build            # first run builds the image; web UI on http://localhost:8000
docker compose up -d --build         # same, in the background
docker compose logs -f namenode      # watch heartbeats, dead-node detection, re-replication
docker compose down                  # stop and remove containers, KEEP the stored data
docker compose down -v               # stop and wipe everything, including stored data
```

Only the web UI port (8000) is published. The NameNode (5000) and DataNodes (6000) are reachable only inside
the compose network; every DataNode listens on 6000 in its own container, so there are no port clashes.
Each node keeps its data in its own named volume, so a stopped/restarted/recreated container comes back with
its blocks, and a full `down` / `up` loses nothing. Containers run as an unprivileged user.

Want a different cluster size or replication factor? Generate the compose file:

```bash
python scripts/gen_compose.py 7 3 > docker-compose.yml     # 7 DataNodes, 3 copies of every block
docker compose up -d --build
```

Re-running that with a bigger N on a live cluster adds nodes without touching the existing ones; they register
within seconds and take their share of new blocks. (Port 8000 busy? Change `"8000:8000"` in the file to e.g.
`"8080:8000"`.)

**Verified under Docker** (Compose v2, containers on a bridge network): fresh build and start; upload/download
round trip; killing a DataNode container (blocks re-replicated, download intact); restarting it (it rejoins,
surplus copies trimmed, never below 2 copies); restarting the NameNode container; `down` then `up` with data
intact; scaling from 3 to 5 nodes on the running cluster; 6 nodes with replication 3 and two simultaneous
node failures.

### Without Docker

```bash
pip install -r requirements.txt
./scripts/run_local.sh 4             # 1 NameNode + 4 DataNodes + web UI on http://localhost:8000
```

Or run the pieces yourself (one DataNode per terminal; each needs its own port and data dir):

```bash
python namenode.py
DATANODE_ID=dn0 DATANODE_PORT=6000 DATA_DIR=./dn0 ADVERTISE_HOST=127.0.0.1 python datanode.py
DATANODE_ID=dn1 DATANODE_PORT=6001 DATA_DIR=./dn1 ADVERTISE_HOST=127.0.0.1 python datanode.py
python client.py
```

On several machines: set `NAMENODE_HOST` to the NameNode's address on every other machine. DataNodes need no
address configuration; the NameNode records the IP they connect from (override with `ADVERTISE_HOST`).

## Live demo: watch it heal

1. Upload a file in the UI. Each coloured square is a block; every block appears on two nodes.
2. Select the file to highlight its blocks.
3. Kill a node: `docker compose stop datanode1` (or `kill -9` a process). Containers take a few seconds to stop.
4. After the heartbeat timeout (15 s) the node is marked dead, the banner turns amber, and its blocks reappear
   on the surviving nodes. Download the file: it is still byte-identical.
5. `docker compose start datanode1`: the node re-registers, reports what it holds, and surplus copies are trimmed
   (the NameNode tracks replicas with a delete order in flight, so a trim can never drop a block below `R`).

## How it works

### Write path
1. Client asks the NameNode to `create_file`; gets a file id and the block size.
2. For each block the client asks `allocate_block`; the NameNode picks `R` live nodes (least-loaded, with enough
   free disk) and returns them.
3. The client sends the block (raw bytes + SHA-256) to the first node, which verifies it, writes it atomically
   (temp file, fsync, rename), and **forwards it to the next node in the pipeline**. The reply lists which nodes
   really stored it.
4. The client `commit_block`s the result. If the first target is unreachable it tries the next one; if the whole
   set fails it asks for new targets, excluding the bad nodes (3 attempts).
5. `complete_file` makes the file visible atomically, after checking every block exists and sizes add up. The
   NameNode **fsyncs its metadata before replying**, so an acknowledged upload survives a NameNode crash.
   A failed or abandoned upload is aborted and its blocks are deleted.

Memory is bounded by one block on every component, regardless of file size.

### Read path
The client asks for block locations (only live replicas, shuffled to spread load), reads each block from one
replica, checks the SHA-256 against the NameNode's record, and on any error or mismatch transparently tries the
next replica and reports the bad copy. The download is streamed to the browser.

### Failure detection and re-replication
* DataNodes heartbeat every 3 s. No heartbeat for 15 s (`HEARTBEAT_TIMEOUT`) => dead; its replicas are removed
  from the block map.
* A monitor loop compares live replicas per block with the file's replication factor. For an under-replicated
  block it orders a node that has a good copy to copy it to a chosen target. **Orders travel in the heartbeat
  response**, so the NameNode never opens connections or touches data (as in HDFS). Copies are rate-limited
  per cycle and tracked as in-flight so they are not issued twice.
* The target reports `block_received`; the NameNode adds the replica.
* If a dead node comes back, its heartbeat is answered with `reregister`; it sends a full block report and any
  now-surplus replicas are deleted. A node that was only paused is never orphaned.

### Consistency and recovery details
* **Block reports** (on registration, then every 60 s) are how the NameNode learns where replicas really are.
  Replica locations are never trusted from disk; only the namespace is persisted. After a NameNode restart,
  locations are rebuilt from reports, and a **safe mode** window suppresses re-replication and replica trimming until
  DataNodes have had time to report (no replication storm).
* Blocks that belong to no file (deleted, aborted upload) are found by block reports and deleted.
* **Cluster id**: the NameNode generates one; DataNodes remember it. A DataNode from a different (or wiped)
  namespace is refused instead of having its blocks mistaken for garbage and deleted.
* **Corruption**: a corrupt replica is quarantined (not served), reported, and replaced from a healthy copy.
  Detection happens on write, on read, and by a background scrubber (`SCRUB_INTERVAL`).
* Block ids are `<random file id>_<index>`, so re-uploading a name creates new blocks, then replaces the old
  version atomically and deletes the old blocks. Block ids are validated, so they cannot escape the storage dir.

### Wire protocol (`common.py`)
`[4-byte header length][JSON header][raw payload]`, one request/response per connection. Control messages have
no payload; block transfers carry raw bytes (no base64). Header and payload sizes are capped; sockets have
timeouts; an optional shared secret (`CLUSTER_TOKEN`) rejects unauthenticated callers.

## Configuration (environment variables)

| Variable | Default | Used by | Meaning |
|---|---|---|---|
| `NAMENODE_HOST` / `NAMENODE_PORT` | `127.0.0.1` / `5000` | all but NameNode | where the NameNode is |
| `REPLICATION_FACTOR` | `2` | NameNode | copies per block |
| `CHUNK_SIZE` | `4194304` | NameNode | block size in bytes (clients obey it) |
| `HEARTBEAT_TIMEOUT` | `15` | NameNode | seconds of silence before a node is dead |
| `HEARTBEAT_INTERVAL` / `BLOCK_REPORT_INTERVAL` | `3` / `60` | NameNode (told to nodes) | heartbeat / report period |
| `SAFE_MODE_SECONDS` | `15` | NameNode | grace period after a restart with data |
| `NAMENODE_DATA_DIR` | `./nn_data` | NameNode | metadata location |
| `DATANODE_ID` | generated, persisted | DataNode | stable node identity |
| `DATANODE_PORT` | `6000` | DataNode | block service port |
| `DATA_DIR` | `./dn_data` | DataNode | block storage location |
| `ADVERTISE_HOST` | (source IP) | DataNode | address other parties should use |
| `SCRUB_INTERVAL` | `600` | DataNode | background integrity-check period |
| `CLUSTER_TOKEN` | (none) | all | shared secret for all RPCs |
| `CLIENT_PORT`, `MAX_UPLOAD_MB` | `8000`, `1024` | web client | |

## Tests

```bash
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

The suite boots real clusters (separate OS processes, real TCP) and injects faults: boundary-size round trips;
replica placement on distinct nodes; **SIGKILL a node and verify automatic re-replication**; losing nodes one
after another; a node returning and surplus replicas being trimmed; a paused node not being orphaned; **bit rot
on disk** (reads fail over, replica is healed); writes while a target is down; **NameNode crash/restart**
(namespace persists, locations rebuilt, no storm); scale-out from 1 to 8 nodes at runtime with balanced
placement; replication factor 3 with two simultaneous failures; DataNode started before the NameNode; token
auth; foreign cluster id refused; the web app end to end.

## Design decisions and known limitations

* **Single NameNode.** It is a single point of failure for *availability* (not durability: the namespace is
  fsynced and blocks are replicated). Real HDFS solves this with an HA pair, a shared edit log and
  failover; that is out of scope here.
* **Whole namespace in memory, snapshot persisted on each commit.** Simple and durable for a project this size;
  HDFS uses an edit log plus periodic checkpoints so writes cost O(1). Rewriting the snapshot is O(files).
* **Placement balances block count and checks free disk** but is not rack-aware, and there is no background
  rebalancer: nodes added later fill up through new writes and re-replication, not by moving old blocks.
* **Block reports are sent in one message** (capped by `MAX_HEADER_BYTES`, 64 MB by default, roughly 400k
  blocks per node); HDFS splits reports per storage volume.
* **Security** is a shared secret over plain TCP: fine inside a private network or Docker network, not a
  substitute for TLS and real authentication. Only the web port is published by the compose file.
* **The web client buffers an upload on disk (via the WSGI server) before streaming it into the cluster**, and
  uploads blocks sequentially. Good enough for a demo; a production client would stream straight through and
  upload blocks in parallel.
* **No append, no directories, no permissions**: a flat namespace of immutable files.
