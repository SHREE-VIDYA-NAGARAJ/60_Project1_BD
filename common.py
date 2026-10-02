"""
Shared wire protocol and helpers used by the NameNode, DataNodes and client.

Wire format (one request and one response per TCP connection):

    +--------------+-----------------------+---------------------------+
    | 4 bytes      | header (JSON, UTF-8)  | payload (raw bytes)       |
    | header length| includes payload_len  | only for block transfers  |
    +--------------+-----------------------+---------------------------+

Control messages (register, heartbeat, allocate, ...) have no payload.
Block data is sent as raw bytes (no base64, no JSON wrapping), so memory use
and network overhead per block stay constant regardless of file size.
"""
import hashlib
import hmac
import json
import logging
import os
import socket
import socketserver
import struct
import sys


# --------------------------------------------------------------------------
# configuration helpers
# --------------------------------------------------------------------------
def env_str(name, default):
    return os.environ.get(name, default)


def env_int(name, default):
    return int(os.environ.get(name, default))


def env_float(name, default):
    return float(os.environ.get(name, default))


CLUSTER_TOKEN = env_str("CLUSTER_TOKEN", "")          # optional shared secret
MAX_HEADER_BYTES = env_int("MAX_HEADER_BYTES", 64 * 1024 * 1024)
MAX_PAYLOAD_BYTES = env_int("MAX_PAYLOAD_BYTES", 128 * 1024 * 1024)
IO_TIMEOUT = env_float("IO_TIMEOUT", 60.0)


class ProtocolError(Exception):
    pass


def setup_logging(name):
    if not logging.getLogger().handlers:
        logging.basicConfig(
            stream=sys.stdout,
            level=env_str("LOG_LEVEL", "INFO").upper(),
            format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        )
    return logging.getLogger(name)


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


# --------------------------------------------------------------------------
# framing
# --------------------------------------------------------------------------
def recv_exact(sock, n):
    """Read exactly n bytes or raise ConnectionError."""
    buf = bytearray(n)
    view = memoryview(buf)
    got = 0
    while got < n:
        r = sock.recv_into(view[got:], min(n - got, 1 << 20))
        if r == 0:
            raise ConnectionError(f"connection closed after {got}/{n} bytes")
        got += r
    return bytes(buf)


def send_msg(sock, header, payload=b""):
    header = dict(header)
    header["payload_len"] = len(payload)
    if CLUSTER_TOKEN:
        header["token"] = CLUSTER_TOKEN
    raw = json.dumps(header, separators=(",", ":")).encode("utf-8")
    sock.sendall(struct.pack("!I", len(raw)) + raw)
    if payload:
        sock.sendall(payload)


def recv_msg(sock):
    (hlen,) = struct.unpack("!I", recv_exact(sock, 4))
    if hlen > MAX_HEADER_BYTES:
        raise ProtocolError(f"header too large: {hlen}")
    header = json.loads(recv_exact(sock, hlen).decode("utf-8"))
    plen = int(header.pop("payload_len", 0))
    if plen < 0 or plen > MAX_PAYLOAD_BYTES:
        raise ProtocolError(f"payload too large: {plen}")
    payload = recv_exact(sock, plen) if plen else b""
    return header, payload


def check_token(header):
    token = header.pop("token", "")
    if CLUSTER_TOKEN and not hmac.compare_digest(str(token), CLUSTER_TOKEN):
        raise ProtocolError("unauthorized")


def rpc(host, port, header, payload=b"", timeout=10.0):
    """Open a connection, send one message, return (header, payload)."""
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.settimeout(timeout)
        send_msg(s, header, payload)
        return recv_msg(s)


# --------------------------------------------------------------------------
# generic threaded server
# --------------------------------------------------------------------------
class _Handler(socketserver.BaseRequestHandler):
    def handle(self):
        log = logging.getLogger("server")
        try:
            self.request.settimeout(IO_TIMEOUT)
            header, payload = recv_msg(self.request)
            check_token(header)
            result = self.server.dispatch(header, payload, self.client_address)
            if isinstance(result, tuple):
                resp, resp_payload = result
            else:
                resp, resp_payload = result, b""
            send_msg(self.request, resp, resp_payload)
        except ProtocolError as e:
            try:
                send_msg(self.request, {"status": "error", "error": str(e)})
            except OSError:
                pass
        except (ConnectionError, socket.timeout, OSError) as e:
            log.debug("connection problem from %s: %s", self.client_address, e)
        except Exception:
            log.exception("unhandled error serving %s", self.client_address)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 128

    def __init__(self, addr, dispatch):
        self.dispatch = dispatch
        super().__init__(addr, _Handler)
