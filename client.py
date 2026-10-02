#!/usr/bin/env python3
"""Web front-end: dashboard, upload, download, delete. Talks to the cluster via DFSClient."""
import logging
import os
import re
from urllib.parse import quote

from flask import Flask, Response, jsonify, render_template, request

from common import env_int, setup_logging
from dfs_client import DFSClient, DFSError

log = setup_logging("webclient")
MAX_UPLOAD_MB = env_int("MAX_UPLOAD_MB", 1024)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_MB * 1024 * 1024
dfs = DFSClient()


def clean_filename(raw):
    """Keep only the base name; reject empty / dot names. Names are only metadata keys
    (blocks are stored under random ids), so nothing here ever touches a filesystem path."""
    name = os.path.basename((raw or "").replace("\\", "/")).strip()
    name = re.sub(r"[\x00-\x1f\x7f]", "", name)[:255]
    if name in ("", ".", ".."):
        return None
    return name


def err(message, code):
    return jsonify({"error": message}), code


@app.route("/")
def index():
    return render_template("dashboard.html", max_upload_mb=MAX_UPLOAD_MB)


@app.route("/api/status")
def api_status():
    try:
        return jsonify(dfs.status())
    except DFSError as e:
        return err(str(e), 503)


@app.route("/api/blocks/<path:filename>")
def api_blocks(filename):
    try:
        return jsonify(dfs.block_map(filename))
    except DFSError as e:
        return err(str(e), 404)


@app.route("/upload", methods=["POST"])
def upload():
    f = request.files.get("file")
    if f is None:
        return err("no file part in request", 400)
    name = clean_filename(f.filename)
    if name is None:
        return err("invalid file name", 400)
    try:
        return jsonify(dfs.upload(name, f.stream))
    except DFSError as e:
        log.warning("upload of %s failed: %s", name, e)
        return err(str(e), 503)


@app.route("/download/<path:filename>")
def download(filename):
    try:
        size, chunks = dfs.open_download(filename)
    except DFSError as e:
        return err(str(e), 404 if "not found" in str(e) else 503)
    ascii_name = filename.encode("ascii", "replace").decode().replace('"', "")
    headers = {
        "Content-Length": str(size),
        "Content-Disposition": f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(filename)}",
    }
    return Response(chunks, mimetype="application/octet-stream", headers=headers)


@app.route("/delete/<path:filename>", methods=["POST"])
def delete(filename):
    try:
        dfs.delete(filename)
        return jsonify({"status": "deleted"})
    except DFSError as e:
        return err(str(e), 404)


@app.errorhandler(413)
def too_large(_):
    return err(f"file exceeds the {MAX_UPLOAD_MB} MB upload limit", 413)


if __name__ == "__main__":
    from waitress import serve
    port = env_int("CLIENT_PORT", 8000)
    log.info("web client on :%d -> namenode %s:%d", port, dfs.nn_host, dfs.nn_port)
    serve(app, host="0.0.0.0", port=port, threads=16,
          max_request_body_size=MAX_UPLOAD_MB * 1024 * 1024)
