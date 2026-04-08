#!/usr/bin/env python3
"""CLI client for the Google Drive file sync service API."""

from __future__ import annotations

import argparse
import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_URL = "http://localhost:8016"


def _request(
    method: str,
    path: str,
    data: bytes | None = None,
    content_type: str | None = None,
) -> tuple[int, bytes]:
    """Send an HTTP request and return (status_code, body_bytes)."""
    url = f"{BASE_URL}{path}"
    req = Request(url, data=data, method=method)
    if content_type:
        req.add_header("Content-Type", content_type)
    try:
        with urlopen(req, timeout=60) as resp:
            return resp.status, resp.read()
    except HTTPError as e:
        return e.code, e.read()
    except URLError as e:
        print(f"Connection error: {e.reason}", file=sys.stderr)
        sys.exit(1)


def _json_request(
    method: str,
    path: str,
    data: dict | None = None,
) -> tuple[int, dict]:
    """Send a JSON request and return (status_code, body_json)."""
    body = json.dumps(data).encode() if data else None
    status, raw = _request(method, path, body, "application/json")
    return status, json.loads(raw.decode())


def cmd_health() -> None:
    """Run health check."""
    status, body = _json_request("GET", "/health")
    print(f"Health: {status}")
    for k, v in body.items():
        print(f"  {k}: {v}")


def cmd_upload(args: argparse.Namespace) -> None:
    """Upload a file."""
    filepath = args.file
    if not os.path.isfile(filepath):
        print(f"File not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    filename = os.path.basename(filepath)
    with open(filepath, "rb") as f:
        file_data = f.read()

    # multipart/form-data 구성
    boundary = "----PythonBoundary7MA4YWxkTrZu0gW"
    body = b""

    # file field
    body += f"--{boundary}\r\n".encode()
    body += f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode()
    body += b"Content-Type: application/octet-stream\r\n\r\n"
    body += file_data
    body += b"\r\n"

    body += f"--{boundary}--\r\n".encode()

    params = urlencode({"user_id": args.user})
    if args.expected_version is not None:
        params += f"&expected_version={args.expected_version}"

    status, raw = _request(
        "POST",
        f"/api/v1/files/upload?{params}",
        body,
        f"multipart/form-data; boundary={boundary}",
    )
    result = json.loads(raw.decode())

    if status == 409:
        print(f"[CONFLICT] {result.get('detail', {}).get('message', 'Conflict detected')}")
        detail = result.get("detail", {})
        print(f"  Your version: {detail.get('your_version')}")
        print(f"  Server version: {detail.get('server_version')}")
    elif status == 200:
        print(f"[OK] {result.get('message')}")
        print(f"  File ID: {result.get('file_id')}")
        print(f"  Version: {result.get('version')}")
        print(f"  Size: {result.get('size')} bytes")
        print(f"  Total blocks: {result.get('total_blocks')}")
        print(f"  New blocks: {result.get('new_blocks')}")
        print(f"  Reused blocks: {result.get('reused_blocks')}")
    else:
        print(f"[{status}] {json.dumps(result, indent=2)}")


def cmd_download(args: argparse.Namespace) -> None:
    """Download a file."""
    path = f"/api/v1/files/{args.file_id}/download"
    if args.version is not None:
        path += f"?version={args.version}"

    status, raw = _request("GET", path)
    if status == 200:
        output = args.output or args.file_id
        with open(output, "wb") as f:
            f.write(raw)
        print(f"[OK] Downloaded to {output} ({len(raw)} bytes)")
    else:
        result = json.loads(raw.decode())
        print(f"[{status}] {result.get('detail', 'Error')}")


def cmd_list(args: argparse.Namespace) -> None:
    """List user's files."""
    params = urlencode({"user_id": args.user})
    status, body = _json_request("GET", f"/api/v1/files?{params}")
    print(f"User: {args.user} ({body.get('count', 0)} files)")
    for f in body.get("files", []):
        fid = f.get("file_id", "?")[:8]
        name = f.get("filename", "?")
        ver = f.get("latest_version", "?")
        size = f.get("size", "?")
        print(f"  {fid}... {name} (v{ver}, {size} bytes)")


def cmd_revisions(args: argparse.Namespace) -> None:
    """Get file revision history."""
    status, body = _json_request("GET", f"/api/v1/files/{args.file_id}/revisions")
    if status == 200:
        print(f"File: {args.file_id[:8]}... ({len(body.get('revisions', []))} versions)")
        for rev in body.get("revisions", []):
            print(
                f"  v{rev['version']}: {rev['size']} bytes, "
                f"{rev['block_count']} blocks, {rev['created_at']}"
            )
    else:
        print(f"[{status}] {body.get('detail', 'Error')}")


def cmd_restore(args: argparse.Namespace) -> None:
    """Restore a file to a specific version."""
    status, body = _json_request(
        "POST", f"/api/v1/files/{args.file_id}/restore/{args.version}",
    )
    if status == 200:
        print(f"[OK] {body.get('message')}")
    else:
        print(f"[{status}] {body.get('detail', 'Error')}")


def cmd_delete(args: argparse.Namespace) -> None:
    """Delete a file."""
    status, body = _json_request("DELETE", f"/api/v1/files/{args.file_id}")
    if status == 200:
        print(f"[OK] {body.get('message')}")
    else:
        print(f"[{status}] {body.get('detail', 'Error')}")


def cmd_poll(args: argparse.Namespace) -> None:
    """Poll for sync events."""
    params = urlencode({"user_id": args.user, "timeout": args.timeout})
    status, body = _json_request("GET", f"/api/v1/sync/poll?{params}")
    print(f"User: {args.user} ({body.get('count', 0)} events)")
    for evt in body.get("events", []):
        print(
            f"  [{evt.get('event_type', '?')}] "
            f"{evt.get('filename', '?')} v{evt.get('version', '?')} "
            f"by {evt.get('user_id', '?')} at {evt.get('timestamp', '?')}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Google Drive File Sync CLI Client")
    parser.add_argument("--health", action="store_true", help="Run health check")
    parser.add_argument("--base-url", type=str, default=BASE_URL, help="API base URL")

    subparsers = parser.add_subparsers(dest="command")

    # upload
    upload_parser = subparsers.add_parser("upload", help="Upload a file")
    upload_parser.add_argument("file", help="File path to upload")
    upload_parser.add_argument("--user", default="anonymous", help="User ID")
    upload_parser.add_argument("--expected-version", type=int, default=None,
                               help="Expected version for conflict detection")

    # download
    download_parser = subparsers.add_parser("download", help="Download a file")
    download_parser.add_argument("file_id", help="File ID")
    download_parser.add_argument("--version", type=int, default=None, help="Version to download")
    download_parser.add_argument("--output", "-o", default=None, help="Output file path")

    # list
    list_parser = subparsers.add_parser("list", help="List user's files")
    list_parser.add_argument("--user", default="anonymous", help="User ID")

    # revisions
    rev_parser = subparsers.add_parser("revisions", help="Get file revision history")
    rev_parser.add_argument("file_id", help="File ID")

    # restore
    restore_parser = subparsers.add_parser("restore", help="Restore to a specific version")
    restore_parser.add_argument("file_id", help="File ID")
    restore_parser.add_argument("--version", type=int, required=True, help="Version to restore")

    # delete
    delete_parser = subparsers.add_parser("delete", help="Delete a file")
    delete_parser.add_argument("file_id", help="File ID")

    # poll
    poll_parser = subparsers.add_parser("poll", help="Poll for sync events")
    poll_parser.add_argument("--user", default="anonymous", help="User ID")
    poll_parser.add_argument("--timeout", type=int, default=5, help="Poll timeout (seconds)")

    args = parser.parse_args()

    global BASE_URL
    BASE_URL = args.base_url

    if args.health:
        cmd_health()
    elif args.command == "upload":
        cmd_upload(args)
    elif args.command == "download":
        cmd_download(args)
    elif args.command == "list":
        cmd_list(args)
    elif args.command == "revisions":
        cmd_revisions(args)
    elif args.command == "restore":
        cmd_restore(args)
    elif args.command == "delete":
        cmd_delete(args)
    elif args.command == "poll":
        cmd_poll(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
