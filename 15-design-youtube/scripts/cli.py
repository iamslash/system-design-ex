#!/usr/bin/env python3
"""CLI client for the video streaming system API."""

from __future__ import annotations

import argparse
import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_URL = "http://localhost:8015"


def _request(
    method: str,
    path: str,
    data: dict | None = None,
    headers: dict | None = None,
) -> tuple[int, bytes]:
    """Send an HTTP request and return (status_code, body_bytes)."""
    url = f"{BASE_URL}{path}"
    body_bytes = json.dumps(data).encode() if data else None
    req = Request(url, data=body_bytes, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urlopen(req, timeout=30) as resp:
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
    """Send an HTTP request and return (status_code, body_json)."""
    status, body = _request(method, path, data)
    return status, json.loads(body.decode())


def _multipart_upload(path: str, file_path: str) -> tuple[int, dict]:
    """Upload a file using multipart/form-data."""
    import mimetypes
    from io import BytesIO

    boundary = "----PythonBoundary"
    filename = os.path.basename(file_path)
    content_type = mimetypes.guess_type(file_path)[0] or "application/octet-stream"

    with open(file_path, "rb") as f:
        file_data = f.read()

    body = BytesIO()
    body.write(f"--{boundary}\r\n".encode())
    body.write(
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode()
    )
    body.write(f"Content-Type: {content_type}\r\n\r\n".encode())
    body.write(file_data)
    body.write(f"\r\n--{boundary}--\r\n".encode())

    url = f"{BASE_URL}{path}"
    req = Request(url, data=body.getvalue(), method="PUT")
    req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
    try:
        with urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode())
    except HTTPError as e:
        return e.code, json.loads(e.read().decode())


def cmd_health() -> None:
    """Run health check."""
    status, body = _json_request("GET", "/health")
    print(f"Health: {status}")
    print(f"  Status: {body.get('status')}")
    print(f"  Redis: {body.get('redis_version', 'unknown')}")


def cmd_upload(args: argparse.Namespace) -> None:
    """Upload a video file."""
    file_path = args.file
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}", file=sys.stderr)
        sys.exit(1)

    title = args.title or os.path.basename(file_path)
    description = args.description or ""

    # Determine number of chunks based on file size
    file_size = os.path.getsize(file_path)
    chunk_size = 1024 * 1024  # 1MB chunks
    total_chunks = max(1, (file_size + chunk_size - 1) // chunk_size)

    print(f"Uploading: {file_path}")
    print(f"  Title: {title}")
    print(f"  Size: {file_size} bytes ({total_chunks} chunks)")

    # 1. Initiate upload
    status, body = _json_request("POST", "/api/v1/videos/upload", {
        "title": title,
        "description": description,
        "total_chunks": total_chunks,
    })

    if status != 200:
        print(f"Upload init failed [{status}]: {body.get('detail', body)}")
        sys.exit(1)

    upload_id = body["upload_id"]
    video_id = body["video_id"]
    print(f"  Upload ID: {upload_id}")
    print(f"  Video ID: {video_id}")

    # 2. Upload chunks
    with open(file_path, "rb") as f:
        for i in range(total_chunks):
            chunk_data = f.read(chunk_size)
            status, result = _multipart_upload(
                f"/api/v1/videos/upload/{upload_id}/chunk/{i}",
                file_path,
            )
            if status != 200:
                print(f"  Chunk {i} failed [{status}]: {result}")
                sys.exit(1)
            print(f"  Chunk {i+1}/{total_chunks} uploaded")

    # 3. Complete upload + transcode
    status, body = _json_request("POST", f"/api/v1/videos/upload/{upload_id}/complete")
    if status != 200:
        print(f"Complete failed [{status}]: {body.get('detail', body)}")
        sys.exit(1)

    print(f"Upload complete!")
    print(f"  Video ID: {body.get('video_id')}")
    print(f"  Status: {body.get('status')}")
    print(f"  Resolutions: {', '.join(body.get('resolutions', []))}")


def cmd_status(args: argparse.Namespace) -> None:
    """Get video status."""
    status, body = _json_request("GET", f"/api/v1/videos/{args.video_id}")
    if status == 404:
        print(f"Video not found: {args.video_id}")
        return

    print(f"Video: {body.get('video_id')}")
    print(f"  Title: {body.get('title')}")
    print(f"  Description: {body.get('description', '')}")
    print(f"  Status: {body.get('status')}")
    print(f"  Views: {body.get('views', 0)}")
    resolutions = body.get("resolutions", [])
    if resolutions:
        print(f"  Resolutions: {', '.join(resolutions)}")


def cmd_list(args: argparse.Namespace) -> None:
    """List videos."""
    status, body = _json_request("GET", "/api/v1/videos")
    videos = body.get("videos", [])
    print(f"Videos ({body.get('count', 0)}):")
    for v in videos:
        res = ", ".join(v.get("resolutions", []))
        print(f"  [{v.get('video_id', '?')[:8]}] {v.get('title', '?')} "
              f"({v.get('status', '?')}) views={v.get('views', 0)} [{res}]")


def cmd_stream(args: argparse.Namespace) -> None:
    """Stream a video (download a range)."""
    resolution = args.resolution or "720p"
    path = f"/api/v1/videos/{args.video_id}/stream?resolution={resolution}"

    # Request first 1KB using Range header (simulation)
    headers = {"Range": "bytes=0-1023"}
    status, body = _request("GET", path, headers=headers)

    if status == 404:
        print(f"Video not found: {args.video_id}")
        return

    print(f"Stream: {args.video_id} ({resolution})")
    print(f"  HTTP Status: {status}")
    print(f"  Received: {len(body)} bytes")
    print(f"  Preview: {body[:200].decode(errors='replace')}")


def main() -> None:
    global BASE_URL
    parser = argparse.ArgumentParser(description="Video Streaming System CLI Client")
    parser.add_argument("--health", action="store_true", help="Run health check")
    parser.add_argument("--base-url", type=str, default=BASE_URL, help="API base URL")

    subparsers = parser.add_subparsers(dest="command")

    # upload
    upload_parser = subparsers.add_parser("upload", help="Upload a video")
    upload_parser.add_argument("file", help="Video file path")
    upload_parser.add_argument("--title", default=None, help="Video title")
    upload_parser.add_argument("--description", default="", help="Video description")

    # status
    status_parser = subparsers.add_parser("status", help="Get video status")
    status_parser.add_argument("video_id", help="Video ID")

    # list
    subparsers.add_parser("list", help="List all videos")

    # stream
    stream_parser = subparsers.add_parser("stream", help="Stream a video")
    stream_parser.add_argument("video_id", help="Video ID")
    stream_parser.add_argument("--resolution", default="720p", help="Resolution (360p, 720p, 1080p)")

    args = parser.parse_args()

    BASE_URL = args.base_url

    if args.health:
        cmd_health()
    elif args.command == "upload":
        cmd_upload(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "list":
        cmd_list(args)
    elif args.command == "stream":
        cmd_stream(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
