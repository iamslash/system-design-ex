"""CLI client for the S3-like object storage API."""

from __future__ import annotations

import argparse
import sys
import urllib.request
import urllib.error
import json

BASE_URL = "http://localhost:8025/api/v1"


def _request(
    method: str,
    path: str,
    data: bytes | None = None,
    headers: dict | None = None,
    content_type: str | None = None,
) -> tuple[int, bytes]:
    """Send an HTTP request and return (status_code, body)."""
    url = f"{BASE_URL}{path}"
    req = urllib.request.Request(url, data=data, method=method)
    if content_type:
        req.add_header("Content-Type", content_type)
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def _json_request(method: str, path: str, body: dict | None = None) -> dict:
    """Send a JSON request and return parsed response."""
    data = json.dumps(body).encode() if body else None
    status, resp = _request(method, path, data=data, content_type="application/json")
    if not resp:
        return {"status_code": status}
    try:
        result = json.loads(resp)
    except json.JSONDecodeError:
        result = {"raw": resp.decode()}
    result["status_code"] = status
    return result


def cmd_create_bucket(args: argparse.Namespace) -> None:
    """Create a bucket."""
    result = _json_request("POST", "/buckets", {"bucket_name": args.bucket})
    print(json.dumps(result, indent=2))


def cmd_list_buckets(args: argparse.Namespace) -> None:
    """List all buckets."""
    result = _json_request("GET", "/buckets")
    print(json.dumps(result, indent=2))


def cmd_delete_bucket(args: argparse.Namespace) -> None:
    """Delete a bucket."""
    result = _json_request("DELETE", f"/buckets/{args.bucket}")
    print(f"Status: {result['status_code']}")


def cmd_upload(args: argparse.Namespace) -> None:
    """Upload a file as an object."""
    with open(args.file, "rb") as f:
        file_data = f.read()

    # Build multipart form data
    boundary = "----PythonCLIBoundary"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{args.key}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode() + file_data + f"\r\n--{boundary}--\r\n".encode()

    status, resp = _request(
        "PUT",
        f"/buckets/{args.bucket}/objects/{args.key}",
        data=body,
        content_type=f"multipart/form-data; boundary={boundary}",
    )
    try:
        print(json.dumps(json.loads(resp), indent=2))
    except json.JSONDecodeError:
        print(f"Status: {status}, Response: {resp.decode()}")


def cmd_download(args: argparse.Namespace) -> None:
    """Download an object."""
    path = f"/buckets/{args.bucket}/objects/{args.key}"
    if args.version_id:
        path += f"?version_id={args.version_id}"
    status, data = _request("GET", path)
    if status == 200:
        if args.output:
            with open(args.output, "wb") as f:
                f.write(data)
            print(f"Downloaded to {args.output} ({len(data)} bytes)")
        else:
            sys.stdout.buffer.write(data)
    else:
        print(f"Error {status}: {data.decode()}")


def cmd_delete_object(args: argparse.Namespace) -> None:
    """Delete an object."""
    result = _json_request("DELETE", f"/buckets/{args.bucket}/objects/{args.key}")
    print(json.dumps(result, indent=2))


def cmd_list_objects(args: argparse.Namespace) -> None:
    """List objects in a bucket."""
    path = f"/buckets/{args.bucket}/objects"
    if args.prefix:
        path += f"?prefix={args.prefix}"
    result = _json_request("GET", path)
    print(json.dumps(result, indent=2))


def cmd_list_versions(args: argparse.Namespace) -> None:
    """List versions of an object."""
    result = _json_request("GET", f"/buckets/{args.bucket}/objects/{args.key}/versions")
    print(json.dumps(result, indent=2))


def cmd_set_versioning(args: argparse.Namespace) -> None:
    """Enable or disable versioning."""
    enabled = args.state.lower() == "enabled"
    result = _json_request(
        "PUT",
        f"/buckets/{args.bucket}/versioning",
        {"enabled": enabled},
    )
    print(json.dumps(result, indent=2))


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="S3-like Object Storage CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    # create-bucket
    p = sub.add_parser("create-bucket", help="Create a bucket")
    p.add_argument("bucket", help="Bucket name")
    p.set_defaults(func=cmd_create_bucket)

    # list-buckets
    p = sub.add_parser("list-buckets", help="List all buckets")
    p.set_defaults(func=cmd_list_buckets)

    # delete-bucket
    p = sub.add_parser("delete-bucket", help="Delete a bucket")
    p.add_argument("bucket", help="Bucket name")
    p.set_defaults(func=cmd_delete_bucket)

    # upload
    p = sub.add_parser("upload", help="Upload a file")
    p.add_argument("bucket", help="Bucket name")
    p.add_argument("key", help="Object key")
    p.add_argument("file", help="Local file path")
    p.set_defaults(func=cmd_upload)

    # download
    p = sub.add_parser("download", help="Download an object")
    p.add_argument("bucket", help="Bucket name")
    p.add_argument("key", help="Object key")
    p.add_argument("-o", "--output", help="Output file path")
    p.add_argument("--version-id", help="Specific version ID")
    p.set_defaults(func=cmd_download)

    # delete-object
    p = sub.add_parser("delete-object", help="Delete an object")
    p.add_argument("bucket", help="Bucket name")
    p.add_argument("key", help="Object key")
    p.set_defaults(func=cmd_delete_object)

    # list-objects
    p = sub.add_parser("list-objects", help="List objects in a bucket")
    p.add_argument("bucket", help="Bucket name")
    p.add_argument("--prefix", default="", help="Key prefix filter")
    p.set_defaults(func=cmd_list_objects)

    # list-versions
    p = sub.add_parser("list-versions", help="List versions of an object")
    p.add_argument("bucket", help="Bucket name")
    p.add_argument("key", help="Object key")
    p.set_defaults(func=cmd_list_versions)

    # set-versioning
    p = sub.add_parser("set-versioning", help="Enable/disable versioning")
    p.add_argument("bucket", help="Bucket name")
    p.add_argument("state", choices=["enabled", "suspended"])
    p.set_defaults(func=cmd_set_versioning)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
