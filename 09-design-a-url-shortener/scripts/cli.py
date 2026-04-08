#!/usr/bin/env python3
"""CLI client to test the URL shortener API."""

from __future__ import annotations

import argparse
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BASE_URL = "http://localhost:8009"


def _request(
    path: str,
    method: str = "GET",
    data: dict | None = None,
) -> tuple[int, dict[str, str], dict]:
    """Send an HTTP request and return (status_code, headers, body_json)."""
    url = f"{BASE_URL}{path}"
    body_bytes = json.dumps(data).encode() if data else None
    req = Request(url, data=body_bytes, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urlopen(req, timeout=5) as resp:
            headers = {k: v for k, v in resp.getheaders()}
            body = json.loads(resp.read().decode())
            return resp.status, headers, body
    except HTTPError as e:
        headers = {k: v for k, v in e.headers.items()}
        try:
            body = json.loads(e.read().decode())
        except Exception:
            body = {"detail": str(e)}
        return e.code, headers, body
    except URLError as e:
        print(f"Connection error: {e.reason}", file=sys.stderr)
        sys.exit(1)


def cmd_health() -> None:
    """Run health check."""
    status, _, body = _request("/health")
    print(f"Health: {status} -> {body}")


def cmd_shorten(long_url: str) -> None:
    """Shorten a URL."""
    status, _, body = _request("/api/v1/shorten", method="POST", data={"url": long_url})
    if status == 200:
        print(f"Short URL : {body['short_url']}")
        print(f"Short Code: {body['short_code']}")
    else:
        print(f"Error ({status}): {body.get('detail', body)}")


def cmd_redirect(short_code: str) -> None:
    """Look up the original URL for a short code (without following redirect)."""
    status, _, body = _request(f"/api/v1/stats/{short_code}")
    if status == 200:
        print(f"Short Code : {body['short_code']}")
        print(f"Long URL   : {body['long_url']}")
        print(f"Clicks     : {body['clicks']}")
        print(f"Created At : {body['created_at']}")
    else:
        print(f"Error ({status}): {body.get('detail', body)}")


def cmd_stats(short_code: str) -> None:
    """Get statistics for a short code."""
    status, _, body = _request(f"/api/v1/stats/{short_code}")
    if status == 200:
        print(f"Short Code : {body['short_code']}")
        print(f"Short URL  : {body['short_url']}")
        print(f"Long URL   : {body['long_url']}")
        print(f"Clicks     : {body['clicks']}")
        print(f"Created At : {body['created_at']}")
    else:
        print(f"Error ({status}): {body.get('detail', body)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="URL Shortener CLI Client")
    parser.add_argument("--health", action="store_true", help="Run health check")
    parser.add_argument("--base-url", type=str, default=BASE_URL, help="API base URL")

    subparsers = parser.add_subparsers(dest="command")

    shorten_parser = subparsers.add_parser("shorten", help="Shorten a URL")
    shorten_parser.add_argument("url", help="Long URL to shorten")

    redirect_parser = subparsers.add_parser("redirect", help="Look up original URL")
    redirect_parser.add_argument("short_code", help="Short code to look up")

    stats_parser = subparsers.add_parser("stats", help="Get URL statistics")
    stats_parser.add_argument("short_code", help="Short code to query")

    args = parser.parse_args()

    global BASE_URL
    BASE_URL = args.base_url

    if args.health:
        cmd_health()
    elif args.command == "shorten":
        cmd_shorten(args.url)
    elif args.command == "redirect":
        cmd_redirect(args.short_code)
    elif args.command == "stats":
        cmd_stats(args.short_code)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
