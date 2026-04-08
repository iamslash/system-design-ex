#!/usr/bin/env python3
"""CLI client to test the rate limiter API."""

from __future__ import annotations

import argparse
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import json


BASE_URL = "http://localhost:8005"


def _request(path: str) -> tuple[int, dict[str, str], dict]:
    """Send a GET request and return (status_code, headers_dict, body_json)."""
    url = f"{BASE_URL}{path}"
    req = Request(url)
    try:
        with urlopen(req, timeout=5) as resp:
            headers = {k: v for k, v in resp.getheaders()}
            body = json.loads(resp.read().decode())
            return resp.status, headers, body
    except HTTPError as e:
        headers = {k: v for k, v in e.headers.items()}
        body = json.loads(e.read().decode())
        return e.code, headers, body
    except URLError as e:
        print(f"Connection error: {e.reason}", file=sys.stderr)
        sys.exit(1)


def cmd_health() -> None:
    """Run health check."""
    status, _, body = _request("/health")
    print(f"Health: {status} -> {body}")


def cmd_config() -> None:
    """Show current rate limit config."""
    status, _, body = _request("/api/config")
    print(f"Config ({status}):")
    for key, value in body.items():
        print(f"  {key}: {value}")


def cmd_burst(n: int) -> None:
    """Send N requests rapidly and show results."""
    passed = 0
    rejected = 0

    for i in range(1, n + 1):
        status, headers, body = _request("/api/limited")

        remaining = headers.get("X-Ratelimit-Remaining", headers.get("x-ratelimit-remaining", "?"))
        limit = headers.get("X-Ratelimit-Limit", headers.get("x-ratelimit-limit", "?"))
        retry_after = headers.get("X-Ratelimit-Retry-After", headers.get("x-ratelimit-retry-after", ""))

        if status == 200:
            passed += 1
            print(f"[{i}/{n}] GET /api/limited -> {status}  Remaining: {remaining}  Limit: {limit}")
        else:
            rejected += 1
            suffix = f"  Retry-After: {retry_after}s" if retry_after else ""
            print(f"[{i}/{n}] GET /api/limited -> {status}{suffix}")

        # Tiny delay to avoid overwhelming the connection
        if i < n:
            time.sleep(0.05)

    print()
    print("--- Summary ---")
    print(f"Passed (200): {passed}")
    print(f"Rejected (429): {rejected}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rate Limiter CLI Client")
    parser.add_argument("--health", action="store_true", help="Run health check")
    parser.add_argument("--burst", type=int, metavar="N", help="Send N requests rapidly")
    parser.add_argument("--config", action="store_true", help="Show rate limit config")
    parser.add_argument("--base-url", type=str, default=BASE_URL, help="API base URL")

    args = parser.parse_args()

    global BASE_URL
    BASE_URL = args.base_url

    if args.health:
        cmd_health()
    elif args.config:
        cmd_config()
    elif args.burst:
        cmd_burst(args.burst)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
