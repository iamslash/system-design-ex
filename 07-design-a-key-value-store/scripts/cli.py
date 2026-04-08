#!/usr/bin/env python3
"""CLI client for the distributed key-value store.

Usage:
    python scripts/cli.py put mykey "hello world"
    python scripts/cli.py get mykey
    python scripts/cli.py delete mykey
    python scripts/cli.py list
    python scripts/cli.py cluster
    python scripts/cli.py health
"""

from __future__ import annotations

import argparse
import json
import sys

import httpx

DEFAULT_NODE = "http://localhost:8071"
TIMEOUT = 5.0


def _url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


def _pretty(data: dict) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_put(base: str, key: str, value: str) -> None:
    resp = httpx.put(
        _url(base, f"store/{key}"),
        json={"value": value},
        timeout=TIMEOUT,
    )
    if resp.status_code >= 400:
        print(f"[ERROR] {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)
    data = resp.json()
    print(f"OK  key={key}")
    if "vector_clock" in data:
        print(f"    vector_clock={data['vector_clock']}")
    if "acks" in data:
        print(f"    acks={data['acks']}")


def cmd_get(base: str, key: str) -> None:
    resp = httpx.get(_url(base, f"store/{key}"), timeout=TIMEOUT)
    if resp.status_code == 404:
        print(f"NOT FOUND  key={key}")
        sys.exit(0)
    if resp.status_code >= 400:
        print(f"[ERROR] {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)
    data = resp.json()

    if data.get("status") == "conflict":
        print(f"CONFLICT  key={key}  ({len(data['versions'])} versions)")
        for i, v in enumerate(data["versions"], 1):
            print(f"  version {i}:")
            print(f"    value={v['value']}")
            print(f"    vector_clock={v['vector_clock']}")
            print(f"    node={v.get('node_id', '?')}")
    else:
        print(f"OK  key={key}  value={data.get('value', '')}")
        if "vector_clock" in data:
            print(f"    vector_clock={data['vector_clock']}")
        if "timestamp" in data:
            print(f"    timestamp={data['timestamp']}")


def cmd_delete(base: str, key: str) -> None:
    resp = httpx.delete(_url(base, f"store/{key}"), timeout=TIMEOUT)
    if resp.status_code >= 400:
        print(f"[ERROR] {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)
    data = resp.json()
    print(f"DELETED  key={key}  acks={data.get('acks', '?')}")


def cmd_list(base: str) -> None:
    resp = httpx.get(_url(base, "store"), timeout=TIMEOUT)
    if resp.status_code >= 400:
        print(f"[ERROR] {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)
    data = resp.json()
    print(f"Node: {data.get('node_id', '?')}")
    keys = data.get("keys", [])
    if not keys:
        print("  (no keys)")
    else:
        for k in keys:
            print(f"  - {k}")


def cmd_cluster(base: str) -> None:
    resp = httpx.get(_url(base, "cluster/info"), timeout=TIMEOUT)
    if resp.status_code >= 400:
        print(f"[ERROR] {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)
    print(_pretty(resp.json()))


def cmd_health(base: str) -> None:
    resp = httpx.get(_url(base, "health"), timeout=TIMEOUT)
    if resp.status_code >= 400:
        print(f"[ERROR] {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)
    print(_pretty(resp.json()))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="KV store CLI client")
    parser.add_argument(
        "--node",
        default=DEFAULT_NODE,
        help=f"Node base URL (default: {DEFAULT_NODE})",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_put = sub.add_parser("put", help="Put a key-value pair")
    p_put.add_argument("key")
    p_put.add_argument("value")

    p_get = sub.add_parser("get", help="Get a value by key")
    p_get.add_argument("key")

    p_del = sub.add_parser("delete", help="Delete a key")
    p_del.add_argument("key")

    sub.add_parser("list", help="List all keys on the node")
    sub.add_parser("cluster", help="Show cluster info")
    sub.add_parser("health", help="Health check")

    args = parser.parse_args()

    try:
        if args.command == "put":
            cmd_put(args.node, args.key, args.value)
        elif args.command == "get":
            cmd_get(args.node, args.key)
        elif args.command == "delete":
            cmd_delete(args.node, args.key)
        elif args.command == "list":
            cmd_list(args.node)
        elif args.command == "cluster":
            cmd_cluster(args.node)
        elif args.command == "health":
            cmd_health(args.node)
    except httpx.ConnectError:
        print(f"[ERROR] Cannot connect to {args.node}", file=sys.stderr)
        sys.exit(1)
    except httpx.TimeoutException:
        print(f"[ERROR] Request to {args.node} timed out", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
