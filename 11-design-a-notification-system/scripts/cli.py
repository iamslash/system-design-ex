#!/usr/bin/env python3
"""CLI client for the notification system API."""

from __future__ import annotations

import argparse
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_URL = "http://localhost:8011"


def _request(
    method: str,
    path: str,
    data: dict | None = None,
) -> tuple[int, dict]:
    """Send an HTTP request and return (status_code, body_json)."""
    url = f"{BASE_URL}{path}"
    body_bytes = json.dumps(data).encode() if data else None
    req = Request(url, data=body_bytes, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except HTTPError as e:
        return e.code, json.loads(e.read().decode())
    except URLError as e:
        print(f"Connection error: {e.reason}", file=sys.stderr)
        sys.exit(1)


def cmd_health() -> None:
    """Run health check."""
    status, body = _request("GET", "/health")
    print(f"Health: {status}")
    print(f"  Status: {body.get('status')}")
    queues = body.get("queues", {})
    for q, length in queues.items():
        print(f"  Queue {q}: {length} pending")


def cmd_send(args: argparse.Namespace) -> None:
    """Send a single notification."""
    params = json.loads(args.params) if args.params else {}
    data = {
        "user_id": args.user,
        "channel": args.channel,
        "template": args.template,
        "params": params,
        "priority": args.priority,
    }
    status, body = _request("POST", "/api/v1/notify", data)
    print(f"[{status}] {json.dumps(body, indent=2)}")


def cmd_batch(args: argparse.Namespace) -> None:
    """Send batch notifications."""
    params = json.loads(args.params) if args.params else {}
    user_ids = [u.strip() for u in args.users.split(",")]
    data = {
        "user_ids": user_ids,
        "channel": args.channel,
        "template": args.template,
        "params": params,
        "priority": args.priority,
    }
    status, body = _request("POST", "/api/v1/notify/batch", data)
    print(f"[{status}] Batch results ({body.get('total', 0)} users):")
    for r in body.get("results", []):
        print(f"  {r.get('notification_id', 'N/A')} -> {r.get('status')}: {r.get('message')}")


def cmd_status(args: argparse.Namespace) -> None:
    """Check notification status."""
    status, body = _request("GET", f"/api/v1/notifications/{args.notification_id}/status")
    if status == 200:
        print(f"Notification: {args.notification_id}")
        for k, v in body.items():
            print(f"  {k}: {v}")
    else:
        print(f"[{status}] {body.get('detail', 'Not found')}")


def cmd_history(args: argparse.Namespace) -> None:
    """View user notification history."""
    status, body = _request("GET", f"/api/v1/notifications/{args.user_id}")
    print(f"User: {args.user_id} ({body.get('count', 0)} notifications)")
    for n in body.get("notifications", []):
        nid = n.get("notification_id", "?")
        ch = n.get("channel", "?")
        st = n.get("status", "?")
        tmpl = n.get("template", "?")
        print(f"  [{st:>8}] {nid[:8]}... channel={ch} template={tmpl}")


def cmd_settings(args: argparse.Namespace) -> None:
    """Get or update user preferences."""
    if args.push is None and args.sms is None and args.email is None:
        # GET preferences
        status, body = _request("GET", f"/api/v1/settings/{args.user_id}")
        print(f"User: {args.user_id}")
        prefs = body.get("preferences", {})
        for ch, enabled in prefs.items():
            state = "on" if enabled else "off"
            print(f"  {ch}: {state}")
    else:
        # PUT preferences — first get current, then update
        _, current = _request("GET", f"/api/v1/settings/{args.user_id}")
        prefs = current.get("preferences", {"push": True, "sms": True, "email": True})
        if args.push is not None:
            prefs["push"] = args.push == "on"
        if args.sms is not None:
            prefs["sms"] = args.sms == "on"
        if args.email is not None:
            prefs["email"] = args.email == "on"
        status, body = _request("PUT", f"/api/v1/settings/{args.user_id}", prefs)
        print(f"[{status}] {body.get('message', 'Updated')}")
        for ch, enabled in body.get("preferences", {}).items():
            state = "on" if enabled else "off"
            print(f"  {ch}: {state}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Notification System CLI Client")
    parser.add_argument("--health", action="store_true", help="Run health check")
    parser.add_argument("--base-url", type=str, default=BASE_URL, help="API base URL")

    subparsers = parser.add_subparsers(dest="command")

    # send
    send_parser = subparsers.add_parser("send", help="Send a notification")
    send_parser.add_argument("--user", required=True, help="User ID")
    send_parser.add_argument("--channel", required=True, choices=["push", "sms", "email"])
    send_parser.add_argument("--template", default="default", help="Template name")
    send_parser.add_argument("--params", default=None, help="JSON params string")
    send_parser.add_argument("--priority", default="normal", choices=["high", "normal", "low"])

    # batch
    batch_parser = subparsers.add_parser("batch", help="Send batch notifications")
    batch_parser.add_argument("--users", required=True, help="Comma-separated user IDs")
    batch_parser.add_argument("--channel", required=True, choices=["push", "sms", "email"])
    batch_parser.add_argument("--template", default="default", help="Template name")
    batch_parser.add_argument("--params", default=None, help="JSON params string")
    batch_parser.add_argument("--priority", default="normal", choices=["high", "normal", "low"])

    # status
    status_parser = subparsers.add_parser("status", help="Check notification status")
    status_parser.add_argument("notification_id", help="Notification ID")

    # history
    history_parser = subparsers.add_parser("history", help="View user notification history")
    history_parser.add_argument("user_id", help="User ID")

    # settings
    settings_parser = subparsers.add_parser("settings", help="Get/update user preferences")
    settings_parser.add_argument("user_id", help="User ID")
    settings_parser.add_argument("--push", choices=["on", "off"], default=None)
    settings_parser.add_argument("--sms", choices=["on", "off"], default=None)
    settings_parser.add_argument("--email", choices=["on", "off"], default=None)

    args = parser.parse_args()

    global BASE_URL
    BASE_URL = args.base_url

    if args.health:
        cmd_health()
    elif args.command == "send":
        cmd_send(args)
    elif args.command == "batch":
        cmd_batch(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "history":
        cmd_history(args)
    elif args.command == "settings":
        cmd_settings(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
