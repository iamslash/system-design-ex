#!/usr/bin/env python3
"""CLI client for the distributed email service."""

from __future__ import annotations

import argparse
import json
import sys

import httpx

BASE_URL = "http://localhost:8024"


def send(args: argparse.Namespace) -> None:
    resp = httpx.post(
        f"{BASE_URL}/api/email/send",
        json={
            "from_addr": args.from_addr,
            "to_addrs": args.to_addrs.split(","),
            "subject": args.subject,
            "body": args.body,
        },
    )
    print(json.dumps(resp.json(), indent=2))


def deliver(args: argparse.Namespace) -> None:
    resp = httpx.post(f"{BASE_URL}/api/email/deliver")
    print(json.dumps(resp.json(), indent=2))


def inbox(args: argparse.Namespace) -> None:
    resp = httpx.get(f"{BASE_URL}/api/folders/{args.user}/inbox")
    print(json.dumps(resp.json(), indent=2))


def read_email(args: argparse.Namespace) -> None:
    resp = httpx.get(f"{BASE_URL}/api/email/{args.email_id}")
    print(json.dumps(resp.json(), indent=2))


def mark_read(args: argparse.Namespace) -> None:
    resp = httpx.post(f"{BASE_URL}/api/email/{args.email_id}/read")
    print(json.dumps(resp.json(), indent=2))


def search(args: argparse.Namespace) -> None:
    resp = httpx.post(
        f"{BASE_URL}/api/email/search",
        json={"user": args.user, "query": args.query},
    )
    print(json.dumps(resp.json(), indent=2))


def thread(args: argparse.Namespace) -> None:
    resp = httpx.get(f"{BASE_URL}/api/thread/{args.thread_id}")
    print(json.dumps(resp.json(), indent=2))


def folders(args: argparse.Namespace) -> None:
    resp = httpx.get(f"{BASE_URL}/api/folders/{args.user}")
    print(json.dumps(resp.json(), indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Email Service CLI")
    sub = parser.add_subparsers(dest="command")

    p_send = sub.add_parser("send", help="Send an email")
    p_send.add_argument("--from", dest="from_addr", required=True)
    p_send.add_argument("--to", dest="to_addrs", required=True, help="Comma-separated")
    p_send.add_argument("--subject", default="")
    p_send.add_argument("--body", default="")
    p_send.set_defaults(func=send)

    p_deliver = sub.add_parser("deliver", help="Trigger manual delivery")
    p_deliver.set_defaults(func=deliver)

    p_inbox = sub.add_parser("inbox", help="Show inbox")
    p_inbox.add_argument("--user", required=True)
    p_inbox.set_defaults(func=inbox)

    p_read = sub.add_parser("read", help="Read an email")
    p_read.add_argument("email_id")
    p_read.set_defaults(func=read_email)

    p_mark = sub.add_parser("mark-read", help="Mark email as read")
    p_mark.add_argument("email_id")
    p_mark.set_defaults(func=mark_read)

    p_search = sub.add_parser("search", help="Search emails")
    p_search.add_argument("--user", required=True)
    p_search.add_argument("--query", required=True)
    p_search.set_defaults(func=search)

    p_thread = sub.add_parser("thread", help="Show email thread")
    p_thread.add_argument("thread_id")
    p_thread.set_defaults(func=thread)

    p_folders = sub.add_parser("folders", help="List folders")
    p_folders.add_argument("--user", required=True)
    p_folders.set_defaults(func=folders)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
