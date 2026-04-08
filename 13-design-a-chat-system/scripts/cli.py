#!/usr/bin/env python3
"""Interactive WebSocket-based chat client for the chat system.

Usage:
    python scripts/cli.py chat alice                     # Interactive chat mode
    python scripts/cli.py history alice bob               # View DM history
    python scripts/cli.py create-group team1 alice bob    # Create a group
    python scripts/cli.py presence alice                  # Check user presence
    python scripts/cli.py --health                        # Health check

In chat mode:
    /msg bob Hello!          -- send 1:1 message
    /group team1 Hi team!    -- send group message
    /history bob             -- view 1:1 chat history
    /online                  -- check who's online
    /quit                    -- disconnect
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

import httpx

BASE_URL = "http://localhost:8013"
WS_URL = "ws://localhost:8013"


# ---------------------------------------------------------------------------
# Non-interactive commands
# ---------------------------------------------------------------------------


def cmd_health() -> None:
    """Check server health."""
    resp = httpx.get(f"{BASE_URL}/health")
    print(json.dumps(resp.json(), indent=2))


def cmd_history(user_a: str, user_b: str) -> None:
    """View DM history between two users."""
    a, b = sorted([user_a, user_b])
    channel_id = f"dm:{a}:{b}"
    resp = httpx.get(f"{BASE_URL}/api/v1/messages/{channel_id}")
    messages = resp.json()
    if not messages:
        print("(no messages)")
        return
    for msg in messages:
        ts = msg.get("timestamp", 0)
        from_user = msg.get("from", "?")
        content = msg.get("content", "")
        print(f"[{ts:.0f}] {from_user}: {content}")


def cmd_create_group(group_id: str, members: list[str]) -> None:
    """Create a chat group."""
    resp = httpx.post(
        f"{BASE_URL}/api/v1/groups",
        json={"group_id": group_id, "name": group_id, "members": members},
    )
    print(json.dumps(resp.json(), indent=2))


def cmd_presence(user_id: str) -> None:
    """Check user presence."""
    resp = httpx.get(f"{BASE_URL}/api/v1/presence/{user_id}")
    print(json.dumps(resp.json(), indent=2))


# ---------------------------------------------------------------------------
# Interactive chat mode
# ---------------------------------------------------------------------------


async def chat_mode(user_id: str) -> None:
    """Interactive WebSocket chat client."""
    try:
        import websockets
    except ImportError:
        print("ERROR: 'websockets' package is required. pip install websockets")
        sys.exit(1)

    # Register user first
    httpx.post(
        f"{BASE_URL}/api/v1/users",
        json={"user_id": user_id, "name": user_id},
    )

    uri = f"{WS_URL}/ws/{user_id}"
    print(f"Connecting to {uri} ...")

    async with websockets.connect(uri) as ws:
        print(f"Connected as '{user_id}'. Type /help for commands.\n")

        async def heartbeat_loop() -> None:
            """Send heartbeat every 5 seconds."""
            while True:
                try:
                    await ws.send(json.dumps({"type": "heartbeat"}))
                    await asyncio.sleep(5)
                except Exception:
                    break

        async def receive_loop() -> None:
            """Receive and display incoming messages."""
            try:
                async for raw in ws:
                    data = json.loads(raw)
                    msg_type = data.get("type")

                    if msg_type == "message":
                        from_user = data.get("from", "?")
                        content = data.get("content", "")
                        if from_user != user_id:
                            print(f"\n  [{from_user}] {content}")
                    elif msg_type == "group_message":
                        from_user = data.get("from", "?")
                        group_id = data.get("group_id", "?")
                        content = data.get("content", "")
                        if from_user != user_id:
                            print(f"\n  [{group_id}/{from_user}] {content}")
                    elif msg_type == "presence":
                        uid = data.get("user_id", "?")
                        status = data.get("status", "?")
                        print(f"\n  * {uid} is now {status}")
                    elif msg_type == "error":
                        detail = data.get("detail", "unknown error")
                        print(f"\n  [ERROR] {detail}")
            except Exception:
                pass

        async def input_loop() -> None:
            """Read user input and send messages."""
            loop = asyncio.get_event_loop()
            while True:
                try:
                    line = await loop.run_in_executor(None, lambda: input("> "))
                except (EOFError, KeyboardInterrupt):
                    break

                line = line.strip()
                if not line:
                    continue

                if line == "/quit":
                    break
                elif line == "/help":
                    print("Commands:")
                    print("  /msg <user> <message>     -- send 1:1 message")
                    print("  /group <group_id> <msg>   -- send group message")
                    print("  /history <user>           -- view DM history")
                    print("  /online                   -- list connected users")
                    print("  /quit                     -- disconnect")
                elif line.startswith("/msg "):
                    parts = line[5:].split(" ", 1)
                    if len(parts) == 2:
                        to_user, content = parts
                        await ws.send(json.dumps({
                            "type": "message",
                            "to": to_user,
                            "content": content,
                        }))
                    else:
                        print("Usage: /msg <user> <message>")
                elif line.startswith("/group "):
                    parts = line[7:].split(" ", 1)
                    if len(parts) == 2:
                        group_id, content = parts
                        await ws.send(json.dumps({
                            "type": "group_message",
                            "group_id": group_id,
                            "content": content,
                        }))
                    else:
                        print("Usage: /group <group_id> <message>")
                elif line.startswith("/history "):
                    other = line[9:].strip()
                    if other:
                        a, b = sorted([user_id, other])
                        channel_id = f"dm:{a}:{b}"
                        resp = httpx.get(f"{BASE_URL}/api/v1/messages/{channel_id}")
                        messages = resp.json()
                        if not messages:
                            print("  (no messages)")
                        else:
                            for msg in messages:
                                from_u = msg.get("from", "?")
                                c = msg.get("content", "")
                                print(f"  [{from_u}] {c}")
                elif line == "/online":
                    # Check presence for some known users
                    print("  (presence checked via REST - use 'presence' command)")
                else:
                    print(f"Unknown command: {line}. Type /help for help.")

        # Run all three loops concurrently
        hb_task = asyncio.create_task(heartbeat_loop())
        recv_task = asyncio.create_task(receive_loop())
        try:
            await input_loop()
        finally:
            hb_task.cancel()
            recv_task.cancel()
            print("\nDisconnected.")


# ---------------------------------------------------------------------------
# CLI argument parser
# ---------------------------------------------------------------------------


def main() -> None:
    """Parse CLI arguments and dispatch to the appropriate command."""
    parser = argparse.ArgumentParser(description="Chat System CLI Client")
    parser.add_argument("--health", action="store_true", help="Check server health")
    sub = parser.add_subparsers(dest="command")

    # chat <user_id>
    chat_parser = sub.add_parser("chat", help="Interactive chat mode")
    chat_parser.add_argument("user_id", help="Your user ID")

    # history <user_a> <user_b>
    hist_parser = sub.add_parser("history", help="View DM history")
    hist_parser.add_argument("user_a", help="First user")
    hist_parser.add_argument("user_b", help="Second user")

    # create-group <group_id> <member1> <member2> ...
    grp_parser = sub.add_parser("create-group", help="Create a chat group")
    grp_parser.add_argument("group_id", help="Group ID")
    grp_parser.add_argument("members", nargs="+", help="Group members")

    # presence <user_id>
    pres_parser = sub.add_parser("presence", help="Check user presence")
    pres_parser.add_argument("user_id", help="User ID to check")

    args = parser.parse_args()

    if args.health:
        cmd_health()
    elif args.command == "chat":
        asyncio.run(chat_mode(args.user_id))
    elif args.command == "history":
        cmd_history(args.user_a, args.user_b)
    elif args.command == "create-group":
        cmd_create_group(args.group_id, args.members)
    elif args.command == "presence":
        cmd_presence(args.user_id)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
