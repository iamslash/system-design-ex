#!/usr/bin/env python3
"""Interactive WebSocket-based client for the nearby friends system.

Usage:
    python scripts/cli.py track alice                     # Interactive location tracking
    python scripts/cli.py nearby alice                    # Query nearby friends
    python scripts/cli.py add-friend alice bob             # Create friendship
    python scripts/cli.py history alice                    # View location history
    python scripts/cli.py --health                         # Health check

In track mode:
    /update 40.7128 -74.0060     -- update your location
    /nearby                       -- find nearby friends
    /nearby 10                    -- find friends within 10 miles
    /quit                         -- disconnect
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

import httpx

BASE_URL = "http://localhost:8018"
WS_URL = "ws://localhost:8018"


# ---------------------------------------------------------------------------
# Non-interactive commands
# ---------------------------------------------------------------------------


def cmd_health() -> None:
    """Check server health."""
    resp = httpx.get(f"{BASE_URL}/health")
    print(json.dumps(resp.json(), indent=2))


def cmd_nearby(user_id: str, radius: float | None = None) -> None:
    """Query nearby friends for a user."""
    params = {}
    if radius is not None:
        params["radius"] = radius
    resp = httpx.get(f"{BASE_URL}/api/v1/nearby/{user_id}", params=params)
    data = resp.json()
    friends = data.get("nearby_friends", [])
    if not friends:
        print("(no nearby friends)")
        return
    for f in friends:
        print(f"  {f['user_id']}: ({f['latitude']:.4f}, {f['longitude']:.4f}) - {f['distance_miles']:.2f} miles")


def cmd_add_friend(user_a: str, user_b: str) -> None:
    """Create a friendship."""
    resp = httpx.post(
        f"{BASE_URL}/api/v1/friends",
        json={"user_a": user_a, "user_b": user_b},
    )
    print(json.dumps(resp.json(), indent=2))


def cmd_history(user_id: str) -> None:
    """View location history."""
    resp = httpx.get(f"{BASE_URL}/api/v1/location-history/{user_id}")
    data = resp.json()
    entries = data.get("history", [])
    if not entries:
        print("(no location history)")
        return
    for e in entries:
        print(f"  ({e['latitude']:.4f}, {e['longitude']:.4f}) @ {e['timestamp']:.0f}")


def cmd_location(user_id: str, lat: float, lon: float) -> None:
    """Update location via REST."""
    resp = httpx.post(
        f"{BASE_URL}/api/v1/location",
        json={"user_id": user_id, "latitude": lat, "longitude": lon},
    )
    print(json.dumps(resp.json(), indent=2))


# ---------------------------------------------------------------------------
# Interactive tracking mode
# ---------------------------------------------------------------------------


async def track_mode(user_id: str) -> None:
    """Interactive WebSocket location tracking client."""
    try:
        import websockets
    except ImportError:
        print("ERROR: 'websockets' package is required. pip install websockets")
        sys.exit(1)

    uri = f"{WS_URL}/ws/{user_id}"
    print(f"Connecting to {uri} ...")

    async with websockets.connect(uri) as ws:
        print(f"Connected as '{user_id}'. Type /help for commands.\n")

        async def receive_loop() -> None:
            """Receive and display incoming messages."""
            try:
                async for raw in ws:
                    data = json.loads(raw)
                    msg_type = data.get("type")

                    if msg_type == "friend_location":
                        uid = data.get("user_id", "?")
                        lat = data.get("latitude", 0)
                        lon = data.get("longitude", 0)
                        dist = data.get("distance_miles", 0)
                        print(f"\n  [NEARBY] {uid}: ({lat:.4f}, {lon:.4f}) - {dist:.2f} miles")
                    elif msg_type == "location_ack":
                        lat = data.get("latitude", 0)
                        lon = data.get("longitude", 0)
                        print(f"\n  [ACK] Location updated: ({lat:.4f}, {lon:.4f})")
                    elif msg_type == "nearby_result":
                        friends = data.get("nearby_friends", [])
                        if not friends:
                            print("\n  (no nearby friends)")
                        else:
                            for f in friends:
                                print(f"\n  {f['user_id']}: ({f['latitude']:.4f}, {f['longitude']:.4f}) - {f['distance_miles']:.2f} miles")
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
                    print("  /update <lat> <lon>   -- update your location")
                    print("  /nearby               -- find nearby friends")
                    print("  /nearby <radius>      -- find friends within <radius> miles")
                    print("  /quit                 -- disconnect")
                elif line.startswith("/update "):
                    parts = line[8:].split()
                    if len(parts) == 2:
                        try:
                            lat, lon = float(parts[0]), float(parts[1])
                            await ws.send(json.dumps({
                                "type": "location_update",
                                "latitude": lat,
                                "longitude": lon,
                            }))
                        except ValueError:
                            print("Usage: /update <lat> <lon> (decimal degrees)")
                    else:
                        print("Usage: /update <lat> <lon>")
                elif line.startswith("/nearby"):
                    parts = line.split()
                    msg: dict = {"type": "get_nearby"}
                    if len(parts) == 2:
                        try:
                            msg["radius_miles"] = float(parts[1])
                        except ValueError:
                            print("Usage: /nearby [radius_miles]")
                            continue
                    await ws.send(json.dumps(msg))
                else:
                    print(f"Unknown command: {line}. Type /help for help.")

        recv_task = asyncio.create_task(receive_loop())
        try:
            await input_loop()
        finally:
            recv_task.cancel()
            print("\nDisconnected.")


# ---------------------------------------------------------------------------
# CLI argument parser
# ---------------------------------------------------------------------------


def main() -> None:
    """Parse CLI arguments and dispatch to the appropriate command."""
    parser = argparse.ArgumentParser(description="Nearby Friends CLI Client")
    parser.add_argument("--health", action="store_true", help="Check server health")
    sub = parser.add_subparsers(dest="command")

    # track <user_id>
    track_parser = sub.add_parser("track", help="Interactive location tracking")
    track_parser.add_argument("user_id", help="Your user ID")

    # nearby <user_id> [--radius R]
    nearby_parser = sub.add_parser("nearby", help="Query nearby friends")
    nearby_parser.add_argument("user_id", help="Your user ID")
    nearby_parser.add_argument("--radius", type=float, help="Radius in miles")

    # add-friend <user_a> <user_b>
    friend_parser = sub.add_parser("add-friend", help="Create a friendship")
    friend_parser.add_argument("user_a", help="First user")
    friend_parser.add_argument("user_b", help="Second user")

    # history <user_id>
    hist_parser = sub.add_parser("history", help="View location history")
    hist_parser.add_argument("user_id", help="User ID")

    # location <user_id> <lat> <lon>
    loc_parser = sub.add_parser("location", help="Update location via REST")
    loc_parser.add_argument("user_id", help="User ID")
    loc_parser.add_argument("latitude", type=float, help="Latitude")
    loc_parser.add_argument("longitude", type=float, help="Longitude")

    args = parser.parse_args()

    if args.health:
        cmd_health()
    elif args.command == "track":
        asyncio.run(track_mode(args.user_id))
    elif args.command == "nearby":
        cmd_nearby(args.user_id, radius=args.radius)
    elif args.command == "add-friend":
        cmd_add_friend(args.user_a, args.user_b)
    elif args.command == "history":
        cmd_history(args.user_id)
    elif args.command == "location":
        cmd_location(args.user_id, args.latitude, args.longitude)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
