#!/usr/bin/env python3
"""CLI client to test the gaming leaderboard API."""

from __future__ import annotations

import argparse
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BASE_URL = "http://localhost:8026"


def _request(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    """Send an HTTP request and return (status_code, body_json)."""
    url = f"{BASE_URL}{path}"
    data = json.dumps(body).encode() if body else None
    req = Request(url, data=data, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except HTTPError as e:
        return e.code, json.loads(e.read().decode())
    except URLError as e:
        print(f"Connection error: {e.reason}", file=sys.stderr)
        sys.exit(1)


def cmd_health() -> None:
    """Run health check."""
    status, body = _request("GET", "/health")
    print(f"Health: {status} -> {body}")


def cmd_score(user_id: str, points: int) -> None:
    """Score points for a user."""
    status, body = _request("POST", "/v1/scores", {"user_id": user_id, "points": points})
    print(f"Score ({status}): {json.dumps(body, indent=2)}")


def cmd_top() -> None:
    """Show top 10 leaderboard."""
    status, body = _request("GET", "/v1/scores")
    print(f"Top 10 ({status}):")
    for entry in body:
        print(f"  #{entry['rank']} {entry['user_id']}: {entry['score']}")


def cmd_rank(user_id: str) -> None:
    """Show a user's rank."""
    status, body = _request("GET", f"/v1/scores/{user_id}")
    if status == 200:
        print(f"Rank: #{body['rank']}  Score: {body['score']}  User: {body['user_id']}")
    else:
        print(f"Error ({status}): {body.get('detail', body)}")


def cmd_around(user_id: str) -> None:
    """Show entries around a user."""
    status, body = _request("GET", f"/v1/scores/{user_id}/around")
    if status == 200:
        print(f"Around {user_id}:")
        for entry in body:
            marker = " <--" if entry["user_id"] == user_id else ""
            print(f"  #{entry['rank']} {entry['user_id']}: {entry['score']}{marker}")
    else:
        print(f"Error ({status}): {body.get('detail', body)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Gaming Leaderboard CLI Client")
    parser.add_argument("--health", action="store_true", help="Run health check")
    parser.add_argument("--score", nargs=2, metavar=("USER_ID", "POINTS"), help="Score points for a user")
    parser.add_argument("--top", action="store_true", help="Show top 10 leaderboard")
    parser.add_argument("--rank", metavar="USER_ID", help="Show a user's rank")
    parser.add_argument("--around", metavar="USER_ID", help="Show entries around a user")
    parser.add_argument("--base-url", type=str, default=BASE_URL, help="API base URL")

    args = parser.parse_args()

    global BASE_URL
    BASE_URL = args.base_url

    if args.health:
        cmd_health()
    elif args.score:
        cmd_score(args.score[0], int(args.score[1]))
    elif args.top:
        cmd_top()
    elif args.rank:
        cmd_rank(args.rank)
    elif args.around:
        cmd_around(args.around)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
