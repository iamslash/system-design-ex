#!/usr/bin/env python3
"""CLI client for the news feed system API."""

from __future__ import annotations

import argparse
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_URL = "http://localhost:8012"


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
    print(f"  Redis: {body.get('redis_version', 'unknown')}")


def cmd_create_user(args: argparse.Namespace) -> None:
    """Create a user."""
    data = {"user_id": args.user_id, "name": args.name or args.user_id}
    status, body = _request("POST", "/api/v1/users", data)
    if status == 200:
        print(f"User created: {body.get('user_id')} ({body.get('name')})")
    else:
        print(f"[{status}] {body.get('detail', body)}")


def cmd_follow(args: argparse.Namespace) -> None:
    """Follow a user."""
    data = {"follower_id": args.follower, "followee_id": args.followee}
    status, body = _request("POST", "/api/v1/follow", data)
    if body.get("status") == "ok":
        print(f"{args.follower} now follows {args.followee}")
    else:
        print(f"[{body.get('status')}] {body.get('message', '')}")


def cmd_unfollow(args: argparse.Namespace) -> None:
    """Unfollow a user."""
    data = {"follower_id": args.follower, "followee_id": args.followee}
    status, body = _request("POST", "/api/v1/unfollow", data)
    if body.get("status") == "ok":
        print(f"{args.follower} unfollowed {args.followee}")
    else:
        print(f"[{body.get('status')}] {body.get('message', '')}")


def cmd_post(args: argparse.Namespace) -> None:
    """Create a post."""
    data = {"user_id": args.user_id, "content": args.content}
    status, body = _request("POST", "/api/v1/posts", data)
    print(f"Post created: {body.get('post_id')}")
    print(f"  Author: {body.get('user_id')}")
    print(f"  Content: {body.get('content')}")
    print(f"  Fanout: {body.get('fanout_count', 0)} followers")


def cmd_feed(args: argparse.Namespace) -> None:
    """Get user's news feed."""
    status, body = _request("GET", f"/api/v1/feed/{args.user_id}")
    count = body.get("count", 0)
    print(f"Feed for {args.user_id} ({count} posts):")
    for item in body.get("feed", []):
        post_id = item.get("post_id", "?")
        author = item.get("author_name", "?")
        content = item.get("content", "")
        likes = item.get("likes", 0)
        print(f"  [{post_id}] @{author}: {content} (likes: {likes})")


def cmd_friends(args: argparse.Namespace) -> None:
    """Get friends list."""
    status, body = _request("GET", f"/api/v1/friends/{args.user_id}")
    print(f"Friends of {args.user_id}:")
    print(f"  Following ({body.get('following_count', 0)}):")
    for u in body.get("following", []):
        print(f"    - {u}")
    print(f"  Followers ({body.get('followers_count', 0)}):")
    for u in body.get("followers", []):
        print(f"    - {u}")


def main() -> None:
    global BASE_URL
    parser = argparse.ArgumentParser(description="News Feed System CLI Client")
    parser.add_argument("--health", action="store_true", help="Run health check")
    parser.add_argument("--base-url", type=str, default=BASE_URL, help="API base URL")

    subparsers = parser.add_subparsers(dest="command")

    # create-user
    user_parser = subparsers.add_parser("create-user", help="Create a user")
    user_parser.add_argument("user_id", help="User ID")
    user_parser.add_argument("--name", default=None, help="Display name (defaults to user_id)")

    # follow
    follow_parser = subparsers.add_parser("follow", help="Follow a user")
    follow_parser.add_argument("follower", help="Follower user ID")
    follow_parser.add_argument("followee", help="Followee user ID")

    # unfollow
    unfollow_parser = subparsers.add_parser("unfollow", help="Unfollow a user")
    unfollow_parser.add_argument("follower", help="Follower user ID")
    unfollow_parser.add_argument("followee", help="Followee user ID")

    # post
    post_parser = subparsers.add_parser("post", help="Create a post")
    post_parser.add_argument("user_id", help="Author user ID")
    post_parser.add_argument("content", help="Post content")

    # feed
    feed_parser = subparsers.add_parser("feed", help="Get user's news feed")
    feed_parser.add_argument("user_id", help="User ID")

    # friends
    friends_parser = subparsers.add_parser("friends", help="Get friends list")
    friends_parser.add_argument("user_id", help="User ID")

    args = parser.parse_args()

    BASE_URL = args.base_url

    if args.health:
        cmd_health()
    elif args.command == "create-user":
        cmd_create_user(args)
    elif args.command == "follow":
        cmd_follow(args)
    elif args.command == "unfollow":
        cmd_unfollow(args)
    elif args.command == "post":
        cmd_post(args)
    elif args.command == "feed":
        cmd_feed(args)
    elif args.command == "friends":
        cmd_friends(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
