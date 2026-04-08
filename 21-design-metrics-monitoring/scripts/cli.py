#!/usr/bin/env python3
"""CLI client for the metrics monitoring system."""

from __future__ import annotations

import argparse
import json
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BASE_URL = "http://localhost:8021"


# -----------------------------------------------------------------------
# HTTP helpers
# -----------------------------------------------------------------------

def _get(path: str) -> tuple[int, dict]:
    url = f"{BASE_URL}{path}"
    try:
        with urlopen(Request(url), timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except HTTPError as e:
        return e.code, json.loads(e.read().decode())
    except URLError as e:
        print(f"Connection error: {e.reason}", file=sys.stderr)
        sys.exit(1)


def _post(path: str, data: dict) -> tuple[int, dict]:
    url = f"{BASE_URL}{path}"
    payload = json.dumps(data).encode()
    req = Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except HTTPError as e:
        return e.code, json.loads(e.read().decode())
    except URLError as e:
        print(f"Connection error: {e.reason}", file=sys.stderr)
        sys.exit(1)


# -----------------------------------------------------------------------
# Commands
# -----------------------------------------------------------------------

def cmd_health() -> None:
    status, body = _get("/health")
    print(f"Health: {status} -> {body}")


def cmd_push(metric: str, labels_str: str, value: float) -> None:
    labels = _parse_labels(labels_str)
    data = {"name": metric, "labels": labels, "value": value, "timestamp": time.time()}
    status, body = _post("/api/v1/metrics", data)
    print(f"Push: {status} -> {body}")


def cmd_query(metric: str, labels_str: str, start: str, agg: str | None) -> None:
    labels = _parse_labels(labels_str)
    start_ts = _parse_duration(start)
    params = f"name={metric}&start={start_ts}"
    if labels:
        labels_param = ",".join(f"{k}={v}" for k, v in labels.items())
        params += f"&labels={labels_param}"
    if agg:
        params += f"&aggregation={agg}"
    status, body = _get(f"/api/v1/query?{params}")
    if status == 200:
        if body.get("aggregated_value") is not None:
            print(f"Aggregation ({agg}): {body['aggregated_value']}")
        points = body.get("data_points", [])
        print(f"Data points: {len(points)}")
        for p in points[:20]:
            print(f"  {p}")
        if len(points) > 20:
            print(f"  ... and {len(points) - 20} more")
    else:
        print(f"Query error: {status} -> {body}")


def cmd_alerts() -> None:
    status, body = _get("/api/v1/alerts")
    alerts = body.get("alerts", [])
    if not alerts:
        print("No active alerts.")
        return
    for a in alerts:
        print(f"[{a['severity']}] {a['rule_name']}: {a['metric_name']} = {a['value']} "
              f"(threshold {a['threshold']}) - {a['status']}")


def cmd_rules() -> None:
    status, body = _get("/api/v1/rules")
    rules = body.get("rules", [])
    if not rules:
        print("No alert rules configured.")
        return
    for r in rules:
        print(f"[{r['id']}] {r['name']}: {r['metric_name']} {r['operator']} {r['threshold']} "
              f"(severity={r['severity']}, duration={r['duration']}s)")


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _parse_labels(raw: str) -> dict[str, str]:
    if not raw:
        return {}
    result: dict[str, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if "=" in pair:
            k, v = pair.split("=", 1)
            result[k.strip()] = v.strip()
    return result


def _parse_duration(s: str) -> float:
    """Parse a duration like '1h', '30m', '7d' into a start timestamp."""
    now = time.time()
    s = s.strip()
    if s.replace(".", "", 1).isdigit():
        return float(s)
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    unit = s[-1]
    if unit in multipliers:
        return now - float(s[:-1]) * multipliers[unit]
    return now - 3600  # default 1h


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Metrics Monitoring CLI")
    parser.add_argument("command", nargs="?", choices=["push", "query", "alerts", "rules"],
                        help="Command to execute")
    parser.add_argument("--health", action="store_true", help="Run health check")
    parser.add_argument("--metric", type=str, help="Metric name")
    parser.add_argument("--labels", type=str, default="", help="Labels (key=value,key=value)")
    parser.add_argument("--value", type=float, help="Metric value")
    parser.add_argument("--start", type=str, default="1h", help="Query start (e.g. 1h, 30m, 7d)")
    parser.add_argument("--agg", type=str, choices=["avg", "max", "min", "sum", "count"],
                        help="Aggregation function")
    parser.add_argument("--base-url", type=str, default=BASE_URL, help="API base URL")

    args = parser.parse_args()

    global BASE_URL
    BASE_URL = args.base_url

    if args.health:
        cmd_health()
    elif args.command == "push":
        if not args.metric or args.value is None:
            parser.error("push requires --metric and --value")
        cmd_push(args.metric, args.labels, args.value)
    elif args.command == "query":
        if not args.metric:
            parser.error("query requires --metric")
        cmd_query(args.metric, args.labels, args.start, args.agg)
    elif args.command == "alerts":
        cmd_alerts()
    elif args.command == "rules":
        cmd_rules()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
