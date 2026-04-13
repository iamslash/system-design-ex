#!/usr/bin/env python3
"""CLI client for the hotel reservation system."""

from __future__ import annotations

import argparse
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BASE_URL = "http://localhost:8023"


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


def _delete(path: str) -> tuple[int, dict]:
    url = f"{BASE_URL}{path}"
    req = Request(url, method="DELETE")
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


def cmd_create_hotel(name: str, address: str) -> None:
    data = {"name": name, "address": address}
    status, body = _post("/api/v1/hotels", data)
    print(f"Create Hotel: {status} -> {json.dumps(body, indent=2)}")


def cmd_list_hotels() -> None:
    status, body = _get("/api/v1/hotels")
    hotels = body.get("hotels", [])
    if not hotels:
        print("No hotels found.")
        return
    for h in hotels:
        print(f"  [{h['id']}] {h['name']} - {h.get('address', '')}")


def cmd_init_inventory(hotel_id: str, room_type_id: str, date: str, total: int) -> None:
    path = f"/api/v1/inventory/init?hotel_id={hotel_id}&room_type_id={room_type_id}&date={date}&total_inventory={total}"
    status, body = _post(path, {})
    print(f"Init Inventory: {status} -> {json.dumps(body, indent=2)}")


def cmd_get_inventory(hotel_id: str, room_type_id: str, date: str) -> None:
    path = f"/api/v1/inventory?hotel_id={hotel_id}&room_type_id={room_type_id}&date={date}"
    status, body = _get(path)
    print(f"Inventory: {status} -> {json.dumps(body, indent=2)}")


def cmd_reserve(reservation_id: str, hotel_id: str, room_type_id: str,
                guest: str, check_in: str, check_out: str, num_rooms: int) -> None:
    data = {
        "reservation_id": reservation_id,
        "hotel_id": hotel_id,
        "room_type_id": room_type_id,
        "guest_name": guest,
        "check_in": check_in,
        "check_out": check_out,
        "num_rooms": num_rooms,
    }
    status, body = _post("/api/v1/reservations", data)
    print(f"Reservation: {status} -> {json.dumps(body, indent=2)}")


def cmd_cancel(reservation_id: str) -> None:
    status, body = _post(f"/api/v1/reservations/{reservation_id}/cancel", {})
    print(f"Cancel: {status} -> {json.dumps(body, indent=2)}")


def cmd_get_reservation(reservation_id: str) -> None:
    status, body = _get(f"/api/v1/reservations/{reservation_id}")
    print(f"Reservation: {status} -> {json.dumps(body, indent=2)}")


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main() -> None:
    global BASE_URL
    parser = argparse.ArgumentParser(description="Hotel Reservation CLI")
    parser.add_argument("command", nargs="?",
                        choices=["hotel-create", "hotel-list", "inventory-init",
                                 "inventory-get", "reserve", "cancel", "get"],
                        help="Command to execute")
    parser.add_argument("--health", action="store_true", help="Run health check")
    parser.add_argument("--hotel-id", type=str, help="Hotel ID")
    parser.add_argument("--room-type-id", type=str, help="Room type ID")
    parser.add_argument("--name", type=str, default="", help="Hotel name")
    parser.add_argument("--address", type=str, default="", help="Hotel address")
    parser.add_argument("--date", type=str, help="Date (YYYY-MM-DD)")
    parser.add_argument("--total", type=int, help="Total inventory")
    parser.add_argument("--reservation-id", type=str, help="Reservation ID")
    parser.add_argument("--guest", type=str, default="Guest", help="Guest name")
    parser.add_argument("--check-in", type=str, help="Check-in date")
    parser.add_argument("--check-out", type=str, help="Check-out date")
    parser.add_argument("--num-rooms", type=int, default=1, help="Number of rooms")
    parser.add_argument("--base-url", type=str, default=BASE_URL, help="API base URL")

    args = parser.parse_args()

    BASE_URL = args.base_url

    if args.health:
        cmd_health()
    elif args.command == "hotel-create":
        cmd_create_hotel(args.name, args.address)
    elif args.command == "hotel-list":
        cmd_list_hotels()
    elif args.command == "inventory-init":
        if not all([args.hotel_id, args.room_type_id, args.date, args.total]):
            parser.error("inventory-init requires --hotel-id, --room-type-id, --date, --total")
        cmd_init_inventory(args.hotel_id, args.room_type_id, args.date, args.total)
    elif args.command == "inventory-get":
        if not all([args.hotel_id, args.room_type_id, args.date]):
            parser.error("inventory-get requires --hotel-id, --room-type-id, --date")
        cmd_get_inventory(args.hotel_id, args.room_type_id, args.date)
    elif args.command == "reserve":
        if not all([args.reservation_id, args.hotel_id, args.room_type_id,
                     args.check_in, args.check_out]):
            parser.error("reserve requires --reservation-id, --hotel-id, --room-type-id, --check-in, --check-out")
        cmd_reserve(args.reservation_id, args.hotel_id, args.room_type_id,
                    args.guest, args.check_in, args.check_out, args.num_rooms)
    elif args.command == "cancel":
        if not args.reservation_id:
            parser.error("cancel requires --reservation-id")
        cmd_cancel(args.reservation_id)
    elif args.command == "get":
        if not args.reservation_id:
            parser.error("get requires --reservation-id")
        cmd_get_reservation(args.reservation_id)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
