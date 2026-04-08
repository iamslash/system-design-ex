"""Reservation logic with idempotent booking and concurrency control.

Key design:
- Idempotent: client provides a reservation_id; if it already exists, the
  existing reservation is returned without re-reserving inventory.
- Multi-date reservation: inventory is reserved for each night in
  [check_in, check_out).
- Cancellation releases inventory for all dates.
- State machine enforces valid transitions.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import redis.asyncio as aioredis

from models import (
    Reservation,
    ReservationRequest,
    ReservationStatus,
    VALID_TRANSITIONS,
)
from reservation.inventory import InventoryService


def _reservation_key(reservation_id: str) -> str:
    return f"reservation:{reservation_id}"


def _hotel_reservations_key(hotel_id: str) -> str:
    return f"hotel_reservations:{hotel_id}"


def _date_range(check_in: str, check_out: str) -> list[str]:
    """Generate list of date strings for each night of the stay."""
    start = datetime.strptime(check_in, "%Y-%m-%d")
    end = datetime.strptime(check_out, "%Y-%m-%d")
    dates: list[str] = []
    current = start
    while current < end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


class ReservationService:
    """Handles reservation lifecycle: create, cancel, status transitions."""

    def __init__(self, redis: aioredis.Redis, inventory: InventoryService) -> None:
        self._redis = redis
        self._inventory = inventory

    # ------------------------------------------------------------------
    # Create (idempotent)
    # ------------------------------------------------------------------

    async def create_reservation(self, req: ReservationRequest) -> Reservation:
        """Create a reservation, or return existing one if idempotent key matches.

        Steps:
        1. Check if reservation_id already exists (idempotency).
        2. Compute date range [check_in, check_out).
        3. Reserve inventory for each date (rollback on failure).
        4. Persist the reservation.
        """
        key = _reservation_key(req.reservation_id)
        existing = await self._redis.get(key)
        if existing is not None:
            # Idempotent: return the already-created reservation
            return Reservation(**json.loads(existing))

        dates = _date_range(req.check_in, req.check_out)
        if not dates:
            raise ValueError("check_out must be after check_in")

        # Reserve inventory for each night, rollback on failure
        reserved_dates: list[str] = []
        try:
            for date in dates:
                await self._inventory.reserve_rooms(
                    hotel_id=req.hotel_id,
                    room_type_id=req.room_type_id,
                    date=date,
                    num_rooms=req.num_rooms,
                )
                reserved_dates.append(date)
        except ValueError:
            # Rollback already-reserved dates
            for rdate in reserved_dates:
                try:
                    await self._inventory.release_rooms(
                        hotel_id=req.hotel_id,
                        room_type_id=req.room_type_id,
                        date=rdate,
                        num_rooms=req.num_rooms,
                    )
                except ValueError:
                    pass  # best-effort rollback
            raise

        reservation = Reservation(
            reservation_id=req.reservation_id,
            hotel_id=req.hotel_id,
            room_type_id=req.room_type_id,
            guest_name=req.guest_name,
            check_in=req.check_in,
            check_out=req.check_out,
            num_rooms=req.num_rooms,
            status=ReservationStatus.CONFIRMED,
        )

        await self._redis.set(key, reservation.model_dump_json())
        # Add to hotel's reservation list for querying
        await self._redis.sadd(
            _hotel_reservations_key(req.hotel_id), req.reservation_id
        )
        return reservation

    # ------------------------------------------------------------------
    # Get
    # ------------------------------------------------------------------

    async def get_reservation(self, reservation_id: str) -> Reservation | None:
        """Look up a reservation by ID."""
        raw = await self._redis.get(_reservation_key(reservation_id))
        if raw is None:
            return None
        return Reservation(**json.loads(raw))

    async def list_reservations(self, hotel_id: str) -> list[Reservation]:
        """List all reservations for a hotel."""
        ids = await self._redis.smembers(_hotel_reservations_key(hotel_id))
        reservations: list[Reservation] = []
        for rid in ids:
            r = await self.get_reservation(rid)
            if r is not None:
                reservations.append(r)
        return reservations

    # ------------------------------------------------------------------
    # Cancel
    # ------------------------------------------------------------------

    async def cancel_reservation(self, reservation_id: str) -> Reservation:
        """Cancel a reservation and release inventory."""
        reservation = await self.get_reservation(reservation_id)
        if reservation is None:
            raise ValueError(f"Reservation {reservation_id} not found")

        self._validate_transition(reservation.status, ReservationStatus.CANCELLED)

        dates = _date_range(reservation.check_in, reservation.check_out)
        for date in dates:
            try:
                await self._inventory.release_rooms(
                    hotel_id=reservation.hotel_id,
                    room_type_id=reservation.room_type_id,
                    date=date,
                    num_rooms=reservation.num_rooms,
                )
            except ValueError:
                pass  # best-effort release

        reservation = reservation.model_copy(
            update={"status": ReservationStatus.CANCELLED}
        )
        await self._redis.set(
            _reservation_key(reservation_id), reservation.model_dump_json()
        )
        return reservation

    # ------------------------------------------------------------------
    # Status transition
    # ------------------------------------------------------------------

    async def update_status(
        self, reservation_id: str, new_status: ReservationStatus
    ) -> Reservation:
        """Transition a reservation to a new status."""
        reservation = await self.get_reservation(reservation_id)
        if reservation is None:
            raise ValueError(f"Reservation {reservation_id} not found")

        self._validate_transition(reservation.status, new_status)

        reservation = reservation.model_copy(update={"status": new_status})
        await self._redis.set(
            _reservation_key(reservation_id), reservation.model_dump_json()
        )
        return reservation

    @staticmethod
    def _validate_transition(
        current: ReservationStatus, target: ReservationStatus
    ) -> None:
        """Enforce valid state machine transitions."""
        allowed = VALID_TRANSITIONS.get(current, set())
        if target not in allowed:
            raise ValueError(
                f"Invalid transition: {current.value} -> {target.value}"
            )
