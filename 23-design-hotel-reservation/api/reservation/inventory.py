"""Room inventory management with optimistic locking.

Key design:
- Each (hotel_id, room_type_id, date) has an inventory record stored as a Redis hash.
- Optimistic locking: read current version, attempt update only if version matches.
- Overbooking: allow reservations up to `total_inventory * overbooking_ratio`.
"""

from __future__ import annotations

import json

import redis.asyncio as aioredis

from config import settings
from models import RoomTypeInventory


def _inventory_key(hotel_id: str, room_type_id: str, date: str) -> str:
    """Redis key for a single inventory record."""
    return f"inventory:{hotel_id}:{room_type_id}:{date}"


class InventoryService:
    """Manages room type inventory with concurrency-safe updates."""

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def get_inventory(
        self, hotel_id: str, room_type_id: str, date: str
    ) -> RoomTypeInventory | None:
        """Return the inventory record for a given hotel/room_type/date."""
        key = _inventory_key(hotel_id, room_type_id, date)
        raw = await self._redis.get(key)
        if raw is None:
            return None
        return RoomTypeInventory(**json.loads(raw))

    async def get_inventory_range(
        self, hotel_id: str, room_type_id: str, dates: list[str]
    ) -> list[RoomTypeInventory]:
        """Return inventory records for multiple dates."""
        results: list[RoomTypeInventory] = []
        for date in dates:
            inv = await self.get_inventory(hotel_id, room_type_id, date)
            if inv is not None:
                results.append(inv)
        return results

    # ------------------------------------------------------------------
    # Initialize
    # ------------------------------------------------------------------

    async def init_inventory(
        self, hotel_id: str, room_type_id: str, date: str, total_inventory: int
    ) -> RoomTypeInventory:
        """Create or reset an inventory record for a date."""
        inv = RoomTypeInventory(
            hotel_id=hotel_id,
            room_type_id=room_type_id,
            date=date,
            total_inventory=total_inventory,
            total_reserved=0,
            version=0,
        )
        key = _inventory_key(hotel_id, room_type_id, date)
        await self._redis.set(key, inv.model_dump_json())
        return inv

    # ------------------------------------------------------------------
    # Reserve (optimistic locking)
    # ------------------------------------------------------------------

    async def reserve_rooms(
        self,
        hotel_id: str,
        room_type_id: str,
        date: str,
        num_rooms: int,
        overbooking_ratio: float | None = None,
    ) -> RoomTypeInventory:
        """Attempt to reserve rooms using optimistic locking.

        Raises ValueError if inventory is not found, capacity exceeded,
        or version conflict (concurrent modification).
        """
        if overbooking_ratio is None:
            overbooking_ratio = settings.OVERBOOKING_RATIO

        key = _inventory_key(hotel_id, room_type_id, date)
        raw = await self._redis.get(key)
        if raw is None:
            raise ValueError(f"No inventory for {hotel_id}/{room_type_id}/{date}")

        inv = RoomTypeInventory(**json.loads(raw))
        max_allowed = int(inv.total_inventory * overbooking_ratio)

        if inv.total_reserved + num_rooms > max_allowed:
            raise ValueError(
                f"Insufficient inventory on {date}: "
                f"reserved={inv.total_reserved}, requested={num_rooms}, "
                f"max_allowed={max_allowed}"
            )

        # Optimistic lock: only update if version has not changed
        new_inv = inv.model_copy(
            update={
                "total_reserved": inv.total_reserved + num_rooms,
                "version": inv.version + 1,
            }
        )

        # Use WATCH + MULTI/EXEC for atomic check-and-set
        async with self._redis.pipeline(transaction=True) as pipe:
            await pipe.watch(key)
            current_raw = await pipe.get(key)
            if current_raw is None:
                raise ValueError(f"Inventory deleted during transaction for {date}")

            current = RoomTypeInventory(**json.loads(current_raw))
            if current.version != inv.version:
                raise ValueError(
                    f"Version conflict on {date}: "
                    f"expected={inv.version}, actual={current.version}"
                )

            pipe.multi()
            pipe.set(key, new_inv.model_dump_json())
            await pipe.execute()

        return new_inv

    # ------------------------------------------------------------------
    # Release (for cancellations)
    # ------------------------------------------------------------------

    async def release_rooms(
        self, hotel_id: str, room_type_id: str, date: str, num_rooms: int
    ) -> RoomTypeInventory:
        """Release previously reserved rooms (e.g. on cancellation)."""
        key = _inventory_key(hotel_id, room_type_id, date)
        raw = await self._redis.get(key)
        if raw is None:
            raise ValueError(f"No inventory for {hotel_id}/{room_type_id}/{date}")

        inv = RoomTypeInventory(**json.loads(raw))
        new_reserved = max(0, inv.total_reserved - num_rooms)

        new_inv = inv.model_copy(
            update={
                "total_reserved": new_reserved,
                "version": inv.version + 1,
            }
        )

        async with self._redis.pipeline(transaction=True) as pipe:
            await pipe.watch(key)
            current_raw = await pipe.get(key)
            if current_raw is None:
                raise ValueError(f"Inventory deleted during release for {date}")

            current = RoomTypeInventory(**json.loads(current_raw))
            if current.version != inv.version:
                raise ValueError(
                    f"Version conflict on {date}: "
                    f"expected={inv.version}, actual={current.version}"
                )

            pipe.multi()
            pipe.set(key, new_inv.model_dump_json())
            await pipe.execute()

        return new_inv
