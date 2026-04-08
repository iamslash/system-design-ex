"""Hotel and room type CRUD operations backed by Redis."""

from __future__ import annotations

import json
import uuid

import redis.asyncio as aioredis

from models import Hotel, RoomType


def _hotel_key(hotel_id: str) -> str:
    return f"hotel:{hotel_id}"


def _room_type_key(room_type_id: str) -> str:
    return f"room_type:{room_type_id}"


def _hotel_room_types_key(hotel_id: str) -> str:
    return f"hotel_room_types:{hotel_id}"


class HotelService:
    """CRUD operations for hotels and room types."""

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    # ------------------------------------------------------------------
    # Hotel CRUD
    # ------------------------------------------------------------------

    async def create_hotel(self, hotel: Hotel) -> Hotel:
        """Create a new hotel and return it with a generated ID."""
        hotel = hotel.model_copy(update={"id": hotel.id or str(uuid.uuid4())[:8]})
        await self._redis.set(_hotel_key(hotel.id), hotel.model_dump_json())
        # Track all hotel IDs
        await self._redis.sadd("hotels", hotel.id)
        return hotel

    async def get_hotel(self, hotel_id: str) -> Hotel | None:
        """Get a hotel by ID."""
        raw = await self._redis.get(_hotel_key(hotel_id))
        if raw is None:
            return None
        return Hotel(**json.loads(raw))

    async def list_hotels(self) -> list[Hotel]:
        """List all hotels."""
        ids = await self._redis.smembers("hotels")
        hotels: list[Hotel] = []
        for hid in ids:
            h = await self.get_hotel(hid)
            if h is not None:
                hotels.append(h)
        return hotels

    async def delete_hotel(self, hotel_id: str) -> bool:
        """Delete a hotel and its room types."""
        deleted = await self._redis.delete(_hotel_key(hotel_id))
        if deleted:
            await self._redis.srem("hotels", hotel_id)
            # Clean up room types
            rt_ids = await self._redis.smembers(_hotel_room_types_key(hotel_id))
            for rt_id in rt_ids:
                await self._redis.delete(_room_type_key(rt_id))
            await self._redis.delete(_hotel_room_types_key(hotel_id))
        return bool(deleted)

    # ------------------------------------------------------------------
    # Room Type CRUD
    # ------------------------------------------------------------------

    async def create_room_type(self, room_type: RoomType) -> RoomType:
        """Create a room type for a hotel."""
        room_type = room_type.model_copy(
            update={"id": room_type.id or str(uuid.uuid4())[:8]}
        )
        await self._redis.set(
            _room_type_key(room_type.id), room_type.model_dump_json()
        )
        await self._redis.sadd(
            _hotel_room_types_key(room_type.hotel_id), room_type.id
        )
        return room_type

    async def get_room_type(self, room_type_id: str) -> RoomType | None:
        """Get a room type by ID."""
        raw = await self._redis.get(_room_type_key(room_type_id))
        if raw is None:
            return None
        return RoomType(**json.loads(raw))

    async def list_room_types(self, hotel_id: str) -> list[RoomType]:
        """List all room types for a hotel."""
        ids = await self._redis.smembers(_hotel_room_types_key(hotel_id))
        room_types: list[RoomType] = []
        for rt_id in ids:
            rt = await self.get_room_type(rt_id)
            if rt is not None:
                room_types.append(rt)
        return room_types

    async def delete_room_type(self, room_type_id: str) -> bool:
        """Delete a room type."""
        rt = await self.get_room_type(room_type_id)
        if rt is None:
            return False
        await self._redis.delete(_room_type_key(room_type_id))
        await self._redis.srem(_hotel_room_types_key(rt.hotel_id), room_type_id)
        return True
