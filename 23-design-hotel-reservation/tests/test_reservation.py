"""Tests for the hotel reservation system (20+ tests)."""

from __future__ import annotations

import asyncio
import sys
import os

import fakeredis.aioredis
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from hotel.service import HotelService
from reservation.inventory import InventoryService, _inventory_key
from reservation.service import ReservationService, _date_range
from models import (
    Hotel,
    Reservation,
    ReservationRequest,
    ReservationStatus,
    RoomType,
    RoomTypeInventory,
    VALID_TRANSITIONS,
)


# ---------------------------------------------------------------------------
# Hotel CRUD
# ---------------------------------------------------------------------------


class TestHotelService:
    """Tests for hotel and room type CRUD."""

    @pytest.mark.asyncio
    async def test_create_and_get_hotel(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Creating a hotel should persist and be retrievable."""
        svc = HotelService(redis_client)
        hotel = await svc.create_hotel(Hotel(name="Grand Hotel", address="123 Main St"))
        assert hotel.id is not None

        fetched = await svc.get_hotel(hotel.id)
        assert fetched is not None
        assert fetched.name == "Grand Hotel"

    @pytest.mark.asyncio
    async def test_list_hotels(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Listing hotels should return all created hotels."""
        svc = HotelService(redis_client)
        await svc.create_hotel(Hotel(name="Hotel A"))
        await svc.create_hotel(Hotel(name="Hotel B"))

        hotels = await svc.list_hotels()
        assert len(hotels) == 2
        names = {h.name for h in hotels}
        assert names == {"Hotel A", "Hotel B"}

    @pytest.mark.asyncio
    async def test_delete_hotel(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Deleting a hotel should remove it from storage."""
        svc = HotelService(redis_client)
        hotel = await svc.create_hotel(Hotel(name="Temp Hotel"))
        assert await svc.delete_hotel(hotel.id)
        assert await svc.get_hotel(hotel.id) is None

    @pytest.mark.asyncio
    async def test_create_room_type(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Creating a room type should associate it with the hotel."""
        svc = HotelService(redis_client)
        hotel = await svc.create_hotel(Hotel(name="Resort"))
        rt = await svc.create_room_type(
            RoomType(hotel_id=hotel.id, name="Deluxe", total_inventory=50, price_per_night=200.0)
        )
        assert rt.id is not None

        room_types = await svc.list_room_types(hotel.id)
        assert len(room_types) == 1
        assert room_types[0].name == "Deluxe"


# ---------------------------------------------------------------------------
# Inventory Management
# ---------------------------------------------------------------------------


class TestInventoryService:
    """Tests for room inventory tracking."""

    @pytest.mark.asyncio
    async def test_init_and_get_inventory(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Initializing inventory should create a retrievable record."""
        svc = InventoryService(redis_client)
        inv = await svc.init_inventory("h1", "rt1", "2025-07-01", 100)
        assert inv.total_inventory == 100
        assert inv.total_reserved == 0
        assert inv.version == 0

        fetched = await svc.get_inventory("h1", "rt1", "2025-07-01")
        assert fetched is not None
        assert fetched.total_inventory == 100

    @pytest.mark.asyncio
    async def test_reserve_rooms(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Reserving rooms should increment total_reserved."""
        svc = InventoryService(redis_client)
        await svc.init_inventory("h1", "rt1", "2025-07-01", 100)

        updated = await svc.reserve_rooms("h1", "rt1", "2025-07-01", 5)
        assert updated.total_reserved == 5
        assert updated.version == 1

    @pytest.mark.asyncio
    async def test_reserve_rooms_exceeds_capacity(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Reserving more than allowed should raise ValueError."""
        svc = InventoryService(redis_client)
        await svc.init_inventory("h1", "rt1", "2025-07-01", 10)

        with pytest.raises(ValueError, match="Insufficient inventory"):
            # 10 * 1.1 = 11, requesting 12 should fail
            await svc.reserve_rooms("h1", "rt1", "2025-07-01", 12)

    @pytest.mark.asyncio
    async def test_overbooking_within_limit(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Reserving up to overbooking ratio (110%) should succeed."""
        svc = InventoryService(redis_client)
        await svc.init_inventory("h1", "rt1", "2025-07-01", 10)

        # Reserve 10 rooms (100%)
        await svc.reserve_rooms("h1", "rt1", "2025-07-01", 10)
        # Reserve 1 more (110%) - within overbooking limit
        updated = await svc.reserve_rooms("h1", "rt1", "2025-07-01", 1)
        assert updated.total_reserved == 11

    @pytest.mark.asyncio
    async def test_overbooking_exceeds_limit(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Reserving beyond overbooking ratio should fail."""
        svc = InventoryService(redis_client)
        await svc.init_inventory("h1", "rt1", "2025-07-01", 10)

        # Reserve 11 (110% limit)
        await svc.reserve_rooms("h1", "rt1", "2025-07-01", 11)

        # One more should fail (would be 12 > 11)
        with pytest.raises(ValueError, match="Insufficient inventory"):
            await svc.reserve_rooms("h1", "rt1", "2025-07-01", 1)

    @pytest.mark.asyncio
    async def test_release_rooms(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Releasing rooms should decrement total_reserved."""
        svc = InventoryService(redis_client)
        await svc.init_inventory("h1", "rt1", "2025-07-01", 100)
        await svc.reserve_rooms("h1", "rt1", "2025-07-01", 10)

        released = await svc.release_rooms("h1", "rt1", "2025-07-01", 5)
        assert released.total_reserved == 5

    @pytest.mark.asyncio
    async def test_release_rooms_floor_at_zero(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Releasing more rooms than reserved should floor at zero."""
        svc = InventoryService(redis_client)
        await svc.init_inventory("h1", "rt1", "2025-07-01", 100)
        await svc.reserve_rooms("h1", "rt1", "2025-07-01", 3)

        released = await svc.release_rooms("h1", "rt1", "2025-07-01", 10)
        assert released.total_reserved == 0

    @pytest.mark.asyncio
    async def test_inventory_not_found(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Reserving on non-existent inventory should raise ValueError."""
        svc = InventoryService(redis_client)
        with pytest.raises(ValueError, match="No inventory"):
            await svc.reserve_rooms("h1", "rt1", "2025-07-01", 1)

    @pytest.mark.asyncio
    async def test_version_increments_on_reserve(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Each reservation should increment the version number."""
        svc = InventoryService(redis_client)
        await svc.init_inventory("h1", "rt1", "2025-07-01", 100)

        v1 = await svc.reserve_rooms("h1", "rt1", "2025-07-01", 1)
        assert v1.version == 1
        v2 = await svc.reserve_rooms("h1", "rt1", "2025-07-01", 1)
        assert v2.version == 2

    @pytest.mark.asyncio
    async def test_get_inventory_range(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Getting inventory range should return records for multiple dates."""
        svc = InventoryService(redis_client)
        for day in range(1, 4):
            await svc.init_inventory("h1", "rt1", f"2025-07-{day:02d}", 100)

        results = await svc.get_inventory_range(
            "h1", "rt1", ["2025-07-01", "2025-07-02", "2025-07-03"]
        )
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_custom_overbooking_ratio(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Custom overbooking ratio should be respected."""
        svc = InventoryService(redis_client)
        await svc.init_inventory("h1", "rt1", "2025-07-01", 10)

        # With 1.0 ratio (no overbooking), 10 rooms max
        await svc.reserve_rooms("h1", "rt1", "2025-07-01", 10, overbooking_ratio=1.0)
        with pytest.raises(ValueError, match="Insufficient inventory"):
            await svc.reserve_rooms("h1", "rt1", "2025-07-01", 1, overbooking_ratio=1.0)


# ---------------------------------------------------------------------------
# Reservation Service
# ---------------------------------------------------------------------------


class TestReservationService:
    """Tests for reservation creation, idempotency, and cancellation."""

    async def _setup(self, redis_client: fakeredis.aioredis.FakeRedis) -> tuple[ReservationService, InventoryService]:
        """Helper to set up services with inventory."""
        inv_svc = InventoryService(redis_client)
        res_svc = ReservationService(redis_client, inv_svc)
        # Initialize 3 nights of inventory
        for day in range(1, 5):
            await inv_svc.init_inventory("h1", "rt1", f"2025-07-{day:02d}", 100)
        return res_svc, inv_svc

    @pytest.mark.asyncio
    async def test_create_reservation(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Creating a reservation should persist it and update inventory."""
        res_svc, inv_svc = await self._setup(redis_client)
        req = ReservationRequest(
            reservation_id="res-001",
            hotel_id="h1",
            room_type_id="rt1",
            guest_name="Alice",
            check_in="2025-07-01",
            check_out="2025-07-03",
            num_rooms=2,
        )
        reservation = await res_svc.create_reservation(req)
        assert reservation.status == ReservationStatus.CONFIRMED
        assert reservation.reservation_id == "res-001"

        # Check inventory was decremented for 2 nights
        inv1 = await inv_svc.get_inventory("h1", "rt1", "2025-07-01")
        inv2 = await inv_svc.get_inventory("h1", "rt1", "2025-07-02")
        assert inv1.total_reserved == 2
        assert inv2.total_reserved == 2

    @pytest.mark.asyncio
    async def test_idempotent_reservation(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Submitting the same reservation_id twice should return the same result
        without double-booking inventory."""
        res_svc, inv_svc = await self._setup(redis_client)
        req = ReservationRequest(
            reservation_id="res-idem",
            hotel_id="h1",
            room_type_id="rt1",
            guest_name="Bob",
            check_in="2025-07-01",
            check_out="2025-07-02",
            num_rooms=3,
        )

        r1 = await res_svc.create_reservation(req)
        r2 = await res_svc.create_reservation(req)

        assert r1.reservation_id == r2.reservation_id
        assert r1.guest_name == r2.guest_name

        # Inventory should reflect only one booking
        inv = await inv_svc.get_inventory("h1", "rt1", "2025-07-01")
        assert inv.total_reserved == 3

    @pytest.mark.asyncio
    async def test_cancel_reservation(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Cancelling should set status to CANCELLED and release inventory."""
        res_svc, inv_svc = await self._setup(redis_client)
        req = ReservationRequest(
            reservation_id="res-cancel",
            hotel_id="h1",
            room_type_id="rt1",
            guest_name="Charlie",
            check_in="2025-07-01",
            check_out="2025-07-02",
            num_rooms=5,
        )
        await res_svc.create_reservation(req)
        cancelled = await res_svc.cancel_reservation("res-cancel")

        assert cancelled.status == ReservationStatus.CANCELLED

        # Inventory should be released
        inv = await inv_svc.get_inventory("h1", "rt1", "2025-07-01")
        assert inv.total_reserved == 0

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_reservation(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Cancelling a non-existent reservation should raise ValueError."""
        res_svc, _ = await self._setup(redis_client)
        with pytest.raises(ValueError, match="not found"):
            await res_svc.cancel_reservation("res-nonexistent")

    @pytest.mark.asyncio
    async def test_get_reservation(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Getting a reservation by ID should return the correct record."""
        res_svc, _ = await self._setup(redis_client)
        req = ReservationRequest(
            reservation_id="res-get",
            hotel_id="h1",
            room_type_id="rt1",
            guest_name="Diana",
            check_in="2025-07-01",
            check_out="2025-07-02",
        )
        await res_svc.create_reservation(req)
        fetched = await res_svc.get_reservation("res-get")
        assert fetched is not None
        assert fetched.guest_name == "Diana"

    @pytest.mark.asyncio
    async def test_list_reservations(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Listing reservations for a hotel should return all bookings."""
        res_svc, _ = await self._setup(redis_client)
        for i in range(3):
            req = ReservationRequest(
                reservation_id=f"res-list-{i}",
                hotel_id="h1",
                room_type_id="rt1",
                guest_name=f"Guest {i}",
                check_in="2025-07-01",
                check_out="2025-07-02",
            )
            await res_svc.create_reservation(req)

        reservations = await res_svc.list_reservations("h1")
        assert len(reservations) == 3

    @pytest.mark.asyncio
    async def test_insufficient_inventory_fails(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Booking more rooms than available should fail."""
        inv_svc = InventoryService(redis_client)
        res_svc = ReservationService(redis_client, inv_svc)
        await inv_svc.init_inventory("h1", "rt1", "2025-07-01", 5)

        req = ReservationRequest(
            reservation_id="res-fail",
            hotel_id="h1",
            room_type_id="rt1",
            guest_name="Eve",
            check_in="2025-07-01",
            check_out="2025-07-02",
            num_rooms=10,
        )
        with pytest.raises(ValueError, match="Insufficient inventory"):
            await res_svc.create_reservation(req)


# ---------------------------------------------------------------------------
# State Machine
# ---------------------------------------------------------------------------


class TestStateMachine:
    """Tests for reservation state transitions."""

    async def _make_reservation(self, redis_client: fakeredis.aioredis.FakeRedis) -> tuple[ReservationService, Reservation]:
        inv_svc = InventoryService(redis_client)
        res_svc = ReservationService(redis_client, inv_svc)
        await inv_svc.init_inventory("h1", "rt1", "2025-07-01", 100)
        req = ReservationRequest(
            reservation_id="res-sm",
            hotel_id="h1",
            room_type_id="rt1",
            guest_name="Frank",
            check_in="2025-07-01",
            check_out="2025-07-02",
        )
        r = await res_svc.create_reservation(req)
        return res_svc, r

    @pytest.mark.asyncio
    async def test_confirmed_to_checked_in(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """CONFIRMED -> CHECKED_IN should be valid."""
        res_svc, r = await self._make_reservation(redis_client)
        updated = await res_svc.update_status(r.reservation_id, ReservationStatus.CHECKED_IN)
        assert updated.status == ReservationStatus.CHECKED_IN

    @pytest.mark.asyncio
    async def test_checked_in_to_checked_out(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """CHECKED_IN -> CHECKED_OUT should be valid."""
        res_svc, r = await self._make_reservation(redis_client)
        await res_svc.update_status(r.reservation_id, ReservationStatus.CHECKED_IN)
        updated = await res_svc.update_status(r.reservation_id, ReservationStatus.CHECKED_OUT)
        assert updated.status == ReservationStatus.CHECKED_OUT

    @pytest.mark.asyncio
    async def test_invalid_transition_checked_out_to_confirmed(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """CHECKED_OUT -> CONFIRMED should be invalid."""
        res_svc, r = await self._make_reservation(redis_client)
        await res_svc.update_status(r.reservation_id, ReservationStatus.CHECKED_IN)
        await res_svc.update_status(r.reservation_id, ReservationStatus.CHECKED_OUT)

        with pytest.raises(ValueError, match="Invalid transition"):
            await res_svc.update_status(r.reservation_id, ReservationStatus.CONFIRMED)

    @pytest.mark.asyncio
    async def test_cancelled_is_terminal(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """CANCELLED is a terminal state with no valid transitions."""
        res_svc, r = await self._make_reservation(redis_client)
        await res_svc.cancel_reservation(r.reservation_id)

        with pytest.raises(ValueError, match="Invalid transition"):
            await res_svc.update_status(r.reservation_id, ReservationStatus.CONFIRMED)


# ---------------------------------------------------------------------------
# Concurrent Booking Prevention
# ---------------------------------------------------------------------------


class TestConcurrency:
    """Tests for optimistic locking under concurrent access."""

    @pytest.mark.asyncio
    async def test_concurrent_reservations_no_oversell(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Multiple concurrent bookings should not exceed the overbooking limit."""
        inv_svc = InventoryService(redis_client)
        await inv_svc.init_inventory("h1", "rt1", "2025-07-01", 10)

        # Try to book 12 rooms concurrently (1 room each), only 11 should succeed
        # (10 * 1.1 = 11 max allowed)
        results = []
        errors = []

        async def book_one(idx: int) -> None:
            try:
                await inv_svc.reserve_rooms("h1", "rt1", "2025-07-01", 1)
                results.append(idx)
            except ValueError:
                errors.append(idx)

        tasks = [book_one(i) for i in range(12)]
        await asyncio.gather(*tasks)

        assert len(results) == 11
        assert len(errors) == 1

        inv = await inv_svc.get_inventory("h1", "rt1", "2025-07-01")
        assert inv.total_reserved == 11

    @pytest.mark.asyncio
    async def test_concurrent_full_reservations(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Two full-service reservations competing for limited inventory."""
        inv_svc = InventoryService(redis_client)
        res_svc = ReservationService(redis_client, inv_svc)
        # Only 5 rooms, overbooking allows 5 (5 * 1.1 = 5 with int truncation)
        await inv_svc.init_inventory("h1", "rt1", "2025-07-01", 5)

        results = []
        errors = []

        async def book(rid: str, rooms: int) -> None:
            try:
                req = ReservationRequest(
                    reservation_id=rid,
                    hotel_id="h1",
                    room_type_id="rt1",
                    guest_name="Guest",
                    check_in="2025-07-01",
                    check_out="2025-07-02",
                    num_rooms=rooms,
                )
                r = await res_svc.create_reservation(req)
                results.append(r)
            except ValueError:
                errors.append(rid)

        # 3 rooms + 3 rooms = 6, but max is 5 (int(5*1.1)=5), so one should fail
        # Actually int(5*1.1) = 5, so max = 5. 3+3=6 > 5.
        await asyncio.gather(
            book("res-a", 3),
            book("res-b", 3),
        )

        assert len(results) == 1
        assert len(errors) == 1


# ---------------------------------------------------------------------------
# Date Range Helpers
# ---------------------------------------------------------------------------


class TestDateRange:
    """Tests for date range utilities."""

    def test_date_range_basic(self) -> None:
        """Date range should generate each night."""
        dates = _date_range("2025-07-01", "2025-07-04")
        assert dates == ["2025-07-01", "2025-07-02", "2025-07-03"]

    def test_date_range_single_night(self) -> None:
        """Single night stay should return one date."""
        dates = _date_range("2025-07-01", "2025-07-02")
        assert dates == ["2025-07-01"]

    def test_date_range_same_day(self) -> None:
        """Same day check-in and check-out should return empty."""
        dates = _date_range("2025-07-01", "2025-07-01")
        assert dates == []
