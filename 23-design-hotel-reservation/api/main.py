"""FastAPI application entry point for the hotel reservation system."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query

from config import settings
from models import (
    Hotel,
    ReservationRequest,
    ReservationStatus,
    RoomType,
)
from hotel.service import HotelService
from reservation.inventory import InventoryService
from reservation.service import ReservationService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global service instances (initialized during lifespan)
_redis: Optional[aioredis.Redis] = None
_hotel_service: Optional[HotelService] = None
_inventory_service: Optional[InventoryService] = None
_reservation_service: Optional[ReservationService] = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    """Startup / shutdown lifecycle."""
    global _redis, _hotel_service, _inventory_service, _reservation_service

    _redis = aioredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True,
    )
    _hotel_service = HotelService(_redis)
    _inventory_service = InventoryService(_redis)
    _reservation_service = ReservationService(_redis, _inventory_service)

    logger.info("Hotel reservation system started (port=%s)", settings.API_PORT)
    yield

    await _redis.aclose()
    logger.info("Hotel reservation system stopped")


app = FastAPI(
    title="Hotel Reservation System",
    version="1.0.0",
    lifespan=lifespan,
)


# -----------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------

@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


# -----------------------------------------------------------------------
# Hotel CRUD
# -----------------------------------------------------------------------

@app.post("/api/v1/hotels")
async def create_hotel(hotel: Hotel) -> dict:
    """Create a new hotel."""
    assert _hotel_service is not None
    created = await _hotel_service.create_hotel(hotel)
    return {"status": "created", "hotel": created.model_dump()}


@app.get("/api/v1/hotels")
async def list_hotels() -> dict:
    """List all hotels."""
    assert _hotel_service is not None
    hotels = await _hotel_service.list_hotels()
    return {"hotels": [h.model_dump() for h in hotels]}


@app.get("/api/v1/hotels/{hotel_id}")
async def get_hotel(hotel_id: str) -> dict:
    """Get a hotel by ID."""
    assert _hotel_service is not None
    hotel = await _hotel_service.get_hotel(hotel_id)
    if hotel is None:
        raise HTTPException(status_code=404, detail="Hotel not found")
    return {"hotel": hotel.model_dump()}


@app.delete("/api/v1/hotels/{hotel_id}")
async def delete_hotel(hotel_id: str) -> dict:
    """Delete a hotel."""
    assert _hotel_service is not None
    deleted = await _hotel_service.delete_hotel(hotel_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Hotel not found")
    return {"status": "deleted", "hotel_id": hotel_id}


# -----------------------------------------------------------------------
# Room Type CRUD
# -----------------------------------------------------------------------

@app.post("/api/v1/hotels/{hotel_id}/room-types")
async def create_room_type(hotel_id: str, room_type: RoomType) -> dict:
    """Create a room type for a hotel."""
    assert _hotel_service is not None
    room_type = room_type.model_copy(update={"hotel_id": hotel_id})
    created = await _hotel_service.create_room_type(room_type)
    return {"status": "created", "room_type": created.model_dump()}


@app.get("/api/v1/hotels/{hotel_id}/room-types")
async def list_room_types(hotel_id: str) -> dict:
    """List room types for a hotel."""
    assert _hotel_service is not None
    room_types = await _hotel_service.list_room_types(hotel_id)
    return {"room_types": [rt.model_dump() for rt in room_types]}


# -----------------------------------------------------------------------
# Inventory
# -----------------------------------------------------------------------

@app.post("/api/v1/inventory/init")
async def init_inventory(
    hotel_id: str = Query(...),
    room_type_id: str = Query(...),
    date: str = Query(..., description="YYYY-MM-DD"),
    total_inventory: int = Query(..., ge=0),
) -> dict:
    """Initialize inventory for a hotel/room_type/date."""
    assert _inventory_service is not None
    inv = await _inventory_service.init_inventory(
        hotel_id, room_type_id, date, total_inventory
    )
    return {"status": "initialized", "inventory": inv.model_dump()}


@app.get("/api/v1/inventory")
async def get_inventory(
    hotel_id: str = Query(...),
    room_type_id: str = Query(...),
    date: str = Query(..., description="YYYY-MM-DD"),
) -> dict:
    """Get inventory for a specific hotel/room_type/date."""
    assert _inventory_service is not None
    inv = await _inventory_service.get_inventory(hotel_id, room_type_id, date)
    if inv is None:
        raise HTTPException(status_code=404, detail="Inventory not found")
    return {"inventory": inv.model_dump()}


@app.get("/api/v1/inventory/range")
async def get_inventory_range(
    hotel_id: str = Query(...),
    room_type_id: str = Query(...),
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date: str = Query(..., description="End date YYYY-MM-DD"),
) -> dict:
    """Get inventory for a date range."""
    assert _inventory_service is not None
    from reservation.service import _date_range

    dates = _date_range(start_date, end_date)
    inventories = await _inventory_service.get_inventory_range(
        hotel_id, room_type_id, dates
    )
    return {"inventories": [inv.model_dump() for inv in inventories]}


# -----------------------------------------------------------------------
# Reservations
# -----------------------------------------------------------------------

@app.post("/api/v1/reservations")
async def create_reservation(req: ReservationRequest) -> dict:
    """Create a reservation (idempotent via reservation_id)."""
    assert _reservation_service is not None
    try:
        reservation = await _reservation_service.create_reservation(req)
        return {"status": "confirmed", "reservation": reservation.model_dump()}
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@app.get("/api/v1/reservations/{reservation_id}")
async def get_reservation(reservation_id: str) -> dict:
    """Get a reservation by ID."""
    assert _reservation_service is not None
    reservation = await _reservation_service.get_reservation(reservation_id)
    if reservation is None:
        raise HTTPException(status_code=404, detail="Reservation not found")
    return {"reservation": reservation.model_dump()}


@app.get("/api/v1/hotels/{hotel_id}/reservations")
async def list_reservations(hotel_id: str) -> dict:
    """List all reservations for a hotel."""
    assert _reservation_service is not None
    reservations = await _reservation_service.list_reservations(hotel_id)
    return {"reservations": [r.model_dump() for r in reservations]}


@app.post("/api/v1/reservations/{reservation_id}/cancel")
async def cancel_reservation(reservation_id: str) -> dict:
    """Cancel a reservation and release inventory."""
    assert _reservation_service is not None
    try:
        reservation = await _reservation_service.cancel_reservation(reservation_id)
        return {"status": "cancelled", "reservation": reservation.model_dump()}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/v1/reservations/{reservation_id}/status")
async def update_reservation_status(
    reservation_id: str,
    new_status: ReservationStatus = Query(...),
) -> dict:
    """Update reservation status (state machine enforced)."""
    assert _reservation_service is not None
    try:
        reservation = await _reservation_service.update_status(
            reservation_id, new_status
        )
        return {"status": "updated", "reservation": reservation.model_dump()}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
