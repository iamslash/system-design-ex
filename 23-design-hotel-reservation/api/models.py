"""Pydantic models for the hotel reservation system."""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Hotel & Room
# ---------------------------------------------------------------------------


class RoomType(BaseModel):
    """A room type offered by a hotel."""

    id: Optional[str] = None
    hotel_id: str = Field(..., description="Parent hotel ID")
    name: str = Field(..., description="Room type name, e.g. Standard, Deluxe")
    total_inventory: int = Field(..., ge=0, description="Total number of rooms of this type")
    price_per_night: float = Field(..., gt=0, description="Price per night in USD")


class Hotel(BaseModel):
    """A hotel entity."""

    id: Optional[str] = None
    name: str = Field(..., description="Hotel name")
    address: str = Field(default="", description="Hotel address")


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------


class RoomTypeInventory(BaseModel):
    """Tracks inventory for a specific hotel/room_type/date combination.

    This is the core data structure for availability management.
    The version field enables optimistic locking for concurrent updates.
    """

    hotel_id: str
    room_type_id: str
    date: str = Field(..., description="Date string YYYY-MM-DD")
    total_inventory: int = Field(..., ge=0)
    total_reserved: int = Field(default=0, ge=0)
    version: int = Field(default=0, description="Optimistic lock version")


# ---------------------------------------------------------------------------
# Reservation
# ---------------------------------------------------------------------------


class ReservationStatus(str, Enum):
    """Reservation state machine: PENDING -> CONFIRMED -> CHECKED_IN -> CHECKED_OUT
                                                      \\-> CANCELLED"""

    PENDING = "pending"
    CONFIRMED = "confirmed"
    CHECKED_IN = "checked_in"
    CHECKED_OUT = "checked_out"
    CANCELLED = "cancelled"


# Valid state transitions
VALID_TRANSITIONS: dict[ReservationStatus, set[ReservationStatus]] = {
    ReservationStatus.PENDING: {ReservationStatus.CONFIRMED, ReservationStatus.CANCELLED},
    ReservationStatus.CONFIRMED: {ReservationStatus.CHECKED_IN, ReservationStatus.CANCELLED},
    ReservationStatus.CHECKED_IN: {ReservationStatus.CHECKED_OUT},
    ReservationStatus.CHECKED_OUT: set(),
    ReservationStatus.CANCELLED: set(),
}


class ReservationRequest(BaseModel):
    """Request to create a reservation."""

    reservation_id: str = Field(..., description="Client-generated idempotency key")
    hotel_id: str
    room_type_id: str
    guest_name: str
    check_in: str = Field(..., description="Check-in date YYYY-MM-DD")
    check_out: str = Field(..., description="Check-out date YYYY-MM-DD")
    num_rooms: int = Field(default=1, ge=1)


class Reservation(BaseModel):
    """A persisted reservation."""

    reservation_id: str
    hotel_id: str
    room_type_id: str
    guest_name: str
    check_in: str
    check_out: str
    num_rooms: int
    status: ReservationStatus = ReservationStatus.CONFIRMED
