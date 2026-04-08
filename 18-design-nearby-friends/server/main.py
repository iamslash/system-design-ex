"""FastAPI application entry point with WebSocket + REST endpoints for nearby friends."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from config import settings
from models import LocationUpdate, FriendshipCreate
from location.tracker import LocationTracker
from location.history import LocationHistory
from friends.nearby import NearbyFinder
from pubsub.channel import LocationPubSub

# ---------------------------------------------------------------------------
# Redis connection & application lifespan
# ---------------------------------------------------------------------------

redis_client: aioredis.Redis | None = None
tracker: LocationTracker | None = None
history: LocationHistory | None = None
finder: NearbyFinder | None = None
pubsub: LocationPubSub | None = None

# Active WebSocket connections: user_id -> WebSocket
_ws_connections: dict[str, WebSocket] = {}


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: create and close Redis connection."""
    global redis_client, tracker, history, finder, pubsub

    redis_client = aioredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True,
    )
    tracker = LocationTracker(redis_client)
    history = LocationHistory(redis_client)
    finder = NearbyFinder(redis_client, tracker)
    pubsub = LocationPubSub(redis_client)

    yield

    await pubsub.close_all()
    await redis_client.aclose()


app = FastAPI(title="Nearby Friends System", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------


@app.post("/api/v1/location")
async def update_location(body: LocationUpdate) -> dict:
    """Update a user's location via REST (alternative to WebSocket)."""
    assert tracker is not None and history is not None and pubsub is not None

    loc = await tracker.update(body.user_id, body.latitude, body.longitude)
    await history.append(body.user_id, body.latitude, body.longitude, loc["timestamp"])
    await pubsub.publish(body.user_id, body.latitude, body.longitude)
    return loc


@app.post("/api/v1/friends")
async def create_friendship(body: FriendshipCreate) -> dict:
    """Create a bidirectional friendship."""
    assert finder is not None
    await finder.add_friendship(body.user_a, body.user_b)
    return {"user_a": body.user_a, "user_b": body.user_b, "status": "friends"}


@app.delete("/api/v1/friends")
async def remove_friendship(body: FriendshipCreate) -> dict:
    """Remove a bidirectional friendship."""
    assert finder is not None
    await finder.remove_friendship(body.user_a, body.user_b)
    return {"user_a": body.user_a, "user_b": body.user_b, "status": "removed"}


@app.get("/api/v1/friends/{user_id}")
async def get_friends(user_id: str) -> dict:
    """List all friends for a user."""
    assert finder is not None
    friends = await finder.get_friends(user_id)
    return {"user_id": user_id, "friends": friends}


@app.get("/api/v1/nearby/{user_id}")
async def get_nearby(user_id: str, radius: float | None = None) -> dict:
    """Find nearby friends within a given radius (default 5 miles)."""
    assert finder is not None
    nearby = await finder.find_nearby(user_id, radius_miles=radius)
    return {"user_id": user_id, "nearby_friends": nearby}


@app.get("/api/v1/location/{user_id}")
async def get_location(user_id: str) -> dict:
    """Get a user's current cached location."""
    assert tracker is not None
    loc = await tracker.get(user_id)
    if loc is None:
        return JSONResponse(
            status_code=404,
            content={"detail": f"No active location for '{user_id}'."},
        )  # type: ignore[return-value]
    return loc


@app.get("/api/v1/location-history/{user_id}")
async def get_location_history(
    user_id: str, limit: int = 100, start: float = 0
) -> dict:
    """Get location history for a user."""
    assert history is not None
    entries = await history.get_range(user_id, start=start, limit=limit)
    return {"user_id": user_id, "history": entries}


# ---------------------------------------------------------------------------
# WebSocket endpoint
# ---------------------------------------------------------------------------


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str) -> None:
    """WebSocket endpoint for real-time location updates.

    Clients connect with their user_id and exchange JSON messages:
      - {"type": "location_update", "latitude": 40.7128, "longitude": -74.0060}
      - {"type": "get_nearby", "radius_miles": 5}  (optional radius override)

    The server pushes nearby friend location updates as:
      - {"type": "friend_location", "user_id": "bob", "latitude": ..., ...}
    """
    assert tracker is not None and history is not None
    assert finder is not None and pubsub is not None

    await websocket.accept()
    _ws_connections[user_id] = websocket

    # Subscribe to friends' location channels
    friends = await finder.get_friends(user_id)

    async def on_friend_update(data: dict) -> None:
        """Forward a friend's location update over WebSocket."""
        try:
            await websocket.send_text(json.dumps({
                "type": "friend_location",
                **data,
            }))
        except Exception:
            pass  # Connection may have closed

    async def my_location_getter() -> dict | None:
        """Return this user's current cached location."""
        return await tracker.get(user_id)

    await pubsub.subscribe(user_id, friends, on_friend_update, my_location_getter)

    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type")

            if msg_type == "location_update":
                lat = data.get("latitude", 0.0)
                lon = data.get("longitude", 0.0)
                loc = await tracker.update(user_id, lat, lon)
                await history.append(user_id, lat, lon, loc["timestamp"])
                await pubsub.publish(user_id, lat, lon)

                # Send back confirmation
                await websocket.send_text(json.dumps({
                    "type": "location_ack",
                    "latitude": lat,
                    "longitude": lon,
                    "timestamp": loc["timestamp"],
                }))

            elif msg_type == "get_nearby":
                radius = data.get("radius_miles")
                nearby = await finder.find_nearby(user_id, radius_miles=radius)
                await websocket.send_text(json.dumps({
                    "type": "nearby_result",
                    "nearby_friends": nearby,
                }))

    except WebSocketDisconnect:
        await pubsub.unsubscribe(user_id)
        _ws_connections.pop(user_id, None)
