"""FastAPI application entry point with WebSocket chat + REST endpoints."""

from __future__ import annotations

import json
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from config import settings
from models import UserCreate, GroupCreate
from chat.connection_manager import manager
from chat.message_handler import MessageHandler, make_dm_channel
from presence.tracker import PresenceTracker
from storage.message_store import MessageStore

# ---------------------------------------------------------------------------
# Redis connection & application lifespan
# ---------------------------------------------------------------------------

redis_client: aioredis.Redis | None = None
message_store: MessageStore | None = None
message_handler: MessageHandler | None = None
presence_tracker: PresenceTracker | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: create and close Redis connection."""
    global redis_client, message_store, message_handler, presence_tracker

    redis_client = aioredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True,
    )
    message_store = MessageStore(redis_client)
    presence_tracker = PresenceTracker(redis_client)
    message_handler = MessageHandler(manager, message_store, redis_client)

    yield

    await redis_client.aclose()


app = FastAPI(title="Chat System Example", version="1.0.0", lifespan=lifespan)


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


@app.post("/api/v1/users")
async def register_user(body: UserCreate) -> dict[str, object]:
    """Register a new user."""
    assert redis_client is not None
    key = f"user:{body.user_id}"
    exists = await redis_client.exists(key)
    if exists:
        return JSONResponse(
            status_code=409,
            content={"detail": f"User '{body.user_id}' already exists."},
        )  # type: ignore[return-value]
    now = time.time()
    await redis_client.hset(key, mapping={"name": body.name, "created_at": str(now)})
    return {"user_id": body.user_id, "name": body.name, "created_at": now}


@app.post("/api/v1/groups")
async def create_group(body: GroupCreate) -> dict[str, object]:
    """Create a chat group with initial members."""
    assert redis_client is not None
    key = f"group:{body.group_id}"
    exists = await redis_client.exists(key)
    if exists:
        return JSONResponse(
            status_code=409,
            content={"detail": f"Group '{body.group_id}' already exists."},
        )  # type: ignore[return-value]
    await redis_client.hset(
        key,
        mapping={"name": body.name, "members": json.dumps(body.members)},
    )
    return {"group_id": body.group_id, "name": body.name, "members": body.members}


@app.get("/api/v1/messages/{channel_id:path}")
async def get_messages(channel_id: str, limit: int = 100, offset: int = 0) -> list[dict]:
    """Get message history for a channel."""
    assert message_store is not None
    messages = await message_store.get_messages(channel_id, limit=limit, offset=offset)
    return messages


@app.get("/api/v1/presence/{user_id}")
async def get_presence(user_id: str) -> dict[str, object]:
    """Get user online/offline status."""
    assert presence_tracker is not None
    return await presence_tracker.get_status(user_id)


# ---------------------------------------------------------------------------
# WebSocket endpoint
# ---------------------------------------------------------------------------


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str) -> None:
    """WebSocket endpoint for real-time chat.

    Clients connect with their user_id and exchange JSON messages:
      - {"type": "message", "to": "bob", "content": "Hi!"}
      - {"type": "group_message", "group_id": "team1", "content": "Hello!"}
      - {"type": "heartbeat"}
    """
    assert message_handler is not None
    assert presence_tracker is not None

    await manager.connect(user_id, websocket)
    await presence_tracker.set_online(user_id)

    # Notify other connected users about this user coming online
    online_notification = {
        "type": "presence",
        "user_id": user_id,
        "status": "online",
    }
    for uid in manager.get_connected_users():
        if uid != user_id:
            await manager.send_to_user(uid, online_notification)

    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type")

            if msg_type == "message":
                # 1:1 direct message
                to_user = data.get("to", "")
                content = data.get("content", "")
                if to_user and content:
                    await message_handler.handle_dm(user_id, to_user, content)

            elif msg_type == "group_message":
                # Group message
                group_id = data.get("group_id", "")
                content = data.get("content", "")
                if group_id and content:
                    result = await message_handler.handle_group_message(
                        user_id, group_id, content
                    )
                    if result is None:
                        await manager.send_to_user(
                            user_id,
                            {"type": "error", "detail": f"Group '{group_id}' not found."},
                        )

            elif msg_type == "heartbeat":
                # Presence heartbeat keepalive
                await presence_tracker.heartbeat(user_id)

    except WebSocketDisconnect:
        manager.disconnect(user_id, websocket)
        await presence_tracker.set_offline(user_id)

        # Notify remaining users about this user going offline
        offline_notification = {
            "type": "presence",
            "user_id": user_id,
            "status": "offline",
        }
        for uid in manager.get_connected_users():
            await manager.send_to_user(uid, offline_notification)
