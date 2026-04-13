"""WebSocket connection manager.

Tracks active WebSocket connections per user and provides methods for
sending messages to specific users or broadcasting to a group.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import WebSocket


class ConnectionManager:
    """Manages active WebSocket connections on a per-user basis.

    Because a single user may be connected from multiple devices,
    multiple connections are supported via a user_id -> list[WebSocket] mapping.
    """

    def __init__(self) -> None:
        # user_id -> list of active WebSocket connections
        self._connections: dict[str, list[WebSocket]] = {}

    async def connect(self, user_id: str, websocket: WebSocket) -> None:
        """Accept a new WebSocket connection and add it to the user's connection list."""
        await websocket.accept()
        if user_id not in self._connections:
            self._connections[user_id] = []
        self._connections[user_id].append(websocket)

    def disconnect(self, user_id: str, websocket: WebSocket) -> None:
        """Remove a WebSocket connection for the given user.

        If this was the user's last connection, the user entry is deleted entirely.
        """
        if user_id in self._connections:
            self._connections[user_id] = [
                ws for ws in self._connections[user_id] if ws is not websocket
            ]
            if not self._connections[user_id]:
                del self._connections[user_id]

    async def send_to_user(self, user_id: str, message: dict[str, Any]) -> None:
        """Send a message to all connections belonging to the specified user.

        If the user is offline the message is not sent (persistence to Redis is
        handled separately).
        """
        if user_id in self._connections:
            payload = json.dumps(message)
            dead: list[WebSocket] = []
            for ws in self._connections[user_id]:
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead.append(ws)
            # Clean up dead connections
            for ws in dead:
                self.disconnect(user_id, ws)

    async def broadcast(self, user_ids: list[str], message: dict[str, Any]) -> None:
        """Broadcast a message to multiple users."""
        for user_id in user_ids:
            await self.send_to_user(user_id, message)

    def is_connected(self, user_id: str) -> bool:
        """Return True if the user currently has at least one active WebSocket connection."""
        return user_id in self._connections and len(self._connections[user_id]) > 0

    def get_connected_users(self) -> list[str]:
        """Return a list of all currently connected user IDs."""
        return list(self._connections.keys())


# Singleton instance
manager = ConnectionManager()
