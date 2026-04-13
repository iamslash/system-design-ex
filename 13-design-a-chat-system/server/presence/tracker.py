"""Online/offline presence tracker with heartbeat mechanism.

Users send periodic heartbeat messages via WebSocket. If no heartbeat
is received within the timeout window, the user is marked offline.

Redis keys:
  presence:{user_id} -> hash {status, last_heartbeat}
"""

from __future__ import annotations

import time

import redis.asyncio as aioredis

from config import settings


class PresenceTracker:
    """Heartbeat-based user online/offline presence tracker.

    How it works:
      - Users send periodic heartbeat messages via WebSocket (default every 5 seconds).
      - The server updates the last_heartbeat timestamp in Redis.
      - If no heartbeat is received within HEARTBEAT_TIMEOUT (default 30 seconds),
        the user is considered offline.
    """

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client
        self._timeout = settings.HEARTBEAT_TIMEOUT

    async def set_online(self, user_id: str) -> None:
        """Mark a user as online and record the current heartbeat timestamp."""
        now = time.time()
        await self._redis.hset(
            f"presence:{user_id}",
            mapping={"status": "online", "last_heartbeat": str(now)},
        )

    async def heartbeat(self, user_id: str) -> None:
        """Receive a heartbeat and update last_heartbeat.

        Called periodically to indicate the user is still active.
        """
        now = time.time()
        await self._redis.hset(
            f"presence:{user_id}",
            mapping={"status": "online", "last_heartbeat": str(now)},
        )

    async def set_offline(self, user_id: str) -> None:
        """Mark a user as offline (called on connection close)."""
        await self._redis.hset(
            f"presence:{user_id}",
            mapping={"status": "offline", "last_heartbeat": str(time.time())},
        )

    async def get_status(self, user_id: str) -> dict[str, object]:
        """Return the current presence status of a user.

        If HEARTBEAT_TIMEOUT seconds have elapsed since the last heartbeat,
        the user is automatically considered offline.
        """
        data = await self._redis.hgetall(f"presence:{user_id}")
        if not data:
            return {"user_id": user_id, "status": "offline", "last_heartbeat": None}

        last_hb = float(data.get("last_heartbeat", "0"))
        status = data.get("status", "offline")

        # Timeout check: if more than timeout seconds have passed since the last heartbeat, mark offline
        if status == "online" and (time.time() - last_hb) > self._timeout:
            status = "offline"
            await self.set_offline(user_id)

        return {
            "user_id": user_id,
            "status": status,
            "last_heartbeat": last_hb if last_hb > 0 else None,
        }
