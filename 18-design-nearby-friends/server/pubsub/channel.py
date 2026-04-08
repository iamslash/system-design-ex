"""Redis Pub/Sub for location broadcast to friends.

Each user has a dedicated Redis pub/sub channel:  location:{user_id}

When a user updates their location the server publishes to their channel.
Friends who are subscribed to that channel receive the update in real time
and can decide (via Haversine distance check) whether the update is relevant.

This design decouples producers (the user moving) from consumers (friends
listening) and scales horizontally because Redis Pub/Sub fans out to all
subscribers.
"""

from __future__ import annotations

import asyncio
import json
from typing import Callable, Awaitable

import redis.asyncio as aioredis

from friends.nearby import haversine_distance
from config import settings


def _channel_name(user_id: str) -> str:
    """Return the Redis pub/sub channel name for a user."""
    return f"location:{user_id}"


class LocationPubSub:
    """Manages per-user location pub/sub channels.

    Publishing: when a user reports a new location, `publish` sends the
    update to their channel so all subscribed friends receive it.

    Subscribing: when a user comes online the server subscribes to all
    of their friends' channels so incoming location updates are forwarded
    over the WebSocket -- but only if the friend is within the radius.
    """

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis
        # user_id -> pubsub object (for cleanup)
        self._subscriptions: dict[str, aioredis.client.PubSub] = {}
        # user_id -> set of channels subscribed
        self._user_channels: dict[str, set[str]] = {}
        # user_id -> background listener task
        self._listener_tasks: dict[str, asyncio.Task] = {}

    async def publish(self, user_id: str, latitude: float, longitude: float) -> int:
        """Publish a location update to the user's channel.

        Returns the number of subscribers who received the message.
        """
        channel = _channel_name(user_id)
        payload = json.dumps({
            "user_id": user_id,
            "latitude": latitude,
            "longitude": longitude,
        })
        return await self._redis.publish(channel, payload)

    async def subscribe(
        self,
        user_id: str,
        friend_ids: list[str],
        on_update: Callable[[dict], Awaitable[None]],
        my_location_getter: Callable[[], Awaitable[dict | None]],
    ) -> None:
        """Subscribe *user_id* to the location channels of *friend_ids*.

        When an update arrives on any friend's channel, the *on_update*
        callback is invoked with the location payload -- but only if the
        friend is within NEARBY_RADIUS_MILES of *user_id*'s current location.
        """
        if not friend_ids:
            return

        pubsub = self._redis.pubsub()
        channels = [_channel_name(fid) for fid in friend_ids]
        await pubsub.subscribe(*channels)

        self._subscriptions[user_id] = pubsub
        self._user_channels[user_id] = set(channels)

        async def _listen() -> None:
            try:
                async for message in pubsub.listen():
                    if message["type"] != "message":
                        continue
                    data = json.loads(message["data"])

                    # Distance filter: only forward if within radius
                    my_loc = await my_location_getter()
                    if my_loc is None:
                        continue
                    dist = haversine_distance(
                        my_loc["latitude"],
                        my_loc["longitude"],
                        data["latitude"],
                        data["longitude"],
                    )
                    if dist <= settings.NEARBY_RADIUS_MILES:
                        data["distance_miles"] = round(dist, 4)
                        await on_update(data)
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(_listen())
        self._listener_tasks[user_id] = task

    async def unsubscribe(self, user_id: str) -> None:
        """Unsubscribe *user_id* from all friend channels and clean up."""
        task = self._listener_tasks.pop(user_id, None)
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        pubsub = self._subscriptions.pop(user_id, None)
        if pubsub is not None:
            channels = self._user_channels.pop(user_id, set())
            if channels:
                await pubsub.unsubscribe(*channels)
            await pubsub.aclose()

    async def close_all(self) -> None:
        """Unsubscribe all users (for graceful shutdown)."""
        user_ids = list(self._subscriptions.keys())
        for uid in user_ids:
            await self.unsubscribe(uid)
