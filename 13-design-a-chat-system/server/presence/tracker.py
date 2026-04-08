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
    """하트비트 기반 사용자 온라인/오프라인 상태 추적기.

    동작 원리:
      - 사용자가 WebSocket 으로 heartbeat 메시지를 주기적으로 전송한다 (기본 5초).
      - 서버는 last_heartbeat 타임스탬프를 Redis 에 갱신한다.
      - HEARTBEAT_TIMEOUT(기본 30초) 내에 하트비트가 없으면 offline 으로 판정한다.
    """

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client
        self._timeout = settings.HEARTBEAT_TIMEOUT

    async def set_online(self, user_id: str) -> None:
        """사용자를 online 으로 설정하고 하트비트 타임스탬프를 기록한다."""
        now = time.time()
        await self._redis.hset(
            f"presence:{user_id}",
            mapping={"status": "online", "last_heartbeat": str(now)},
        )

    async def heartbeat(self, user_id: str) -> None:
        """하트비트를 수신하여 last_heartbeat 를 갱신한다.

        주기적으로 호출되어 사용자가 아직 활성 상태임을 알린다.
        """
        now = time.time()
        await self._redis.hset(
            f"presence:{user_id}",
            mapping={"status": "online", "last_heartbeat": str(now)},
        )

    async def set_offline(self, user_id: str) -> None:
        """사용자를 offline 으로 설정한다 (연결 종료 시 호출)."""
        await self._redis.hset(
            f"presence:{user_id}",
            mapping={"status": "offline", "last_heartbeat": str(time.time())},
        )

    async def get_status(self, user_id: str) -> dict[str, object]:
        """사용자의 현재 접속 상태를 조회한다.

        last_heartbeat 이후 HEARTBEAT_TIMEOUT 초가 지나면
        자동으로 offline 으로 판정한다.
        """
        data = await self._redis.hgetall(f"presence:{user_id}")
        if not data:
            return {"user_id": user_id, "status": "offline", "last_heartbeat": None}

        last_hb = float(data.get("last_heartbeat", "0"))
        status = data.get("status", "offline")

        # 타임아웃 확인: 마지막 하트비트로부터 timeout 초가 지나면 offline
        if status == "online" and (time.time() - last_hb) > self._timeout:
            status = "offline"
            await self.set_offline(user_id)

        return {
            "user_id": user_id,
            "status": status,
            "last_heartbeat": last_hb if last_hb > 0 else None,
        }
