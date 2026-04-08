"""Per-user, per-channel notification rate limiter.

Redis Sorted Set 을 사용하여 슬라이딩 윈도우 기반으로
사용자별/채널별 전송 횟수를 제한한다.
기본 제한: push 10/h, sms 5/h, email 20/h (환경 변수로 설정 가능).
"""

from __future__ import annotations

import time

from redis.asyncio import Redis

from config import settings

# 채널별 시간당 최대 전송 수
RATE_LIMITS: dict[str, int] = {
    "push": settings.RATE_LIMIT_PUSH,
    "sms": settings.RATE_LIMIT_SMS,
    "email": settings.RATE_LIMIT_EMAIL,
}

# 윈도우 크기 (초) — 1시간
WINDOW_SIZE: int = 3600


# Lua 스크립트: 윈도우 정리 + 카운트 확인 + 삽입을 원자적으로 실행
_LUA_RATE_LIMIT = """
local key = KEYS[1]
local window_size = tonumber(ARGV[1])
local max_count = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local member = ARGV[4]

-- 윈도우 밖의 오래된 레코드 제거
local window_start = now - window_size
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

-- 현재 윈도우 내 카운트 조회
local current_count = redis.call('ZCARD', key)

if current_count >= max_count then
    return 0
end

-- 허용: 현재 타임스탬프를 기록
redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, window_size + 1)
return 1
"""

_script_cache: dict[int, object] = {}


async def check_rate_limit(
    redis: Redis,
    user_id: str,
    channel: str,
    window_size: int = WINDOW_SIZE,
    limits: dict[str, int] | None = None,
) -> bool:
    """사용자의 채널별 rate limit 을 확인한다.

    Lua 스크립트로 윈도우 정리 + 카운트 확인 + 삽입을 원자적으로 실행하여
    동시 요청 시에도 rate limit 을 정확하게 보장한다.

    Args:
        redis: Redis 클라이언트.
        user_id: 사용자 ID.
        channel: 알림 채널 (push/sms/email).
        window_size: 슬라이딩 윈도우 크기 (초).
        limits: 채널별 최대 전송 수 오버라이드 (테스트용).

    Returns:
        True 이면 전송 허용, False 이면 rate limit 초과.
    """
    effective_limits = limits or RATE_LIMITS
    max_count = effective_limits.get(channel, 10)
    key = f"rate_limit:{user_id}:{channel}"
    now = time.time()
    member = f"{now}:{user_id}:{channel}"

    client_id = id(redis)
    if client_id not in _script_cache:
        _script_cache[client_id] = redis.register_script(_LUA_RATE_LIMIT)

    result = await _script_cache[client_id](keys=[key], args=[window_size, max_count, now, member])
    return bool(result)
