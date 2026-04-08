"""Counter-based ID generator using Redis INCR.

Redis 의 INCR 명령으로 원자적(atomic) 자동 증가 카운터를 구현한다.
생성된 ID 를 Base62 로 인코딩하여 단축 코드로 사용한다.
"""

from __future__ import annotations

from redis.asyncio import Redis

# Redis 카운터 키
COUNTER_KEY = "url:id_counter"

# 시작 값 (1억부터 시작하면 최소 5자 이상의 코드가 생성됨)
START_VALUE = 100_000_000


async def next_id(redis: Redis) -> int:
    """다음 고유 ID 를 생성한다.

    Redis INCR 을 사용하여 원자적으로 카운터를 증가시킨다.
    첫 호출 시 START_VALUE 부터 시작한다.

    Args:
        redis: Redis 클라이언트.

    Returns:
        새로운 고유 정수 ID.
    """
    # 카운터가 존재하지 않으면 START_VALUE 로 초기화
    exists = await redis.exists(COUNTER_KEY)
    if not exists:
        await redis.set(COUNTER_KEY, START_VALUE)

    return await redis.incr(COUNTER_KEY)
