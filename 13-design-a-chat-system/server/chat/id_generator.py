"""Snowflake-like message ID generator.

Generates time-sortable unique IDs using millisecond timestamp + sequence counter.
Format: "{timestamp_ms}-{sequence}"

This ensures:
  - IDs are globally unique within a single server process.
  - IDs are naturally sorted by creation time.
  - Multiple messages within the same millisecond get distinct IDs.
"""

from __future__ import annotations

import threading
import time


class IdGenerator:
    """시간 기반 정렬 가능한 고유 메시지 ID 생성기.

    Snowflake 방식과 유사하게 밀리초 타임스탬프 + 시퀀스 카운터로 구성된다.
    같은 밀리초 내에서도 시퀀스 번호로 고유성을 보장한다.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_timestamp_ms: int = 0
        self._sequence: int = 0

    def generate(self) -> str:
        """고유하고 시간순 정렬 가능한 메시지 ID 를 생성한다.

        Returns:
            "{timestamp_ms}-{sequence}" 형태의 문자열 ID.
        """
        with self._lock:
            now_ms = int(time.time() * 1000)

            if now_ms == self._last_timestamp_ms:
                # 같은 밀리초 내에서 시퀀스 증가
                self._sequence += 1
            else:
                # 새로운 밀리초이므로 시퀀스 초기화
                self._last_timestamp_ms = now_ms
                self._sequence = 0

            return f"{now_ms}-{self._sequence}"


# 싱글톤 인스턴스
id_generator = IdGenerator()
