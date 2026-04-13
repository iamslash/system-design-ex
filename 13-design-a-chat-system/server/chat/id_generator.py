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
    """Time-based sortable unique message ID generator.

    Composed of a millisecond timestamp and a sequence counter, similar to
    the Snowflake approach. Uniqueness within the same millisecond is
    guaranteed by the sequence number.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_timestamp_ms: int = 0
        self._sequence: int = 0

    def generate(self) -> str:
        """Generate a unique, time-sortable message ID.

        Returns:
            A string ID in the form "{timestamp_ms}-{sequence}".
        """
        with self._lock:
            now_ms = int(time.time() * 1000)

            if now_ms == self._last_timestamp_ms:
                # Increment sequence within the same millisecond
                self._sequence += 1
            else:
                # New millisecond, reset sequence
                self._last_timestamp_ms = now_ms
                self._sequence = 0

            return f"{now_ms}-{self._sequence}"


# Singleton instance
id_generator = IdGenerator()
