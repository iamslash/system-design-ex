"""Twitter Snowflake-based 64-bit unique ID generator.

Bit layout (64 bits total):
    | 1 bit (sign, always 0) | 41 bits (timestamp ms) | 5 bits (datacenter) | 5 bits (machine) | 12 bits (sequence) |

The 41-bit timestamp gives ~69 years of unique IDs from the epoch.
The 12-bit sequence allows 4096 IDs per millisecond per worker.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

# Twitter snowflake epoch: Nov 04, 2010, 01:42:54.657 UTC
EPOCH = 1288834974657

# Bit lengths
SEQUENCE_BITS = 12
MACHINE_ID_BITS = 5
DATACENTER_ID_BITS = 5
TIMESTAMP_BITS = 41

# Max values
MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1          # 4095
MAX_MACHINE_ID = (1 << MACHINE_ID_BITS) - 1      # 31
MAX_DATACENTER_ID = (1 << DATACENTER_ID_BITS) - 1  # 31

# Bit shifts
MACHINE_ID_SHIFT = SEQUENCE_BITS                           # 12
DATACENTER_ID_SHIFT = SEQUENCE_BITS + MACHINE_ID_BITS      # 17
TIMESTAMP_SHIFT = SEQUENCE_BITS + MACHINE_ID_BITS + DATACENTER_ID_BITS  # 22


class SnowflakeGenerator:
    """Thread-safe Snowflake ID generator.

    Each generator instance is identified by a (datacenter_id, machine_id) pair,
    producing globally unique 64-bit IDs that are roughly time-sortable.
    """

    def __init__(
        self,
        datacenter_id: int,
        machine_id: int,
        epoch: int = EPOCH,
    ) -> None:
        """Initialize the generator.

        Args:
            datacenter_id: Datacenter identifier (0-31, 5 bits).
            machine_id: Machine identifier within the datacenter (0-31, 5 bits).
            epoch: Custom epoch in milliseconds since Unix epoch.

        Raises:
            ValueError: If datacenter_id or machine_id is out of range.
        """
        if not (0 <= datacenter_id <= MAX_DATACENTER_ID):
            raise ValueError(
                f"datacenter_id must be between 0 and {MAX_DATACENTER_ID}, "
                f"got {datacenter_id}"
            )
        if not (0 <= machine_id <= MAX_MACHINE_ID):
            raise ValueError(
                f"machine_id must be between 0 and {MAX_MACHINE_ID}, "
                f"got {machine_id}"
            )

        self._datacenter_id = datacenter_id
        self._machine_id = machine_id
        self._epoch = epoch
        self._sequence = 0
        self._last_timestamp = -1
        self._lock = threading.Lock()

    @property
    def datacenter_id(self) -> int:
        return self._datacenter_id

    @property
    def machine_id(self) -> int:
        return self._machine_id

    def generate(self) -> int:
        """Generate a new unique 64-bit ID.

        Returns:
            A positive 64-bit integer ID.

        Raises:
            RuntimeError: If the system clock moved backwards.
        """
        with self._lock:
            timestamp = self._current_millis()

            # Clock moved backwards -- refuse to generate
            if timestamp < self._last_timestamp:
                raise RuntimeError(
                    f"Clock moved backwards. "
                    f"Refusing to generate ID for {self._last_timestamp - timestamp} ms"
                )

            if timestamp == self._last_timestamp:
                # Same millisecond: increment sequence
                self._sequence = (self._sequence + 1) & MAX_SEQUENCE
                if self._sequence == 0:
                    # Sequence overflow: wait for next millisecond
                    timestamp = self._wait_next_millis(self._last_timestamp)
            else:
                # New millisecond: reset sequence
                self._sequence = 0

            self._last_timestamp = timestamp

            # Compose the 64-bit ID
            snowflake_id = (
                ((timestamp - self._epoch) << TIMESTAMP_SHIFT)
                | (self._datacenter_id << DATACENTER_ID_SHIFT)
                | (self._machine_id << MACHINE_ID_SHIFT)
                | self._sequence
            )
            return snowflake_id

    @staticmethod
    def parse(snowflake_id: int, epoch: int = EPOCH) -> dict:
        """Parse a snowflake ID back into its components.

        Args:
            snowflake_id: The 64-bit snowflake ID to parse.
            epoch: The epoch used when the ID was generated.

        Returns:
            A dict with keys: timestamp_ms, datetime, datacenter_id,
            machine_id, sequence.
        """
        sequence = snowflake_id & MAX_SEQUENCE
        machine_id = (snowflake_id >> MACHINE_ID_SHIFT) & MAX_MACHINE_ID
        datacenter_id = (snowflake_id >> DATACENTER_ID_SHIFT) & MAX_DATACENTER_ID
        timestamp_offset = snowflake_id >> TIMESTAMP_SHIFT
        timestamp_ms = timestamp_offset + epoch

        dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)

        return {
            "timestamp_ms": timestamp_ms,
            "datetime": dt.strftime("%Y-%m-%d %H:%M:%S.%f UTC"),
            "datacenter_id": datacenter_id,
            "machine_id": machine_id,
            "sequence": sequence,
        }

    def _current_millis(self) -> int:
        """Return current time in milliseconds since Unix epoch."""
        return int(time.time() * 1000)

    def _wait_next_millis(self, last_ts: int) -> int:
        """Spin-wait until the clock advances past last_ts."""
        ts = self._current_millis()
        while ts <= last_ts:
            ts = self._current_millis()
        return ts
