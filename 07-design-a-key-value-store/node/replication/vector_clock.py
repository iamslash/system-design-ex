"""Vector clock implementation for conflict detection.

Each stored value carries a vector clock.  On write the coordinator
increments its own entry.  On read, if multiple replicas return
divergent clocks we surface all conflicting versions to the client.
"""

from __future__ import annotations

import copy
from enum import Enum


class Ordering(Enum):
    BEFORE = "BEFORE"      # self happened-before other
    AFTER = "AFTER"        # self happened-after other
    CONCURRENT = "CONCURRENT"
    EQUAL = "EQUAL"


class VectorClock:
    """A dict-backed vector clock keyed by node id."""

    def __init__(self, clock: dict[str, int] | None = None) -> None:
        self._clock: dict[str, int] = dict(clock) if clock else {}

    # -- mutators --------------------------------------------------------

    def increment(self, node_id: str) -> VectorClock:
        """Return a **new** VectorClock with *node_id* incremented."""
        new_clock = dict(self._clock)
        new_clock[node_id] = new_clock.get(node_id, 0) + 1
        return VectorClock(new_clock)

    def merge(self, other: VectorClock) -> VectorClock:
        """Return a **new** VectorClock that is the element-wise max."""
        all_keys = set(self._clock) | set(other._clock)
        merged = {
            k: max(self._clock.get(k, 0), other._clock.get(k, 0))
            for k in all_keys
        }
        return VectorClock(merged)

    # -- comparison ------------------------------------------------------

    def compare(self, other: VectorClock) -> Ordering:
        """Compare two vector clocks.

        Returns:
            BEFORE   – self happened-before other (self is ancestor)
            AFTER    – self happened-after other  (self is descendant)
            EQUAL    – identical clocks
            CONCURRENT – neither dominates (conflict)
        """
        all_keys = set(self._clock) | set(other._clock)

        self_leq = True   # every entry in self <= other
        other_leq = True   # every entry in other <= self

        for k in all_keys:
            sv = self._clock.get(k, 0)
            ov = other._clock.get(k, 0)
            if sv > ov:
                self_leq = False
            if ov > sv:
                other_leq = False

        if self_leq and other_leq:
            return Ordering.EQUAL
        if self_leq:
            return Ordering.BEFORE
        if other_leq:
            return Ordering.AFTER
        return Ordering.CONCURRENT

    # -- serialisation ---------------------------------------------------

    def to_dict(self) -> dict[str, int]:
        return dict(self._clock)

    @classmethod
    def from_dict(cls, data: dict[str, int] | None) -> VectorClock:
        return cls(data or {})

    # -- dunder ----------------------------------------------------------

    def __repr__(self) -> str:
        return f"VectorClock({self._clock})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return NotImplemented
        return self._clock == other._clock

    def copy(self) -> VectorClock:
        return VectorClock(copy.deepcopy(self._clock))
