"""Tests for the VectorClock implementation."""

from __future__ import annotations

import pytest

from node.replication.vector_clock import Ordering, VectorClock


class TestIncrement:
    def test_increment_new_node(self) -> None:
        vc = VectorClock()
        vc2 = vc.increment("A")
        assert vc2.to_dict() == {"A": 1}

    def test_increment_existing_node(self) -> None:
        vc = VectorClock({"A": 3})
        vc2 = vc.increment("A")
        assert vc2.to_dict() == {"A": 4}

    def test_increment_is_immutable(self) -> None:
        vc = VectorClock({"A": 1})
        vc2 = vc.increment("A")
        assert vc.to_dict() == {"A": 1}
        assert vc2.to_dict() == {"A": 2}


class TestMerge:
    def test_merge_disjoint(self) -> None:
        vc1 = VectorClock({"A": 1})
        vc2 = VectorClock({"B": 2})
        merged = vc1.merge(vc2)
        assert merged.to_dict() == {"A": 1, "B": 2}

    def test_merge_overlapping(self) -> None:
        vc1 = VectorClock({"A": 3, "B": 1})
        vc2 = VectorClock({"A": 1, "B": 5})
        merged = vc1.merge(vc2)
        assert merged.to_dict() == {"A": 3, "B": 5}

    def test_merge_is_immutable(self) -> None:
        vc1 = VectorClock({"A": 1})
        vc2 = VectorClock({"A": 2})
        merged = vc1.merge(vc2)
        assert vc1.to_dict() == {"A": 1}
        assert merged.to_dict() == {"A": 2}


class TestCompare:
    def test_equal(self) -> None:
        vc1 = VectorClock({"A": 1, "B": 2})
        vc2 = VectorClock({"A": 1, "B": 2})
        assert vc1.compare(vc2) == Ordering.EQUAL

    def test_before(self) -> None:
        vc1 = VectorClock({"A": 1, "B": 1})
        vc2 = VectorClock({"A": 2, "B": 1})
        assert vc1.compare(vc2) == Ordering.BEFORE

    def test_after(self) -> None:
        vc1 = VectorClock({"A": 2, "B": 1})
        vc2 = VectorClock({"A": 1, "B": 1})
        assert vc1.compare(vc2) == Ordering.AFTER

    def test_concurrent(self) -> None:
        vc1 = VectorClock({"A": 2, "B": 1})
        vc2 = VectorClock({"A": 1, "B": 2})
        assert vc1.compare(vc2) == Ordering.CONCURRENT

    def test_ancestor_with_missing_key(self) -> None:
        vc1 = VectorClock({"A": 1})
        vc2 = VectorClock({"A": 1, "B": 1})
        assert vc1.compare(vc2) == Ordering.BEFORE

    def test_descendant_with_missing_key(self) -> None:
        vc1 = VectorClock({"A": 1, "B": 1})
        vc2 = VectorClock({"A": 1})
        assert vc1.compare(vc2) == Ordering.AFTER

    def test_empty_clocks_are_equal(self) -> None:
        vc1 = VectorClock()
        vc2 = VectorClock()
        assert vc1.compare(vc2) == Ordering.EQUAL


class TestSerialization:
    def test_round_trip(self) -> None:
        vc = VectorClock({"X": 5, "Y": 3})
        d = vc.to_dict()
        vc2 = VectorClock.from_dict(d)
        assert vc == vc2

    def test_from_none(self) -> None:
        vc = VectorClock.from_dict(None)
        assert vc.to_dict() == {}
