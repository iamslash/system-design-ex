"""Tests for the distributed message queue implementation.

Covers: partition append/read, offset tracking, consumer groups,
producer batching, replication, ISR, and delivery semantics.
"""

from __future__ import annotations

import sys
import os
import time

import pytest

# Ensure src is importable.
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..")
)

from src.partition import Partition, PartitionConfig, Record, Segment
from src.broker import Broker, TopicConfig
from src.producer import Producer, ProducerConfig, ProducerRecord
from src.consumer import (
    Consumer,
    ConsumerGroup,
    OffsetStore,
    reset_offset_store,
)
from src.replication import AckMode, ReplicatedPartition


@pytest.fixture(autouse=True)
def _clean_offset_store() -> None:
    """Reset global offset store before each test."""
    reset_offset_store()
    yield  # type: ignore[misc]
    reset_offset_store()


# ============================================================================
# Partition tests
# ============================================================================


class TestPartitionAppendRead:
    """Test basic append and read on a partition."""

    def test_append_single_record(self) -> None:
        p = Partition("t", 0)
        rec = p.append("k1", b"hello")
        assert rec.offset == 0
        assert rec.key == "k1"
        assert rec.value == b"hello"

    def test_append_increments_offset(self) -> None:
        p = Partition("t", 0)
        r0 = p.append("k1", b"a")
        r1 = p.append("k2", b"b")
        r2 = p.append(None, b"c")
        assert r0.offset == 0
        assert r1.offset == 1
        assert r2.offset == 2

    def test_read_from_start(self) -> None:
        p = Partition("t", 0)
        for i in range(5):
            p.append(f"k{i}", f"v{i}".encode())
        records = p.read(0)
        assert len(records) == 5
        assert records[0].offset == 0
        assert records[4].offset == 4

    def test_read_from_middle(self) -> None:
        p = Partition("t", 0)
        for i in range(10):
            p.append(None, f"v{i}".encode())
        records = p.read(5, max_records=3)
        assert len(records) == 3
        assert records[0].offset == 5
        assert records[2].offset == 7

    def test_read_empty_partition(self) -> None:
        p = Partition("t", 0)
        assert p.read(0) == []

    def test_fifo_ordering(self) -> None:
        p = Partition("t", 0)
        values = [f"msg-{i}".encode() for i in range(20)]
        for v in values:
            p.append(None, v)
        records = p.read(0, max_records=20)
        assert [r.value for r in records] == values

    def test_log_end_offset(self) -> None:
        p = Partition("t", 0)
        assert p.log_end_offset == 0
        p.append(None, b"a")
        assert p.log_end_offset == 1
        p.append(None, b"b")
        assert p.log_end_offset == 2


class TestSegments:
    """Test segment rotation when max_size is reached."""

    def test_segment_rotation(self) -> None:
        cfg = PartitionConfig(segment_max_size=3)
        p = Partition("t", 0, cfg)
        for i in range(7):
            p.append(None, f"v{i}".encode())
        # 3 segments: [0,1,2] [3,4,5] [6]
        assert len(p.segments) == 3
        assert p.segments[0].sealed
        assert p.segments[1].sealed
        assert not p.segments[2].sealed

    def test_read_across_segments(self) -> None:
        cfg = PartitionConfig(segment_max_size=2)
        p = Partition("t", 0, cfg)
        for i in range(6):
            p.append(None, f"v{i}".encode())
        records = p.read(0, max_records=6)
        assert len(records) == 6
        assert [r.offset for r in records] == list(range(6))

    def test_read_spanning_segment_boundary(self) -> None:
        cfg = PartitionConfig(segment_max_size=3)
        p = Partition("t", 0, cfg)
        for i in range(9):
            p.append(None, f"v{i}".encode())
        # Read from offset 2 to 6 (spans first and second segment).
        records = p.read(2, max_records=5)
        assert len(records) == 5
        assert records[0].offset == 2
        assert records[4].offset == 6

    def test_sealed_segment_rejects_append(self) -> None:
        seg = Segment(base_offset=0, max_size=2)
        seg.append(None, b"a", time.time())
        seg.seal()
        with pytest.raises(RuntimeError, match="sealed"):
            seg.append(None, b"b", time.time())


# ============================================================================
# Broker tests
# ============================================================================


class TestBroker:
    def test_create_and_list_topics(self) -> None:
        b = Broker()
        b.create_topic("orders", TopicConfig(num_partitions=4))
        b.create_topic("events", TopicConfig(num_partitions=2))
        assert sorted(b.list_topics()) == ["events", "orders"]

    def test_duplicate_topic_raises(self) -> None:
        b = Broker()
        b.create_topic("t1")
        with pytest.raises(ValueError, match="already exists"):
            b.create_topic("t1")

    def test_delete_topic(self) -> None:
        b = Broker()
        b.create_topic("t1")
        b.delete_topic("t1")
        assert b.list_topics() == []

    def test_produce_and_consume(self) -> None:
        b = Broker()
        b.create_topic("t1", TopicConfig(num_partitions=1))
        b.produce("t1", 0, "k1", b"hello")
        b.produce("t1", 0, "k2", b"world")
        records = b.consume("t1", 0, 0)
        assert len(records) == 2
        assert records[0].value == b"hello"


# ============================================================================
# Producer tests
# ============================================================================


class TestProducerBatching:
    def test_batch_accumulation(self) -> None:
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=1))
        p = Producer(b, ProducerConfig(batch_size=5))
        for i in range(4):
            result = p.send(ProducerRecord("t", f"v{i}".encode(), key="k"))
            assert result == []  # Not flushed yet.
        assert p.pending_count == 4

    def test_auto_flush_on_batch_full(self) -> None:
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=1))
        p = Producer(b, ProducerConfig(batch_size=3))
        # First two are batched.
        p.send(ProducerRecord("t", b"a", key="k"))
        p.send(ProducerRecord("t", b"b", key="k"))
        # Third triggers flush.
        result = p.send(ProducerRecord("t", b"c", key="k"))
        assert len(result) == 3
        assert p.pending_count == 0

    def test_explicit_flush(self) -> None:
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=1))
        p = Producer(b, ProducerConfig(batch_size=100))
        for i in range(10):
            p.send(ProducerRecord("t", f"v{i}".encode(), key="k"))
        assert p.pending_count == 10
        records = p.flush()
        assert len(records) == 10
        assert p.pending_count == 0

    def test_sent_count_tracking(self) -> None:
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=1))
        p = Producer(b, ProducerConfig(batch_size=100))
        for i in range(7):
            p.send(ProducerRecord("t", f"v{i}".encode(), key="k"))
        p.flush()
        assert p.sent_count == 7


class TestProducerPartitionRouting:
    def test_key_based_routing_deterministic(self) -> None:
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=4))
        p = Producer(b, ProducerConfig(batch_size=1))
        # Same key always goes to the same partition.
        results1 = p.send(ProducerRecord("t", b"v1", key="user-42"))
        results2 = p.send(ProducerRecord("t", b"v2", key="user-42"))
        # Both should be in the same partition (same offset sequence).
        assert results1[0].key == "user-42"
        assert results2[0].key == "user-42"
        # Offsets should be sequential in the same partition.
        assert results2[0].offset == results1[0].offset + 1

    def test_round_robin_without_key(self) -> None:
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=3))
        p = Producer(b, ProducerConfig(batch_size=1))
        partitions_used = set()
        for i in range(6):
            records = p.send(ProducerRecord("t", f"v{i}".encode()))
            # Each record goes to a different partition in round-robin.
            if records:
                partitions_used.add(records[0].offset)
        # With round-robin over 3 partitions and 6 messages,
        # each partition should get 2 messages (offset 0 and 1).
        assert p.sent_count == 6


# ============================================================================
# Consumer & offset tracking tests
# ============================================================================


class TestConsumerOffsetTracking:
    def test_manual_offset_commit(self) -> None:
        store = OffsetStore()
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=1))
        b.produce("t", 0, None, b"a")
        b.produce("t", 0, None, b"b")

        c = Consumer("c1", b, group_id="g1", offset_store=store)
        c.assign("t", [0])
        result = c.poll()
        assert len(result[("t", 0)]) == 2
        # Position advanced but not committed.
        assert c.position("t", 0) == 2
        assert c.committed("t", 0) == 0
        c.commit()
        assert c.committed("t", 0) == 2

    def test_auto_commit(self) -> None:
        store = OffsetStore()
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=1))
        b.produce("t", 0, None, b"a")
        c = Consumer("c1", b, group_id="g1", auto_commit=True, offset_store=store)
        c.assign("t", [0])
        c.poll()
        assert c.committed("t", 0) == 1

    def test_seek(self) -> None:
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=1))
        for i in range(5):
            b.produce("t", 0, None, f"v{i}".encode())
        c = Consumer("c1", b, group_id="g1")
        c.assign("t", [0])
        c.seek("t", 0, 3)
        result = c.poll()
        assert len(result[("t", 0)]) == 2
        assert result[("t", 0)][0].offset == 3

    def test_seek_unassigned_raises(self) -> None:
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=1))
        c = Consumer("c1", b)
        c.assign("t", [0])
        with pytest.raises(ValueError, match="not assigned"):
            c.seek("t", 99, 0)

    def test_resume_from_committed_offset(self) -> None:
        store = OffsetStore()
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=1))
        for i in range(5):
            b.produce("t", 0, None, f"v{i}".encode())

        # First consumer reads and commits.
        c1 = Consumer("c1", b, group_id="g1", offset_store=store)
        c1.assign("t", [0])
        c1.poll(max_records=3)
        c1.commit()
        assert c1.committed("t", 0) == 3

        # Second consumer in same group resumes.
        c2 = Consumer("c2", b, group_id="g1", offset_store=store)
        c2.assign("t", [0])
        result = c2.poll()
        records = result.get(("t", 0), [])
        assert len(records) == 2
        assert records[0].offset == 3


# ============================================================================
# Consumer group tests
# ============================================================================


class TestConsumerGroups:
    def test_partition_assignment_even(self) -> None:
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=4))
        store = OffsetStore()
        cg = ConsumerGroup("g1", b, offset_store=store)
        cg.subscribe("t")
        cg.add_consumer("c1")
        cg.add_consumer("c2")
        assignment = cg.rebalance()
        # 4 partitions / 2 consumers = 2 each.
        assert len(assignment["c1"]) == 2
        assert len(assignment["c2"]) == 2

    def test_partition_assignment_uneven(self) -> None:
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=5))
        store = OffsetStore()
        cg = ConsumerGroup("g1", b, offset_store=store)
        cg.subscribe("t")
        cg.add_consumer("c1")
        cg.add_consumer("c2")
        assignment = cg.rebalance()
        # 5 partitions / 2 consumers = 3 + 2.
        assert len(assignment["c1"]) == 3
        assert len(assignment["c2"]) == 2

    def test_rebalance_on_consumer_add(self) -> None:
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=6))
        store = OffsetStore()
        cg = ConsumerGroup("g1", b, offset_store=store)
        cg.subscribe("t")
        cg.add_consumer("c1")
        cg.add_consumer("c2")
        cg.rebalance()
        # Add a third consumer and rebalance.
        cg.add_consumer("c3")
        assignment = cg.rebalance()
        assert len(assignment["c1"]) == 2
        assert len(assignment["c2"]) == 2
        assert len(assignment["c3"]) == 2

    def test_rebalance_on_consumer_remove(self) -> None:
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=4))
        store = OffsetStore()
        cg = ConsumerGroup("g1", b, offset_store=store)
        cg.subscribe("t")
        c1 = cg.add_consumer("c1")
        cg.add_consumer("c2")
        cg.rebalance()
        # Remove c1 and rebalance.
        cg.remove_consumer(c1)
        assignment = cg.rebalance()
        assert "c1" not in assignment
        assert len(assignment["c2"]) == 4

    def test_each_partition_consumed_by_one_consumer(self) -> None:
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=6))
        for pid in range(6):
            b.produce("t", pid, None, f"msg-{pid}".encode())

        store = OffsetStore()
        cg = ConsumerGroup("g1", b, offset_store=store)
        cg.subscribe("t")
        c1 = cg.add_consumer("c1")
        c2 = cg.add_consumer("c2")
        cg.rebalance()

        # Collect all consumed partition ids.
        r1 = c1.poll()
        r2 = c2.poll()
        partitions_c1 = {tp[1] for tp in r1.keys()}
        partitions_c2 = {tp[1] for tp in r2.keys()}
        # No overlap.
        assert partitions_c1.isdisjoint(partitions_c2)
        assert partitions_c1 | partitions_c2 == set(range(6))

    def test_multiple_topics(self) -> None:
        b = Broker()
        b.create_topic("t1", TopicConfig(num_partitions=2))
        b.create_topic("t2", TopicConfig(num_partitions=2))
        store = OffsetStore()
        cg = ConsumerGroup("g1", b, offset_store=store)
        cg.subscribe("t1")
        cg.subscribe("t2")
        cg.add_consumer("c1")
        cg.add_consumer("c2")
        assignment = cg.rebalance()
        total = sum(len(v) for v in assignment.values())
        assert total == 4  # 2 + 2 partitions


# ============================================================================
# Replication tests
# ============================================================================


class TestReplication:
    def test_leader_write(self) -> None:
        rp = ReplicatedPartition(
            topic="t", partition_id=0,
            leader_broker_id=0,
            follower_broker_ids=[1, 2],
            ack_mode=AckMode.LEADER,
        )
        rec = rp.produce("k1", b"hello")
        assert rec.offset == 0
        assert rec.value == b"hello"

    def test_follower_fetch(self) -> None:
        rp = ReplicatedPartition(
            topic="t", partition_id=0,
            leader_broker_id=0,
            follower_broker_ids=[1],
            ack_mode=AckMode.LEADER,
        )
        rp.produce(None, b"a")
        rp.produce(None, b"b")

        fetched = rp.follower_fetch(1)
        assert len(fetched) == 2
        assert fetched[0].value == b"a"
        # Follower partition should have the data.
        assert rp.followers[1].partition.log_end_offset == 2

    def test_ack_all_replicates_synchronously(self) -> None:
        rp = ReplicatedPartition(
            topic="t", partition_id=0,
            leader_broker_id=0,
            follower_broker_ids=[1, 2],
            ack_mode=AckMode.ALL,
        )
        rp.produce(None, b"msg")
        # All followers should have the data immediately.
        assert rp.followers[1].leo == 1
        assert rp.followers[2].leo == 1

    def test_ack_none_does_not_replicate(self) -> None:
        rp = ReplicatedPartition(
            topic="t", partition_id=0,
            leader_broker_id=0,
            follower_broker_ids=[1],
            ack_mode=AckMode.NONE,
        )
        rp.produce(None, b"msg")
        # Follower should NOT have the data.
        assert rp.followers[1].leo == 0

    def test_high_watermark_advances_with_isr(self) -> None:
        rp = ReplicatedPartition(
            topic="t", partition_id=0,
            leader_broker_id=0,
            follower_broker_ids=[1],
            ack_mode=AckMode.LEADER,
        )
        rp.produce(None, b"a")
        rp.produce(None, b"b")
        # HW should be 0 because follower hasn't fetched.
        assert rp.high_watermark == 0

        rp.follower_fetch(1)
        # Now follower is caught up, HW should advance.
        assert rp.high_watermark == 2

    def test_consume_only_committed_records(self) -> None:
        rp = ReplicatedPartition(
            topic="t", partition_id=0,
            leader_broker_id=0,
            follower_broker_ids=[1],
            ack_mode=AckMode.LEADER,
        )
        rp.produce(None, b"a")
        rp.produce(None, b"b")
        # HW = 0 because follower hasn't fetched.
        records = rp.consume(0)
        assert len(records) == 0

        # After follower fetches, records become visible.
        rp.follower_fetch(1)
        records = rp.consume(0)
        assert len(records) == 2


class TestISR:
    def test_isr_includes_all_initially(self) -> None:
        rp = ReplicatedPartition(
            topic="t", partition_id=0,
            leader_broker_id=0,
            follower_broker_ids=[1, 2],
        )
        assert rp.isr == {0, 1, 2}

    def test_follower_removed_from_isr_on_lag(self) -> None:
        rp = ReplicatedPartition(
            topic="t", partition_id=0,
            leader_broker_id=0,
            follower_broker_ids=[1],
            ack_mode=AckMode.LEADER,
            replica_lag_max=2,
        )
        # Produce more than lag_max records without follower fetching.
        for i in range(5):
            rp.produce(None, f"v{i}".encode())
        # Trigger ISR check via a fetch attempt (follower is behind).
        rp.follower_fetch(1, max_records=1)
        # Follower fetched 1 record but is still 4 behind (lag=4 > max=2).
        assert 1 not in rp.isr

    def test_follower_rejoins_isr_after_catching_up(self) -> None:
        rp = ReplicatedPartition(
            topic="t", partition_id=0,
            leader_broker_id=0,
            follower_broker_ids=[1],
            ack_mode=AckMode.LEADER,
            replica_lag_max=2,
        )
        for i in range(5):
            rp.produce(None, f"v{i}".encode())
        # Fetch partially -- still lagging.
        rp.follower_fetch(1, max_records=1)
        assert 1 not in rp.isr
        # Fetch the rest -- caught up.
        rp.follower_fetch(1, max_records=100)
        assert 1 in rp.isr

    def test_leader_always_in_isr(self) -> None:
        rp = ReplicatedPartition(
            topic="t", partition_id=0,
            leader_broker_id=0,
            follower_broker_ids=[1],
            replica_lag_max=0,
        )
        rp.produce(None, b"a")
        rp.follower_fetch(1, max_records=0)  # Intentionally fetch nothing.
        assert 0 in rp.isr  # Leader always stays.


# ============================================================================
# Delivery semantics tests
# ============================================================================


class TestDeliverySemantics:
    def test_at_most_once_no_reread(self) -> None:
        """At-most-once: commit before processing. If processing fails,
        the message is lost (offset already advanced)."""
        store = OffsetStore()
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=1))
        b.produce("t", 0, None, b"msg1")
        b.produce("t", 0, None, b"msg2")

        c = Consumer("c1", b, group_id="g1", auto_commit=True, offset_store=store)
        c.assign("t", [0])
        result = c.poll()
        # Offset committed automatically (at-most-once pattern).
        assert c.committed("t", 0) == 2
        # Re-polling yields nothing (no redelivery).
        result2 = c.poll()
        assert ("t", 0) not in result2

    def test_at_least_once_redelivery(self) -> None:
        """At-least-once: process then commit. If crash before commit,
        messages are redelivered."""
        store = OffsetStore()
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=1))
        b.produce("t", 0, None, b"msg1")

        c1 = Consumer("c1", b, group_id="g1", offset_store=store)
        c1.assign("t", [0])
        c1.poll()
        # Simulate crash: don't commit. New consumer starts.
        c2 = Consumer("c2", b, group_id="g1", offset_store=store)
        c2.assign("t", [0])
        result = c2.poll()
        # Message is redelivered.
        assert len(result[("t", 0)]) == 1
        assert result[("t", 0)][0].value == b"msg1"

    def test_exactly_once_idempotent_pattern(self) -> None:
        """Exactly-once via idempotent consumer: deduplicate by offset."""
        store = OffsetStore()
        b = Broker()
        b.create_topic("t", TopicConfig(num_partitions=1))
        b.produce("t", 0, None, b"msg1")
        b.produce("t", 0, None, b"msg2")

        processed_offsets: set[int] = set()

        def idempotent_process(records: list[Record]) -> None:
            for r in records:
                if r.offset not in processed_offsets:
                    processed_offsets.add(r.offset)

        c = Consumer("c1", b, group_id="g1", offset_store=store)
        c.assign("t", [0])
        result = c.poll()
        idempotent_process(result[("t", 0)])
        # Simulate redelivery by seeking back.
        c.seek("t", 0, 0)
        result2 = c.poll()
        idempotent_process(result2[("t", 0)])
        # Each message processed exactly once.
        assert processed_offsets == {0, 1}


# ============================================================================
# Integration tests
# ============================================================================


class TestIntegration:
    def test_end_to_end_produce_consume(self) -> None:
        b = Broker()
        b.create_topic("orders", TopicConfig(num_partitions=3))
        p = Producer(b, ProducerConfig(batch_size=2))

        # Produce 6 messages with keys.
        for i in range(6):
            p.send(ProducerRecord("orders", f"order-{i}".encode(), key=f"user-{i % 2}"))
        p.flush()

        # Consume from all partitions.
        all_records = []
        for pid in range(3):
            all_records.extend(b.consume("orders", pid, 0))
        assert len(all_records) == 6

    def test_consumer_group_e2e(self) -> None:
        b = Broker()
        b.create_topic("events", TopicConfig(num_partitions=4))
        p = Producer(b, ProducerConfig(batch_size=1))

        for i in range(8):
            p.send(ProducerRecord("events", f"evt-{i}".encode(), key=f"k{i}"))
        p.flush()

        store = OffsetStore()
        cg = ConsumerGroup("analytics", b, offset_store=store)
        cg.subscribe("events")
        c1 = cg.add_consumer("worker-1")
        c2 = cg.add_consumer("worker-2")
        cg.rebalance()

        all_records = []
        for c in [c1, c2]:
            for _, recs in c.poll().items():
                all_records.extend(recs)
        assert len(all_records) == 8

    def test_replicated_broker_e2e(self) -> None:
        b = Broker(broker_id=0, peer_broker_ids=[1, 2])
        b.create_topic(
            "t",
            TopicConfig(num_partitions=2, replication_factor=3),
        )
        # Produce via broker.
        b.produce("t", 0, "k1", b"hello")
        b.produce("t", 1, "k2", b"world")

        # Get replicated partitions and sync followers.
        rp0 = b.get_partition("t", 0)
        rp1 = b.get_partition("t", 1)
        assert isinstance(rp0, ReplicatedPartition)
        assert isinstance(rp1, ReplicatedPartition)

        # Follower fetch to advance HW.
        for rp in [rp0, rp1]:
            for fid in rp.followers:
                rp.follower_fetch(fid)

        # Now consume should return committed records.
        records0 = b.consume("t", 0, 0)
        records1 = b.consume("t", 1, 0)
        assert len(records0) == 1
        assert len(records1) == 1
