#!/usr/bin/env python3
"""Demo: Distributed Message Queue

Creates topics with partitions, produces messages with keys,
consumes with consumer groups, shows offset tracking,
and demonstrates replication with ACK modes.
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.broker import Broker, TopicConfig
from src.producer import Producer, ProducerConfig, ProducerRecord
from src.consumer import Consumer, ConsumerGroup, OffsetStore
from src.replication import AckMode, ReplicatedPartition


def separator(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def demo_basic_produce_consume() -> None:
    separator("1. Basic Produce / Consume")

    broker = Broker()
    broker.create_topic("orders", TopicConfig(num_partitions=3))
    print(f"Created topic 'orders' with 3 partitions")

    producer = Producer(broker, ProducerConfig(batch_size=2))
    for i in range(6):
        key = f"user-{i % 3}"
        value = f"order-{i}".encode()
        records = producer.send(ProducerRecord("orders", value, key=key))
        if records:
            for r in records:
                print(f"  Flushed: offset={r.offset} key={r.key} value={r.value}")
    remaining = producer.flush()
    for r in remaining:
        print(f"  Flushed: offset={r.offset} key={r.key} value={r.value}")

    print(f"\nTotal sent: {producer.sent_count} messages")

    # Read from each partition.
    for pid in range(3):
        records = broker.consume("orders", pid, 0)
        if records:
            print(f"\n  Partition {pid}: {len(records)} records")
            for r in records:
                print(f"    offset={r.offset} key={r.key} value={r.value}")


def demo_consumer_groups() -> None:
    separator("2. Consumer Groups with Partition Assignment")

    broker = Broker()
    broker.create_topic("events", TopicConfig(num_partitions=6))
    producer = Producer(broker, ProducerConfig(batch_size=1))

    # Produce 12 messages.
    for i in range(12):
        producer.send(ProducerRecord("events", f"event-{i}".encode(), key=f"k{i % 4}"))
    producer.flush()
    print(f"Produced 12 messages to 'events' (6 partitions)")

    # Create consumer group with 3 consumers.
    store = OffsetStore()
    group = ConsumerGroup("analytics-group", broker, offset_store=store)
    group.subscribe("events")
    consumers = [group.add_consumer(f"worker-{i}") for i in range(3)]
    assignment = group.rebalance()

    print(f"\nPartition assignment:")
    for cid, parts in assignment.items():
        print(f"  {cid}: {parts}")

    # Each consumer polls.
    total = 0
    for c in consumers:
        result = c.poll()
        count = sum(len(recs) for recs in result.values())
        total += count
        print(f"\n  {c.consumer_id} consumed {count} records:")
        for (topic, pid), recs in result.items():
            for r in recs:
                print(f"    [{topic}:{pid}] offset={r.offset} value={r.value}")
        c.commit()
        print(f"  Committed offsets for {c.consumer_id}")

    print(f"\nTotal consumed: {total}")


def demo_offset_tracking() -> None:
    separator("3. Offset Tracking and Seek")

    broker = Broker()
    broker.create_topic("logs", TopicConfig(num_partitions=1))
    for i in range(10):
        broker.produce("logs", 0, None, f"log-{i}".encode())

    store = OffsetStore()
    consumer = Consumer("reader", broker, group_id="readers", offset_store=store)
    consumer.assign("logs", [0])

    # Read first 5.
    result = consumer.poll(max_records=5)
    records = result.get(("logs", 0), [])
    print(f"First poll: {len(records)} records (offsets {records[0].offset}-{records[-1].offset})")
    print(f"  Position: {consumer.position('logs', 0)}")
    consumer.commit()
    print(f"  Committed offset: {consumer.committed('logs', 0)}")

    # Read next 5.
    result = consumer.poll(max_records=5)
    records = result.get(("logs", 0), [])
    print(f"\nSecond poll: {len(records)} records (offsets {records[0].offset}-{records[-1].offset})")

    # Seek back to beginning.
    consumer.seek("logs", 0, 0)
    print(f"\nAfter seek(0): position={consumer.position('logs', 0)}")
    result = consumer.poll(max_records=3)
    records = result.get(("logs", 0), [])
    print(f"Re-read: {len(records)} records starting at offset {records[0].offset}")


def demo_replication() -> None:
    separator("4. Replication and ISR")

    # ACK=ALL: synchronous replication.
    print("--- ACK=ALL (synchronous replication) ---")
    rp = ReplicatedPartition(
        topic="critical-data",
        partition_id=0,
        leader_broker_id=0,
        follower_broker_ids=[1, 2],
        ack_mode=AckMode.ALL,
    )
    rp.produce("k1", b"important-msg-1")
    rp.produce("k2", b"important-msg-2")

    print(f"ISR: {rp.isr}")
    print(f"High watermark: {rp.high_watermark}")
    print(f"Leader LEO: {rp.leader.leo}")
    for bid, f in rp.followers.items():
        print(f"  Follower {bid} LEO: {f.leo}")

    committed = rp.consume(0)
    print(f"Committed records: {len(committed)}")

    # ACK=1: leader-only acknowledgement.
    print("\n--- ACK=1 (leader-only) ---")
    rp2 = ReplicatedPartition(
        topic="less-critical",
        partition_id=0,
        leader_broker_id=0,
        follower_broker_ids=[1],
        ack_mode=AckMode.LEADER,
    )
    rp2.produce(None, b"msg-a")
    rp2.produce(None, b"msg-b")
    print(f"Leader LEO: {rp2.leader.leo}")
    print(f"Follower LEO: {rp2.followers[1].leo}")
    print(f"High watermark (before fetch): {rp2.high_watermark}")

    rp2.follower_fetch(1)
    print(f"High watermark (after fetch): {rp2.high_watermark}")

    # ACK=0: fire and forget.
    print("\n--- ACK=0 (fire and forget) ---")
    rp3 = ReplicatedPartition(
        topic="metrics",
        partition_id=0,
        leader_broker_id=0,
        follower_broker_ids=[1],
        ack_mode=AckMode.NONE,
    )
    rp3.produce(None, b"metric-1")
    print(f"Leader LEO: {rp3.leader.leo}")
    print(f"Follower LEO (not replicated): {rp3.followers[1].leo}")


def demo_delivery_semantics() -> None:
    separator("5. Delivery Semantics")

    broker = Broker()
    broker.create_topic("tx", TopicConfig(num_partitions=1))
    broker.produce("tx", 0, None, b"payment-1")
    broker.produce("tx", 0, None, b"payment-2")

    # At-most-once: auto-commit before processing.
    print("--- At-most-once ---")
    store1 = OffsetStore()
    c1 = Consumer("c1", broker, group_id="g1", auto_commit=True, offset_store=store1)
    c1.assign("tx", [0])
    c1.poll()
    print(f"  Committed offset (auto): {c1.committed('tx', 0)}")
    result = c1.poll()
    print(f"  Re-poll: {len(result)} partitions with data (no redelivery)")

    # At-least-once: manual commit after processing.
    print("\n--- At-least-once ---")
    store2 = OffsetStore()
    c2 = Consumer("c2", broker, group_id="g2", offset_store=store2)
    c2.assign("tx", [0])
    c2.poll()
    print(f"  Position: {c2.position('tx', 0)}, Committed: {c2.committed('tx', 0)}")
    # Simulate crash -- don't commit. New consumer resumes from committed.
    c3 = Consumer("c3", broker, group_id="g2", offset_store=store2)
    c3.assign("tx", [0])
    result = c3.poll()
    redelivered = result.get(("tx", 0), [])
    print(f"  Redelivered: {len(redelivered)} records (messages re-processed)")


def main() -> None:
    print("Distributed Message Queue Demo")
    print("=" * 60)

    demo_basic_produce_consume()
    demo_consumer_groups()
    demo_offset_tracking()
    demo_replication()
    demo_delivery_semantics()

    separator("Done!")


if __name__ == "__main__":
    main()
