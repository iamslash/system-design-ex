"""Gossip-based failure detection.

Each node maintains a membership list with heartbeat counters.  A background
asyncio task periodically sends its own heartbeat to a random peer.  Peers
that have not been heard from are marked *suspected* and eventually *down*.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum

import httpx

logger = logging.getLogger(__name__)


class NodeStatus(str, Enum):
    ALIVE = "alive"
    SUSPECTED = "suspected"
    DOWN = "down"


@dataclass
class MemberInfo:
    node_id: str
    address: str  # host:port
    heartbeat: int = 0
    last_updated: float = field(default_factory=time.time)
    status: NodeStatus = NodeStatus.ALIVE


class GossipProtocol:
    """Simple push-based gossip protocol."""

    SUSPECT_TIMEOUT = 5.0   # seconds without heartbeat -> suspected
    DOWN_TIMEOUT = 10.0     # seconds suspected -> down

    def __init__(
        self,
        node_id: str,
        address: str,
        cluster_nodes: list[str],
        interval: float = 1.0,
    ) -> None:
        self.node_id = node_id
        self.address = address
        self.interval = interval
        self.members: dict[str, MemberInfo] = {}
        self._heartbeat_counter = 0
        self._task: asyncio.Task[None] | None = None

        # Bootstrap membership list from known cluster nodes
        for peer in cluster_nodes:
            # peer format: "host:port"
            peer_id = peer.split(":")[0]
            self.members[peer_id] = MemberInfo(
                node_id=peer_id,
                address=peer,
                last_updated=time.time(),
            )

        # Ensure self is in the list
        self.members[node_id] = MemberInfo(
            node_id=node_id,
            address=address,
            last_updated=time.time(),
        )

    # -- lifecycle -------------------------------------------------------

    def start(self) -> None:
        """Start the background gossip loop."""
        if self._task is None:
            self._task = asyncio.create_task(self._gossip_loop())
            logger.info("[gossip] started for %s", self.node_id)

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # -- gossip loop -----------------------------------------------------

    async def _gossip_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.interval)
                self._heartbeat_counter += 1
                # Update own entry
                self.members[self.node_id].heartbeat = self._heartbeat_counter
                self.members[self.node_id].last_updated = time.time()
                self.members[self.node_id].status = NodeStatus.ALIVE

                self._update_statuses()
                await self._send_heartbeat_to_random_peer()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[gossip] loop error")

    def _update_statuses(self) -> None:
        now = time.time()
        for mid, member in self.members.items():
            if mid == self.node_id:
                continue
            elapsed = now - member.last_updated
            if member.status == NodeStatus.DOWN:
                continue
            if elapsed > self.DOWN_TIMEOUT:
                if member.status != NodeStatus.DOWN:
                    logger.warning("[gossip] %s marked DOWN", mid)
                    member.status = NodeStatus.DOWN
            elif elapsed > self.SUSPECT_TIMEOUT:
                if member.status != NodeStatus.SUSPECTED:
                    logger.warning("[gossip] %s marked SUSPECTED", mid)
                    member.status = NodeStatus.SUSPECTED

    async def _send_heartbeat_to_random_peer(self) -> None:
        peers = [
            m
            for m in self.members.values()
            if m.node_id != self.node_id and m.status != NodeStatus.DOWN
        ]
        if not peers:
            return
        peer = random.choice(peers)
        payload = {
            mid: {
                "heartbeat": m.heartbeat,
                "status": m.status.value,
            }
            for mid, m in self.members.items()
        }
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                await client.post(
                    f"http://{peer.address}/internal/gossip",
                    json={"from_node": self.node_id, "members": payload},
                )
        except Exception:
            logger.debug("[gossip] failed to reach %s", peer.node_id)

    # -- incoming gossip -------------------------------------------------

    def receive_gossip(self, from_node: str, members: dict) -> None:
        """Merge incoming gossip data."""
        now = time.time()
        for mid, info in members.items():
            incoming_hb = info.get("heartbeat", 0)
            existing = self.members.get(mid)
            if existing is None:
                # New node discovered
                # We don't know its address yet; use mid:8000 as convention
                self.members[mid] = MemberInfo(
                    node_id=mid,
                    address=f"{mid}:8000",
                    heartbeat=incoming_hb,
                    last_updated=now,
                    status=NodeStatus.ALIVE,
                )
            elif incoming_hb > existing.heartbeat:
                existing.heartbeat = incoming_hb
                existing.last_updated = now
                if existing.status in (NodeStatus.SUSPECTED, NodeStatus.DOWN):
                    logger.info("[gossip] %s revived", mid)
                existing.status = NodeStatus.ALIVE

    # -- queries ---------------------------------------------------------

    def alive_nodes(self) -> list[str]:
        """Return addresses of nodes considered alive."""
        return [
            m.address
            for m in self.members.values()
            if m.status == NodeStatus.ALIVE
        ]

    def membership_info(self) -> list[dict]:
        return [
            {
                "node_id": m.node_id,
                "address": m.address,
                "heartbeat": m.heartbeat,
                "status": m.status.value,
            }
            for m in self.members.values()
        ]
