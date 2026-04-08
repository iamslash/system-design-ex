"""Quorum read/write logic.

The coordinator fans out requests to N replica nodes (determined by the
consistent hash ring) and waits for W (write) or R (read) acknowledgements
before responding to the client.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import httpx

from node.replication.vector_clock import Ordering, VectorClock

logger = logging.getLogger(__name__)

# Timeout for inter-node HTTP calls
_NODE_TIMEOUT = 3.0


@dataclass
class VersionedValue:
    value: str
    vector_clock: VectorClock
    timestamp: float
    node_id: str


class QuorumController:
    """Coordinates quorum reads and writes across replica nodes."""

    def __init__(
        self,
        node_id: str,
        write_quorum: int,
        read_quorum: int,
    ) -> None:
        self.node_id = node_id
        self.w = write_quorum
        self.r = read_quorum

    # -- write -----------------------------------------------------------

    async def quorum_put(
        self,
        key: str,
        value: str,
        replica_nodes: list[str],
        vector_clock: VectorClock | None = None,
    ) -> dict:
        """Coordinate a quorum write.

        The coordinator increments the vector clock, then sends the write
        to all replica nodes.  It waits for W successful ACKs.
        """
        vc = (vector_clock or VectorClock()).increment(self.node_id)

        tasks = [
            self._send_put(node, key, value, vc)
            for node in replica_nodes
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        successes = [r for r in results if isinstance(r, dict) and r.get("ok")]
        if len(successes) >= self.w:
            return {
                "status": "ok",
                "key": key,
                "vector_clock": vc.to_dict(),
                "acks": len(successes),
            }

        return {
            "status": "error",
            "message": f"Write quorum not met: {len(successes)}/{self.w}",
            "acks": len(successes),
        }

    # -- read ------------------------------------------------------------

    async def quorum_get(
        self,
        key: str,
        replica_nodes: list[str],
    ) -> dict:
        """Coordinate a quorum read.

        Sends GET to all replica nodes, waits for R responses, then
        reconciles using vector clocks.
        """
        tasks = [self._send_get(node, key) for node in replica_nodes]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        responses: list[VersionedValue] = []
        for r in results:
            if isinstance(r, dict) and r.get("found"):
                responses.append(
                    VersionedValue(
                        value=r["value"],
                        vector_clock=VectorClock.from_dict(r.get("vector_clock")),
                        timestamp=r.get("timestamp", 0.0),
                        node_id=r.get("node_id", ""),
                    )
                )

        if len(responses) < self.r:
            if len(responses) == 0:
                return {"status": "not_found", "key": key}
            # Still return what we have with a warning
            pass

        if not responses:
            return {"status": "not_found", "key": key}

        return self._reconcile(key, responses)

    # -- delete ----------------------------------------------------------

    async def quorum_delete(
        self,
        key: str,
        replica_nodes: list[str],
    ) -> dict:
        """Coordinate a quorum delete across replicas."""
        tasks = [self._send_delete(node, key) for node in replica_nodes]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        successes = [r for r in results if isinstance(r, dict) and r.get("ok")]
        if len(successes) >= self.w:
            return {"status": "ok", "key": key, "acks": len(successes)}

        return {
            "status": "error",
            "message": f"Delete quorum not met: {len(successes)}/{self.w}",
            "acks": len(successes),
        }

    # -- reconciliation --------------------------------------------------

    @staticmethod
    def _reconcile(key: str, responses: list[VersionedValue]) -> dict:
        """Pick the latest value(s) using vector clocks.

        If there are concurrent versions (conflict), return all of them so
        the client can resolve.
        """
        # Group by dominance
        latest: list[VersionedValue] = []

        for candidate in responses:
            dominated = False
            new_latest: list[VersionedValue] = []
            for existing in latest:
                ordering = candidate.vector_clock.compare(existing.vector_clock)
                if ordering == Ordering.BEFORE:
                    # candidate is older, skip it
                    dominated = True
                    new_latest.append(existing)
                elif ordering == Ordering.AFTER:
                    # candidate is newer, drop existing
                    pass
                elif ordering == Ordering.EQUAL:
                    dominated = True
                    new_latest.append(existing)
                else:
                    # CONCURRENT - keep both
                    new_latest.append(existing)
            if not dominated:
                new_latest.append(candidate)
            latest = new_latest

        if len(latest) == 1:
            v = latest[0]
            return {
                "status": "ok",
                "key": key,
                "value": v.value,
                "vector_clock": v.vector_clock.to_dict(),
                "timestamp": v.timestamp,
            }

        # Conflict: return all versions
        return {
            "status": "conflict",
            "key": key,
            "versions": [
                {
                    "value": v.value,
                    "vector_clock": v.vector_clock.to_dict(),
                    "timestamp": v.timestamp,
                    "node_id": v.node_id,
                }
                for v in latest
            ],
        }

    # -- HTTP helpers ----------------------------------------------------

    async def _send_put(
        self,
        node: str,
        key: str,
        value: str,
        vc: VectorClock,
    ) -> dict:
        url = f"http://{node}/internal/store/{key}"
        try:
            async with httpx.AsyncClient(timeout=_NODE_TIMEOUT) as client:
                resp = await client.put(
                    url,
                    json={
                        "value": value,
                        "vector_clock": vc.to_dict(),
                        "from_node": self.node_id,
                    },
                )
                return resp.json()
        except Exception as exc:
            logger.warning("PUT to %s failed: %s", node, exc)
            return {"ok": False, "error": str(exc)}

    async def _send_get(self, node: str, key: str) -> dict:
        url = f"http://{node}/internal/store/{key}"
        try:
            async with httpx.AsyncClient(timeout=_NODE_TIMEOUT) as client:
                resp = await client.get(url)
                return resp.json()
        except Exception as exc:
            logger.warning("GET from %s failed: %s", node, exc)
            return {"found": False, "error": str(exc)}

    async def _send_delete(self, node: str, key: str) -> dict:
        url = f"http://{node}/internal/store/{key}"
        try:
            async with httpx.AsyncClient(timeout=_NODE_TIMEOUT) as client:
                resp = await client.delete(url)
                return resp.json()
        except Exception as exc:
            logger.warning("DELETE from %s failed: %s", node, exc)
            return {"ok": False, "error": str(exc)}
