"""FastAPI node server for the distributed key-value store.

Each node exposes:
  - Public API  (client-facing, performs quorum coordination)
  - Internal API (used by other nodes for replica operations + gossip)
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from node.config import get_config
from node.gossip.protocol import GossipProtocol
from node.replication.consistent_hash import ConsistentHashRing
from node.replication.quorum import QuorumController
from node.replication.vector_clock import VectorClock
from node.store.engine import StorageEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global state (initialised in lifespan)
# ---------------------------------------------------------------------------
config = get_config()

engine: StorageEngine
ring: ConsistentHashRing
quorum: QuorumController
gossip: GossipProtocol

# Per-key vector clocks stored alongside values in this node
_vector_clocks: dict[str, VectorClock] = {}


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine, ring, quorum, gossip

    logger.info("Node %s starting – cluster: %s", config.node_id, config.cluster_nodes)

    engine = StorageEngine(
        data_dir=config.data_dir,
        memtable_threshold=config.memtable_threshold,
    )

    ring = ConsistentHashRing(virtual_nodes=150)
    for node_addr in config.cluster_nodes:
        ring.add_node(node_addr)

    quorum = QuorumController(
        node_id=config.node_id,
        write_quorum=config.write_quorum,
        read_quorum=config.read_quorum,
    )

    own_address = f"{config.node_id}:{config.port}"
    gossip = GossipProtocol(
        node_id=config.node_id,
        address=own_address,
        cluster_nodes=config.cluster_nodes,
        interval=config.gossip_interval,
    )
    gossip.start()

    yield

    await gossip.stop()


app = FastAPI(title="Distributed KV Store Node", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------
class PutRequest(BaseModel):
    value: str


class InternalPutRequest(BaseModel):
    value: str
    vector_clock: dict[str, int] | None = None
    from_node: str | None = None


class GossipPayload(BaseModel):
    from_node: str
    members: dict[str, Any]


# ---------------------------------------------------------------------------
# Public API (client-facing, coordinator role)
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {
        "node_id": config.node_id,
        "status": "ok",
        "members": gossip.membership_info(),
    }


@app.put("/store/{key}")
async def put_key(key: str, body: PutRequest):
    replicas = ring.get_replica_nodes(key, config.replication_factor)
    if not replicas:
        raise HTTPException(status_code=503, detail="No nodes available")

    # Read existing VC for this key (if any) to build upon
    existing_vc = _vector_clocks.get(key)
    result = await quorum.quorum_put(key, body.value, replicas, existing_vc)

    if result.get("status") == "error":
        raise HTTPException(status_code=503, detail=result.get("message"))
    return result


@app.get("/store/{key}")
async def get_key(key: str):
    replicas = ring.get_replica_nodes(key, config.replication_factor)
    if not replicas:
        raise HTTPException(status_code=503, detail="No nodes available")

    result = await quorum.quorum_get(key, replicas)
    if result.get("status") == "not_found":
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return result


@app.delete("/store/{key}")
async def delete_key(key: str):
    replicas = ring.get_replica_nodes(key, config.replication_factor)
    if not replicas:
        raise HTTPException(status_code=503, detail="No nodes available")

    result = await quorum.quorum_delete(key, replicas)
    if result.get("status") == "error":
        raise HTTPException(status_code=503, detail=result.get("message"))
    return result


@app.get("/store")
async def list_keys():
    return {"node_id": config.node_id, "keys": engine.keys()}


@app.get("/cluster/info")
async def cluster_info():
    return {
        "node_id": config.node_id,
        "ring": ring.ring_info(),
        "members": gossip.membership_info(),
        "config": {
            "replication_factor": config.replication_factor,
            "write_quorum": config.write_quorum,
            "read_quorum": config.read_quorum,
        },
    }


# ---------------------------------------------------------------------------
# Internal API (node-to-node, used by quorum controller)
# ---------------------------------------------------------------------------
@app.put("/internal/store/{key}")
async def internal_put(key: str, body: InternalPutRequest):
    """Replica-level PUT — store the value locally."""
    vc = VectorClock.from_dict(body.vector_clock)
    ts = engine.put(key, body.value)
    _vector_clocks[key] = vc
    return {"ok": True, "node_id": config.node_id, "timestamp": ts}


@app.get("/internal/store/{key}")
async def internal_get(key: str):
    """Replica-level GET — return local value + vector clock."""
    stored = engine.get(key)
    if stored is None:
        return {"found": False, "node_id": config.node_id}
    vc = _vector_clocks.get(key, VectorClock())
    return {
        "found": True,
        "node_id": config.node_id,
        "value": stored.value,
        "vector_clock": vc.to_dict(),
        "timestamp": stored.timestamp,
    }


@app.delete("/internal/store/{key}")
async def internal_delete(key: str):
    """Replica-level DELETE — tombstone locally."""
    ts = engine.delete(key)
    _vector_clocks.pop(key, None)
    return {"ok": True, "node_id": config.node_id, "timestamp": ts}


@app.post("/internal/gossip")
async def internal_gossip(payload: GossipPayload):
    """Receive gossip heartbeat from a peer."""
    gossip.receive_gossip(payload.from_node, payload.members)
    return {"ok": True}
