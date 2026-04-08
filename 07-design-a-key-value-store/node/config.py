"""Configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    node_id: str = field(default_factory=lambda: os.getenv("NODE_ID", "node1"))
    cluster_nodes: list[str] = field(default_factory=lambda: _parse_cluster_nodes())
    replication_factor: int = field(
        default_factory=lambda: int(os.getenv("REPLICATION_FACTOR", "3"))
    )
    write_quorum: int = field(
        default_factory=lambda: int(os.getenv("WRITE_QUORUM", "2"))
    )
    read_quorum: int = field(
        default_factory=lambda: int(os.getenv("READ_QUORUM", "2"))
    )
    gossip_interval: float = field(
        default_factory=lambda: float(os.getenv("GOSSIP_INTERVAL", "1"))
    )
    memtable_threshold: int = field(
        default_factory=lambda: int(os.getenv("MEMTABLE_THRESHOLD", "100"))
    )
    data_dir: str = field(
        default_factory=lambda: os.getenv("DATA_DIR", "/data")
    )
    port: int = field(
        default_factory=lambda: int(os.getenv("PORT", "8000"))
    )


def _parse_cluster_nodes() -> list[str]:
    raw = os.getenv("CLUSTER_NODES", "node1:8000,node2:8000,node3:8000")
    return [n.strip() for n in raw.split(",") if n.strip()]


def get_config() -> Config:
    return Config()
