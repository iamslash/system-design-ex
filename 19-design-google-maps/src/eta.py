"""ETA estimation with traffic conditions.

Computes estimated time of arrival based on road distance, speed limits,
and a per-edge traffic multiplier (1.0 = free-flow, >1.0 = congestion).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .graph import Edge, RoadGraph


@dataclass
class TrafficCondition:
    """Traffic state for a specific road segment."""

    src: str
    dst: str
    multiplier: float  # 1.0 = normal, 2.0 = double travel time


class TrafficModel:
    """Manages traffic conditions across the road network."""

    def __init__(self) -> None:
        self._conditions: dict[tuple[str, str], float] = {}

    def set_traffic(self, src: str, dst: str, multiplier: float) -> None:
        """Set traffic multiplier for a road segment."""
        if multiplier <= 0:
            raise ValueError("Traffic multiplier must be positive")
        self._conditions[(src, dst)] = multiplier

    def get_multiplier(self, src: str, dst: str) -> float:
        """Get traffic multiplier for a road segment (default 1.0)."""
        return self._conditions.get((src, dst), 1.0)

    def clear(self) -> None:
        """Reset all traffic conditions to normal."""
        self._conditions.clear()

    @property
    def congested_segments(self) -> list[tuple[str, str, float]]:
        """Return all segments with non-default traffic."""
        return [(s, d, m) for (s, d), m in self._conditions.items() if m != 1.0]


@dataclass
class ETAResult:
    """Result of an ETA calculation."""

    path: list[str]
    total_distance_km: float
    total_time_hours: float
    total_time_minutes: float
    segment_times: list[float]  # minutes per segment

    @property
    def average_speed_kmh(self) -> float:
        if self.total_time_hours == 0:
            return 0.0
        return self.total_distance_km / self.total_time_hours


def compute_eta(
    graph: RoadGraph,
    path: list[str],
    traffic: TrafficModel | None = None,
) -> ETAResult:
    """Compute ETA for a given path through the road network.

    For each segment along *path*, travel time is:

        time = distance / speed_limit * traffic_multiplier
    """
    if len(path) < 2:
        return ETAResult(
            path=path,
            total_distance_km=0.0,
            total_time_hours=0.0,
            total_time_minutes=0.0,
            segment_times=[],
        )

    total_dist = 0.0
    total_time = 0.0  # hours
    seg_times: list[float] = []

    for i in range(len(path) - 1):
        src, dst = path[i], path[i + 1]
        edge = _find_edge(graph, src, dst)
        if edge is None:
            raise ValueError(f"No edge from '{src}' to '{dst}' in graph")

        multiplier = traffic.get_multiplier(src, dst) if traffic else 1.0
        travel_hours = (edge.distance_km / edge.speed_limit_kmh) * multiplier

        total_dist += edge.distance_km
        total_time += travel_hours
        seg_times.append(travel_hours * 60)  # convert to minutes

    return ETAResult(
        path=path,
        total_distance_km=total_dist,
        total_time_hours=total_time,
        total_time_minutes=total_time * 60,
        segment_times=seg_times,
    )


def _find_edge(graph: RoadGraph, src: str, dst: str) -> Edge | None:
    """Find the edge from src to dst."""
    for edge in graph.get_neighbors(src):
        if edge.dst == dst:
            return edge
    return None
