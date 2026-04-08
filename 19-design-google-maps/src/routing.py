"""A* and Dijkstra pathfinding on the road graph.

Both algorithms return a list of node IDs representing the shortest path
and the total distance in kilometres.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass

from .graph import RoadGraph, haversine


@dataclass
class RouteResult:
    """Result of a pathfinding query."""

    path: list[str]
    distance_km: float
    nodes_explored: int


def dijkstra(graph: RoadGraph, start: str, end: str) -> RouteResult | None:
    """Classic Dijkstra shortest-path by distance."""
    if not graph.has_node(start) or not graph.has_node(end):
        return None

    dist: dict[str, float] = {start: 0.0}
    prev: dict[str, str | None] = {start: None}
    visited: set[str] = set()
    heap: list[tuple[float, str]] = [(0.0, start)]
    explored = 0

    while heap:
        cost, node = heapq.heappop(heap)
        if node in visited:
            continue
        visited.add(node)
        explored += 1

        if node == end:
            return RouteResult(
                path=_reconstruct(prev, end),
                distance_km=cost,
                nodes_explored=explored,
            )

        for edge in graph.get_neighbors(node):
            if edge.dst in visited:
                continue
            new_cost = cost + edge.distance_km
            if new_cost < dist.get(edge.dst, float("inf")):
                dist[edge.dst] = new_cost
                prev[edge.dst] = node
                heapq.heappush(heap, (new_cost, edge.dst))

    return None  # no path


def astar(graph: RoadGraph, start: str, end: str) -> RouteResult | None:
    """A* pathfinding using Haversine distance as the heuristic."""
    if not graph.has_node(start) or not graph.has_node(end):
        return None

    goal_node = graph.get_node(end)

    g_score: dict[str, float] = {start: 0.0}
    prev: dict[str, str | None] = {start: None}
    visited: set[str] = set()
    explored = 0

    h_start = haversine(graph.get_node(start), goal_node)
    heap: list[tuple[float, str]] = [(h_start, start)]

    while heap:
        _f, node = heapq.heappop(heap)
        if node in visited:
            continue
        visited.add(node)
        explored += 1

        if node == end:
            return RouteResult(
                path=_reconstruct(prev, end),
                distance_km=g_score[end],
                nodes_explored=explored,
            )

        for edge in graph.get_neighbors(node):
            if edge.dst in visited:
                continue
            tentative_g = g_score[node] + edge.distance_km
            if tentative_g < g_score.get(edge.dst, float("inf")):
                g_score[edge.dst] = tentative_g
                prev[edge.dst] = node
                h = haversine(graph.get_node(edge.dst), goal_node)
                heapq.heappush(heap, (tentative_g + h, edge.dst))

    return None  # no path


def _reconstruct(prev: dict[str, str | None], end: str) -> list[str]:
    """Walk backwards through the prev map to build the path."""
    path: list[str] = []
    cur: str | None = end
    while cur is not None:
        path.append(cur)
        cur = prev[cur]
    path.reverse()
    return path
