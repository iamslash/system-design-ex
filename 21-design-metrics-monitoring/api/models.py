"""Pydantic models for the metrics monitoring system."""

from __future__ import annotations

import time
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class MetricPoint(BaseModel):
    """A single metric data point received via push endpoint."""

    name: str = Field(..., description="Metric name, e.g. cpu.load")
    labels: dict[str, str] = Field(default_factory=dict, description="Key-value labels")
    value: float = Field(..., description="Metric value")
    timestamp: Optional[float] = Field(default=None, description="Unix timestamp; auto-filled if omitted")

    def effective_timestamp(self) -> float:
        return self.timestamp if self.timestamp is not None else time.time()


class MetricBatch(BaseModel):
    """Batch of metric data points."""

    metrics: list[MetricPoint]


class AggregationType(str, Enum):
    """Supported aggregation functions."""

    AVG = "avg"
    MAX = "max"
    MIN = "min"
    SUM = "sum"
    COUNT = "count"


class QueryRequest(BaseModel):
    """Parameters for a time-series range query."""

    name: str = Field(..., description="Metric name")
    labels: dict[str, str] = Field(default_factory=dict)
    start: float = Field(..., description="Range start (unix timestamp)")
    end: Optional[float] = Field(default=None, description="Range end (unix timestamp); defaults to now")
    aggregation: Optional[AggregationType] = Field(default=None, description="Aggregation function")
    downsample: Optional[int] = Field(default=None, description="Downsample bucket size in seconds")


class QueryResult(BaseModel):
    """Result of a time-series query."""

    name: str
    labels: dict[str, str]
    data_points: list[dict]
    aggregation: Optional[AggregationType] = None
    aggregated_value: Optional[float] = None


class AlertSeverity(str, Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class ComparisonOperator(str, Enum):
    """Comparison operators for alert rules."""

    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    EQ = "eq"
    NEQ = "neq"


class AlertRule(BaseModel):
    """Definition of an alert rule."""

    id: Optional[str] = None
    name: str = Field(..., description="Alert rule name")
    metric_name: str = Field(..., description="Metric to watch")
    labels: dict[str, str] = Field(default_factory=dict)
    operator: ComparisonOperator = Field(..., description="Comparison operator")
    threshold: float = Field(..., description="Threshold value")
    duration: int = Field(default=60, description="Evaluation window in seconds")
    severity: AlertSeverity = Field(default=AlertSeverity.WARNING)
    notification_channels: list[str] = Field(default_factory=lambda: ["email"])


class AlertStatus(str, Enum):
    """Current status of an alert."""

    FIRING = "firing"
    RESOLVED = "resolved"


class Alert(BaseModel):
    """An active or resolved alert instance."""

    id: str
    rule_id: str
    rule_name: str
    metric_name: str
    labels: dict[str, str]
    status: AlertStatus
    value: float
    threshold: float
    severity: AlertSeverity
    fired_at: float
    resolved_at: Optional[float] = None
    notification_channels: list[str] = Field(default_factory=list)
