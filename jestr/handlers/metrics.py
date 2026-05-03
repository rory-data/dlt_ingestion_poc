"""Metrics handler: accumulates pipelines metric in memory across events."""

from typing import Any

from jestr.common.events import (
    PipelineCompleted,
    PipelineEvent,
    PipelineFailed,
    PipelineStarted,
)


class MetricsHandler:
    """In-memory accumulator for pipeline lifecycle metrics.

    Stores the latest state from each event type.
    Does not replace :class: `~jestr.engines.dlt.transformers.PipelineMetricsCollector` which is pending refactor.
    """

    def __init__(self) -> None:
        """Initialise an empty metrics store."""
        self._metrics: dict[str, Any] = {}

    @property
    def metrics(self) -> dict[str, Any]:
        """Return the current accumulated metrics (read-only)."""
        return dict(self._metrics)

    def handle(self, event: PipelineEvent) -> None:
        """Update internal metrics state based on the type of *event* type."""
        if isinstance(event, PipelineStarted):
            self._metrics = {
                "pipeline_name": event.pipeline_name,
                "source_type": event.source_type,
                "resource_name": event.resource_name,
                "batch_date": event.batch_date,
                "status": "running",
            }
        elif isinstance(event, PipelineCompleted):
            self._metrics.update(
                {
                    "status": "completed",
                    "duration_sec": event.duration_sec,
                    "validation_metrics": event.validation_metrics,
                }
            )
        elif isinstance(event, PipelineFailed):
            self._metrics.update(
                {
                    "status": "failed",
                    "error": str(event.error),
                    "error_type": type(event.error).__name__,
                }
            )
        # SchemaEvolved: no-op.
