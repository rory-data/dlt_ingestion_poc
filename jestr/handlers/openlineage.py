"""OpenLineage handler: routes pipeline events to the telemetry reporter."""

from typing import TYPE_CHECKING

from jestr.common.events import (
    PipelineCompleted,
    PipelineEvent,
    PipelineFailed,
    PipelineStarted,
)

if TYPE_CHECKING:
    from jestr.telemetry.reporter import PipelineReporter


class OpenLineageHandler:
    """Routes pipeline events to a :class:`~jestr.telemetry.reporter.PipelineReporter` instance."""

    def __init__(self, reporter: "PipelineReporter") -> None:
        """Initialise with a :class:`~jestr.telemetry.reporter.PipelineReporter` instance."""
        self.reporter = reporter

    def handle(self, event: PipelineEvent) -> None:
        """Route *event* to the appropriate method on the telemetry reporter."""
        if isinstance(event, PipelineStarted):
            self.reporter.emit_start()
        elif isinstance(event, PipelineCompleted):
            self.reporter.emit_complete()
        elif isinstance(event, PipelineFailed):
            self.reporter.emit_fail(event.error)
        # SchemaEvolved: no-op for now.
