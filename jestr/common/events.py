"""Typed pipeline events, EventBus, and EventHandler protocol."""

from dataclasses import dataclass
from typing import Any, Protocol

from jestr.contracts.manifest import ChangeManifest


@dataclass(frozen=True)
class PipelineStarted:
    """Event emitted when a pipeline run starts."""

    pipeline_name: str
    source_type: str
    resource_name: str
    batch_date: str


@dataclass(frozen=True)
class PipelineCompleted:
    """Event emitted when a pipeline run completes successfully."""

    pipeline_name: str
    duration_sec: float
    validation_metrics: dict[str, Any]


@dataclass(frozen=True)
class PipelineFailed:
    """Event emitted when a pipeline run fails due to an unhandled exception."""

    pipeline_name: str
    error: Exception


@dataclass(frozen=True)
class SchemaEvolved:
    """Event emitted when a schema evolution is detected during a pipeline run."""

    manifest: ChangeManifest


PipelineEvent = PipelineStarted | PipelineCompleted | PipelineFailed | SchemaEvolved


class EventHandler(Protocol):
    """Protocol that all pipeline event handlers must satisfy."""

    def handle(self, event: PipelineEvent) -> None:
        """Handle a pipeline event."""
        ...


class EventBus:
    """Simple synchronous event bus. Handlers are called in registration order."""

    def __init__(self, handlers: list[EventHandler] | None = None) -> None:
        """Initialise the event bus with an optional list of pre-registered handlers."""
        self._handlers: list[EventHandler] = handlers or []

    def register(self, handler: EventHandler) -> None:
        """Append a handler to the bus."""
        self._handlers.append(handler)

    def emit(self, event: PipelineEvent) -> None:
        """Dispatch a pipeline event to all registered handlers in order."""
        for handler in self._handlers:
            handler.handle(event)
