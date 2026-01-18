"""Base protocols for pipeline execution engines.

This module defines interfaces for orchestrating data pipelines.
Engines coordinate readers, validators, transformers, and writers.

Design Principles:
- Separation of orchestration from execution
- All data flows through Arrow Tables
- Clear state management and error handling
- Support both batch and streaming execution
"""

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

import pyarrow as pa


class ExecutionMode(str, Enum):
    """Execution modes supported by pipeline engines."""

    BATCH = "batch"
    """Process all data at once."""

    STREAMING = "streaming"
    """Process data in batches/micro-batches."""


class ExecutionStatus(str, Enum):
    """Status of pipeline execution."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


@dataclass(frozen=True)
class ExecutionResult:
    """Immutable result of pipeline execution.

    Attributes:
        status: Overall execution status
        rows_processed: Total rows processed
        rows_clean: Rows that passed validation
        rows_bad: Rows that failed validation
        error: Error message if failed (None if success)
        metadata: Additional execution metadata
    """

    status: ExecutionStatus
    rows_processed: int = 0
    rows_clean: int = 0
    rows_bad: int = 0
    error: str | None = None
    metadata: dict[str, str | int | float] | None = None

    @property
    def success(self) -> bool:
        """Check if execution succeeded."""
        return self.status == ExecutionStatus.SUCCESS

    @property
    def failed(self) -> bool:
        """Check if execution failed."""
        return self.status == ExecutionStatus.FAILED

    @property
    def clean_percentage(self) -> float:
        """Get percentage of rows that passed validation."""
        total = self.rows_clean + self.rows_bad
        if total == 0:
            return 0.0
        return (self.rows_clean / total) * 100


class PipelineEngine(Protocol):
    """Protocol for pipeline execution engines.

    Engines orchestrate the flow of data through readers, validators,
    transformers, and writers. Implementations should:

    1. Support configurable stages (read → validate → transform → write)
    2. Handle errors gracefully with informative messages
    3. Track state and progress
    4. Support both batch and streaming execution

    Example:
        >>> engine = DltEngine()
        >>> result = engine.execute(
        ...     reader=my_reader,
        ...     validators=[utf8_validator, schema_validator],
        ...     transformer=my_transformer,
        ...     writer=my_writer,
        ... )
        >>> print(f"Status: {result.status}, Clean: {result.rows_clean}")
    """

    def execute(
        self,
        reader,  # Reader protocol
        validators: list | None = None,
        transformer: Callable[[pa.Table], pa.Table] | None = None,
        writer=None,  # Writer protocol
        mode: ExecutionMode = ExecutionMode.BATCH,
    ) -> ExecutionResult:
        """Execute a complete data pipeline.

        Args:
            reader: Data reader (implements Reader protocol)
            validators: List of validators (implement Validator protocol)
            transformer: Optional transformation function
            writer: Data writer (implements Writer protocol)
            mode: Execution mode (batch or streaming)

        Returns:
            ExecutionResult with status and metrics

        Raises:
            ValueError: If configuration is invalid
            Exception: For execution errors (may return FAILED result instead)
        """
        ...


class StreamingEngine(Protocol):
    """Protocol for streaming pipeline engines.

    For continuous or near-real-time data processing.
    Processes data as it arrives in micro-batches.

    Example:
        >>> engine = StreamingEngine()
        >>> for result in engine.execute_stream(
        ...     reader=streaming_reader,
        ...     validators=[validator],
        ...     writer=writer,
        ... ):
        ...     print(f"Batch: {result.rows_processed} rows")
    """

    def execute_stream(
        self,
        reader,  # StreamingReader protocol
        validators: list | None = None,
        transformer: Callable[[pa.Table], pa.Table] | None = None,
        writer=None,  # Writer protocol
        batch_size: int = 50_000,
    ) -> list[ExecutionResult]:
        """Execute pipeline with streaming input.

        Args:
            reader: Streaming data reader
            validators: List of validators
            transformer: Optional transformation function
            writer: Data writer
            batch_size: Batch size for processing

        Yields:
            ExecutionResult for each batch

        Raises:
            ValueError: If configuration is invalid
        """
        ...


class PipelineBuilder(Protocol):
    """Protocol for building pipelines declaratively.

    Builder pattern for complex pipeline construction.

    Example:
        >>> builder = PipelineBuilder()
        >>> engine = builder.with_reader(reader)
        ...     .add_validator(utf8_validator)
        ...     .add_validator(schema_validator)
        ...     .with_transformer(my_transformer)
        ...     .with_writer(writer)
        ...     .build()
        >>> result = engine.execute()
    """

    def with_reader(self, reader) -> "PipelineBuilder":
        """Set the data reader.

        Args:
            reader: Reader implementation

        Returns:
            Self for chaining
        """
        ...

    def add_validator(self, validator) -> "PipelineBuilder":
        """Add a validator to the pipeline.

        Validators are applied in order.

        Args:
            validator: Validator implementation

        Returns:
            Self for chaining
        """
        ...

    def with_transformer(
        self, transformer: Callable[[pa.Table], pa.Table]
    ) -> "PipelineBuilder":
        """Set the transformation function.

        Args:
            transformer: Function that transforms Arrow Table

        Returns:
            Self for chaining
        """
        ...

    def with_writer(self, writer) -> "PipelineBuilder":
        """Set the data writer.

        Args:
            writer: Writer implementation

        Returns:
            Self for chaining
        """
        ...

    def build(self) -> PipelineEngine:
        """Build the configured pipeline engine.

        Returns:
            PipelineEngine ready for execution

        Raises:
            ValueError: If required components are missing
        """
        ...
