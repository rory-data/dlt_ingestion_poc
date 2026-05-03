"""Schema change handler: logs schema evolution event via structlog."""

import structlog

from jestr.common.events import PipelineEvent, SchemaEvolved

logger = structlog.get_logger(__name__)


class SchemaChangeHandler:
    """Logs :class:`~jestr.common.events.SchemaEvolved` events using structlog."""

    def handle(self, event: PipelineEvent) -> None:
        """Log the manifest when *event* is :class:`~jestr.common.events.SchemaEvolved` else no-op."""
        if isinstance(event, SchemaEvolved):
            manifest = event.manifest
            logger.info(
                "Schema evolution detected during pipeline execution",
                contract_id=manifest.contract_id,
                contract_version=manifest.contract_version,
                contract_status=manifest.contract_status,
                breaking_changes=manifest.breaking_changes,
                total_changes=manifest.total_changes,
                tables_affected=manifest.tables_affected,
                load_id=manifest.load_id,
            )
        # All other event types: no-op.
