"""Module for handling schema evolution during pipeline runs, including detection and representation of schema changes."""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import structlog
from dlt.common.pipeline import LoadInfo

from jestr.common.constants import ChangeType

logger = structlog.get_logger(__name__)


@dataclass
class SchemaChange:
    """Representation of a single schema change, including the type of change and the affected field."""

    table_name: str
    change_type: ChangeType
    column_name: str
    old_value: str | None = None
    new_value: str | None = None
    timestamp: str = ""

    def __post_init__(self):
        """Ensure that the timestamp is set to the current time if not provided."""
        if not self.timestamp:
            self.timestamp = datetime.now(UTC).isoformat()


def extract_schema_changes(load_info: LoadInfo) -> list[SchemaChange]:
    """Extract schema changes from the pipeline load information.

    Args:
        load_info (LoadInfo): The load information containing details about the pipeline run, including any schema updates.

    Returns:
        list[SchemaChange]: A list of detected schema changes during the pipeline run.
    """
    changes: list[SchemaChange] = []

    if not load_info.load_packages:
        logger.warning("No load packages found in load info.")
        return changes

    for package in load_info.load_packages:
        if not package.schema_update:
            continue

        schema_update = package.schema_update
        tables = cast(dict[str, dict[str, Any]], schema_update.get("tables", {}))
        for table_name, table_update in tables.items():
            for column_name, column_defn in table_update.get(
                "added_columns", {}
            ).items():
                changes.append(
                    SchemaChange(
                        table_name=table_name,
                        change_type=ChangeType.ADDED,
                        column_name=column_name,
                        new_value=str(column_defn.get("data_type", "unknown")),
                    )
                )

        for column_name in table_update.get("removed_columns", []):
            changes.append(
                SchemaChange(
                    table_name=table_name,
                    change_type=ChangeType.DROPPED,
                    column_name=column_name,
                )
            )

        for column_name, type_change in table_update.get("type_changes", {}).items():
            changes.append(
                SchemaChange(
                    table_name=table_name,
                    change_type=ChangeType.MODIFIED,
                    column_name=column_name,
                    old_value=str(type_change.get("old_type", "unknown")),
                    new_value=str(type_change.get("new_type", "unknown")),
                )
            )

    logger.warning(
        "Detected schema changes during pipeline run.",
        change_count=len(changes),
        tables_affected=len({c.table_name for c in changes}),
    )
    return changes
