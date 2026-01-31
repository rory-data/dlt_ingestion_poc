"""Change manifest generation and management for schema evolution."""

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import structlog
from dlt.common.pipeline import LoadInfo

from .odcs import ODCSContract

logger = structlog.get_logger()


@dataclass
class SchemaChange:
    """Represents a detected schema change."""

    table_name: str
    change_type: str  # e.g. "added", "dropped", "modified"
    column_name: str
    old_value: str | None = None
    new_value: str | None = None
    timestamp: str = ""

    def __post_init__(self):
        """Set timestamp if not provided."""
        if not self.timestamp:
            self.timestamp = datetime.now(tz=UTC).isoformat()


def extract_schema_changes(
    dlt_load_info: LoadInfo,
) -> list[SchemaChange]:
    """Extract schema changes from dlt pipeline load_info."""
    changes: list[SchemaChange] = []

    if not dlt_load_info.load_packages:
        return changes

    for package in dlt_load_info.load_packages:
        if not package.schema_update:
            continue
        schema_update = package.schema_update

        # Track table-level changes
        for table_name, table_update in schema_update.get("tables", {}).items():
            # Columns added
            for col_name, col_def in table_update.get("added_columns", {}).items():
                changes.append(
                    SchemaChange(
                        table_name=table_name,
                        change_type="added",
                        column_name=col_name,
                        new_value=str(col_def.get("data_type", "unknown")),
                    )
                )

            # Columns dropped
            for (col_name,) in table_update.get("removed_columns", []):
                changes.append(
                    SchemaChange(
                        table_name=table_name,
                        change_type="dropped",
                        column_name=col_name,
                    )
                )

            # Columns modified
            for col_name, type_change in table_update.get("type_changes", {}).items():
                changes.append(
                    SchemaChange(
                        table_name=table_name,
                        change_type="modified",
                        column_name=col_name,
                        old_value=str(type_change.get("old_type")),
                        new_value=str(type_change.get("new_type")),
                    )
                )

        logger.warning(
            "Detected schema changes",
            change_count=len(changes),
            tables_affected=len({c.table_name for c in changes}),
        )
    return changes


def is_breaking_change(contract: ODCSContract, change: SchemaChange) -> bool:
    """Determine if a schema change is breaking, based on governance rules."""
    return bool(
        contract.evolution_mode == "strict"
        or contract.status in ("deprecated", "retired")
        or change.change_type == "data_type"
    )


def fail_on_breaking_changes(
    changes: list[SchemaChange], contract: ODCSContract
) -> bool:
    """Specific if breaking changes should cause a failure based on contract rules."""
    breaking_changes = [c for c in changes if c.change_type in {"dropped", "modified"}]
    return bool(breaking_changes) and contract.status in ("active", "retired")


@dataclass(frozen=True)
class ChangeManifest:
    """Manifest for tracking schema changes in contracts."""

    contract_id: str
    contract_version: str
    contract_status: str
    dlt_load_id: str
    detected_at: str
    detection_source: str  # e.g. "pipeline" | "user" | "scheduled"
    total_changes: int
    changes_by_type: dict[str, int]
    tables_affected: list[str]
    breaking_changes: bool
    resolved: bool = False
    resolved_at: str | None = None
    resolution_action: str | None = None
    metadata: dict | None = None

    @classmethod
    def from_detected_changes(
        cls,
        contract: ODCSContract,
        dlt_load_id: str,
        changes: list[SchemaChange],
        detection_source: str = "pipeline",
    ) -> "ChangeManifest":
        """Generate a ChangeManifest from detected schema changes."""
        changes_by_type = {
            "added": len([c for c in changes if c.change_type == "added"]),
            "dropped": len([c for c in changes if c.change_type == "dropped"]),
            "modified": len([c for c in changes if c.change_type == "modified"]),
        }

        tables_affected = list({c.table_name for c in changes})
        breaking = any(c.change_type in {"dropped", "modified"} for c in changes)

        return cls(
            contract_id=contract.contract_id,
            contract_version=contract.version,
            contract_status=contract.status,
            dlt_load_id=dlt_load_id,
            detected_at=datetime.now(tz=UTC).isoformat(),
            detection_source=detection_source,
            total_changes=len(changes),
            changes_by_type=changes_by_type,
            tables_affected=tables_affected,
            breaking_changes=breaking,
        )

    def to_dict(self) -> dict:
        """Serialise the ChangeManifest to a dictionary."""
        return asdict(self)

    def to_file(self, output_path: Path) -> None:
        """Write the ChangeManifest to a JSON file."""
        import orjson

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with Path(output_path).open("wb") as f:
            f.write(orjson.dumps(self.to_dict(), option=orjson.OPT_INDENT_2))

        logger.info("Saved change manifest to file", path=str(output_path))

    def log_manifest_summary(self) -> None:
        """Log a summary of the ChangeManifest."""
        logger.info(
            "Schema evolution tracked",
            contract_id=self.contract_id,
            contract_version=self.contract_version,
            contract_status=self.contract_status,
            breaking_changes=self.breaking_changes,
            total_changes=self.total_changes,
            tables_affected=self.tables_affected,
            dlt_load_id=self.dlt_load_id,
        )

    def send_change_notification(
        self, webhook_url: str, channel: str | None = None
    ) -> None:
        """Send a notification about the ChangeManifest to a webhook URL."""
        raise NotImplementedError("Notification sending not implemented yet.")
