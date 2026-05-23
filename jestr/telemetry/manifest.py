"""ChangeManifest for tracking schema evolution in data contracts.

Provides a structured way to capture and log details of detected schema changes during pipeline
execution.
"""

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import structlog

from jestr.common.constants import ChangeType
from jestr.contracts.odcs import ODCSContract
from jestr.pipeline.evolution import SchemaChange

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class ChangeManifest:
    """Audit trail for schema changes, capturing the details of each change and the overall impact on the data contract."""

    contract_id: str
    contract_version: str
    contract_status: str
    load_id: str
    detected_at: str
    detection_source: str  # "pipeline" | "users" | "scheduled"
    total_changes: int
    changes_by_type: dict[str, int]  # {"added": 5, "dropped": 2, "modified": 3}
    tables_affected: list[str]
    breaking_changes: bool
    resolved: bool = False
    resolved_at: str | None = None
    resolution_action: str | None = None
    metadata: dict[str, str] | None = None

    def to_dict(self) -> dict:
        """Convert the ChangeManifest dataclass to a dictionary for serialisation."""
        return asdict(self)


def generate_manifest(
    contract: ODCSContract,
    load_id: str,
    changes: list[SchemaChange],
    detection_source: str = "pipeline",
) -> ChangeManifest:
    """Generate a ChangeManifest based on the detected schema changes and the associated data contract.

    Args:
        contract (ODCSContract): The data contract associated with the schema changes.
        load_id (str): The unique identifier for the pipeline load during which the changes were detected.
        changes (list[SchemaChange]): A list of detected schema changes to be included in the manifest.
        detection_source (str, optional): The source of the schema change detection. Defaults to "pipeline".

    Returns:
        ChangeManifest: A populated ChangeManifest instance summarising the detected schema changes.
    """
    counts = Counter(c.change_type for c in changes)
    changes_by_type = {k.value: counts[k] for k in ChangeType}
    tables_affected = list({c.table_name for c in changes})
    breaking = any(c.change_type in ["dropped", "modified"] for c in changes)

    manifest = ChangeManifest(
        contract_id=contract.id,
        contract_version=contract.version,
        contract_status=contract.status,
        load_id=load_id,
        detected_at=datetime.now(UTC).isoformat(),
        detection_source=detection_source,
        total_changes=len(changes),
        changes_by_type=changes_by_type,
        tables_affected=tables_affected,
        breaking_changes=breaking,
    )

    logger.info(
        "Generated change manifest",
        contract_id=contract.id,
        breaking_changes=breaking,
        total_changes=len(changes),
    )

    return manifest


def save_manifest(manifest: ChangeManifest, output_path: Path) -> None:
    """Save the ChangeManifest to a JSON file for record-keeping and future reference.

    Args:
        manifest (ChangeManifest): The ChangeManifest instance to be saved.
        output_path (Path): The file path where the manifest should be saved as JSON.
    """
    import json

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with Path(output_path).open("w") as f:
        json.dump(manifest.to_dict(), f, indent=2)

    logger.info("Saved change manifest to file", path=str(output_path))


def log_manifest_summary(manifest: ChangeManifest) -> None:
    """Log a summary of the ChangeManifest for quick visibility into the detected schema changes.

    Args:
        manifest (ChangeManifest): The ChangeManifest instance containing the details of the detected schema changes.
    """
    logger.info(
        "Schema evolution tracked",
        contract_id=manifest.contract_id,
        contract_version=manifest.contract_version,
        contract_status=manifest.contract_status,
        breaking_changes=manifest.breaking_changes,
        total_changes=manifest.total_changes,
        tables_affected=manifest.tables_affected,
        load_id=manifest.load_id,
    )
