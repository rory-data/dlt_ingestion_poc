"""Pipeline configuration classes."""

from dataclasses import dataclass
from enum import StrEnum


class RunMode(StrEnum):
    """Enumeration of pipeline run modes."""

    EXTRACT = "extract"
    REPLAY = "replay"


@dataclass(frozen=True)
class DestinationConfig:
    """Immutable configuration for dlt destination output formatting."""

    layout: str = "{schema}/{table_name}/{resource_name}_{batch_date}.{ext}"
    resource_name: str = ""
    batch_date: str = ""

    @property
    def extra_placeholders(self) -> dict[str, str]:
        """Return extra placeholders for filesystem desintation layout."""
        return {
            "resource_name": self.resource_name,
            "batch_date": self.batch_date,
        }
