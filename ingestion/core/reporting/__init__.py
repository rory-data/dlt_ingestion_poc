"""Validation reporting and state management for dlt pipelines.

Provides data quality issue reporting, validation summary tracking,
and dlt pipeline state management utilities.
"""

from .config import DataQualityIssue, Severity
from .state import (
    add_record_counts,
    get_validation_summary,
    log_validation_summary,
    update_validation_state,
)
from .validation import DataQualityError, ValidationSummary

__all__ = [
    "DataQualityError",
    "DataQualityIssue",
    "Severity",
    "ValidationSummary",
    "add_record_counts",
    "get_validation_summary",
    "log_validation_summary",
    "update_validation_state",
]
