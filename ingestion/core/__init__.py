"""Core ingestion functionality for validation and reporting.

Provides data quality validation, validation state management, and reporting.
"""

from .reporting import DataQualityError, DataQualityIssue, Severity, ValidationSummary
from .validate import ArrowValidator

__all__ = [
    "ArrowValidator",
    "DataQualityError",
    "DataQualityIssue",
    "Severity",
    "ValidationSummary",
]
