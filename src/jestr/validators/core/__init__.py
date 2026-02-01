"""Core configs, errors, and results for validators."""

from .base import Validator, _partition_by_validity
from .issues import DataQualityError, DataQualityIssue, Severity
from .result import ValidationResult, get_validation_summary, log_validation_summary

__all__ = [
    "DataQualityError",
    "DataQualityIssue",
    "Severity",
    "ValidationResult",
    "Validator",
    "_partition_by_validity",
    "get_validation_summary",
    "log_validation_summary",
]
