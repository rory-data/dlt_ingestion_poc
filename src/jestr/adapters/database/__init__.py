"""Database adapters and configs."""

from .oracle import OracleAdapter, OracleConfig
from .teradata import TeradataAdapter, TeradataConfig

__all__ = [
    "OracleAdapter",
    "OracleConfig",
    "TeradataAdapter",
    "TeradataConfig",
]
