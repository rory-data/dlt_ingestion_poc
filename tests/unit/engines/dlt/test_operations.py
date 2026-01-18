"""Tests for pipeline operations."""

from unittest.mock import patch

import pytest

from ingestion.engines.dlt.operations import (
    load_odcs_schemas,
    setup_validator,
    validate_source_file,
)


def test_validate_source_file_exists(tmp_path):
    """Test validate_source_file with existing file."""
    f = tmp_path / "test.txt"
    f.write_text("hello")
    # Should return path
    assert validate_source_file(f) == f


def test_validate_source_file_missing():
    """Test validate_source_file with missing file should exit."""
    with pytest.raises(SystemExit):
        validate_source_file("non_existent_file.txt")


def test_setup_validator():
    """Test validator initialization."""
    v = setup_validator(issue_action="reject")
    assert v.issue_action == "reject"


@patch("ingestion.engines.dlt.operations.get_dlt_schemas")
def test_load_odcs_schemas_success(mock_get_schemas):
    """Test successful schema loading."""
    mock_get_schemas.return_value = {"table": {}}
    schemas = load_odcs_schemas("contract.yaml")
    assert schemas == {"table": {}}


@patch("ingestion.engines.dlt.operations.get_dlt_schemas")
def test_load_odcs_schemas_failure(mock_get_schemas):
    """Test schema loading failure should exit."""
    mock_get_schemas.side_effect = Exception("error")
    with pytest.raises(SystemExit):
        load_odcs_schemas("contract.yaml")
