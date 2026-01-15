"""Tests for ODCS schema conversion."""

import pytest
from open_data_contract_standard.model import (
    Description,
    OpenDataContractStandard,
    SchemaProperty,
)
from open_data_contract_standard.model import SchemaObject as OdcsSchema

from ingestion.schema.odcs import (
    _get_description_str,
    _get_logical_type_option,
    _get_type,
    get_dlt_type_from_odcs_prop,
    get_table_requirements,
    to_dlt,
)


@pytest.mark.parametrize(
    ("input_desc", "expected"),
    [
        (None, None),
        ("Simple description", "Simple description"),
    ],
)
def test_get_description_str(input_desc, expected):
    """Test description extraction from string and None."""
    assert _get_description_str(input_desc) == expected


def test_get_description_str_with_object():
    """Test description extraction from Description object with whitespace/newlines."""
    desc_obj = Description(purpose="  Purpose with\nnewline  ")
    assert _get_description_str(desc_obj) == "Purpose with newline"


@pytest.mark.parametrize(
    "missing_key",
    ["precision", "scale", "missing"],
)
def test_get_logical_type_option(missing_key):
    """Test extracting logical type options."""
    prop = SchemaProperty(name="test", logicalType="string")
    prop.logicalTypeOptions = {"precision": 10, "scale": 2}

    if missing_key == "missing":
        assert _get_logical_type_option(prop, missing_key) is None
    else:
        assert _get_logical_type_option(prop, missing_key) == (
            10 if missing_key == "precision" else 2
        )


def test_get_logical_type_option_no_options():
    """Test extracting logical type options when options dict is None."""
    prop = SchemaProperty(name="test", logicalType="string")
    prop.logicalTypeOptions = None
    assert _get_logical_type_option(prop, "precision") is None


def test_get_type():
    """Test type preference (physical vs logical)."""
    prop = SchemaProperty(name="test", logicalType="string")
    assert _get_type(prop) == "string"

    prop.physicalType = "varchar(100)"
    assert _get_type(prop) == "varchar(100)"


def test_get_dlt_type_from_odcs_prop():
    """Test mapping ODCS property to dlt type."""
    prop = SchemaProperty(name="test", physicalType="number", logicalType="string")
    prop.logicalTypeOptions = {"precision": 38, "scale": 9}

    dlt_type, extras = get_dlt_type_from_odcs_prop(prop)
    # py_type_to_sc_type(float) is 'double'
    assert dlt_type == "double"
    assert extras == {"precision": 38, "scale": 9}


def test_to_dlt():
    """Test converting a full ODCS contract to dlt schemas."""
    # Create real ODCS objects for better fidelity
    prop = SchemaProperty(
        name="id",
        physicalType="integer",
        required=True,
        primaryKey=True,
        description="Primary ID",
    )

    table = OdcsSchema(name="users", properties=[prop], description="User table")

    contract = OpenDataContractStandard.model_validate(
        {
            "version": "1.0.0",
            "id": "test-contract",
            "schema": [table.model_dump() if hasattr(table, "model_dump") else table],
        }
    )

    schemas = to_dlt(contract)

    assert "users" in schemas
    user_schema = schemas["users"]
    assert user_schema["name"] == "users"
    assert user_schema["description"] == "User table"
    assert "id" in user_schema["columns"]

    col = user_schema["columns"]["id"]
    assert col["name"] == "id"
    assert col["data_type"] == "bigint"  # dlt mapping for int
    assert col["nullable"] is False
    assert col["primary_key"] is True
    assert col["description"] == "Primary ID"


def test_get_table_requirements():
    """Test extracting requirements from dlt schemas."""
    schemas = {
        "users": {
            "name": "users",
            "columns": {
                "id": {"name": "id", "primary_key": True},
                "name": {"name": "name"},
            },
        }
    }

    reqs = get_table_requirements(schemas, "users")
    assert reqs["column_names"] == ["id", "name"]
    assert reqs["primary_keys"] == ["id"]
    assert len(reqs["columns"]) == 2

    # Missing table
    reqs_missing = get_table_requirements(schemas, "missing")
    assert reqs_missing["column_names"] == []
    assert reqs_missing["primary_keys"] == []
