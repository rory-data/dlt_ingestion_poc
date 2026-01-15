"""Tests for multi-record type file handling."""

import pyarrow as pa
import pytest
from hypothesis import given
from hypothesis import strategies as st

from ingestion.io.multirecord import (
    get_record_type_counts,
    parse_trailer_counts,
    partition_by_record_type,
)


@pytest.fixture
def trailer_batch():
    """Provides a batch containing trailer records."""
    return pa.RecordBatch.from_arrays(
        [
            pa.array(["H", "D", "D", "T"]),
            pa.array(["H1", "D1", "D2", "10-2"]),
            pa.array(["H2", "D3", "D4", "20-5"]),
        ],
        names=["type", "col1", "col2"],
    )


@pytest.fixture
def multi_record_batch():
    """Provides a batch with multiple record types."""
    return pa.RecordBatch.from_arrays(
        [
            pa.array(["10", "10", "20", "20", "20", "30"]),
            pa.array(["v1", "v2", "v3", "v4", "v5", "v6"]),
        ],
        names=["type", "data"],
    )


def test_parse_trailer_counts(trailer_batch):
    """Test extraction of counts from trailer records."""
    counts = parse_trailer_counts(trailer_batch)
    assert counts == {"10": 2, "20": 5}


def test_parse_trailer_counts_no_trailer():
    """Test parse_trailer_counts with no trailer records."""
    batch = pa.RecordBatch.from_arrays(
        [pa.array(["D", "D"]), pa.array(["v1", "v2"])], names=["type", "data"]
    )
    counts = parse_trailer_counts(batch)
    assert counts == {}


def test_partition_by_record_type_homogeneous():
    """Test partitioning a homogeneous batch."""
    batch = pa.RecordBatch.from_arrays(
        [pa.array(["10", "10"]), pa.array(["v1", "v2"])], names=["type", "data"]
    )
    partitions = list(partition_by_record_type(batch))
    assert len(partitions) == 1
    rt, data = partitions[0]
    assert rt == "10"
    assert data.num_rows == 2


def test_partition_by_record_type_heterogeneous(multi_record_batch):
    """Test partitioning a heterogeneous batch."""
    partitions = list(partition_by_record_type(multi_record_batch))
    assert len(partitions) == 3

    # Sort partitions by record type for validation
    parts_dict = dict(partitions)
    assert "10" in parts_dict
    assert "20" in parts_dict
    assert "30" in parts_dict

    assert parts_dict["10"].num_rows == 2
    assert parts_dict["20"].num_rows == 3
    assert parts_dict["30"].num_rows == 1


def test_get_record_type_counts(multi_record_batch):
    """Test counting records by type."""
    counts = get_record_type_counts(multi_record_batch)
    assert counts == {"10": 2, "20": 3, "30": 1}


def test_filter_expected_records(multi_record_batch):
    """Test filtering records by valid types."""
    from ingestion.io.multirecord import filter_expected_records

    filtered = filter_expected_records(multi_record_batch, ["10", "30"])
    assert filtered.num_rows == 3

    # Check that type 20 is gone
    counts = get_record_type_counts(filtered)
    assert "20" not in counts
    assert counts["10"] == 2
    assert counts["30"] == 1


@given(st.lists(st.sampled_from(["10", "20", "30"]), min_size=1, max_size=100))
def test_partition_preserves_row_count(record_types):
    """Property test: partitioning must preserve total row count."""
    batch = pa.RecordBatch.from_arrays(
        [pa.array(record_types), pa.array([f"v{i}" for i in range(len(record_types))])],
        names=["type", "data"],
    )

    partitions = list(partition_by_record_type(batch))
    total_rows_after = sum(data.num_rows for _, data in partitions)

    assert total_rows_after == len(record_types)


@given(st.lists(st.sampled_from(["10", "20", "30"]), min_size=1, max_size=100))
def test_partition_record_counts_match(record_types):
    """Property test: partition counts must match expected counts."""
    batch = pa.RecordBatch.from_arrays(
        [pa.array(record_types), pa.array([f"v{i}" for i in range(len(record_types))])],
        names=["type", "data"],
    )

    expected_counts = get_record_type_counts(batch)
    partitions = list(partition_by_record_type(batch))
    actual_counts = {rt: data.num_rows for rt, data in partitions}

    assert actual_counts == expected_counts
