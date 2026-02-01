"""Tests for dlt pipeline configuration."""

import pytest

from jestr.engines.dlt.config import DestinationConfig, RunMode


@pytest.mark.unit
class TestRunMode:
    """Test the RunMode enumeration."""

    @pytest.mark.parametrize(
        ("mode", "value"),
        [
            (RunMode.EXTRACT, "extract"),
            (RunMode.REPLAY, "replay"),
        ],
    )
    def test_run_mode_values(self, mode, value):
        """Test RunMode values."""
        assert mode == value
        assert mode.value == value

    def test_run_mode_is_enum(self):
        """Test that RunMode is a StrEnum."""
        assert hasattr(RunMode, "EXTRACT")
        assert hasattr(RunMode, "REPLAY")

    def test_run_mode_comparison(self):
        """Test RunMode comparison with strings."""
        assert RunMode.EXTRACT == "extract"
        assert RunMode("extract") == RunMode.EXTRACT

    def test_run_mode_invalid(self):
        """Test that invalid run mode raises ValueError."""
        with pytest.raises(ValueError, match="invalid_mode"):
            RunMode("invalid_mode")


@pytest.mark.unit
class TestDestinationConfig:
    """Test the DestinationConfig dataclass."""

    def test_destination_config_default_layout(self):
        """Test default layout pattern."""
        config = DestinationConfig()
        assert (
            config.layout == "{schema}/{table_name}/{resource_name}_{batch_date}.{ext}"
        )

    def test_destination_config_default_placeholders(self):
        """Test default placeholder values."""
        config = DestinationConfig()
        assert config.resource_name == ""
        assert config.batch_date == ""

    def test_destination_config_custom_layout(self):
        """Test custom layout pattern."""
        custom_layout = "{schema}/{table}/{batch_date}.parquet"
        config = DestinationConfig(layout=custom_layout)
        assert config.layout == custom_layout

    def test_destination_config_with_placeholders(self):
        """Test setting placeholder values."""
        config = DestinationConfig(
            resource_name="my_resource",
            batch_date="2024-01-15",
        )
        assert config.resource_name == "my_resource"
        assert config.batch_date == "2024-01-15"

    def test_destination_config_is_frozen(self):
        """Test that DestinationConfig is immutable."""
        config = DestinationConfig()
        with pytest.raises(AttributeError, match="cannot assign to field"):
            config.layout = "new_layout"  # type: ignore[assignment]

    def test_extra_placeholders_property(self):
        """Test that extra_placeholders returns a dict."""
        config = DestinationConfig(
            resource_name="test_resource",
            batch_date="2024-01-15",
        )
        placeholders = config.extra_placeholders
        assert isinstance(placeholders, dict)
        assert placeholders["resource_name"] == "test_resource"
        assert placeholders["batch_date"] == "2024-01-15"

    def test_extra_placeholders_empty_by_default(self):
        """Test that extra_placeholders are empty by default."""
        config = DestinationConfig()
        placeholders = config.extra_placeholders
        assert placeholders["resource_name"] == ""
        assert placeholders["batch_date"] == ""

    def test_destination_config_all_parameters(self):
        """Test creating config with all parameters."""
        config = DestinationConfig(
            layout="{schema}/{table}/{date}.parquet",
            resource_name="orders",
            batch_date="2024-01-15",
        )
        assert config.layout == "{schema}/{table}/{date}.parquet"
        assert config.resource_name == "orders"
        assert config.batch_date == "2024-01-15"

    def test_destination_config_equality(self):
        """Test comparing two DestinationConfig instances."""
        config1 = DestinationConfig(
            layout="test_layout",
            resource_name="test",
            batch_date="2024-01-15",
        )
        config2 = DestinationConfig(
            layout="test_layout",
            resource_name="test",
            batch_date="2024-01-15",
        )
        assert config1 == config2

    def test_destination_config_inequality(self):
        """Test that different configs are not equal."""
        config1 = DestinationConfig(resource_name="test1")
        config2 = DestinationConfig(resource_name="test2")
        assert config1 != config2

    def test_destination_config_repr(self):
        """Test that repr is informative."""
        config = DestinationConfig(
            layout="test_layout",
            resource_name="test_resource",
            batch_date="2024-01-15",
        )
        repr_str = repr(config)
        assert "DestinationConfig" in repr_str
        assert "test_layout" in repr_str

    def test_destination_config_hash_frozen(self):
        """Test that frozen config can be hashed."""
        config1 = DestinationConfig(resource_name="test")
        config2 = DestinationConfig(resource_name="test")
        # Both should be hashable and equal configs should have same hash
        assert hash(config1) == hash(config2)

    def test_extra_placeholders_is_dict_copy(self):
        """Test that extra_placeholders returns a fresh dict."""
        config = DestinationConfig(
            resource_name="test",
            batch_date="2024-01-15",
        )
        p1 = config.extra_placeholders
        p2 = config.extra_placeholders
        assert p1 == p2
        assert p1 is not p2  # Different dict objects


@pytest.mark.unit
class TestDestinationConfigBehavior:
    """Unit tests for DestinationConfig behavior logic."""

    def test_layout_formatting_with_placeholders(self):
        """Test that layout can be used for string formatting."""
        config = DestinationConfig(
            layout="{schema}/{table_name}/{resource_name}_{batch_date}.{ext}",
            resource_name="orders",
            batch_date="2024-01-15",
        )

        # Simulate formatting
        formatted = config.layout.format(
            schema="public",
            table_name="orders_table",
            **config.extra_placeholders,
            ext="parquet",
        )
        assert formatted == "public/orders_table/orders_2024-01-15.parquet"

    def test_config_immutability_prevents_state_mutation(self):
        """Test that frozen config prevents accidental state changes."""
        config = DestinationConfig()
        original_layout = config.layout

        # Attempting to mutate should fail
        with pytest.raises(AttributeError, match="cannot assign to field"):
            config.layout = "modified"  # type: ignore[assignment]

        # Original should be unchanged
        assert config.layout == original_layout
