"""Tests for ODCS contract handling."""

from unittest.mock import MagicMock

import pytest

from jestr.contracts import ODCSContract


@pytest.mark.unit
class TestODCSContractProperties:
    """Test ODCSContract property accessors."""

    @pytest.fixture
    def mock_odcs_with_contract(self):
        """Create a mock ODCS contract object."""
        mock_odcs = MagicMock()
        mock_odcs.id = "test_contract_123"
        mock_odcs.version = "1.0.0"
        mock_odcs.status = "active"
        mock_odcs.schema_ = []
        mock_odcs.customProperties = {
            "evolutionMode": "notify",
            "validationMode": "reject",
        }
        return mock_odcs

    def test_odcs_contract_from_id(self, mock_odcs_with_contract, tmp_path):
        """Test accessing contract ID."""
        contract_path = tmp_path / "contract.yaml"
        contract = ODCSContract(
            odcs=mock_odcs_with_contract,
            contract_path=contract_path,
        )
        assert contract.id == "test_contract_123"

    @pytest.mark.parametrize(
        ("attr", "expected_default"),
        [
            ("id", ""),
            ("version", "1.0.0"),
            ("status", "draft"),
        ],
    )
    def test_odcs_contract_defaults(self, tmp_path, attr, expected_default):
        """Test that default values are returned when properties are missing."""
        mock_odcs = MagicMock()

        # Ensure other required attributes are present to avoid side-effects in init if any
        mock_odcs.id = "test"
        mock_odcs.version = "1.0.0"
        mock_odcs.status = "active"

        # Reset the specific attribute we are testing to None/Empty
        # Note: The property implementation handles None or checks logic
        setattr(mock_odcs, attr, None)

        contract_path = tmp_path / "contract.yaml"
        contract = ODCSContract(odcs=mock_odcs, contract_path=contract_path)
        assert getattr(contract, attr) == expected_default

    def test_odcs_contract_custom_properties(self, mock_odcs_with_contract, tmp_path):
        """Test accessing custom properties."""
        contract_path = tmp_path / "contract.yaml"
        contract = ODCSContract(
            odcs=mock_odcs_with_contract,
            contract_path=contract_path,
        )
        props = contract.custom_properties
        assert props["evolutionMode"] == "notify"
        assert props["validationMode"] == "reject"

    @pytest.mark.parametrize(
        ("custom_props", "expected"),
        [
            ("not a dict", {}),
            (None, {}),
        ],
    )
    def test_odcs_contract_custom_properties_invalid(
        self, tmp_path, custom_props, expected
    ):
        """Test invalid customProperties handling."""
        mock_odcs = MagicMock()
        mock_odcs.id = "test"
        mock_odcs.version = "1.0.0"
        mock_odcs.status = "active"

        if custom_props is None:
            # Logic to simulate missing attribute by not setting it (if possible)
            # or setting it to None if the class handles None.
            # Based on previous tests, getattr(self.odcs, "customProperties", None) is used.
            # So we can just not set it on the mock, or set it to None.
            mock_odcs.customProperties = None
        else:
            mock_odcs.customProperties = custom_props

        contract_path = tmp_path / "contract.yaml"
        contract = ODCSContract(odcs=mock_odcs, contract_path=contract_path)
        assert contract.custom_properties == expected


@pytest.mark.unit
class TestODCSContractEvolutionMode:
    """Test schema evolution mode handling."""

    @pytest.mark.parametrize(
        ("mode", "expected"),
        [
            ("strict", "strict"),
            ("notify", "notify"),
            ("auto_evolve", "auto_evolve"),
            ("invalid_mode", "notify"),
            (None, "notify"),
        ],
    )
    def test_evolution_mode(self, tmp_path, mode, expected):
        """Test various evolution mode configurations.

        Args:
            tmp_path: Pytest temporary path fixture.
            mode: The evolutionMode string to set in customProperties.
            expected: The expected EvolutionMode enum/literal value.
        """
        mock_odcs = MagicMock()
        mock_odcs.id = "test"
        mock_odcs.version = "1.0.0"
        mock_odcs.status = "active"
        if mode is None:
            mock_odcs.customProperties = {}
        else:
            mock_odcs.customProperties = {"evolutionMode": mode}

        contract_path = tmp_path / "contract.yaml"
        contract = ODCSContract(odcs=mock_odcs, contract_path=contract_path)
        assert contract.evolution_mode == expected


@pytest.mark.unit
class TestODCSContractValidationMode:
    """Test data validation mode handling."""

    @pytest.mark.parametrize(
        ("mode", "expected"),
        [
            ("reject", "reject"),
            ("quarantine", "quarantine"),
            ("invalid_mode", "reject"),
            (None, "reject"),
        ],
    )
    def test_validation_mode(self, tmp_path, mode, expected):
        """Test various validation mode configurations.

        Args:
            tmp_path: Pytest temporary path fixture.
            mode: The validationMode string to set in customProperties.
            expected: The expected ValidationMode enum/literal value.
        """
        mock_odcs = MagicMock()
        mock_odcs.id = "test"
        mock_odcs.version = "1.0.0"
        mock_odcs.status = "active"
        if mode is None:
            mock_odcs.customProperties = {}
        else:
            mock_odcs.customProperties = {"validationMode": mode}

        contract_path = tmp_path / "contract.yaml"
        contract = ODCSContract(odcs=mock_odcs, contract_path=contract_path)
        assert contract.validation_mode == expected


@pytest.mark.unit
class TestODCSContractToDltSchemas:
    """Test conversion to dlt schema format."""

    def test_to_dlt_schemas_empty(self, tmp_path):
        """Test converting empty contract to dlt schemas."""
        mock_odcs = MagicMock()
        mock_odcs.id = "test"
        mock_odcs.version = "1.0.0"
        mock_odcs.status = "active"
        mock_odcs.schema_ = None
        contract_path = tmp_path / "contract.yaml"
        contract = ODCSContract(odcs=mock_odcs, contract_path=contract_path)
        schemas = contract.to_dlt_schemas()
        assert isinstance(schemas, dict)
        assert len(schemas) == 0

    def test_to_dlt_schemas_skips_invalid_tables(self, tmp_path):
        """Test that tables without name or properties are skipped."""
        mock_odcs = MagicMock()
        mock_odcs.id = "test"
        mock_odcs.version = "1.0.0"
        mock_odcs.status = "active"

        # Create mock table without name
        invalid_table = MagicMock()
        invalid_table.name = None
        invalid_table.properties = [MagicMock()]

        mock_odcs.schema_ = [invalid_table]
        contract_path = tmp_path / "contract.yaml"
        contract = ODCSContract(odcs=mock_odcs, contract_path=contract_path)
        schemas = contract.to_dlt_schemas()
        assert len(schemas) == 0
