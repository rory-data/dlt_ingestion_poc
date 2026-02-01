"""Integration tests for ODCSContract."""

from unittest.mock import MagicMock

import pytest

from jestr.contracts import ODCSContract


@pytest.mark.integration
class TestODCSContractIntegration:
    """Integration tests for ODCSContract."""

    def test_contract_instantiation(self, tmp_path):
        """Test basic contract instantiation with filesystem interaction."""
        mock_odcs = MagicMock()
        mock_odcs.id = "test_contract"
        mock_odcs.version = "1.0.0"
        mock_odcs.status = "active"
        mock_odcs.schema_ = []

        contract_path = tmp_path / "contract.yaml"
        # In a real integration test we might write a real yaml file here
        contract_path.touch()

        contract = ODCSContract(
            odcs=mock_odcs,
            contract_path=contract_path,
        )
        assert contract.id == "test_contract"
        assert contract.version == "1.0.0"
        assert contract.status == "active"

    def test_contract_with_schema_objects(self, tmp_path):
        """Test contract initialized with schema objects."""
        mock_odcs = MagicMock()
        mock_odcs.id = "test"
        mock_odcs.version = "1.0.0"
        mock_odcs.status = "active"

        schema_obj = MagicMock()
        contract_path = tmp_path / "contract.yaml"
        contract_path.touch()

        contract = ODCSContract(
            odcs=mock_odcs,
            contract_path=contract_path,
            schema_objects=[schema_obj],
        )
        assert contract.schema_objects == [schema_obj]

    def test_contract_properties_chain(self, tmp_path):
        """Test accessing multiple properties in sequence."""
        mock_odcs = MagicMock()
        mock_odcs.id = "chain_test"
        mock_odcs.version = "2.0.0"
        mock_odcs.status = "production"
        mock_odcs.customProperties = {
            "evolutionMode": "strict",
            "validationMode": "quarantine",
        }
        contract_path = tmp_path / "contract.yaml"
        contract_path.touch()

        contract = ODCSContract(odcs=mock_odcs, contract_path=contract_path)

        # Chain access
        assert contract.id == "chain_test"
        assert contract.version == "2.0.0"
        assert contract.status == "production"
        assert contract.evolution_mode == "strict"
        assert contract.validation_mode == "quarantine"
