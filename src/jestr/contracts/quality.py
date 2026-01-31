"""ODCS qualirt rules extraction and mapping."""

from typing import Any

from open_data_contract_standard.model import SchemaProperty


def extract_quality_rules(
    prop: SchemaProperty,
) -> list[dict[str, Any]]:
    """Extract quality rules from ODCS property."""
    return [rule.model_dump() for rule in prop.quality if prop.quality]


def get_column_quality_rules(columns: list[SchemaProperty]) -> dict[str, list[dict]]:
    """Get quality rules for a specific column in a table schema."""
    rules: dict[str, list[dict]] = {}
    for col in columns:
        col_rules = extract_quality_rules(col)
        if col_rules:
            rules[col.name] = col_rules
    return rules


def validate_rule_type(rule: Any) -> bool:
    """Validate that a quality rule has required structure."""
    if not isinstance(rule, dict):
        return False
    required_fields = {"type", "metric"}
    return any(field in rule and rule[field] is not None for field in required_fields)


def get_quality_rules_summary(table_properties: list[SchemaProperty]) -> dict[str, int]:
    """Get a summary of quality rules for all columns in a table schema."""
    summary: dict[str, int] = {}
    all_rules = get_column_quality_rules(table_properties)

    for rules in all_rules.values():
        for rule in rules:
            rule_type = rule.get("metric") or rule.get("type", "unknown")
            summary[rule_type] = summary.get(rule_type, 0) + 1
    return summary
