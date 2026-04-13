"""Unit tests for the _normalize_filters defensive filter normalization layer."""

from bellona.services.agent_service import _normalize_filters


class TestNormalizeFilters:
    def test_already_correct_condition(self):
        raw = {"property": "name", "operator": "eq", "value": "Alice"}
        assert _normalize_filters(raw) == raw

    def test_already_correct_group(self):
        raw = {
            "op": "and",
            "conditions": [
                {"property": "name", "operator": "eq", "value": "Alice"},
            ],
        }
        assert _normalize_filters(raw) == raw

    def test_field_renamed_to_property(self):
        raw = {"field": "name", "operator": "eq", "value": "Alice"}
        result = _normalize_filters(raw)
        assert result["property"] == "name"
        assert "field" not in result

    def test_and_key_to_op_conditions(self):
        raw = {
            "and": [
                {"property": "age", "operator": "gte", "value": 18},
            ],
        }
        result = _normalize_filters(raw)
        assert result["op"] == "and"
        assert len(result["conditions"]) == 1

    def test_or_key_to_op_conditions(self):
        raw = {
            "or": [
                {"property": "status", "operator": "eq", "value": "active"},
                {"property": "status", "operator": "eq", "value": "pending"},
            ],
        }
        result = _normalize_filters(raw)
        assert result["op"] == "or"
        assert len(result["conditions"]) == 2

    def test_inline_operator_extraction(self):
        """Agent returns {"field": "species", "neq": "[]"} style."""
        raw = {"field": "species", "neq": "[]"}
        result = _normalize_filters(raw)
        assert result["property"] == "species"
        assert result["operator"] == "neq"
        assert result["value"] == "[]"

    def test_nested_normalization(self):
        """Full normalization of a deeply wrong agent output."""
        raw = {
            "and": [
                {"field": "species", "neq": "[]"},
                {"field": "gender", "eq": "male"},
            ],
        }
        result = _normalize_filters(raw)
        assert result["op"] == "and"
        assert result["conditions"][0]["property"] == "species"
        assert result["conditions"][0]["operator"] == "neq"
        assert result["conditions"][1]["property"] == "gender"
        assert result["conditions"][1]["operator"] == "eq"

    def test_mixed_correct_and_incorrect(self):
        """Group with one correct and one incorrect condition."""
        raw = {
            "op": "and",
            "conditions": [
                {"property": "name", "operator": "contains", "value": "Luke"},
                {"field": "height", "gt": "170"},
            ],
        }
        result = _normalize_filters(raw)
        assert result["conditions"][0]["property"] == "name"
        assert result["conditions"][1]["property"] == "height"
        assert result["conditions"][1]["operator"] == "gt"

    def test_passthrough_when_no_issues(self):
        """A well-formed nested group passes through unchanged."""
        raw = {
            "op": "or",
            "conditions": [
                {"property": "status", "operator": "eq", "value": "active"},
                {
                    "op": "and",
                    "conditions": [
                        {"property": "age", "operator": "gte", "value": 18},
                        {"property": "age", "operator": "lt", "value": 65},
                    ],
                },
            ],
        }
        result = _normalize_filters(raw)
        assert result == raw
