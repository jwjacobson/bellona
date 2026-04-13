from datetime import date, datetime

from bellona.ontology.validator import FieldError, validate_record
from bellona.schemas.ontology import DataType, PropertyDefinitionCreate


def make_prop(
    name: str,
    data_type: DataType,
    required: bool = False,
    constraints: dict | None = None,
):
    return PropertyDefinitionCreate(
        name=name, data_type=data_type, required=required, constraints=constraints
    )


# ── Basic validation ─────────────────────────────────────────────────────────


def test_validate_valid_record():
    props = [
        make_prop("name", "string", required=True),
        make_prop("age", "integer"),
    ]
    record = {"name": "Alice", "age": 30}
    result = validate_record(record, props)
    assert result.valid is True
    assert result.errors == []
    assert result.coerced["name"] == "Alice"
    assert result.coerced["age"] == 30


def test_validate_missing_required_field():
    props = [make_prop("name", "string", required=True)]
    result = validate_record({}, props)
    assert result.valid is False
    assert any("name" in e.field for e in result.errors)


def test_validate_none_on_optional_field():
    props = [make_prop("notes", "string", required=False)]
    result = validate_record({"notes": None}, props)
    assert result.valid is True
    assert result.coerced["notes"] is None


def test_validate_extra_fields_allowed():
    props = [make_prop("name", "string")]
    result = validate_record({"name": "Test", "extra_field": "extra"}, props)
    assert result.valid is True
    assert result.coerced["extra_field"] == "extra"


# ── Type coercion: string ────────────────────────────────────────────────────


def test_coerce_string():
    props = [make_prop("name", "string")]
    result = validate_record({"name": "Alice"}, props)
    assert result.valid is True
    assert result.coerced["name"] == "Alice"


# ── Type coercion: integer ───────────────────────────────────────────────────


def test_coerce_integer_from_string():
    props = [make_prop("count", "integer")]
    result = validate_record({"count": "42"}, props)
    assert result.valid is True
    assert result.coerced["count"] == 42


def test_coerce_integer_failure():
    props = [make_prop("count", "integer")]
    result = validate_record({"count": "not_a_number"}, props)
    assert result.valid is False
    assert any("count" in e.field for e in result.errors)


# ── Type coercion: float ─────────────────────────────────────────────────────


def test_coerce_float_from_string():
    props = [make_prop("score", "float")]
    result = validate_record({"score": "3.14"}, props)
    assert result.valid is True
    assert abs(result.coerced["score"] - 3.14) < 0.001


# ── Type coercion: boolean ───────────────────────────────────────────────────


def test_coerce_boolean_native():
    props = [make_prop("flag", "boolean")]
    result = validate_record({"flag": True}, props)
    assert result.valid is True
    assert result.coerced["flag"] is True

    result = validate_record({"flag": False}, props)
    assert result.valid is True
    assert result.coerced["flag"] is False


def test_coerce_boolean_true_strings():
    props = [make_prop("flag", "boolean")]
    for val in ("true", "1", "yes", "True", "YES"):
        result = validate_record({"flag": val}, props)
        assert result.valid is True, f"Expected '{val}' to coerce to True"
        assert result.coerced["flag"] is True


def test_coerce_boolean_false_strings():
    props = [make_prop("flag", "boolean")]
    for val in ("false", "0", "no", "False", "NO"):
        result = validate_record({"flag": val}, props)
        assert result.valid is True, f"Expected '{val}' to coerce to False"
        assert result.coerced["flag"] is False


def test_coerce_boolean_invalid_string():
    props = [make_prop("flag", "boolean")]
    result = validate_record({"flag": "maybe"}, props)
    assert result.valid is False
    assert any("boolean" in e.message for e in result.errors)


def test_coerce_boolean_non_string_invalid():
    props = [make_prop("flag", "boolean")]
    result = validate_record({"flag": [1, 2]}, props)
    assert result.valid is False


# ── Type coercion: date ──────────────────────────────────────────────────────


def test_coerce_date_from_string():
    props = [make_prop("born", "date")]
    result = validate_record({"born": "2024-01-15"}, props)
    assert result.valid is True
    assert result.coerced["born"] == date(2024, 1, 15)


def test_coerce_date_native():
    props = [make_prop("born", "date")]
    result = validate_record({"born": date(2024, 1, 15)}, props)
    assert result.valid is True
    assert result.coerced["born"] == date(2024, 1, 15)


def test_coerce_date_invalid():
    props = [make_prop("born", "date")]
    result = validate_record({"born": "not-a-date"}, props)
    assert result.valid is False


# ── Type coercion: datetime ──────────────────────────────────────────────────


def test_coerce_datetime_from_string():
    props = [make_prop("created", "datetime")]
    result = validate_record({"created": "2024-01-15T10:30:00"}, props)
    assert result.valid is True
    assert result.coerced["created"] == datetime(2024, 1, 15, 10, 30, 0)


def test_coerce_datetime_native():
    props = [make_prop("created", "datetime")]
    dt = datetime(2024, 1, 15, 10, 30, 0)
    result = validate_record({"created": dt}, props)
    assert result.valid is True
    assert result.coerced["created"] == dt


def test_coerce_datetime_invalid():
    props = [make_prop("created", "datetime")]
    result = validate_record({"created": "yesterday"}, props)
    assert result.valid is False


# ── Type coercion: enum / json ───────────────────────────────────────────────


def test_coerce_enum_passes_through():
    props = [
        make_prop("status", "enum", constraints={"values": ["active", "inactive"]})
    ]
    result = validate_record({"status": "active"}, props)
    assert result.valid is True
    assert result.coerced["status"] == "active"


def test_coerce_json_passes_through():
    props = [make_prop("meta", "json")]
    result = validate_record({"meta": {"key": "value"}}, props)
    assert result.valid is True
    assert result.coerced["meta"] == {"key": "value"}


# ── Null sentinel coercion ───────────────────────────────────────────────────


def test_sentinel_unknown_on_integer_coerces_to_none():
    props = [make_prop("population", "integer")]
    result = validate_record({"population": "unknown"}, props)
    assert result.valid is True
    assert result.coerced["population"] is None


def test_sentinel_na_on_float_coerces_to_none():
    props = [make_prop("surface_water", "float")]
    result = validate_record({"surface_water": "n/a"}, props)
    assert result.valid is True
    assert result.coerced["surface_water"] is None


def test_sentinel_empty_string_on_integer_coerces_to_none():
    props = [make_prop("count", "integer")]
    result = validate_record({"count": ""}, props)
    assert result.valid is True
    assert result.coerced["count"] is None


def test_sentinel_dash_on_float_coerces_to_none():
    props = [make_prop("ratio", "float")]
    result = validate_record({"ratio": "-"}, props)
    assert result.valid is True
    assert result.coerced["ratio"] is None


def test_sentinel_null_string_on_integer_coerces_to_none():
    props = [make_prop("age", "integer")]
    result = validate_record({"age": "null"}, props)
    assert result.valid is True
    assert result.coerced["age"] is None


def test_sentinel_none_string_on_boolean_coerces_to_none():
    props = [make_prop("active", "boolean")]
    result = validate_record({"active": "none"}, props)
    assert result.valid is True
    assert result.coerced["active"] is None


def test_sentinel_unknown_on_date_coerces_to_none():
    props = [make_prop("born", "date")]
    result = validate_record({"born": "unknown"}, props)
    assert result.valid is True
    assert result.coerced["born"] is None


def test_sentinel_unknown_on_datetime_coerces_to_none():
    props = [make_prop("updated", "datetime")]
    result = validate_record({"updated": "N/A"}, props)
    assert result.valid is True
    assert result.coerced["updated"] is None


def test_sentinel_on_string_passes_through():
    """Sentinel values should NOT be coerced for string fields."""
    props = [make_prop("name", "string")]
    result = validate_record({"name": "unknown"}, props)
    assert result.valid is True
    assert result.coerced["name"] == "unknown"


def test_sentinel_on_enum_not_coerced():
    """Sentinel values should NOT be coerced for enum fields."""
    props = [
        make_prop("status", "enum", constraints={"values": ["active", "inactive"]})
    ]
    result = validate_record({"status": "unknown"}, props)
    assert result.valid is False


def test_sentinel_on_required_integer_field():
    """A sentinel on a required field coerces to None. The required check
    already passed (value was not None in the record), so it stores as null."""
    props = [make_prop("diameter", "integer", required=True)]
    result = validate_record({"diameter": "unknown"}, props)
    assert result.valid is True
    assert result.coerced["diameter"] is None


def test_sentinel_case_insensitive():
    """Sentinel detection should be case-insensitive."""
    props = [make_prop("count", "integer")]
    for val in ("Unknown", "UNKNOWN", "N/A", "None", "NULL"):
        result = validate_record({"count": val}, props)
        assert result.valid is True, f"Expected '{val}' to be treated as sentinel"
        assert result.coerced["count"] is None


def test_sentinel_with_whitespace():
    """Sentinel detection should strip whitespace."""
    props = [make_prop("count", "integer")]
    result = validate_record({"count": " unknown "}, props)
    assert result.valid is True
    assert result.coerced["count"] is None


# ── Constraints ──────────────────────────────────────────────────────────────


def test_constraint_min():
    props = [make_prop("age", "integer", constraints={"min": 0})]
    result = validate_record({"age": -1}, props)
    assert result.valid is False
    assert any("below minimum" in e.message for e in result.errors)


def test_constraint_max():
    props = [make_prop("age", "integer", constraints={"max": 100})]
    result = validate_record({"age": 101}, props)
    assert result.valid is False
    assert any("exceeds maximum" in e.message for e in result.errors)


def test_constraint_max_at_boundary():
    props = [make_prop("age", "integer", constraints={"max": 100})]
    result = validate_record({"age": 100}, props)
    assert result.valid is True


def test_constraint_min_max_valid():
    props = [make_prop("age", "integer", constraints={"min": 0, "max": 150})]
    result = validate_record({"age": 75}, props)
    assert result.valid is True


def test_constraint_min_max_exceeded():
    props = [make_prop("age", "integer", constraints={"min": 0, "max": 150})]
    result = validate_record({"age": 200}, props)
    assert result.valid is False


def test_constraint_pattern():
    props = [make_prop("code", "string", constraints={"pattern": r"^[A-Z]{3}$"})]
    result = validate_record({"code": "ABC"}, props)
    assert result.valid is True

    result_bad = validate_record({"code": "abc"}, props)
    assert result_bad.valid is False
    assert any("pattern" in e.message for e in result_bad.errors)


def test_constraint_enum_valid():
    props = [
        make_prop(
            "status", "enum", constraints={"values": ["active", "inactive", "pending"]}
        )
    ]
    result = validate_record({"status": "active"}, props)
    assert result.valid is True


def test_constraint_enum_invalid():
    props = [
        make_prop(
            "status", "enum", constraints={"values": ["active", "inactive", "pending"]}
        )
    ]
    result = validate_record({"status": "deleted"}, props)
    assert result.valid is False
    assert any("not in allowed values" in e.message for e in result.errors)


def test_constraint_on_none_value_skipped():
    """Constraints should not be checked when value is None."""
    props = [make_prop("age", "integer", constraints={"min": 0, "max": 150})]
    result = validate_record({"age": None}, props)
    assert result.valid is True
    assert result.coerced["age"] is None
