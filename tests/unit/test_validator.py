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


def test_validate_type_coercion_integer():
    props = [make_prop("count", "integer")]
    result = validate_record({"count": "42"}, props)
    assert result.valid is True
    assert result.coerced["count"] == 42


def test_validate_type_coercion_float():
    props = [make_prop("score", "float")]
    result = validate_record({"score": "3.14"}, props)
    assert result.valid is True
    assert abs(result.coerced["score"] - 3.14) < 0.001


def test_validate_type_coercion_boolean():
    props = [make_prop("active", "boolean")]
    result = validate_record({"active": "true"}, props)
    assert result.valid is True
    assert result.coerced["active"] is True


def test_validate_type_coercion_failure():
    props = [make_prop("count", "integer")]
    result = validate_record({"count": "not_a_number"}, props)
    assert result.valid is False
    assert any("count" in e.field for e in result.errors)


def test_validate_none_on_optional_field():
    props = [make_prop("notes", "string", required=False)]
    result = validate_record({"notes": None}, props)
    assert result.valid is True
    assert result.coerced["notes"] is None


def test_validate_constraint_min_max():
    props = [make_prop("age", "integer", constraints={"min": 0, "max": 150})]
    result = validate_record({"age": 200}, props)
    assert result.valid is False
    assert any("age" in e.field for e in result.errors)


def test_validate_constraint_pattern():
    props = [make_prop("code", "string", constraints={"pattern": r"^[A-Z]{3}$"})]
    result = validate_record({"code": "ABC"}, props)
    assert result.valid is True

    result_bad = validate_record({"code": "abc"}, props)
    assert result_bad.valid is False


def test_validate_enum_constraint():
    props = [
        make_prop(
            "status", "enum", constraints={"values": ["active", "inactive", "pending"]}
        )
    ]
    result = validate_record({"status": "active"}, props)
    assert result.valid is True

    result_bad = validate_record({"status": "unknown"}, props)
    assert result_bad.valid is False


def test_validate_extra_fields_allowed():
    props = [make_prop("name", "string")]
    result = validate_record({"name": "Test", "extra_field": "extra"}, props)
    assert result.valid is True
