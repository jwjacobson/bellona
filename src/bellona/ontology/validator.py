import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from bellona.schemas.ontology import PropertyDefinitionCreate

_NULL_SENTINELS = {"unknown", "n/a", "na", "none", "-", "null", ""}

@dataclass
class FieldError:
    field: str
    message: str


@dataclass
class ValidationResult:
    valid: bool
    errors: list[FieldError] = field(default_factory=list)
    coerced: dict[str, Any] = field(default_factory=dict)


def _coerce(value: Any, data_type: str) -> tuple[Any, str | None]:
    """Attempt to coerce value to data_type. Returns (coerced_value, error_message)."""
    if value is None:
        return None, None

    if (
        isinstance(value, str)
        and data_type not in ("string", "enum")
        and value.lower().strip() in _NULL_SENTINELS
    ):
        return None, None

    try:
        match data_type:
            case "string":
                return str(value), None
            case "integer":
                return int(value), None
            case "float":
                return float(value), None
            case "boolean":
                if isinstance(value, bool):
                    return value, None
                if isinstance(value, str):
                    if value.lower() in ("true", "1", "yes"):
                        return True, None
                    if value.lower() in ("false", "0", "no"):
                        return False, None
                return None, f"Cannot coerce '{value}' to boolean"
            case "date":
                if isinstance(value, date):
                    return value, None
                return date.fromisoformat(str(value)), None
            case "datetime":
                if isinstance(value, datetime):
                    return value, None
                return datetime.fromisoformat(str(value)), None
            case "enum" | "json":
                return value, None
            case _:
                return value, None
    except (ValueError, TypeError) as exc:
        return None, str(exc)


def _check_constraints(value: Any, constraints: dict, data_type: str) -> str | None:
    if value is None:
        return None

    if "min" in constraints and value < constraints["min"]:
        return f"Value {value} is below minimum {constraints['min']}"
    if "max" in constraints and value > constraints["max"]:
        return f"Value {value} exceeds maximum {constraints['max']}"
    if "pattern" in constraints:
        if not re.fullmatch(constraints["pattern"], str(value)):
            return f"Value '{value}' does not match pattern '{constraints['pattern']}'"
    if data_type == "enum" and "values" in constraints:
        if value not in constraints["values"]:
            return f"Value '{value}' not in allowed values: {constraints['values']}"

    return None


def validate_record(
    record: dict[str, Any],
    property_definitions: list[PropertyDefinitionCreate],
) -> ValidationResult:
    errors: list[FieldError] = []
    coerced: dict[str, Any] = {}

    for prop in property_definitions:
        value = record.get(prop.name)

        if value is None and prop.required:
            errors.append(
                FieldError(field=prop.name, message=f"'{prop.name}' is required")
            )
            continue

        coerced_value, coerce_error = _coerce(value, prop.data_type)
        if coerce_error:
            errors.append(FieldError(field=prop.name, message=coerce_error))
            continue

        if coerced_value is not None and prop.constraints:
            constraint_error = _check_constraints(
                coerced_value, prop.constraints, prop.data_type
            )
            if constraint_error:
                errors.append(FieldError(field=prop.name, message=constraint_error))
                continue

        coerced[prop.name] = coerced_value

    # Pass through fields not in property definitions
    defined_names = {p.name for p in property_definitions}
    for key, value in record.items():
        if key not in defined_names:
            coerced[key] = value

    return ValidationResult(valid=len(errors) == 0, errors=errors, coerced=coerced)
