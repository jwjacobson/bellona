"""Unit tests for the query filter model and clause builder."""

import pytest
from sqlalchemy import and_, or_
from sqlalchemy.sql.elements import BinaryExpression, BooleanClauseList

from bellona.schemas.query import EntityQuery, FilterCondition, FilterGroup, SortClause
from bellona.services.query import _filter_node_to_clause


# ── FilterCondition parsing ───────────────────────────────────────────────────


def test_filter_condition_eq() -> None:
    cond = FilterCondition(property="name", operator="eq", value="Alice")
    assert cond.property == "name"
    assert cond.operator == "eq"
    assert cond.value == "Alice"


def test_filter_condition_numeric() -> None:
    cond = FilterCondition(property="age", operator="gte", value=30)
    assert cond.value == 30


def test_filter_condition_is_null() -> None:
    cond = FilterCondition(property="email", operator="is_null")
    assert cond.value is None


# ── FilterGroup parsing ───────────────────────────────────────────────────────


def test_filter_group_and() -> None:
    group = FilterGroup(
        op="and",
        conditions=[
            FilterCondition(property="a", operator="eq", value="x"),
            FilterCondition(property="b", operator="gt", value=5),
        ],
    )
    assert group.op == "and"
    assert len(group.conditions) == 2


def test_filter_group_nested() -> None:
    group = FilterGroup(
        op="and",
        conditions=[
            FilterCondition(property="founded_year", operator="gte", value=2020),
            FilterGroup(
                op="or",
                conditions=[
                    FilterCondition(property="status", operator="eq", value="active"),
                    FilterCondition(
                        property="employee_count", operator="gte", value=100
                    ),
                ],
            ),
        ],
    )
    assert group.op == "and"
    assert len(group.conditions) == 2
    inner = group.conditions[1]
    assert isinstance(inner, FilterGroup)
    assert inner.op == "or"
    assert len(inner.conditions) == 2


def test_filter_group_from_dict_nested() -> None:
    """Pydantic parses the nested dict structure from the spec example."""
    raw = {
        "op": "and",
        "conditions": [
            {"property": "founded_year", "operator": "gte", "value": 2020},
            {
                "op": "or",
                "conditions": [
                    {"property": "status", "operator": "eq", "value": "active"},
                    {"property": "employee_count", "operator": "gte", "value": 100},
                ],
            },
        ],
    }
    group = FilterGroup.model_validate(raw)
    assert isinstance(group.conditions[0], FilterCondition)
    assert isinstance(group.conditions[1], FilterGroup)


# ── EntityQuery parsing ───────────────────────────────────────────────────────


def test_entity_query_defaults() -> None:
    q = EntityQuery()
    assert q.entity_type_id is None
    assert q.filters is None
    assert q.sort == []
    assert q.page == 1
    assert q.page_size == 20


def test_entity_query_with_filter() -> None:
    q = EntityQuery(
        filters=FilterCondition(property="name", operator="eq", value="Alice"),
    )
    assert isinstance(q.filters, FilterCondition)


def test_entity_query_sort() -> None:
    q = EntityQuery(sort=[SortClause(property="name", direction="desc")])
    assert q.sort[0].direction == "desc"


# ── Clause builder ────────────────────────────────────────────────────────────


def test_build_eq_clause_returns_expression() -> None:
    cond = FilterCondition(property="name", operator="eq", value="Alice")
    clause = _filter_node_to_clause(cond)
    assert clause is not None


def test_build_gte_clause_returns_expression() -> None:
    cond = FilterCondition(property="age", operator="gte", value=30)
    clause = _filter_node_to_clause(cond)
    assert clause is not None


def test_build_in_clause_returns_expression() -> None:
    cond = FilterCondition(
        property="status", operator="in", value=["active", "pending"]
    )
    clause = _filter_node_to_clause(cond)
    assert clause is not None


def test_build_is_null_clause_returns_expression() -> None:
    cond = FilterCondition(property="email", operator="is_null")
    clause = _filter_node_to_clause(cond)
    assert clause is not None


def test_build_and_group_clause() -> None:
    group = FilterGroup(
        op="and",
        conditions=[
            FilterCondition(property="a", operator="eq", value="x"),
            FilterCondition(property="b", operator="gt", value=5),
        ],
    )
    clause = _filter_node_to_clause(group)
    assert clause is not None


def test_build_or_group_clause() -> None:
    group = FilterGroup(
        op="or",
        conditions=[
            FilterCondition(property="a", operator="eq", value="x"),
            FilterCondition(property="b", operator="eq", value="y"),
        ],
    )
    clause = _filter_node_to_clause(group)
    assert clause is not None


def test_build_nested_group_clause() -> None:
    group = FilterGroup(
        op="and",
        conditions=[
            FilterCondition(property="founded_year", operator="gte", value=2020),
            FilterGroup(
                op="or",
                conditions=[
                    FilterCondition(property="status", operator="eq", value="active"),
                    FilterCondition(
                        property="employee_count", operator="gte", value=100
                    ),
                ],
            ),
        ],
    )
    clause = _filter_node_to_clause(group)
    assert clause is not None


def test_unknown_operator_raises() -> None:
    """FilterCondition with invalid operator fails at model validation."""
    with pytest.raises(Exception):
        FilterCondition(property="x", operator="invalid_op", value="y")  # type: ignore[arg-type]
