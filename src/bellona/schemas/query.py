"""Query layer schemas: filter models, entity read/page, relationship read."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


# ── Filter model (recursive) ──────────────────────────────────────────────────


class FilterCondition(BaseModel):
    """A single property comparison."""

    property: str
    operator: Literal[
        "eq", "neq", "gt", "gte", "lt", "lte",
        "contains", "in", "is_null", "not_null",
    ]
    value: Any = None


class FilterGroup(BaseModel):
    """A logical group of conditions (AND / OR)."""

    op: Literal["and", "or"]
    conditions: list[Union[FilterGroup, FilterCondition]]


FilterGroup.model_rebuild()

FilterNode = Union[FilterGroup, FilterCondition]


# ── Query request ─────────────────────────────────────────────────────────────


class SortClause(BaseModel):
    property: str
    direction: Literal["asc", "desc"] = "asc"
    data_type: Literal["string", "numeric", "date"] = "string"


class EntityQuery(BaseModel):
    entity_type_id: uuid.UUID | None = None
    filters: Union[FilterGroup, FilterCondition, None] = None
    sort: list[SortClause] = Field(default_factory=list)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=200)


# ── Entity read / page ────────────────────────────────────────────────────────


class EntityRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    entity_type_id: uuid.UUID
    properties: dict[str, Any]
    schema_version: int
    source_connector_id: uuid.UUID | None
    source_record_id: str | None
    created_at: datetime
    updated_at: datetime


class EntityPage(BaseModel):
    items: list[EntityRead]
    total: int
    page: int
    page_size: int
    pages: int


# ── Relationship read ─────────────────────────────────────────────────────────


class RelationshipRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    relationship_type_id: uuid.UUID
    source_entity_id: uuid.UUID
    target_entity_id: uuid.UUID
    properties: dict[str, Any] | None
    created_at: datetime
