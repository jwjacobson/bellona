import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

DataType = Literal[
    "string", "integer", "float", "boolean", "date", "datetime", "enum", "json"
]
Cardinality = Literal["one-to-one", "one-to-many", "many-to-many"]


# ── Property Definitions ─────────────────────────────────────────────────────


class PropertyDefinitionCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    data_type: DataType
    required: bool = False
    constraints: dict | None = None
    description: str | None = None


class PropertyDefinitionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    entity_type_id: uuid.UUID
    name: str
    data_type: str
    required: bool
    constraints: dict | None
    description: str | None
    schema_version: int


# ── Entity Types ─────────────────────────────────────────────────────────────


class EntityTypeCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    properties: list[PropertyDefinitionCreate] = Field(default_factory=list)


class EntityTypePatch(BaseModel):
    description: str | None = None
    add_properties: list[PropertyDefinitionCreate] = Field(default_factory=list)


class EntityTypeRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    description: str | None
    schema_version: int
    created_at: datetime
    updated_at: datetime
    property_definitions: list[PropertyDefinitionRead] = Field(default_factory=list)


class EntityTypeList(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    description: str | None
    schema_version: int
    created_at: datetime
    updated_at: datetime


# ── Relationship Types ───────────────────────────────────────────────────────


class RelationshipTypeCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    source_entity_type_id: uuid.UUID
    target_entity_type_id: uuid.UUID
    cardinality: Cardinality
    properties: dict | None = None


class RelationshipTypeRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    source_entity_type_id: uuid.UUID
    target_entity_type_id: uuid.UUID
    cardinality: str
    properties: dict | None
