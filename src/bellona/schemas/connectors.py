import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


ConnectorType = Literal["csv", "rest_api"]
MappingStatus = Literal["proposed", "confirmed", "archived"]


# ── Field Mapping ─────────────────────────────────────────────────────────────


class FieldMappingEntry(BaseModel):
    source_field: str
    target_property: str


class MappingConfig(BaseModel):
    mappings: list[FieldMappingEntry]


class FieldMappingCreate(BaseModel):
    connector_id: uuid.UUID
    entity_type_id: uuid.UUID
    mapping_config: MappingConfig
    proposed_by: str = "user"


class FieldMappingRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    connector_id: uuid.UUID
    entity_type_id: uuid.UUID
    mapping_config: dict[str, Any]
    status: str
    proposed_by: str


# ── Connector ─────────────────────────────────────────────────────────────────


class ConnectorCreate(BaseModel):
    type: ConnectorType
    name: str = Field(min_length=1, max_length=255)
    config: dict[str, Any]


class ConnectorRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    type: str
    name: str
    config: dict[str, Any]
    status: str
    last_sync_at: datetime | None


class ConnectorPatch(BaseModel):
    name: str | None = None
    config: dict[str, Any] | None = None

# ── Ingestion Job ─────────────────────────────────────────────────────────────


class IngestionJobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    connector_id: uuid.UUID
    status: str
    records_processed: int
    records_failed: int
    error_log: dict[str, Any] | None
    started_at: datetime | None
    completed_at: datetime | None


# ── Schema Discovery ──────────────────────────────────────────────────────────


class SchemaFieldRead(BaseModel):
    name: str
    inferred_type: str
    nullable: bool
    sample_values: list[Any]


class SchemaDiscoveryRead(BaseModel):
    fields: list[SchemaFieldRead]
    record_count_estimate: int | None
