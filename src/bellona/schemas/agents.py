import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


# ── Agent output models (used as Agno response_model / output_model) ──────────


class FieldMappingProposedEntry(BaseModel):
    source_field: str
    target_property: str
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str


class MappingProposalContent(BaseModel):
    mappings: list[FieldMappingProposedEntry]
    overall_confidence: float = Field(ge=0.0, le=1.0)
    notes: str = ""


class ProposedPropertyDefinition(BaseModel):
    name: str
    data_type: Literal["string", "integer", "float", "boolean", "date", "datetime", "enum", "json"]
    required: bool = False
    description: str = ""


class EntityTypeProposalContent(BaseModel):
    entity_type_name: str
    description: str = ""
    properties: list[ProposedPropertyDefinition]
    reasoning: str
    confidence: float = Field(ge=0.0, le=1.0)


class QualityIssue(BaseModel):
    issue_type: Literal["missing_value", "potential_duplicate", "outlier", "type_mismatch"]
    field: str | None = None
    entity_ids: list[str] = Field(default_factory=list)
    description: str
    severity: Literal["low", "medium", "high"]


class QualityReport(BaseModel):
    entity_type_name: str
    total_entities: int
    issues: list[QualityIssue]
    overall_quality_score: float = Field(ge=0.0, le=1.0)
    summary: str


# ── API request/response schemas ──────────────────────────────────────────────


class MappingProposeRequest(BaseModel):
    connector_id: uuid.UUID
    entity_type_id: uuid.UUID


class SchemaProposeRequest(BaseModel):
    connector_id: uuid.UUID


class AgentProposalRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    proposal_type: str
    status: str
    content: dict[str, Any]
    confidence: float | None
    connector_id: uuid.UUID | None
    entity_type_id: uuid.UUID | None
    created_at: datetime


# ── Query Agent ────────────────────────────────────────────────────────────────


class QueryAgentResult(BaseModel):
    """Structured output from the Query Agent."""

    entity_type_name: str | None = None
    filters: dict[str, Any] | None = None
    sort: list[dict[str, Any]] = Field(default_factory=list)
    explanation: str
    confidence: float = Field(ge=0.0, le=1.0)


class NaturalLanguageQueryRequest(BaseModel):
    question: str
    entity_type_id: uuid.UUID | None = None


class NaturalLanguageQueryResponse(BaseModel):
    question: str
    explanation: str
    query_used: dict[str, Any] | None
    results: list[dict[str, Any]]
    total_results: int
