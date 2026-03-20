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


# ── Discovery Agent ───────────────────────────────────────────────────────────


class FieldSummary(BaseModel):
    """Inferred field from sample records."""

    name: str
    inferred_type: str
    required: bool
    sample_values: list[str] = Field(default_factory=list)


class PaginationConfig(BaseModel):
    """Pagination configuration for a resource."""

    strategy: Literal["offset", "cursor", "link_header", "none"]
    page_param: str | None = None
    size_param: str | None = None
    cursor_param: str | None = None
    next_field_jsonpath: str | None = None


class AuthDetection(BaseModel):
    """What the agent detected about API authentication."""

    auth_required: bool
    detected_scheme: Literal["none", "bearer", "api_key", "basic", "unknown"] = "none"
    details: str | None = None


class DiscoveredResource(BaseModel):
    """A single API resource discovered by the agent."""

    resource_name: str
    endpoint_path: str
    records_jsonpath: str
    pagination: PaginationConfig
    sample_record: dict[str, Any]
    schema_summary: list[FieldSummary]
    record_count_estimate: int | None = None


class DiscoveryProposalContent(BaseModel):
    """Full output of the Discovery Agent."""

    base_url: str
    api_description: str
    auth: AuthDetection
    resources: list[DiscoveredResource]
    agent_notes: str | None = None


class DiscoveryRequest(BaseModel):
    base_url: str
    auth_config: dict[str, Any] | None = None


class ConfirmDiscoveryRequest(BaseModel):
    selected_resources: list[int] | None = None
