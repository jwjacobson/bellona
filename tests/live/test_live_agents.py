"""Live integration tests that call real LLM APIs.

These tests are skipped by default and only run when --live is passed:

    uv run pytest tests/live/ --live

They require CLAUDE_API_KEY to be set in the environment (via .env or .env.test).
"""
import pytest

from bellona.agents.mapper_agent import MapperAgent
from bellona.agents.quality_agent import QualityAgent
from bellona.agents.schema_agent import SchemaAgent
from bellona.connectors.base import SchemaDiscovery, SchemaField
from bellona.core.config import get_settings
from bellona.schemas.agents import (
    EntityTypeProposalContent,
    MappingProposalContent,
    QualityReport,
)

pytestmark = [
    pytest.mark.asyncio(loop_scope="session"),
    pytest.mark.live,
]


def _api_key() -> str:
    return get_settings().claude_api_key

def _model() -> str:
    return get_settings().claude_model

PERSON_SCHEMA = SchemaDiscovery(
    fields=[
        SchemaField(name="full_name", inferred_type="string", nullable=False, sample_values=["Alice Smith", "Bob Jones"]),
        SchemaField(name="years_old", inferred_type="integer", nullable=True, sample_values=[30, 25, 42]),
        SchemaField(name="email_address", inferred_type="string", nullable=True, sample_values=["alice@example.com"]),
    ],
    record_count_estimate=200,
)

PERSON_ENTITY_TYPES = [
    {
        "name": "Person",
        "description": "A human being",
        "properties": [
            {"name": "name", "data_type": "string", "required": True, "description": "Full name"},
            {"name": "age", "data_type": "integer", "required": False, "description": "Age in years"},
            {"name": "email", "data_type": "string", "required": False, "description": "Email address"},
        ],
    }
]

STOCK_SCHEMA = SchemaDiscovery(
    fields=[
        SchemaField(name="ticker", inferred_type="string", nullable=False, sample_values=["AAPL", "GOOG", "MSFT"]),
        SchemaField(name="close_price", inferred_type="float", nullable=False, sample_values=[150.5, 2800.0, 310.2]),
        SchemaField(name="trade_volume", inferred_type="integer", nullable=True, sample_values=[1000000, 5000000]),
        SchemaField(name="trade_date", inferred_type="date", nullable=False, sample_values=["2024-01-01", "2024-01-02"]),
    ],
    record_count_estimate=500,
)


# ── Mapper Agent live tests ────────────────────────────────────────────────────


async def test_live_mapper_agent_returns_valid_proposal() -> None:
    agent = MapperAgent(api_key=_api_key(), model=_model())
    result = await agent.propose(PERSON_SCHEMA, PERSON_ENTITY_TYPES)

    assert isinstance(result, MappingProposalContent)
    assert len(result.mappings) > 0
    assert 0.0 <= result.overall_confidence <= 1.0

    source_fields = {m.source_field for m in result.mappings}
    # The agent should map at least one of these obvious fields
    assert source_fields & {"full_name", "years_old", "email_address"}


async def test_live_mapper_agent_confidence_scores_valid() -> None:
    agent = MapperAgent(api_key=_api_key(), model=_model())
    result = await agent.propose(PERSON_SCHEMA, PERSON_ENTITY_TYPES)

    for m in result.mappings:
        assert 0.0 <= m.confidence <= 1.0
        assert m.source_field
        assert m.target_property
        assert m.reasoning


# ── Schema Agent live tests ───────────────────────────────────────────────────


async def test_live_schema_agent_proposes_entity_type() -> None:
    agent = SchemaAgent(api_key=_api_key(), model=_model())
    result = await agent.propose(STOCK_SCHEMA, [])

    assert isinstance(result, EntityTypeProposalContent)
    assert result.entity_type_name
    assert len(result.properties) > 0
    assert 0.0 <= result.confidence <= 1.0

    prop_names = {p.name for p in result.properties}
    # The agent should pick up at least the ticker and price fields
    assert prop_names & {"ticker", "close_price", "price"}


async def test_live_schema_agent_avoids_existing_types() -> None:
    """Agent should propose a name distinct from existing 'Person' type."""
    agent = SchemaAgent(api_key=_api_key(), model=_model())
    existing = [{"name": "Person", "description": "A human being", "properties": []}]
    result = await agent.propose(STOCK_SCHEMA, existing)

    assert result.entity_type_name != "Person"


async def test_live_schema_agent_valid_property_data_types() -> None:
    agent = SchemaAgent(api_key=_api_key(), model=_model())
    result = await agent.propose(STOCK_SCHEMA, [])

    valid_types = {"string", "integer", "float", "boolean", "date", "datetime", "enum", "json"}
    for prop in result.properties:
        assert prop.data_type in valid_types, f"Invalid data_type: {prop.data_type!r}"


# ── Quality Agent live tests ──────────────────────────────────────────────────


async def test_live_quality_agent_on_clean_data() -> None:
    entity_type = {
        "name": "Person",
        "properties": [
            {"name": "name", "data_type": "string", "required": True},
            {"name": "age", "data_type": "integer", "required": False},
        ],
    }
    entities = [
        {"id": "1", "properties": {"name": "Alice", "age": 30}},
        {"id": "2", "properties": {"name": "Bob", "age": 25}},
        {"id": "3", "properties": {"name": "Carol", "age": 40}},
    ]

    agent = QualityAgent(api_key=_api_key(), model=_model())
    result = await agent.check(entity_type, entities)

    assert isinstance(result, QualityReport)
    assert result.total_entities == 3
    assert 0.0 <= result.overall_quality_score <= 1.0
    # Clean data should score reasonably high
    assert result.overall_quality_score >= 0.7


async def test_live_quality_agent_detects_issues() -> None:
    entity_type = {
        "name": "Person",
        "properties": [
            {"name": "name", "data_type": "string", "required": True},
            {"name": "age", "data_type": "integer", "required": False},
        ],
    }
    entities = [
        {"id": "1", "properties": {"name": "Alice", "age": 30}},
        {"id": "2", "properties": {"name": "Alice", "age": 30}},  # duplicate
        {"id": "3", "properties": {"name": "Dave", "age": 9999}},  # outlier
        {"id": "4", "properties": {"name": None, "age": 25}},  # missing required
    ]

    agent = QualityAgent(api_key=_api_key(), model=_model())
    result = await agent.check(entity_type, entities)

    assert isinstance(result, QualityReport)
    assert len(result.issues) > 0
    # Data with clear issues should score lower
    assert result.overall_quality_score < 1.0
