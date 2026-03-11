"""Unit tests for MapperAgent. Agno/LLM calls are fully mocked."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bellona.agents.mapper_agent import MapperAgent
from bellona.connectors.base import SchemaDiscovery, SchemaField
from bellona.schemas.agents import FieldMappingProposedEntry, MappingProposalContent


SAMPLE_SCHEMA = SchemaDiscovery(
    fields=[
        SchemaField(name="full_name", inferred_type="string", nullable=False, sample_values=["Alice", "Bob"]),
        SchemaField(name="years_old", inferred_type="integer", nullable=True, sample_values=[30, 25]),
        SchemaField(name="email_addr", inferred_type="string", nullable=True, sample_values=["a@example.com"]),
    ],
    record_count_estimate=100,
)

SAMPLE_ENTITY_TYPES = [
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

MOCK_PROPOSAL = MappingProposalContent(
    mappings=[
        FieldMappingProposedEntry(
            source_field="full_name",
            target_property="name",
            confidence=0.95,
            reasoning="'full_name' clearly maps to the 'name' property",
        ),
        FieldMappingProposedEntry(
            source_field="years_old",
            target_property="age",
            confidence=0.90,
            reasoning="'years_old' maps to 'age' — both represent age in years",
        ),
        FieldMappingProposedEntry(
            source_field="email_addr",
            target_property="email",
            confidence=0.88,
            reasoning="'email_addr' is clearly an email address",
        ),
    ],
    overall_confidence=0.91,
    notes="All source fields map cleanly to Person properties.",
)


async def test_mapper_agent_returns_mapping_proposal() -> None:
    agent = MapperAgent(api_key="test-key")

    with patch.object(agent, "_run_agent", new=AsyncMock(return_value=MOCK_PROPOSAL)):
        result = await agent.propose(SAMPLE_SCHEMA, SAMPLE_ENTITY_TYPES)

    assert isinstance(result, MappingProposalContent)
    assert len(result.mappings) == 3
    assert result.overall_confidence == 0.91


async def test_mapper_agent_mapping_fields() -> None:
    agent = MapperAgent(api_key="test-key")

    with patch.object(agent, "_run_agent", new=AsyncMock(return_value=MOCK_PROPOSAL)):
        result = await agent.propose(SAMPLE_SCHEMA, SAMPLE_ENTITY_TYPES)

    fields = {m.source_field: m.target_property for m in result.mappings}
    assert fields["full_name"] == "name"
    assert fields["years_old"] == "age"
    assert fields["email_addr"] == "email"


async def test_mapper_agent_passes_schema_and_entity_types_to_agent() -> None:
    agent = MapperAgent(api_key="test-key")
    run_agent_mock = AsyncMock(return_value=MOCK_PROPOSAL)

    with patch.object(agent, "_run_agent", new=run_agent_mock):
        await agent.propose(SAMPLE_SCHEMA, SAMPLE_ENTITY_TYPES)

    run_agent_mock.assert_awaited_once()
    prompt_arg = run_agent_mock.call_args[0][0]
    assert "full_name" in prompt_arg
    assert "Person" in prompt_arg


async def test_mapper_agent_confidence_scores_in_range() -> None:
    agent = MapperAgent(api_key="test-key")

    with patch.object(agent, "_run_agent", new=AsyncMock(return_value=MOCK_PROPOSAL)):
        result = await agent.propose(SAMPLE_SCHEMA, SAMPLE_ENTITY_TYPES)

    for m in result.mappings:
        assert 0.0 <= m.confidence <= 1.0
    assert 0.0 <= result.overall_confidence <= 1.0


async def test_mapper_agent_empty_entity_types() -> None:
    """Agent still runs with no existing entity types."""
    agent = MapperAgent(api_key="test-key")
    empty_proposal = MappingProposalContent(
        mappings=[], overall_confidence=0.0, notes="No entity types to map to."
    )

    with patch.object(agent, "_run_agent", new=AsyncMock(return_value=empty_proposal)):
        result = await agent.propose(SAMPLE_SCHEMA, [])

    assert result.mappings == []
