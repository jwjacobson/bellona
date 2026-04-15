"""Unit tests for SchemaAgent. Agno/LLM calls are fully mocked."""

from unittest.mock import AsyncMock

import pytest

from bellona.agents.schema_agent import SchemaAgent
from bellona.connectors.base import SchemaDiscovery, SchemaField
from bellona.schemas.agents import (
    EntityTypeProposalContent,
    PotentialRelationship,
    ProposedPropertyDefinition,
)


SAMPLE_SCHEMA = SchemaDiscovery(
    fields=[
        SchemaField(
            name="ticker",
            inferred_type="string",
            nullable=False,
            sample_values=["AAPL", "GOOG"],
        ),
        SchemaField(
            name="price",
            inferred_type="float",
            nullable=False,
            sample_values=[150.5, 2800.0],
        ),
        SchemaField(
            name="volume",
            inferred_type="integer",
            nullable=True,
            sample_values=[1000000, 5000000],
        ),
        SchemaField(
            name="trade_date",
            inferred_type="date",
            nullable=False,
            sample_values=["2024-01-01"],
        ),
    ],
    record_count_estimate=500,
)

EXISTING_ENTITY_TYPES = [
    {"name": "Company", "description": "A business entity", "properties": []},
]

MOCK_PROPOSAL = EntityTypeProposalContent(
    entity_type_name="StockPrice",
    description="A daily stock price record for a traded security.",
    properties=[
        ProposedPropertyDefinition(
            name="ticker",
            data_type="string",
            required=True,
            description="Stock ticker symbol",
        ),
        ProposedPropertyDefinition(
            name="price", data_type="float", required=True, description="Closing price"
        ),
        ProposedPropertyDefinition(
            name="volume",
            data_type="integer",
            required=False,
            description="Daily trade volume",
        ),
        ProposedPropertyDefinition(
            name="trade_date",
            data_type="date",
            required=True,
            description="Date of trade",
        ),
    ],
    reasoning="The source fields describe stock price data. 'StockPrice' is a natural entity type name.",
    confidence=0.88,
)


async def test_schema_agent_returns_entity_type_proposal() -> None:
    agent = SchemaAgent(api_key="test-key")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", AsyncMock(return_value=MOCK_PROPOSAL))
        result = await agent.propose(SAMPLE_SCHEMA, EXISTING_ENTITY_TYPES)

    assert isinstance(result, EntityTypeProposalContent)
    assert result.entity_type_name == "StockPrice"
    assert len(result.properties) == 4
    assert result.confidence == 0.88


async def test_schema_agent_proposal_has_valid_property_types() -> None:
    agent = SchemaAgent(api_key="test-key")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", AsyncMock(return_value=MOCK_PROPOSAL))
        result = await agent.propose(SAMPLE_SCHEMA, EXISTING_ENTITY_TYPES)

    valid_types = {
        "string",
        "integer",
        "float",
        "boolean",
        "date",
        "datetime",
        "enum",
        "json",
    }
    for prop in result.properties:
        assert prop.data_type in valid_types


async def test_schema_agent_passes_schema_and_existing_types_to_agent() -> None:
    agent = SchemaAgent(api_key="test-key")
    run_agent_mock = AsyncMock(return_value=MOCK_PROPOSAL)

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", run_agent_mock)
        await agent.propose(SAMPLE_SCHEMA, EXISTING_ENTITY_TYPES)

    run_agent_mock.assert_awaited_once()
    prompt_arg = run_agent_mock.call_args[0][0]
    assert "ticker" in prompt_arg
    assert "Company" in prompt_arg


async def test_schema_agent_no_existing_types() -> None:
    agent = SchemaAgent(api_key="test-key")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", AsyncMock(return_value=MOCK_PROPOSAL))
        result = await agent.propose(SAMPLE_SCHEMA, [])

    assert result.entity_type_name == "StockPrice"


async def test_schema_agent_returns_potential_relationships() -> None:
    """Schema agent may emit potential relationship signals from field-name patterns."""
    proposal_with_rels = EntityTypeProposalContent(
        entity_type_name="Employee",
        description="",
        properties=[
            ProposedPropertyDefinition(name="id", data_type="integer", required=True),
            ProposedPropertyDefinition(
                name="manager_id", data_type="integer", required=False
            ),
        ],
        reasoning="self-reference detected",
        confidence=0.9,
        potential_relationships=[
            PotentialRelationship(
                source_field="manager_id",
                target_entity_type_name="Employee",
                basis="naming convention: '_id' suffix matching own entity type",
            ),
        ],
    )
    agent = SchemaAgent(api_key="test-key")
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", AsyncMock(return_value=proposal_with_rels))
        result = await agent.propose(SAMPLE_SCHEMA, [])

    assert len(result.potential_relationships) == 1
    signal = result.potential_relationships[0]
    assert signal.source_field == "manager_id"
    assert signal.target_entity_type_name == "Employee"
    assert "naming convention" in signal.basis


async def test_entity_type_proposal_defaults_to_empty_relationships() -> None:
    proposal = EntityTypeProposalContent(
        entity_type_name="Foo",
        properties=[],
        reasoning="",
        confidence=0.5,
    )
    assert proposal.potential_relationships == []


async def test_schema_agent_confidence_in_range() -> None:
    agent = SchemaAgent(api_key="test-key")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", AsyncMock(return_value=MOCK_PROPOSAL))
        result = await agent.propose(SAMPLE_SCHEMA, EXISTING_ENTITY_TYPES)

    assert 0.0 <= result.confidence <= 1.0
