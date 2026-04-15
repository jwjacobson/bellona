"""Unit tests for RelationshipAgent. Agno/LLM calls are fully mocked."""

from unittest.mock import AsyncMock

import pytest

from bellona.agents.relationship_agent import RelationshipAgent
from bellona.schemas.agents import (
    PotentialRelationship,
    ProposedRelationship,
    RelationshipProposalContent,
)


SIGNALS = [
    PotentialRelationship(
        source_field="manager_id",
        target_entity_type_name="Employee",
        basis="naming convention",
    ),
]

SAMPLE_RECORDS = [
    {"id": "1", "name": "Alice", "manager_id": None},
    {"id": "2", "name": "Bob", "manager_id": "1"},
    {"id": "3", "name": "Carol", "manager_id": "1"},
    {"id": "4", "name": "Dan", "manager_id": "2"},
]

EXISTING_ENTITY_TYPES = [
    {"name": "Employee", "description": "A person employed by the company", "properties": []},
]

MOCK_RESULT = RelationshipProposalContent(
    relationships=[
        ProposedRelationship(
            source_entity_type="Employee",
            target_entity_type="Employee",
            source_field="manager_id",
            relationship_name="reports_to",
            cardinality="many-to-one",
            confidence=0.93,
            reasoning="manager_id values repeat — many employees point to one manager",
        )
    ],
    overall_confidence=0.93,
    notes="",
)


async def test_relationship_agent_returns_proposal() -> None:
    agent = RelationshipAgent(api_key="test-key")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", AsyncMock(return_value=MOCK_RESULT))
        result = await agent.propose(
            source_entity_type_name="Employee",
            signals=SIGNALS,
            sample_records=SAMPLE_RECORDS,
            existing_entity_types=EXISTING_ENTITY_TYPES,
        )

    assert isinstance(result, RelationshipProposalContent)
    assert len(result.relationships) == 1
    rel = result.relationships[0]
    assert rel.source_entity_type == "Employee"
    assert rel.target_entity_type == "Employee"
    assert rel.source_field == "manager_id"
    assert rel.cardinality == "many-to-one"
    assert 0.0 <= rel.confidence <= 1.0


async def test_relationship_agent_prompt_contains_signals_and_samples() -> None:
    agent = RelationshipAgent(api_key="test-key")
    run_mock = AsyncMock(return_value=MOCK_RESULT)

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", run_mock)
        await agent.propose(
            source_entity_type_name="Employee",
            signals=SIGNALS,
            sample_records=SAMPLE_RECORDS,
            existing_entity_types=EXISTING_ENTITY_TYPES,
        )

    run_mock.assert_awaited_once()
    prompt = run_mock.call_args[0][0]
    assert "manager_id" in prompt
    assert "Employee" in prompt
    # Sample values should be present so agent can infer cardinality
    assert "Alice" in prompt or "Bob" in prompt


async def test_relationship_agent_no_signals_returns_empty_gracefully() -> None:
    empty_result = RelationshipProposalContent(
        relationships=[], overall_confidence=0.0, notes="no signals"
    )
    agent = RelationshipAgent(api_key="test-key")
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", AsyncMock(return_value=empty_result))
        result = await agent.propose(
            source_entity_type_name="Employee",
            signals=[],
            sample_records=SAMPLE_RECORDS,
            existing_entity_types=EXISTING_ENTITY_TYPES,
        )

    assert result.relationships == []
