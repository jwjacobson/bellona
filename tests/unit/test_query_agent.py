"""Unit tests for QueryAgent. Agno/LLM calls are fully mocked."""

from unittest.mock import AsyncMock, patch

import pytest

from bellona.agents.query_agent import QueryAgent
from bellona.schemas.agents import QueryAgentResult

SAMPLE_ONTOLOGY = [
    {
        "name": "Company",
        "description": "A business entity",
        "properties": [
            {"name": "name", "data_type": "string", "required": True},
            {"name": "founded_year", "data_type": "integer", "required": False},
            {"name": "status", "data_type": "string", "required": False},
            {"name": "employee_count", "data_type": "integer", "required": False},
        ],
    }
]

MOCK_RESULT = QueryAgentResult(
    entity_type_name="Company",
    filters={
        "op": "and",
        "conditions": [
            {"property": "founded_year", "operator": "gte", "value": 2020},
            {"property": "status", "operator": "eq", "value": "active"},
        ],
    },
    sort=[{"property": "name", "direction": "asc"}],
    explanation="Find companies founded after 2020 that are active.",
    confidence=0.9,
)


async def test_query_agent_returns_result() -> None:
    agent = QueryAgent(api_key="test-key")

    with patch.object(agent, "_run_agent", new=AsyncMock(return_value=MOCK_RESULT)):
        result = await agent.translate(
            "Find active companies founded after 2020", SAMPLE_ONTOLOGY
        )

    assert isinstance(result, QueryAgentResult)
    assert result.entity_type_name == "Company"
    assert result.explanation != ""


async def test_query_agent_passes_question_to_prompt() -> None:
    agent = QueryAgent(api_key="test-key")
    run_mock = AsyncMock(return_value=MOCK_RESULT)

    with patch.object(agent, "_run_agent", new=run_mock):
        await agent.translate(
            "Find active companies founded after 2020", SAMPLE_ONTOLOGY
        )

    run_mock.assert_awaited_once()
    prompt = run_mock.call_args[0][0]
    assert "active companies" in prompt
    assert "Company" in prompt


async def test_query_agent_includes_ontology_in_prompt() -> None:
    agent = QueryAgent(api_key="test-key")
    run_mock = AsyncMock(return_value=MOCK_RESULT)

    with patch.object(agent, "_run_agent", new=run_mock):
        await agent.translate("query", SAMPLE_ONTOLOGY)

    prompt = run_mock.call_args[0][0]
    assert "founded_year" in prompt
    assert "employee_count" in prompt


async def test_query_agent_confidence_in_range() -> None:
    agent = QueryAgent(api_key="test-key")

    with patch.object(agent, "_run_agent", new=AsyncMock(return_value=MOCK_RESULT)):
        result = await agent.translate("query", SAMPLE_ONTOLOGY)

    assert 0.0 <= result.confidence <= 1.0


async def test_query_agent_no_filter_result() -> None:
    agent = QueryAgent(api_key="test-key")
    no_filter_result = QueryAgentResult(
        entity_type_name="Company",
        filters=None,
        sort=[],
        explanation="Return all companies.",
        confidence=0.95,
    )

    with patch.object(
        agent, "_run_agent", new=AsyncMock(return_value=no_filter_result)
    ):
        result = await agent.translate("List all companies", SAMPLE_ONTOLOGY)

    assert result.filters is None


async def test_query_agent_empty_ontology() -> None:
    agent = QueryAgent(api_key="test-key")
    empty_result = QueryAgentResult(
        entity_type_name=None,
        filters=None,
        sort=[],
        explanation="No entity types available to query.",
        confidence=0.1,
    )

    with patch.object(agent, "_run_agent", new=AsyncMock(return_value=empty_result)):
        result = await agent.translate("Find something", [])

    assert result.entity_type_name is None
