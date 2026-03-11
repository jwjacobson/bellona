"""Unit tests for QualityAgent. Agno/LLM calls are fully mocked."""
from unittest.mock import AsyncMock

import pytest

from bellona.agents.quality_agent import QualityAgent
from bellona.schemas.agents import QualityIssue, QualityReport


SAMPLE_ENTITY_TYPE = {
    "name": "Person",
    "properties": [
        {"name": "name", "data_type": "string", "required": True},
        {"name": "age", "data_type": "integer", "required": False},
        {"name": "email", "data_type": "string", "required": False},
    ],
}

SAMPLE_ENTITIES = [
    {"id": "1", "properties": {"name": "Alice", "age": 30, "email": "alice@example.com"}},
    {"id": "2", "properties": {"name": "Bob", "age": None, "email": "bob@example.com"}},
    {"id": "3", "properties": {"name": "Alice", "age": 30, "email": "alice2@example.com"}},
    {"id": "4", "properties": {"name": "Dave", "age": 999, "email": None}},
]

MOCK_REPORT = QualityReport(
    entity_type_name="Person",
    total_entities=4,
    issues=[
        QualityIssue(
            issue_type="missing_value",
            field="age",
            entity_ids=["2"],
            description="Entity 2 has null age",
            severity="low",
        ),
        QualityIssue(
            issue_type="potential_duplicate",
            field="name",
            entity_ids=["1", "3"],
            description="Entities 1 and 3 both have name 'Alice' with identical age",
            severity="medium",
        ),
        QualityIssue(
            issue_type="outlier",
            field="age",
            entity_ids=["4"],
            description="Age 999 is unusually high",
            severity="high",
        ),
    ],
    overall_quality_score=0.65,
    summary="3 issues found: 1 missing value, 1 potential duplicate, 1 outlier.",
)


async def test_quality_agent_returns_report() -> None:
    agent = QualityAgent(api_key="test-key")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", AsyncMock(return_value=MOCK_REPORT))
        result = await agent.check(SAMPLE_ENTITY_TYPE, SAMPLE_ENTITIES)

    assert isinstance(result, QualityReport)
    assert result.entity_type_name == "Person"
    assert result.total_entities == 4
    assert len(result.issues) == 3


async def test_quality_agent_issue_types() -> None:
    agent = QualityAgent(api_key="test-key")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", AsyncMock(return_value=MOCK_REPORT))
        result = await agent.check(SAMPLE_ENTITY_TYPE, SAMPLE_ENTITIES)

    issue_types = {i.issue_type for i in result.issues}
    assert "missing_value" in issue_types
    assert "potential_duplicate" in issue_types
    assert "outlier" in issue_types


async def test_quality_agent_overall_score_in_range() -> None:
    agent = QualityAgent(api_key="test-key")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", AsyncMock(return_value=MOCK_REPORT))
        result = await agent.check(SAMPLE_ENTITY_TYPE, SAMPLE_ENTITIES)

    assert 0.0 <= result.overall_quality_score <= 1.0


async def test_quality_agent_passes_entity_data_to_agent() -> None:
    agent = QualityAgent(api_key="test-key")
    run_agent_mock = AsyncMock(return_value=MOCK_REPORT)

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", run_agent_mock)
        await agent.check(SAMPLE_ENTITY_TYPE, SAMPLE_ENTITIES)

    run_agent_mock.assert_awaited_once()
    prompt_arg = run_agent_mock.call_args[0][0]
    assert "Person" in prompt_arg
    assert "Alice" in prompt_arg


async def test_quality_agent_empty_entities() -> None:
    """Agent handles empty entity list gracefully."""
    agent = QualityAgent(api_key="test-key")
    empty_report = QualityReport(
        entity_type_name="Person",
        total_entities=0,
        issues=[],
        overall_quality_score=1.0,
        summary="No entities to check.",
    )

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", AsyncMock(return_value=empty_report))
        result = await agent.check(SAMPLE_ENTITY_TYPE, [])

    assert result.total_entities == 0
    assert result.issues == []
    assert result.overall_quality_score == 1.0
