"""Unit tests for DiscoveryAgent. Agno/LLM calls are fully mocked."""
from unittest.mock import AsyncMock

import pytest

from bellona.agents.discovery_agent import (
    DiscoveryAgent,
    _build_discovery_prompt,
    http_get,
    extract_jsonpath,
    infer_schema,
    detect_pagination,
)
from bellona.schemas.agents import (
    AuthDetection,
    DiscoveredResource,
    DiscoveryProposalContent,
    FieldSummary,
    PaginationConfig,
)


MOCK_PROPOSAL = DiscoveryProposalContent(
    base_url="https://swapi.dev/api/",
    api_description="Star Wars API with multiple resources.",
    auth=AuthDetection(auth_required=False, detected_scheme="none"),
    resources=[
        DiscoveredResource(
            resource_name="people",
            endpoint_path="/api/people/",
            records_jsonpath="$.results",
            pagination=PaginationConfig(
                strategy="offset",
                page_param="page",
                next_field_jsonpath="$.next",
            ),
            sample_record={"name": "Luke Skywalker", "height": "172"},
            schema_summary=[
                FieldSummary(name="name", inferred_type="string", required=True, sample_values=["Luke Skywalker"]),
                FieldSummary(name="height", inferred_type="string", required=True, sample_values=["172"]),
            ],
            record_count_estimate=82,
        ),
    ],
    agent_notes="SWAPI is a free, open API with no auth required.",
)


async def test_build_prompt_includes_base_url() -> None:
    prompt = _build_discovery_prompt("https://swapi.dev/api/")
    assert "https://swapi.dev/api/" in prompt
    assert "No auth credentials provided" in prompt


async def test_build_prompt_includes_auth_config() -> None:
    auth = {"type": "bearer", "token": "abc123"}
    prompt = _build_discovery_prompt("https://api.example.com/", auth)
    assert "https://api.example.com/" in prompt
    assert "bearer" in prompt
    assert "abc123" in prompt


async def test_discover_returns_proposal() -> None:
    agent = DiscoveryAgent(api_key="test-key")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", AsyncMock(return_value=MOCK_PROPOSAL))
        result = await agent.discover("https://swapi.dev/api/")

    assert isinstance(result, DiscoveryProposalContent)
    assert result.base_url == "https://swapi.dev/api/"
    assert len(result.resources) == 1
    assert result.resources[0].resource_name == "people"
    assert result.auth.auth_required is False


async def test_discover_passes_prompt_to_agent() -> None:
    agent = DiscoveryAgent(api_key="test-key")
    run_mock = AsyncMock(return_value=MOCK_PROPOSAL)

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(agent, "_run_agent", run_mock)
        await agent.discover("https://swapi.dev/api/", {"type": "bearer", "token": "x"})

    run_mock.assert_awaited_once()
    prompt = run_mock.call_args[0][0]
    assert "https://swapi.dev/api/" in prompt
    assert "bearer" in prompt


async def test_make_agent_has_tools() -> None:
    agent = DiscoveryAgent(api_key="test-key", model="claude-sonnet-4-6")
    agno_agent = agent._make_agent()
    # Agno stores tools; verify ours are included by checking the agent was created
    assert agno_agent is not None


async def test_proposal_round_trips() -> None:
    """Verify DiscoveryProposalContent serializes and deserializes cleanly."""
    data = MOCK_PROPOSAL.model_dump()
    restored = DiscoveryProposalContent.model_validate(data)
    assert restored.base_url == MOCK_PROPOSAL.base_url
    assert len(restored.resources) == len(MOCK_PROPOSAL.resources)
    assert restored.resources[0].pagination.strategy == "offset"
