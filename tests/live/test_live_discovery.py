"""Live tests for Discovery Agent — hit real APIs."""
import pytest

from bellona.agents.discovery_agent import DiscoveryAgent
from bellona.core.config import get_settings
from bellona.schemas.agents import DiscoveryProposalContent

pytestmark = [
    pytest.mark.live,
    pytest.mark.asyncio(loop_scope="session"),
]


def _make_agent() -> DiscoveryAgent:
    settings = get_settings()
    return DiscoveryAgent(api_key=settings.claude_api_key, model=settings.claude_model)


async def test_discover_swapi() -> None:
    agent = _make_agent()
    result = await agent.discover("https://swapi.dev/api/")

    assert isinstance(result, DiscoveryProposalContent)
    assert result.base_url == "https://swapi.dev/api/"
    assert result.auth.auth_required is False

    # SWAPI should have at least 5 resources
    assert len(result.resources) >= 5

    resource_names = {r.resource_name.lower() for r in result.resources}
    assert "people" in resource_names

    # Check that at least one resource has correct pagination detection
    people = next(r for r in result.resources if r.resource_name.lower() == "people")
    assert people.records_jsonpath in ("$.results", "results")
    assert people.pagination.strategy == "offset"
    assert people.sample_record  # Should have a sample record
    assert len(people.schema_summary) > 0


async def test_discover_auth_required() -> None:
    agent = _make_agent()
    result = await agent.discover("https://httpbin.org/bearer")

    assert isinstance(result, DiscoveryProposalContent)
    assert result.auth.auth_required is True
