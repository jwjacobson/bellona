"""Integration tests for discovery API endpoints."""

import uuid
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.system import AgentProposal
from bellona.schemas.agents import (
    AuthDetection,
    DiscoveredResource,
    DiscoveryProposalContent,
    FieldSummary,
    PaginationConfig,
)

pytestmark = pytest.mark.asyncio(loop_scope="session")


MOCK_DISCOVERY = DiscoveryProposalContent(
    base_url="https://swapi.dev/api/",
    api_description="Star Wars API",
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
            sample_record={"name": "Luke Skywalker"},
            schema_summary=[
                FieldSummary(
                    name="name",
                    inferred_type="string",
                    required=True,
                    sample_values=["Luke Skywalker"],
                ),
            ],
            record_count_estimate=82,
        ),
        DiscoveredResource(
            resource_name="planets",
            endpoint_path="/api/planets/",
            records_jsonpath="$.results",
            pagination=PaginationConfig(
                strategy="offset",
                page_param="page",
                next_field_jsonpath="$.next",
            ),
            sample_record={"name": "Tatooine"},
            schema_summary=[
                FieldSummary(
                    name="name",
                    inferred_type="string",
                    required=True,
                    sample_values=["Tatooine"],
                ),
            ],
            record_count_estimate=60,
        ),
    ],
)


async def test_discover_api_endpoint(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    with patch("bellona.services.agent_service.DiscoveryAgent") as mock_cls:
        instance = mock_cls.return_value
        instance.discover = AsyncMock(return_value=MOCK_DISCOVERY)

        resp = await client.post(
            "/api/v1/discovery/discover",
            json={"base_url": "https://swapi.dev/api/"},
        )

    assert resp.status_code == 201
    data = resp.json()
    assert data["proposal_type"] == "discovery"
    assert data["status"] == "proposed"
    assert data["content"]["base_url"] == "https://swapi.dev/api/"
    assert len(data["content"]["resources"]) == 2


async def test_confirm_discovery_endpoint(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    # Create proposal via mock
    with patch("bellona.services.agent_service.DiscoveryAgent") as mock_cls:
        instance = mock_cls.return_value
        instance.discover = AsyncMock(return_value=MOCK_DISCOVERY)
        resp = await client.post(
            "/api/v1/discovery/discover",
            json={"base_url": "https://swapi.dev/api/"},
        )
    proposal_id = resp.json()["id"]

    # Confirm it
    with patch("bellona.services.agent_service.propose_schema", new_callable=AsyncMock):
        resp = await client.post(f"/api/v1/discovery/{proposal_id}/confirm")

    assert resp.status_code == 200
    connectors = resp.json()
    assert len(connectors) == 2
    assert connectors[0]["type"] == "rest_api"
    assert connectors[0]["name"] == "people (https://swapi.dev/api/)"
    assert connectors[0]["config"]["endpoint"] == "/people/"


async def test_confirm_with_selection(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    with patch("bellona.services.agent_service.DiscoveryAgent") as mock_cls:
        instance = mock_cls.return_value
        instance.discover = AsyncMock(return_value=MOCK_DISCOVERY)
        resp = await client.post(
            "/api/v1/discovery/discover",
            json={"base_url": "https://swapi.dev/api/"},
        )
    proposal_id = resp.json()["id"]

    with patch("bellona.services.agent_service.propose_schema", new_callable=AsyncMock):
        resp = await client.post(
            f"/api/v1/discovery/{proposal_id}/confirm",
            json={"selected_resources": [0]},
        )

    assert resp.status_code == 200
    connectors = resp.json()
    assert len(connectors) == 1
    assert connectors[0]["name"] == "people (https://swapi.dev/api/)"


async def test_generic_confirm_dispatches_discovery(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    with patch("bellona.services.agent_service.DiscoveryAgent") as mock_cls:
        instance = mock_cls.return_value
        instance.discover = AsyncMock(return_value=MOCK_DISCOVERY)
        resp = await client.post(
            "/api/v1/discovery/discover",
            json={"base_url": "https://swapi.dev/api/"},
        )
    proposal_id = resp.json()["id"]

    # Use the generic confirm endpoint
    with patch("bellona.services.agent_service.propose_schema", new_callable=AsyncMock):
        resp = await client.post(f"/api/v1/proposals/{proposal_id}/confirm")

    assert resp.status_code == 200
    # Generic confirm returns list of connectors for discovery type
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 2
