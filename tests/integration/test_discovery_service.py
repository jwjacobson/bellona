"""Integration tests for discovery service functions."""
import uuid
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.system import AgentProposal, Connector
from bellona.schemas.agents import (
    AuthDetection,
    DiscoveredResource,
    DiscoveryProposalContent,
    FieldSummary,
    PaginationConfig,
)
from bellona.services.agent_service import (
    ProposalError,
    confirm_discovery_proposal,
    discover_api,
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
            sample_record={"name": "Luke Skywalker", "height": "172"},
            schema_summary=[
                FieldSummary(name="name", inferred_type="string", required=True, sample_values=["Luke Skywalker"]),
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
            sample_record={"name": "Tatooine", "climate": "arid"},
            schema_summary=[
                FieldSummary(name="name", inferred_type="string", required=True, sample_values=["Tatooine"]),
            ],
            record_count_estimate=60,
        ),
    ],
)


async def test_discover_api(db_session: AsyncSession) -> None:
    proposal = await discover_api(
        db_session,
        "https://swapi.dev/api/",
        _mock_result=MOCK_DISCOVERY,
    )

    assert proposal.proposal_type == "discovery"
    assert proposal.status == "proposed"
    assert proposal.content["base_url"] == "https://swapi.dev/api/"
    assert len(proposal.content["resources"]) == 2
    assert proposal.connector_id is None
    assert proposal.entity_type_id is None


async def test_confirm_discovery_creates_connectors(db_session: AsyncSession) -> None:
    proposal = await discover_api(
        db_session,
        "https://swapi.dev/api/",
        _mock_result=MOCK_DISCOVERY,
    )

    with patch("bellona.services.agent_service.propose_schema", new_callable=AsyncMock):
        connectors = await confirm_discovery_proposal(db_session, proposal.id)

    assert len(connectors) == 2
    assert connectors[0].type == "rest_api"
    assert connectors[0].name == "people (https://swapi.dev/api/)"
    assert connectors[0].config["base_url"] == "https://swapi.dev/api/"
    assert connectors[0].config["endpoint"] == "/people/"
    assert connectors[0].config["records_jsonpath"] == "$.results"
    assert connectors[0].config["pagination"]["strategy"] == "offset"

    assert connectors[1].name == "planets (https://swapi.dev/api/)"

    # Verify proposal is confirmed
    refreshed = await db_session.get(AgentProposal, proposal.id)
    assert refreshed.status == "confirmed"


async def test_confirm_discovery_selected_resources(db_session: AsyncSession) -> None:
    proposal = await discover_api(
        db_session,
        "https://swapi.dev/api/",
        _mock_result=MOCK_DISCOVERY,
    )

    with patch("bellona.services.agent_service.propose_schema", new_callable=AsyncMock):
        connectors = await confirm_discovery_proposal(db_session, proposal.id, selected_resources=[1])

    assert len(connectors) == 1
    assert connectors[0].name == "planets (https://swapi.dev/api/)"


async def test_confirm_discovery_queues_schema_proposals(db_session: AsyncSession) -> None:
    proposal = await discover_api(
        db_session,
        "https://swapi.dev/api/",
        _mock_result=MOCK_DISCOVERY,
    )

    with patch("bellona.services.agent_service.propose_schema", new_callable=AsyncMock) as mock_propose:
        connectors = await confirm_discovery_proposal(db_session, proposal.id)

    assert mock_propose.await_count == 2
    # Verify each connector got a schema proposal queued
    called_connector_ids = {call.args[1] for call in mock_propose.call_args_list}
    assert len(called_connector_ids) == 2


async def test_confirm_discovery_wrong_type(db_session: AsyncSession) -> None:
    # Create a non-discovery proposal
    proposal = AgentProposal(
        proposal_type="mapping",
        status="proposed",
        content={"mappings": []},
    )
    db_session.add(proposal)
    await db_session.flush()

    with pytest.raises(ProposalError, match="not 'discovery'"):
        await confirm_discovery_proposal(db_session, proposal.id)


async def test_confirm_discovery_already_confirmed(db_session: AsyncSession) -> None:
    proposal = await discover_api(
        db_session,
        "https://swapi.dev/api/",
        _mock_result=MOCK_DISCOVERY,
    )

    with patch("bellona.services.agent_service.propose_schema", new_callable=AsyncMock):
        await confirm_discovery_proposal(db_session, proposal.id)

    # Try to confirm again
    with pytest.raises(ProposalError, match="already"):
        await confirm_discovery_proposal(db_session, proposal.id)
