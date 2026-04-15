"""UI tests for the agent proposals panel."""

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.system import AgentProposal
from bellona.schemas.agents import (
    EntityTypeProposalContent,
    FieldMappingProposedEntry,
    MappingProposalContent,
    PotentialRelationship,
    ProposedPropertyDefinition,
    ProposedRelationship,
    RelationshipProposalContent,
)
from bellona.schemas.ontology import EntityTypeCreate, PropertyDefinitionCreate
from bellona.services.entity_type import create_entity_type
from bellona.services.ingestion import create_connector

pytestmark = pytest.mark.asyncio(loop_scope="session")

SAMPLE_CSV = "name,age\nAlice,30\nBob,25\n"


async def test_proposals_index(client: AsyncClient) -> None:
    response = await client.get("/ui/proposals")
    assert response.status_code == 200
    assert "Proposals" in response.text
    assert "Propose Mapping" in response.text
    assert "Ingestion Flow" in response.text


async def test_confirm_proposal_redirects(
    client: AsyncClient, db_session: AsyncSession, tmp_path
) -> None:
    csv_file = tmp_path / "proposals_test.csv"
    csv_file.write_text(SAMPLE_CSV)

    connector = await create_connector(
        db_session,
        "csv",
        f"ui-prop-conn-{uuid.uuid4().hex[:4]}",
        {"file_path": str(csv_file)},
    )
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"UIConfirmType-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(name="name", data_type="string"),
                PropertyDefinitionCreate(name="age", data_type="integer"),
            ],
        ),
    )
    await db_session.flush()

    mock_content = MappingProposalContent(
        mappings=[
            FieldMappingProposedEntry(
                source_field="name",
                target_property="name",
                confidence=0.9,
                reasoning="name matches name",
            ),
            FieldMappingProposedEntry(
                source_field="age",
                target_property="age",
                confidence=0.8,
                reasoning="age matches age",
            ),
        ],
        overall_confidence=0.85,
    )

    proposal = AgentProposal(
        proposal_type="mapping",
        status="proposed",
        content=mock_content.model_dump(),
        confidence=0.85,
        connector_id=connector.id,
        entity_type_id=et.id,
    )
    db_session.add(proposal)
    await db_session.flush()
    proposal_id = proposal.id

    response = await client.post(
        f"/ui/proposals/{proposal_id}/confirm",
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == f"/ui/connectors/{connector.id}"


async def test_reject_proposal_redirects(
    client: AsyncClient, db_session: AsyncSession, tmp_path
) -> None:
    csv_file = tmp_path / "proposals_reject_test.csv"
    csv_file.write_text(SAMPLE_CSV)

    connector = await create_connector(
        db_session,
        "csv",
        f"ui-rej-conn-{uuid.uuid4().hex[:4]}",
        {"file_path": str(csv_file)},
    )
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"UIRejectType-{uuid.uuid4().hex[:6]}",
            properties=[PropertyDefinitionCreate(name="name", data_type="string")],
        ),
    )
    await db_session.flush()

    proposal = AgentProposal(
        proposal_type="mapping",
        status="proposed",
        content={"mappings": [], "overall_confidence": 0.5},
        confidence=0.5,
        connector_id=connector.id,
        entity_type_id=et.id,
    )
    db_session.add(proposal)
    await db_session.flush()
    proposal_id = proposal.id

    response = await client.post(
        f"/ui/proposals/{proposal_id}/reject",
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/ui/proposals"


async def test_proposals_propose_mapping_redirects(client: AsyncClient) -> None:
    conn_resp = await client.post(
        "/api/v1/connectors",
        json={
            "type": "rest_api",
            "name": "ProposalsMappingTest",
            "config": {
                "base_url": "https://example.com",
                "endpoint": "/data",
                "records_jsonpath": "$.data",
                "pagination": {"strategy": "none"},
            },
        },
    )
    conn = conn_resp.json()
    et_resp = await client.post(
        "/api/v1/entity-types",
        json={
            "name": "ProposalsMappingET",
            "properties": [{"name": "name", "data_type": "string"}],
        },
    )
    et = et_resp.json()

    with patch("bellona.api.ui.proposals.propose_mapping") as mock_propose:
        mock_propose.return_value = AsyncMock()
        response = await client.post(
            "/ui/proposals/propose-mapping",
            data={
                "connector_id": conn["id"],
                "entity_type_id": et["id"],
            },
            follow_redirects=False,
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/ui/proposals"


async def test_proposals_propose_mapping_shows_error_on_failure(
    client: AsyncClient,
) -> None:
    from bellona.services.agent_service import ProposalError

    conn_resp = await client.post(
        "/api/v1/connectors",
        json={
            "type": "rest_api",
            "name": "ProposalsFailTest",
            "config": {
                "base_url": "https://example.com",
                "endpoint": "/data",
                "records_jsonpath": "$.data",
                "pagination": {"strategy": "none"},
            },
        },
    )
    conn = conn_resp.json()
    et_resp = await client.post(
        "/api/v1/entity-types",
        json={
            "name": "ProposalsFailET",
            "properties": [{"name": "x", "data_type": "string"}],
        },
    )
    et = et_resp.json()

    with patch("bellona.api.ui.proposals.propose_mapping") as mock_propose:
        mock_propose.side_effect = ProposalError("Mapper agent failed: test error")
        response = await client.post(
            "/ui/proposals/propose-mapping",
            data={
                "connector_id": conn["id"],
                "entity_type_id": et["id"],
            },
        )

    assert response.status_code == 422
    assert "Mapper agent failed" in response.text


# ── Relationship proposals ────────────────────────────────────────────────────


EMPLOYEES_CSV = "id,name,manager_id\n1,Alice,\n2,Bob,1\n3,Carol,1\n4,Dan,2\n"


async def _setup_confirmed_schema_with_signals(
    db_session: AsyncSession, tmp_path
):
    csv_file = tmp_path / f"emp-ui-{uuid.uuid4().hex[:4]}.csv"
    csv_file.write_text(EMPLOYEES_CSV)
    connector = await create_connector(
        db_session,
        "csv",
        f"ui-rel-{uuid.uuid4().hex[:4]}",
        {"file_path": str(csv_file)},
    )
    et_name = f"UIEmp-{uuid.uuid4().hex[:6]}"
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=et_name,
            properties=[
                PropertyDefinitionCreate(name="id", data_type="integer", required=True),
                PropertyDefinitionCreate(name="name", data_type="string"),
                PropertyDefinitionCreate(name="manager_id", data_type="integer"),
            ],
        ),
    )
    content = EntityTypeProposalContent(
        entity_type_name=et_name,
        properties=[
            ProposedPropertyDefinition(name="id", data_type="integer", required=True),
            ProposedPropertyDefinition(name="name", data_type="string"),
            ProposedPropertyDefinition(name="manager_id", data_type="integer"),
        ],
        reasoning="",
        confidence=0.9,
        potential_relationships=[
            PotentialRelationship(
                source_field="manager_id",
                target_entity_type_name=et_name,
                basis="self-ref",
            )
        ],
    )
    schema_proposal = AgentProposal(
        proposal_type="entity_type",
        status="confirmed",
        content=content.model_dump(),
        confidence=0.9,
        connector_id=connector.id,
        entity_type_id=et.id,
    )
    db_session.add(schema_proposal)
    await db_session.flush()
    return connector, et, schema_proposal, et_name


async def test_proposals_page_shows_propose_relationships_form(
    client: AsyncClient, db_session: AsyncSession, tmp_path
) -> None:
    _, _, schema_proposal, et_name = await _setup_confirmed_schema_with_signals(
        db_session, tmp_path
    )

    response = await client.get("/ui/proposals")
    assert response.status_code == 200
    assert "Propose Relationships" in response.text
    assert et_name in response.text


async def test_proposals_propose_relationships_redirects(
    client: AsyncClient, db_session: AsyncSession, tmp_path
) -> None:
    _, _, schema_proposal, et_name = await _setup_confirmed_schema_with_signals(
        db_session, tmp_path
    )

    mock_result = RelationshipProposalContent(
        relationships=[
            ProposedRelationship(
                source_entity_type=et_name,
                target_entity_type=et_name,
                source_field="manager_id",
                relationship_name="reports_to",
                cardinality="many-to-one",
                confidence=0.9,
                reasoning="",
            )
        ],
        overall_confidence=0.9,
    )

    with patch(
        "bellona.services.agent_service.RelationshipAgent.propose",
        new=AsyncMock(return_value=mock_result),
    ):
        response = await client.post(
            "/ui/proposals/propose-relationships",
            data={"schema_proposal_id": str(schema_proposal.id)},
            follow_redirects=False,
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/ui/proposals"


async def test_proposals_page_renders_relationship_proposal(
    client: AsyncClient, db_session: AsyncSession, tmp_path
) -> None:
    _, et, _, et_name = await _setup_confirmed_schema_with_signals(
        db_session, tmp_path
    )
    rel_proposal = AgentProposal(
        proposal_type="relationship",
        status="proposed",
        content=RelationshipProposalContent(
            relationships=[
                ProposedRelationship(
                    source_entity_type=et_name,
                    target_entity_type=et_name,
                    source_field="manager_id",
                    relationship_name="reports_to",
                    cardinality="many-to-one",
                    confidence=0.92,
                    reasoning="values repeat",
                )
            ],
            overall_confidence=0.92,
        ).model_dump(),
        confidence=0.92,
        entity_type_id=et.id,
    )
    db_session.add(rel_proposal)
    await db_session.flush()

    response = await client.get("/ui/proposals")
    assert response.status_code == 200
    assert "reports_to" in response.text
    assert "many-to-one" in response.text
    assert "manager_id" in response.text


async def test_confirm_relationship_proposal_ui_redirects(
    client: AsyncClient, db_session: AsyncSession, tmp_path
) -> None:
    _, et, _, et_name = await _setup_confirmed_schema_with_signals(
        db_session, tmp_path
    )
    rel_proposal = AgentProposal(
        proposal_type="relationship",
        status="proposed",
        content={
            "relationships": [
                {
                    "source_entity_type": et_name,
                    "target_entity_type": et_name,
                    "source_field": "manager_id",
                    "relationship_name": "reports_to",
                    "cardinality": "many-to-one",
                    "confidence": 0.92,
                    "reasoning": "",
                }
            ],
            "overall_confidence": 0.92,
            "notes": "",
        },
        confidence=0.92,
        entity_type_id=et.id,
    )
    db_session.add(rel_proposal)
    await db_session.flush()
    rel_id = rel_proposal.id

    response = await client.post(
        f"/ui/proposals/{rel_id}/confirm",
        follow_redirects=False,
    )
    assert response.status_code == 303
