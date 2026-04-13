"""Integration tests for agent API endpoints. Agent LLM calls are mocked via service layer."""

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
    ProposedPropertyDefinition,
    QualityIssue,
    QualityReport,
)
from bellona.schemas.ontology import EntityTypeCreate, PropertyDefinitionCreate
from bellona.services.entity_type import create_entity_type
from bellona.services.ingestion import create_connector

pytestmark = pytest.mark.asyncio(loop_scope="session")

SAMPLE_CSV = "ticker,price\nAAPL,150\nGOOG,2800\n"

MOCK_MAPPING_PROPOSAL = MappingProposalContent(
    mappings=[
        FieldMappingProposedEntry(
            source_field="ticker",
            target_property="symbol",
            confidence=0.95,
            reasoning="obvious",
        ),
    ],
    overall_confidence=0.95,
    notes="",
)

MOCK_SCHEMA_PROPOSAL = EntityTypeProposalContent(
    entity_type_name="StockPriceAPI",
    description="Stock price record",
    properties=[
        ProposedPropertyDefinition(name="ticker", data_type="string", required=True),
        ProposedPropertyDefinition(name="price", data_type="float", required=True),
    ],
    reasoning="These are stock price fields.",
    confidence=0.85,
)

MOCK_QUALITY_REPORT = QualityReport(
    entity_type_name="Stock",
    total_entities=0,
    issues=[],
    overall_quality_score=1.0,
    summary="No issues.",
)


# ── POST /api/v1/mappings/propose ─────────────────────────────────────────────


async def test_propose_mapping(
    client: AsyncClient, db_session: AsyncSession, tmp_path
) -> None:
    csv_file = tmp_path / "stocks.csv"
    csv_file.write_text(SAMPLE_CSV)
    connector = await create_connector(
        db_session,
        "csv",
        f"api-pm-{uuid.uuid4().hex[:4]}",
        {"file_path": str(csv_file)},
    )
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"ApiPM-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(
                    name="symbol", data_type="string", required=True
                )
            ],
        ),
    )
    await db_session.flush()

    with patch(
        "bellona.services.agent_service.MapperAgent.propose",
        new=AsyncMock(return_value=MOCK_MAPPING_PROPOSAL),
    ):
        response = await client.post(
            "/api/v1/mappings/propose",
            json={"connector_id": str(connector.id), "entity_type_id": str(et.id)},
        )

    assert response.status_code == 201
    data = response.json()
    assert data["proposal_type"] == "mapping"
    assert data["status"] == "proposed"
    assert data["confidence"] == pytest.approx(0.95, abs=0.001)
    assert data["connector_id"] == str(connector.id)
    assert "id" in data


async def test_propose_mapping_connector_not_found(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/mappings/propose",
        json={"connector_id": str(uuid.uuid4()), "entity_type_id": str(uuid.uuid4())},
    )
    assert response.status_code == 404


async def test_confirm_mapping(
    client: AsyncClient, db_session: AsyncSession, tmp_path
) -> None:
    csv_file = tmp_path / "confirm.csv"
    csv_file.write_text(SAMPLE_CSV)
    connector = await create_connector(
        db_session,
        "csv",
        f"api-cm-{uuid.uuid4().hex[:4]}",
        {"file_path": str(csv_file)},
    )
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"ApiCM-{uuid.uuid4().hex[:6]}",
            properties=[PropertyDefinitionCreate(name="symbol", data_type="string")],
        ),
    )
    proposal = AgentProposal(
        proposal_type="mapping",
        status="proposed",
        content={
            "mappings": [
                {
                    "source_field": "ticker",
                    "target_property": "symbol",
                    "confidence": 0.9,
                    "reasoning": "ok",
                }
            ],
            "overall_confidence": 0.9,
            "notes": "",
        },
        confidence=0.9,
        connector_id=connector.id,
        entity_type_id=et.id,
    )
    db_session.add(proposal)
    await db_session.flush()

    response = await client.post(f"/api/v1/proposals/{proposal.id}/confirm")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "confirmed"
    assert data["proposed_by"] == "agent"


async def test_confirm_mapping_not_found(client: AsyncClient) -> None:
    response = await client.post(f"/api/v1/proposals/{uuid.uuid4()}/confirm")
    assert response.status_code == 404


# ── POST /api/v1/schema/propose ───────────────────────────────────────────────


async def test_propose_schema(
    client: AsyncClient, db_session: AsyncSession, tmp_path
) -> None:
    csv_file = tmp_path / "schema.csv"
    csv_file.write_text(SAMPLE_CSV)
    connector = await create_connector(
        db_session,
        "csv",
        f"api-ps-{uuid.uuid4().hex[:4]}",
        {"file_path": str(csv_file)},
    )
    await db_session.flush()

    with patch(
        "bellona.services.agent_service.SchemaAgent.propose",
        new=AsyncMock(return_value=MOCK_SCHEMA_PROPOSAL),
    ):
        response = await client.post(
            "/api/v1/schema/propose",
            json={"connector_id": str(connector.id)},
        )

    assert response.status_code == 201
    data = response.json()
    assert data["proposal_type"] == "entity_type"
    assert data["status"] == "proposed"
    assert data["content"]["entity_type_name"] == "StockPriceAPI"


async def test_propose_schema_connector_not_found(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/schema/propose",
        json={"connector_id": str(uuid.uuid4())},
    )
    assert response.status_code == 404


# ── POST /api/v1/proposals/{id}/confirm ───────────────────────────────────────


async def test_confirm_schema_proposal(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    unique_name = f"ApiConfirm-{uuid.uuid4().hex[:6]}"
    proposal = AgentProposal(
        proposal_type="entity_type",
        status="proposed",
        content={
            "entity_type_name": unique_name,
            "description": "Agent proposed",
            "properties": [
                {
                    "name": "ticker",
                    "data_type": "string",
                    "required": True,
                    "description": "",
                },
            ],
            "reasoning": "Makes sense",
            "confidence": 0.88,
        },
        confidence=0.88,
    )
    db_session.add(proposal)
    await db_session.flush()

    response = await client.post(f"/api/v1/proposals/{proposal.id}/confirm")

    assert response.status_code == 200
    data = response.json()
    assert data["name"] == unique_name
    assert len(data["property_definitions"]) == 1


async def test_confirm_schema_proposal_not_found(client: AsyncClient) -> None:
    response = await client.post(f"/api/v1/proposals/{uuid.uuid4()}/confirm")
    assert response.status_code == 404


async def test_confirm_mapping_already_approved(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    proposal = AgentProposal(
        proposal_type="mapping",
        status="approved",
        content={"mappings": [], "overall_confidence": 0.5, "notes": ""},
        confidence=0.5,
    )
    db_session.add(proposal)
    await db_session.flush()

    response = await client.post(f"/api/v1/proposals/{proposal.id}/confirm")
    assert response.status_code == 422


async def test_confirm_schema_already_approved(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    proposal = AgentProposal(
        proposal_type="entity_type",
        status="approved",
        content={
            "entity_type_name": "Foo",
            "description": "",
            "properties": [],
            "reasoning": "",
            "confidence": 0.5,
        },
        confidence=0.5,
    )
    db_session.add(proposal)
    await db_session.flush()

    response = await client.post(f"/api/v1/proposals/{proposal.id}/confirm")
    assert response.status_code == 422


# ── POST /api/v1/proposals/{id}/reject ────────────────────────────────────────


async def test_reject_proposal(client: AsyncClient, db_session: AsyncSession) -> None:
    proposal = AgentProposal(
        proposal_type="mapping",
        status="proposed",
        content={"mappings": [], "overall_confidence": 0.5, "notes": ""},
        confidence=0.5,
    )
    db_session.add(proposal)
    await db_session.flush()

    response = await client.post(f"/api/v1/proposals/{proposal.id}/reject")

    assert response.status_code == 200
    assert response.json()["status"] == "rejected"


async def test_reject_proposal_not_found(client: AsyncClient) -> None:
    response = await client.post(f"/api/v1/proposals/{uuid.uuid4()}/reject")
    assert response.status_code == 404


# ── GET /api/v1/proposals ─────────────────────────────────────────────────────


async def test_list_proposals(client: AsyncClient, db_session: AsyncSession) -> None:
    proposed = AgentProposal(
        proposal_type="mapping",
        status="proposed",
        content={"mappings": [], "overall_confidence": 0.5, "notes": ""},
        confidence=0.5,
    )
    approved = AgentProposal(
        proposal_type="mapping",
        status="approved",
        content={"mappings": [], "overall_confidence": 0.5, "notes": ""},
    )
    db_session.add_all([proposed, approved])
    await db_session.flush()

    response = await client.get("/api/v1/proposals")

    assert response.status_code == 200
    ids = [p["id"] for p in response.json()]
    assert str(proposed.id) in ids
    assert str(approved.id) not in ids


# ── POST /api/v1/quality/check/{entity_type_id} ───────────────────────────────


async def test_quality_check(client: AsyncClient, db_session: AsyncSession) -> None:
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"QA-API-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(name="name", data_type="string", required=True)
            ],
        ),
    )
    await db_session.flush()

    with patch(
        "bellona.services.agent_service.QualityAgent.check",
        new=AsyncMock(return_value=MOCK_QUALITY_REPORT),
    ):
        response = await client.post(f"/api/v1/quality/check/{et.id}")

    assert response.status_code == 200
    data = response.json()
    assert data["entity_type_name"] == "Stock"
    assert data["overall_quality_score"] == 1.0
    assert data["issues"] == []


async def test_quality_check_not_found(client: AsyncClient) -> None:
    response = await client.post(f"/api/v1/quality/check/{uuid.uuid4()}")
    assert response.status_code == 404
