"""UI tests for the agent proposals panel."""
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.system import AgentProposal
from bellona.schemas.agents import (
    FieldMappingProposedEntry,
    MappingProposalContent,
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
    assert response.headers["location"] == "/ui/proposals"


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
