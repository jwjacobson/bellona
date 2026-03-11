"""Integration tests for the agent service layer. Agent LLM calls are mocked."""
import uuid
from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.system import AgentProposal, Connector, FieldMapping
from bellona.models.ontology import EntityType
from bellona.schemas.agents import (
    EntityTypeProposalContent,
    FieldMappingProposedEntry,
    MappingProposalContent,
    ProposedPropertyDefinition,
)
from bellona.schemas.ontology import EntityTypeCreate, PropertyDefinitionCreate
from bellona.services.agent_service import (
    check_quality,
    confirm_mapping_proposal,
    confirm_schema_proposal,
    list_proposals,
    propose_mapping,
    propose_schema,
    reject_proposal,
    ProposalError
)
from bellona.services.entity_type import create_entity_type
from bellona.services.ingestion import create_connector

pytestmark = pytest.mark.asyncio(loop_scope="session")

SAMPLE_CSV = "name,age\nAlice,30\nBob,25\n"


# ── propose_mapping ────────────────────────────────────────────────────────────


async def test_propose_mapping_creates_agent_proposal(
    db_session: AsyncSession,
    tmp_path,
) -> None:
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(SAMPLE_CSV)
    connector = await create_connector(
        db_session, "csv", f"pm-conn-{uuid.uuid4().hex[:4]}", {"file_path": str(csv_file)}
    )
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"PM-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(name="name", data_type="string", required=True),
                PropertyDefinitionCreate(name="age", data_type="integer"),
            ],
        ),
    )
    await db_session.flush()

    mock_proposal = MappingProposalContent(
        mappings=[
            FieldMappingProposedEntry(
                source_field="name", target_property="name", confidence=0.95, reasoning="obvious"
            ),
            FieldMappingProposedEntry(
                source_field="age", target_property="age", confidence=0.90, reasoning="obvious"
            ),
        ],
        overall_confidence=0.92,
        notes="Clean mapping.",
    )

    proposal = await propose_mapping(
        db=db_session,
        connector_id=connector.id,
        entity_type_id=et.id,
        _mock_result=mock_proposal,
    )

    assert isinstance(proposal, AgentProposal)
    assert proposal.proposal_type == "mapping"
    assert proposal.status == "proposed"
    assert proposal.connector_id == connector.id
    assert proposal.entity_type_id == et.id
    assert proposal.confidence == pytest.approx(0.92, abs=0.001)
    assert len(proposal.content["mappings"]) == 2


async def test_propose_mapping_404_connector(db_session: AsyncSession) -> None:
    from bellona.services.agent_service import ProposalError
    with pytest.raises(ProposalError, match="Connector"):
        await propose_mapping(
            db=db_session,
            connector_id=uuid.uuid4(),
            entity_type_id=uuid.uuid4(),
        )


async def test_propose_mapping_404_entity_type(
    db_session: AsyncSession, tmp_path
) -> None:
    from bellona.services.agent_service import ProposalError
    csv_file = tmp_path / "test2.csv"
    csv_file.write_text(SAMPLE_CSV)
    connector = await create_connector(
        db_session, "csv", f"pm-404-{uuid.uuid4().hex[:4]}", {"file_path": str(csv_file)}
    )
    await db_session.flush()

    with pytest.raises(ProposalError, match="Entity type"):
        await propose_mapping(
            db=db_session,
            connector_id=connector.id,
            entity_type_id=uuid.uuid4(),
        )


# ── propose_schema ─────────────────────────────────────────────────────────────


async def test_propose_schema_creates_agent_proposal(
    db_session: AsyncSession, tmp_path
) -> None:
    csv_file = tmp_path / "stocks.csv"
    csv_file.write_text("ticker,price\nAAPL,150\nGOOG,2800\n")
    connector = await create_connector(
        db_session, "csv", f"ps-conn-{uuid.uuid4().hex[:4]}", {"file_path": str(csv_file)}
    )
    await db_session.flush()

    mock_proposal = EntityTypeProposalContent(
        entity_type_name="StockPrice",
        description="Stock price record",
        properties=[
            ProposedPropertyDefinition(name="ticker", data_type="string", required=True),
            ProposedPropertyDefinition(name="price", data_type="float", required=True),
        ],
        reasoning="These fields describe stock prices.",
        confidence=0.85,
    )

    proposal = await propose_schema(
        db=db_session,
        connector_id=connector.id,
        _mock_result=mock_proposal,
    )

    assert isinstance(proposal, AgentProposal)
    assert proposal.proposal_type == "entity_type"
    assert proposal.status == "proposed"
    assert proposal.connector_id == connector.id
    assert proposal.confidence == pytest.approx(0.85, abs=0.001)
    assert proposal.content["entity_type_name"] == "StockPrice"


# ── confirm_mapping_proposal ───────────────────────────────────────────────────


async def test_confirm_mapping_proposal_creates_field_mapping(
    db_session: AsyncSession, tmp_path
) -> None:
    csv_file = tmp_path / "confirm_test.csv"
    csv_file.write_text(SAMPLE_CSV)
    connector = await create_connector(
        db_session, "csv", f"cm-conn-{uuid.uuid4().hex[:4]}", {"file_path": str(csv_file)}
    )
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"CM-{uuid.uuid4().hex[:6]}",
            properties=[PropertyDefinitionCreate(name="name", data_type="string")],
        ),
    )
    await db_session.flush()

    proposal = AgentProposal(
        proposal_type="mapping",
        status="proposed",
        content={
            "mappings": [{"source_field": "name", "target_property": "name", "confidence": 0.9, "reasoning": "ok"}],
            "overall_confidence": 0.9,
            "notes": "",
        },
        confidence=0.9,
        connector_id=connector.id,
        entity_type_id=et.id,
    )
    db_session.add(proposal)
    await db_session.flush()

    field_mapping = await confirm_mapping_proposal(db_session, proposal.id)

    assert isinstance(field_mapping, FieldMapping)
    assert field_mapping.status == "confirmed"
    assert field_mapping.proposed_by == "agent"
    assert field_mapping.connector_id == connector.id
    assert field_mapping.entity_type_id == et.id

    await db_session.refresh(proposal)
    assert proposal.status == "approved"


async def test_confirm_mapping_proposal_wrong_type(db_session: AsyncSession) -> None:
    from bellona.services.agent_service import ProposalError
    proposal = AgentProposal(
        proposal_type="entity_type",
        status="proposed",
        content={"entity_type_name": "Foo", "properties": [], "reasoning": "", "confidence": 0.5},
        confidence=0.5,
    )
    db_session.add(proposal)
    await db_session.flush()

    with pytest.raises(ProposalError, match="mapping"):
        await confirm_mapping_proposal(db_session, proposal.id)


# ── confirm_schema_proposal ────────────────────────────────────────────────────


async def test_confirm_schema_proposal_creates_entity_type(
    db_session: AsyncSession,
) -> None:
    unique_name = f"Confirmed-{uuid.uuid4().hex[:6]}"
    proposal = AgentProposal(
        proposal_type="entity_type",
        status="proposed",
        content={
            "entity_type_name": unique_name,
            "description": "Agent-proposed entity type",
            "properties": [
                {"name": "ticker", "data_type": "string", "required": True, "description": "Symbol"},
                {"name": "price", "data_type": "float", "required": False, "description": "Price"},
            ],
            "reasoning": "Makes sense",
            "confidence": 0.88,
        },
        confidence=0.88,
    )
    db_session.add(proposal)
    await db_session.flush()

    entity_type = await confirm_schema_proposal(db_session, proposal.id)

    assert isinstance(entity_type, EntityType)
    assert entity_type.name == unique_name
    assert len(entity_type.property_definitions) == 2

    await db_session.refresh(proposal)
    assert proposal.status == "approved"


async def test_confirm_schema_proposal_wrong_type(db_session: AsyncSession) -> None:
    from bellona.services.agent_service import ProposalError
    proposal = AgentProposal(
        proposal_type="mapping",
        status="proposed",
        content={"mappings": [], "overall_confidence": 0.5, "notes": ""},
        confidence=0.5,
    )
    db_session.add(proposal)
    await db_session.flush()

    with pytest.raises(ProposalError, match="entity_type"):
        await confirm_schema_proposal(db_session, proposal.id)


async def test_confirm_already_approved_proposal(db_session: AsyncSession) -> None:
    proposal = AgentProposal(
        proposal_type="mapping",
        status="approved",
        content={"mappings": [], "overall_confidence": 0.5, "notes": ""},
        confidence=0.5,
    )
    db_session.add(proposal)
    await db_session.flush()

    with pytest.raises(ProposalError, match="already"):
        await confirm_mapping_proposal(db_session, proposal.id)

# ── reject_proposal ────────────────────────────────────────────────────────────


async def test_reject_proposal(db_session: AsyncSession) -> None:
    proposal = AgentProposal(
        proposal_type="mapping",
        status="proposed",
        content={"mappings": [], "overall_confidence": 0.5, "notes": ""},
        confidence=0.5,
    )
    db_session.add(proposal)
    await db_session.flush()

    result = await reject_proposal(db_session, proposal.id)

    assert result.status == "rejected"


async def test_reject_nonexistent_proposal(db_session: AsyncSession) -> None:
    from bellona.services.agent_service import ProposalError
    with pytest.raises(ProposalError, match="not found"):
        await reject_proposal(db_session, uuid.uuid4())


async def test_reject_already_approved_proposal(db_session: AsyncSession) -> None:
    proposal = AgentProposal(
        proposal_type="mapping",
        status="approved",
        content={"mappings": [], "overall_confidence": 0.5, "notes": ""},
        confidence=0.5,
    )
    db_session.add(proposal)
    await db_session.flush()

    with pytest.raises(ProposalError, match="already"):
        await reject_proposal(db_session, proposal.id)

# ── list_proposals ─────────────────────────────────────────────────────────────


async def test_list_proposals_returns_only_proposed(db_session: AsyncSession) -> None:
    base_content = {"mappings": [], "overall_confidence": 0.5, "notes": ""}
    proposed = AgentProposal(proposal_type="mapping", status="proposed", content=base_content)
    approved = AgentProposal(proposal_type="mapping", status="approved", content=base_content)
    rejected = AgentProposal(proposal_type="entity_type", status="rejected", content=base_content)
    db_session.add_all([proposed, approved, rejected])
    await db_session.flush()

    proposals = await list_proposals(db_session)

    ids = [p.id for p in proposals]
    assert proposed.id in ids
    assert approved.id not in ids
    assert rejected.id not in ids


# ── check_quality ──────────────────────────────────────────────────────────────


async def test_check_quality_returns_report(db_session: AsyncSession) -> None:
    from bellona.schemas.agents import QualityReport
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"QA-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(name="name", data_type="string", required=True),
                PropertyDefinitionCreate(name="score", data_type="float"),
            ],
        ),
    )
    await db_session.flush()

    mock_report = QualityReport(
        entity_type_name=et.name,
        total_entities=0,
        issues=[],
        overall_quality_score=1.0,
        summary="No issues found.",
    )

    report = await check_quality(db_session, et.id, _mock_result=mock_report)

    assert isinstance(report, QualityReport)
    assert report.entity_type_name == et.name


async def test_check_quality_404_entity_type(db_session: AsyncSession) -> None:
    with pytest.raises(ProposalError, match="Entity type"):
        await check_quality(db_session, uuid.uuid4())
