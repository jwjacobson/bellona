"""Integration tests for relationship proposal/confirm service layer."""

import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.ontology import RelationshipType
from bellona.models.system import AgentProposal
from bellona.schemas.agents import (
    EntityTypeProposalContent,
    PotentialRelationship,
    ProposedPropertyDefinition,
    ProposedRelationship,
    RelationshipProposalContent,
)
from bellona.schemas.ontology import EntityTypeCreate, PropertyDefinitionCreate
from bellona.services.agent_service import (
    ProposalError,
    confirm_relationship_proposal,
    propose_relationships,
)
from bellona.services.entity_type import create_entity_type
from bellona.services.ingestion import create_connector

pytestmark = pytest.mark.asyncio(loop_scope="session")

EMPLOYEES_CSV = (
    "id,name,manager_id\n"
    "1,Alice,\n"
    "2,Bob,1\n"
    "3,Carol,1\n"
    "4,Dan,2\n"
)


async def _create_employee_flow(db_session: AsyncSession, tmp_path):
    """Helper: creates a connector, an Employee entity type, and a confirmed schema proposal."""
    csv_file = tmp_path / "employees.csv"
    csv_file.write_text(EMPLOYEES_CSV)
    connector = await create_connector(
        db_session,
        "csv",
        f"rel-conn-{uuid.uuid4().hex[:4]}",
        {"file_path": str(csv_file)},
    )

    et_name = f"Employee-{uuid.uuid4().hex[:6]}"
    entity_type = await create_entity_type(
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

    schema_content = EntityTypeProposalContent(
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
                basis="naming convention + self-reference",
            ),
        ],
    )
    schema_proposal = AgentProposal(
        proposal_type="entity_type",
        status="confirmed",
        content=schema_content.model_dump(),
        confidence=0.9,
        connector_id=connector.id,
        entity_type_id=entity_type.id,
    )
    db_session.add(schema_proposal)
    await db_session.flush()
    return connector, entity_type, schema_proposal, et_name


async def test_propose_relationships_creates_proposal(
    db_session: AsyncSession, tmp_path
) -> None:
    _, _, schema_proposal, et_name = await _create_employee_flow(db_session, tmp_path)

    mock_result = RelationshipProposalContent(
        relationships=[
            ProposedRelationship(
                source_entity_type=et_name,
                target_entity_type=et_name,
                source_field="manager_id",
                relationship_name="reports_to",
                cardinality="many-to-one",
                confidence=0.92,
                reasoning="manager_id values repeat",
            )
        ],
        overall_confidence=0.92,
    )

    proposal = await propose_relationships(
        db=db_session,
        schema_proposal_id=schema_proposal.id,
        _mock_result=mock_result,
    )

    assert isinstance(proposal, AgentProposal)
    assert proposal.proposal_type == "relationship"
    assert proposal.status == "proposed"
    assert proposal.confidence == pytest.approx(0.92, abs=0.001)
    assert len(proposal.content["relationships"]) == 1
    assert proposal.content["relationships"][0]["source_field"] == "manager_id"


async def test_propose_relationships_unknown_proposal(
    db_session: AsyncSession,
) -> None:
    with pytest.raises(ProposalError, match="not found"):
        await propose_relationships(
            db=db_session,
            schema_proposal_id=uuid.uuid4(),
        )


async def test_propose_relationships_rejects_unconfirmed_schema(
    db_session: AsyncSession, tmp_path
) -> None:
    _, _, schema_proposal, _ = await _create_employee_flow(db_session, tmp_path)
    # Make it not-yet-confirmed
    schema_proposal.status = "proposed"
    await db_session.flush()

    with pytest.raises(ProposalError, match="confirmed"):
        await propose_relationships(
            db=db_session,
            schema_proposal_id=schema_proposal.id,
            _mock_result=RelationshipProposalContent(
                relationships=[], overall_confidence=0.0
            ),
        )


async def test_propose_relationships_filters_unknown_targets(
    db_session: AsyncSession, tmp_path
) -> None:
    """Signals pointing at a nonexistent entity type are dropped before the agent call."""
    _, _, schema_proposal, et_name = await _create_employee_flow(db_session, tmp_path)

    # Replace schema proposal content: one self-ref signal + one bogus target
    content = schema_proposal.content
    content["potential_relationships"] = [
        {
            "source_field": "manager_id",
            "target_entity_type_name": et_name,  # self, exists
            "basis": "self-ref",
        },
        {
            "source_field": "department_id",
            "target_entity_type_name": "NonExistentDepartment",
            "basis": "naming",
        },
    ]
    schema_proposal.content = content
    from sqlalchemy.orm.attributes import flag_modified

    flag_modified(schema_proposal, "content")
    await db_session.flush()

    captured: dict = {}

    async def fake_propose(self, **kwargs):
        captured.update(kwargs)
        return RelationshipProposalContent(
            relationships=[], overall_confidence=0.0
        )

    from bellona.agents.relationship_agent import RelationshipAgent

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(RelationshipAgent, "propose", fake_propose)
        await propose_relationships(
            db=db_session, schema_proposal_id=schema_proposal.id
        )

    filtered_signals = captured["signals"]
    assert len(filtered_signals) == 1
    assert filtered_signals[0].source_field == "manager_id"


async def test_confirm_relationship_proposal_creates_relationship_types(
    db_session: AsyncSession, tmp_path
) -> None:
    _, entity_type, schema_proposal, et_name = await _create_employee_flow(
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
                confidence=0.92,
                reasoning="repeats",
            )
        ],
        overall_confidence=0.92,
    )
    proposal = await propose_relationships(
        db=db_session,
        schema_proposal_id=schema_proposal.id,
        _mock_result=mock_result,
    )

    created = await confirm_relationship_proposal(db_session, proposal.id)

    assert len(created) == 1
    rt = created[0]
    assert isinstance(rt, RelationshipType)
    assert rt.source_entity_type_id == entity_type.id
    assert rt.target_entity_type_id == entity_type.id
    assert rt.cardinality == "many-to-one"

    await db_session.refresh(proposal)
    assert proposal.status == "confirmed"

    # Persisted
    rows = (
        (
            await db_session.execute(
                select(RelationshipType).where(RelationshipType.id == rt.id)
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == 1


async def test_confirm_relationship_wrong_type_fails(
    db_session: AsyncSession,
) -> None:
    proposal = AgentProposal(
        proposal_type="mapping",
        status="proposed",
        content={"mappings": [], "overall_confidence": 0.5, "notes": ""},
        confidence=0.5,
    )
    db_session.add(proposal)
    await db_session.flush()
    with pytest.raises(ProposalError, match="relationship"):
        await confirm_relationship_proposal(db_session, proposal.id)
