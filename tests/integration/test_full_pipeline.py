"""Mocked full-pipeline e2e test: CSV upload → schema proposal → mapping proposal
→ ingestion → structured query → natural language query.

All agent calls use the _mock_result injection pattern (no patch).
"""
import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.entities import Entity
from bellona.models.system import IngestionJob
from bellona.schemas.agents import (
    EntityTypeProposalContent,
    FieldMappingProposedEntry,
    MappingProposalContent,
    ProposedPropertyDefinition,
    QueryAgentResult,
)
from bellona.services.agent_service import (
    propose_mapping,
    propose_schema,
    run_nl_query,
)
from bellona.services.ingestion import _execute_ingestion_job

pytestmark = pytest.mark.asyncio(loop_scope="session")

# ── Sample data ───────────────────────────────────────────────────────────────

COMPANY_CSV = (
    "name,founded_year,employee_count,status\n"
    "Acme Corp,2018,150,active\n"
    "TechStart Inc,2021,25,active\n"
    "OldGuard LLC,2005,500,inactive\n"
    "NewWave Co,2022,10,active\n"
    "Steady State,2015,75,active\n"
)


# ── Full pipeline test ────────────────────────────────────────────────────────


async def test_full_pipeline(
    client: AsyncClient,
    db_session: AsyncSession,
    tmp_path,
) -> None:
    # Use a unique entity type name to avoid conflicts across test runs.
    et_name = f"FPCompany-{uuid.uuid4().hex[:6]}"

    # ── Step 1: Upload CSV file ───────────────────────────────────────────────
    upload_resp = await client.post(
        "/api/v1/connectors/csv/upload",
        files={"file": ("companies.csv", COMPANY_CSV.encode(), "text/csv")},
        data={"name": f"fp-companies-{uuid.uuid4().hex[:4]}"},
    )
    assert upload_resp.status_code == 201, upload_resp.text
    connector_id = uuid.UUID(upload_resp.json()["id"])

    # ── Step 2: Schema Agent proposes a Company entity type ───────────────────
    mock_schema = EntityTypeProposalContent(
        entity_type_name=et_name,
        description="A business entity parsed from the companies CSV.",
        properties=[
            ProposedPropertyDefinition(name="name", data_type="string", required=True,
                                       description="Company name"),
            ProposedPropertyDefinition(name="founded_year", data_type="integer",
                                       description="Year the company was founded"),
            ProposedPropertyDefinition(name="employee_count", data_type="integer",
                                       description="Number of employees"),
            ProposedPropertyDefinition(name="status", data_type="string",
                                       description="Operational status"),
        ],
        reasoning="The CSV contains standard company fields.",
        confidence=0.95,
    )

    schema_proposal = await propose_schema(
        db_session, connector_id, _mock_result=mock_schema
    )
    assert schema_proposal.proposal_type == "entity_type"
    assert schema_proposal.status == "proposed"

    # ── Step 3: Confirm schema proposal → creates the entity type ─────────────
    confirm_schema_resp = await client.post(
        f"/api/v1/proposals/{schema_proposal.id}/confirm"
    )
    assert confirm_schema_resp.status_code == 200, confirm_schema_resp.text
    entity_type_data = confirm_schema_resp.json()
    assert entity_type_data["name"] == et_name
    assert len(entity_type_data["property_definitions"]) == 4
    entity_type_id = uuid.UUID(entity_type_data["id"])

    # ── Step 4: Mapper Agent proposes field mappings ──────────────────────────
    mock_mapping = MappingProposalContent(
        mappings=[
            FieldMappingProposedEntry(
                source_field="name",
                target_property="name",
                confidence=0.98,
                reasoning="Exact column-to-property name match.",
            ),
            FieldMappingProposedEntry(
                source_field="founded_year",
                target_property="founded_year",
                confidence=0.97,
                reasoning="Exact column-to-property name match.",
            ),
            FieldMappingProposedEntry(
                source_field="employee_count",
                target_property="employee_count",
                confidence=0.97,
                reasoning="Exact column-to-property name match.",
            ),
            FieldMappingProposedEntry(
                source_field="status",
                target_property="status",
                confidence=0.96,
                reasoning="Exact column-to-property name match.",
            ),
        ],
        overall_confidence=0.97,
        notes="All CSV columns map directly to Company properties.",
    )

    mapping_proposal = await propose_mapping(
        db_session,
        connector_id=connector_id,
        entity_type_id=entity_type_id,
        _mock_result=mock_mapping,
    )
    assert mapping_proposal.proposal_type == "mapping"
    assert mapping_proposal.status == "proposed"
    assert len(mapping_proposal.content["mappings"]) == 4

    # ── Step 5: Confirm mapping proposal → creates FieldMapping ───────────────
    confirm_mapping_resp = await client.post(
        f"/api/v1/proposals/{mapping_proposal.id}/confirm"
    )
    assert confirm_mapping_resp.status_code == 200, confirm_mapping_resp.text
    field_mapping_data = confirm_mapping_resp.json()
    assert field_mapping_data["status"] == "confirmed"
    assert field_mapping_data["connector_id"] == str(connector_id)
    assert field_mapping_data["entity_type_id"] == str(entity_type_id)

    # ── Step 6: Ingest CSV data ───────────────────────────────────────────────
    # Create the job within the test session so _execute_ingestion_job can see it.
    job = IngestionJob(connector_id=connector_id, status="pending")
    db_session.add(job)
    await db_session.flush()

    await _execute_ingestion_job(job.id, db_session)

    # Verify all 5 rows were ingested.
    result = await db_session.execute(
        select(Entity).where(Entity.entity_type_id == entity_type_id)
    )
    entities = list(result.scalars().all())
    assert len(entities) == 5

    names = {e.properties["name"] for e in entities}
    assert "Acme Corp" in names
    assert "TechStart Inc" in names
    assert "NewWave Co" in names

    # Verify type coercion: founded_year should be an integer.
    acme = next(e for e in entities if e.properties["name"] == "Acme Corp")
    assert acme.properties["founded_year"] == 2018
    assert acme.properties["employee_count"] == 150

    # ── Step 7: Structured query — companies founded in 2020 or later ─────────
    query_resp = await client.post(
        "/api/v1/entities/query",
        json={
            "entity_type_id": str(entity_type_id),
            "filters": {
                "property": "founded_year",
                "operator": "gte",
                "value": 2020,
            },
        },
    )
    assert query_resp.status_code == 200, query_resp.text
    query_data = query_resp.json()

    # TechStart Inc (2021) and NewWave Co (2022) match; the others don't.
    assert query_data["total"] == 2
    recent_names = {item["properties"]["name"] for item in query_data["items"]}
    assert recent_names == {"TechStart Inc", "NewWave Co"}

    browse_resp = await client.get(
    f"/api/v1/entities?entity_type_id={entity_type_id}"
)
    assert browse_resp.status_code == 200
    assert browse_resp.json()["total"] == 5

    # ── Step 8: Structured query — active companies with nested filter ─────────
    nested_resp = await client.post(
        "/api/v1/entities/query",
        json={
            "entity_type_id": str(entity_type_id),
            "filters": {
                "op": "and",
                "conditions": [
                    {"property": "status", "operator": "eq", "value": "active"},
                    {"property": "employee_count", "operator": "gte", "value": 50},
                ],
            },
        },
    )
    assert nested_resp.status_code == 200, nested_resp.text
    nested_data = nested_resp.json()

    # Active companies with >= 50 employees: Acme Corp (150), Steady State (75).
    assert nested_data["total"] == 2
    large_active = {item["properties"]["name"] for item in nested_data["items"]}
    assert large_active == {"Acme Corp", "Steady State"}

    # ── Step 9: Natural language query via Query Agent (mocked) ───────────────
    mock_nl = QueryAgentResult(
        entity_type_name=et_name,
        filters={"property": "status", "operator": "eq", "value": "active"},
        sort=[{"property": "name", "direction": "asc"}],
        explanation="Find all active companies, sorted by name.",
        confidence=0.92,
    )

    nl_response = await run_nl_query(
        db_session,
        question="Which companies are currently active?",
        entity_type_id=entity_type_id,
        _mock_result=mock_nl,
    )

    # 4 of the 5 companies have status="active".
    assert nl_response.total_results == 4
    assert nl_response.explanation == "Find all active companies, sorted by name."
    assert nl_response.query_used is not None

    # Results should be sorted by name ascending.
    result_names = [r["properties"]["name"] for r in nl_response.results]
    assert result_names == sorted(result_names)
    assert "OldGuard LLC" not in result_names  # inactive
