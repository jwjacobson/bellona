"""Tests for connector pipeline status and jobs fragment route."""

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from unittest.mock import AsyncMock, patch

from bellona.models.system import AgentProposal, Connector, FieldMapping, IngestionJob
from bellona.services.ingestion import create_connector

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def _create_connector(db: AsyncSession, name: str = "pipeline-test") -> Connector:
    connector = await create_connector(
        db,
        "rest_api",
        name,
        {
            "base_url": "https://example.com",
            "endpoint": "/data",
            "records_jsonpath": "$.data",
            "pagination": {"strategy": "none"},
        },
    )
    await db.flush()
    return connector


async def _create_schema_proposal(
    db: AsyncSession, connector_id: uuid.UUID, status: str = "proposed"
) -> AgentProposal:
    proposal = AgentProposal(
        proposal_type="entity_type",
        status=status,
        content={"entity_type_name": "TestType", "properties": []},
        confidence=0.95,
        connector_id=connector_id,
    )
    db.add(proposal)
    await db.flush()
    return proposal


async def _create_mapping_proposal(
    db: AsyncSession, connector_id: uuid.UUID, status: str = "proposed"
) -> AgentProposal:
    proposal = AgentProposal(
        proposal_type="mapping",
        status=status,
        content={"mappings": [{"source_field": "x", "target_property": "x"}]},
        confidence=1.0,
        connector_id=connector_id,
    )
    db.add(proposal)
    await db.flush()
    return proposal


async def _create_field_mapping(
    db: AsyncSession, connector_id: uuid.UUID, entity_type_id: uuid.UUID
) -> FieldMapping:
    fm = FieldMapping(
        connector_id=connector_id,
        entity_type_id=entity_type_id,
        mapping_config={"mappings": [{"source_field": "x", "target_property": "x"}]},
        status="confirmed",
        proposed_by="agent",
    )
    db.add(fm)
    await db.flush()
    return fm


# ── Connectors index pipeline status ─────────────────────────────────────────


async def test_index_pipeline_shows_none_by_default(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    connector = await _create_connector(db_session, "pipeline-none")
    response = await client.get("/ui/connectors")
    assert response.status_code == 200
    # Both schema and mapping should show "none"
    assert "none" in response.text


async def test_index_pipeline_shows_schema_proposed(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    connector = await _create_connector(db_session, "pipeline-schema-proposed")
    await _create_schema_proposal(db_session, connector.id, status="proposed")
    response = await client.get("/ui/connectors")
    assert response.status_code == 200
    assert "proposed" in response.text


async def test_index_pipeline_shows_schema_confirmed(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    connector = await _create_connector(db_session, "pipeline-schema-confirmed")
    await _create_schema_proposal(db_session, connector.id, status="confirmed")
    response = await client.get("/ui/connectors")
    assert response.status_code == 200
    assert "confirmed" in response.text


async def test_index_pipeline_shows_mapping_confirmed(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    from bellona.schemas.ontology import EntityTypeCreate, PropertyDefinitionCreate
    from bellona.services.entity_type import create_entity_type

    connector = await _create_connector(db_session, "pipeline-mapping-confirmed")
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"MappingET-{uuid.uuid4().hex[:6]}",
            properties=[PropertyDefinitionCreate(name="x", data_type="string")],
        ),
    )
    await _create_field_mapping(db_session, connector.id, et.id)
    response = await client.get("/ui/connectors")
    assert response.status_code == 200
    assert "confirmed" in response.text


async def test_index_pipeline_shows_mapping_proposed(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    connector = await _create_connector(db_session, "pipeline-mapping-proposed")
    await _create_mapping_proposal(db_session, connector.id, status="proposed")
    response = await client.get("/ui/connectors")
    assert response.status_code == 200
    assert "proposed" in response.text


# ── GET /ui/connectors/{id}/jobs ─────────────────────────────────────────────


async def test_jobs_fragment_returns_html(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    connector = await _create_connector(db_session, "jobs-fragment-test")
    response = await client.get(f"/ui/connectors/{connector.id}/jobs")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


async def test_jobs_fragment_shows_completed_job(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    connector = await _create_connector(db_session, "jobs-fragment-completed")
    from datetime import datetime, UTC

    job = IngestionJob(
        connector_id=connector.id,
        status="completed",
        records_processed=42,
        records_failed=0,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )
    db_session.add(job)
    await db_session.flush()

    response = await client.get(f"/ui/connectors/{connector.id}/jobs")
    assert response.status_code == 200
    assert "completed" in response.text
    assert "42" in response.text


async def test_jobs_fragment_shows_empty_state(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    connector = await _create_connector(db_session, "jobs-fragment-empty")
    response = await client.get(f"/ui/connectors/{connector.id}/jobs")
    assert response.status_code == 200
    assert "No ingestion jobs" in response.text


async def test_jobs_fragment_shows_running_job(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    connector = await _create_connector(db_session, "jobs-fragment-running")
    from datetime import datetime, UTC

    job = IngestionJob(
        connector_id=connector.id,
        status="running",
        records_processed=10,
        records_failed=0,
        started_at=datetime.now(UTC),
    )
    db_session.add(job)
    await db_session.flush()

    response = await client.get(f"/ui/connectors/{connector.id}/jobs")
    assert response.status_code == 200
    assert "running" in response.text


async def test_jobs_fragment_includes_oob_sync_status(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    connector = await _create_connector(db_session, "jobs-fragment-oob")
    response = await client.get(f"/ui/connectors/{connector.id}/jobs")
    assert response.status_code == 200
    assert "pipeline-sync" in response.text
