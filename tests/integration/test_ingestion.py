import uuid
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.connectors.rest_connector import RESTConnector
from bellona.models.entities import Entity
from bellona.models.ontology import EntityType
from bellona.models.system import Connector, FieldMapping, IngestionJob
from bellona.schemas.ontology import EntityTypeCreate, PropertyDefinitionCreate
from bellona.services import ingestion as ingestion_module
from bellona.services.entity_type import create_entity_type
from bellona.services.ingestion import (
    _execute_ingestion_job,
    create_connector,
    create_field_mapping,
    get_connector,
    get_ingestion_job,
    list_connectors,
)

pytestmark = pytest.mark.asyncio(loop_scope="session")

_DUMMY_REQUEST = httpx.Request("GET", "https://api.example.com/companies")


def _make_response(data: dict, status_code: int = 200) -> httpx.Response:
    return httpx.Response(status_code, json=data, request=_DUMMY_REQUEST)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def person_csv(tmp_path: Path) -> Path:
    f = tmp_path / "people.csv"
    f.write_text("name,age\nAlice,30\nBob,25\n")
    return f


@pytest.fixture
def person_csv_with_invalid(tmp_path: Path) -> Path:
    f = tmp_path / "people_invalid.csv"
    f.write_text("name,age\nAlice,30\n,not_a_number\n")
    return f


# ── Connector CRUD ────────────────────────────────────────────────────────────


async def test_create_connector(db_session: AsyncSession) -> None:
    connector = await create_connector(
        db_session,
        connector_type="csv",
        name="my-csv",
        config={"file_path": "/tmp/test.csv"},
    )
    await db_session.flush()
    assert connector.id is not None
    assert connector.type == "csv"
    assert connector.name == "my-csv"
    assert connector.status == "active"


async def test_get_connector(db_session: AsyncSession) -> None:
    connector = await create_connector(
        db_session,
        connector_type="csv",
        name="get-test",
        config={"file_path": "/tmp/get.csv"},
    )
    await db_session.flush()
    fetched = await get_connector(db_session, connector.id)
    assert fetched is not None
    assert fetched.id == connector.id


async def test_get_connector_not_found(db_session: AsyncSession) -> None:
    result = await get_connector(db_session, uuid.uuid4())
    assert result is None


async def test_list_connectors(db_session: AsyncSession) -> None:
    before = await list_connectors(db_session)
    await create_connector(db_session, "csv", "list-test-1", {"file_path": "/tmp/a.csv"})
    await create_connector(db_session, "csv", "list-test-2", {"file_path": "/tmp/b.csv"})
    await db_session.flush()
    after = await list_connectors(db_session)
    assert len(after) == len(before) + 2


# ── Field Mapping CRUD ────────────────────────────────────────────────────────


async def test_create_field_mapping(db_session: AsyncSession) -> None:
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"MappingTestType-{uuid.uuid4().hex[:6]}",
            properties=[PropertyDefinitionCreate(name="name", data_type="string")],
        ),
    )
    connector = await create_connector(
        db_session, "csv", "mapping-conn", {"file_path": "/tmp/m.csv"}
    )
    await db_session.flush()

    mapping = await create_field_mapping(
        db_session,
        connector_id=connector.id,
        entity_type_id=et.id,
        mapping_config={"mappings": [{"source_field": "full_name", "target_property": "name"}]},
    )
    await db_session.flush()
    assert mapping.id is not None
    assert mapping.status == "confirmed"
    assert mapping.proposed_by == "user"


# ── CSV Ingestion Pipeline ────────────────────────────────────────────────────


async def _setup_ingestion(
    db_session: AsyncSession,
    csv_path: Path,
    entity_type: EntityType,
    mapping_entries: list[dict],
) -> IngestionJob:
    """Helper: create connector, mapping, and job for a test."""
    connector = await create_connector(
        db_session, "csv", f"ingest-conn-{uuid.uuid4().hex[:6]}", {"file_path": str(csv_path)}
    )
    mapping = await create_field_mapping(
        db_session,
        connector_id=connector.id,
        entity_type_id=entity_type.id,
        mapping_config={"mappings": mapping_entries},
    )
    job = IngestionJob(connector_id=connector.id, status="pending")
    db_session.add(job)
    await db_session.flush()
    return job


async def test_execute_ingestion_job_creates_entities(
    db_session: AsyncSession, person_csv: Path
) -> None:
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"Person-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(name="name", data_type="string", required=True),
                PropertyDefinitionCreate(name="age", data_type="integer"),
            ],
        ),
    )
    job = await _setup_ingestion(
        db_session,
        person_csv,
        et,
        [
            {"source_field": "name", "target_property": "name"},
            {"source_field": "age", "target_property": "age"},
        ],
    )

    await _execute_ingestion_job(job.id, db_session)

    result = await db_session.execute(
        select(Entity).where(Entity.entity_type_id == et.id)
    )
    entities = list(result.scalars().all())
    assert len(entities) == 2
    names = {e.properties["name"] for e in entities}
    assert names == {"Alice", "Bob"}


async def test_execute_ingestion_job_updates_job_status(
    db_session: AsyncSession, person_csv: Path
) -> None:
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"Person2-{uuid.uuid4().hex[:6]}",
            properties=[PropertyDefinitionCreate(name="name", data_type="string")],
        ),
    )
    job = await _setup_ingestion(
        db_session,
        person_csv,
        et,
        [{"source_field": "name", "target_property": "name"}],
    )

    await _execute_ingestion_job(job.id, db_session)

    updated_job = await get_ingestion_job(db_session, job.id)
    assert updated_job is not None
    assert updated_job.status == "completed"
    assert updated_job.records_processed == 2
    assert updated_job.records_failed == 0
    assert updated_job.started_at is not None
    assert updated_job.completed_at is not None


async def test_execute_ingestion_job_tracks_failed_records(
    db_session: AsyncSession, person_csv_with_invalid: Path
) -> None:
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"Person3-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(name="name", data_type="string", required=True),
                PropertyDefinitionCreate(name="age", data_type="integer", required=True),
            ],
        ),
    )
    job = await _setup_ingestion(
        db_session,
        person_csv_with_invalid,
        et,
        [
            {"source_field": "name", "target_property": "name"},
            {"source_field": "age", "target_property": "age"},
        ],
    )

    await _execute_ingestion_job(job.id, db_session)

    updated_job = await get_ingestion_job(db_session, job.id)
    assert updated_job is not None
    assert updated_job.status == "completed"
    assert updated_job.records_processed == 1
    assert updated_job.records_failed == 1
    assert updated_job.error_log is not None


async def test_execute_ingestion_job_coerces_types(
    db_session: AsyncSession, person_csv: Path
) -> None:
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"Person4-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(name="name", data_type="string"),
                PropertyDefinitionCreate(name="age", data_type="integer"),
            ],
        ),
    )
    job = await _setup_ingestion(
        db_session,
        person_csv,
        et,
        [
            {"source_field": "name", "target_property": "name"},
            {"source_field": "age", "target_property": "age"},
        ],
    )

    await _execute_ingestion_job(job.id, db_session)

    result = await db_session.execute(
        select(Entity).where(Entity.entity_type_id == et.id)
    )
    entities = list(result.scalars().all())
    alice = next(e for e in entities if e.properties["name"] == "Alice")
    assert alice.properties["age"] == 30  # coerced from string "30" to int


async def test_execute_ingestion_job_fails_without_mapping(
    db_session: AsyncSession, person_csv: Path
) -> None:
    connector = await create_connector(
        db_session, "csv", f"no-mapping-{uuid.uuid4().hex[:6]}", {"file_path": str(person_csv)}
    )
    job = IngestionJob(connector_id=connector.id, status="pending")
    db_session.add(job)
    await db_session.flush()

    await _execute_ingestion_job(job.id, db_session)

    updated_job = await get_ingestion_job(db_session, job.id)
    assert updated_job is not None
    assert updated_job.status == "failed"


# ── REST Ingestion Pipeline ──────────────────────────────────────────────────


async def test_rest_connector_ingestion_pipeline(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"Company-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(name="company_name", data_type="string", required=True),
                PropertyDefinitionCreate(name="employee_count", data_type="integer"),
            ],
        ),
    )

    rest_config = {
        "base_url": "https://api.example.com",
        "endpoint": "/companies",
        "auth": {"type": "bearer", "value": "fake-token"},
        "records_jsonpath": "$.results",
        "pagination": {"strategy": "none"},
    }
    connector = await create_connector(
        db_session, "rest_api", "company-api", rest_config
    )

    await create_field_mapping(
        db_session,
        connector_id=connector.id,
        entity_type_id=et.id,
        mapping_config={
            "mappings": [
                {"source_field": "name", "target_property": "company_name"},
                {"source_field": "employees", "target_property": "employee_count"},
            ]
        },
    )

    job = IngestionJob(connector_id=connector.id, status="pending")
    db_session.add(job)
    await db_session.flush()

    api_response = _make_response({
        "results": [
            {"name": "Acme Corp", "employees": 150},
            {"name": "Widgets Inc", "employees": 42},
        ]
    })

    original_create = ingestion_module._create_connector_instance

    def _patched_create(connector_model):
        instance = original_create(connector_model)
        if isinstance(instance, RESTConnector):
            mock_client = AsyncMock(spec=httpx.AsyncClient)
            mock_client.get.return_value = api_response
            instance._client = mock_client
        return instance

    monkeypatch.setattr(ingestion_module, "_create_connector_instance", _patched_create)

    await _execute_ingestion_job(job.id, db_session)

    updated_job = await get_ingestion_job(db_session, job.id)
    assert updated_job is not None
    assert updated_job.status == "completed"
    assert updated_job.records_processed == 2
    assert updated_job.records_failed == 0

    result = await db_session.execute(
        select(Entity).where(Entity.entity_type_id == et.id)
    )
    entities = list(result.scalars().all())
    assert len(entities) == 2
    names = {e.properties["company_name"] for e in entities}
    assert names == {"Acme Corp", "Widgets Inc"}

    acme = next(e for e in entities if e.properties["company_name"] == "Acme Corp")
    assert acme.properties["employee_count"] == 150
