import uuid
from pathlib import Path

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.system import Connector, IngestionJob
from bellona.schemas.ontology import EntityTypeCreate, PropertyDefinitionCreate
from bellona.services.entity_type import create_entity_type
from bellona.services.ingestion import (
    _execute_ingestion_job,
    create_connector,
    create_field_mapping,
)

pytestmark = pytest.mark.asyncio(loop_scope="session")

SAMPLE_CSV = "name,age\nAlice,30\nBob,25\n"


# ── POST /connectors ──────────────────────────────────────────────────────────


async def test_create_connector(client: AsyncClient) -> None:
    payload = {
        "type": "rest_api",
        "name": "My REST API",
        "config": {
            "base_url": "https://api.example.com",
            "endpoint": "/users",
            "auth": {"type": "none"},
            "records_jsonpath": "$.data",
            "pagination": {"strategy": "none"},
        },
    }
    response = await client.post("/api/v1/connectors", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["type"] == "rest_api"
    assert data["name"] == "My REST API"
    assert data["status"] == "active"
    assert "id" in data


async def test_create_connector_invalid_type(client: AsyncClient) -> None:
    payload = {"type": "ftp", "name": "Bad Connector", "config": {}}
    response = await client.post("/api/v1/connectors", json=payload)
    assert response.status_code == 422


# ── GET /connectors ───────────────────────────────────────────────────────────


async def test_list_connectors(client: AsyncClient, db_session: AsyncSession) -> None:
    await create_connector(
        db_session, "csv", "list-conn-1", {"file_path": "/tmp/a.csv"}
    )
    await create_connector(
        db_session, "csv", "list-conn-2", {"file_path": "/tmp/b.csv"}
    )
    await db_session.flush()

    response = await client.get("/api/v1/connectors")
    assert response.status_code == 200
    data = response.json()
    names = [c["name"] for c in data]
    assert "list-conn-1" in names
    assert "list-conn-2" in names


# ── GET /connectors/{id} ──────────────────────────────────────────────────────


async def test_get_connector(client: AsyncClient, db_session: AsyncSession) -> None:
    connector = await create_connector(
        db_session, "csv", "get-conn", {"file_path": "/tmp/g.csv"}
    )
    await db_session.flush()

    response = await client.get(f"/api/v1/connectors/{connector.id}")
    assert response.status_code == 200
    assert response.json()["name"] == "get-conn"


async def test_get_connector_not_found(client: AsyncClient) -> None:
    response = await client.get(f"/api/v1/connectors/{uuid.uuid4()}")
    assert response.status_code == 404


# ── POST /connectors/csv/upload ───────────────────────────────────────────────


async def test_csv_upload(client: AsyncClient, tmp_path: Path) -> None:
    response = await client.post(
        "/api/v1/connectors/csv/upload",
        files={"file": ("people.csv", SAMPLE_CSV.encode(), "text/csv")},
        data={"name": "uploaded-csv"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["type"] == "csv"
    assert data["name"] == "uploaded-csv"
    assert "file_path" in data["config"]


# ── GET /connectors/{id}/schema ───────────────────────────────────────────────


async def test_discover_schema(
    client: AsyncClient, db_session: AsyncSession, tmp_path: Path
) -> None:
    csv_file = tmp_path / "schema_test.csv"
    csv_file.write_text("city,population\nBoston,675000\nSeattle,750000\n")
    connector = await create_connector(
        db_session, "csv", "schema-conn", {"file_path": str(csv_file)}
    )
    await db_session.flush()

    response = await client.get(f"/api/v1/connectors/{connector.id}/schema")
    assert response.status_code == 200
    data = response.json()
    field_names = [f["name"] for f in data["fields"]]
    assert "city" in field_names
    assert "population" in field_names


async def test_discover_schema_not_found(client: AsyncClient) -> None:
    response = await client.get(f"/api/v1/connectors/{uuid.uuid4()}/schema")
    assert response.status_code == 404


# ── POST /mappings ────────────────────────────────────────────────────────────


async def test_create_field_mapping(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"MapTarget-{uuid.uuid4().hex[:6]}",
            properties=[PropertyDefinitionCreate(name="city_name", data_type="string")],
        ),
    )
    connector = await create_connector(
        db_session, "csv", "map-conn", {"file_path": "/tmp/map.csv"}
    )
    await db_session.flush()

    payload = {
        "connector_id": str(connector.id),
        "entity_type_id": str(et.id),
        "mapping_config": {
            "mappings": [{"source_field": "city", "target_property": "city_name"}]
        },
    }
    response = await client.post("/api/v1/mappings", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "confirmed"
    assert data["proposed_by"] == "user"


# ── POST /connectors/{id}/sync ────────────────────────────────────────────────


async def test_trigger_sync_creates_job(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"SyncTarget-{uuid.uuid4().hex[:6]}",
            properties=[PropertyDefinitionCreate(name="name", data_type="string")],
        ),
    )
    connector = await create_connector(
        db_session, "csv", "sync-conn", {"file_path": "/tmp/sync.csv"}
    )
    await create_field_mapping(
        db_session,
        connector_id=connector.id,
        entity_type_id=et.id,
        mapping_config={
            "mappings": [{"source_field": "name", "target_property": "name"}]
        },
    )
    await db_session.flush()

    response = await client.post(f"/api/v1/connectors/{connector.id}/sync")
    assert response.status_code == 202
    data = response.json()
    assert data["connector_id"] == str(connector.id)
    assert "id" in data


async def test_trigger_sync_not_found(client: AsyncClient) -> None:
    response = await client.post(f"/api/v1/connectors/{uuid.uuid4()}/sync")
    assert response.status_code == 404


# ── GET /ingestion-jobs/{id} ──────────────────────────────────────────────────


async def test_get_ingestion_job(client: AsyncClient, db_session: AsyncSession) -> None:
    connector = await create_connector(
        db_session, "csv", "job-conn", {"file_path": "/tmp/job.csv"}
    )
    job = IngestionJob(connector_id=connector.id, status="pending")
    db_session.add(job)
    await db_session.flush()

    response = await client.get(f"/api/v1/ingestion-jobs/{job.id}")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "pending"
    assert data["connector_id"] == str(connector.id)


async def test_get_ingestion_job_not_found(client: AsyncClient) -> None:
    response = await client.get(f"/api/v1/ingestion-jobs/{uuid.uuid4()}")
    assert response.status_code == 404


# ── Full CSV Pipeline ─────────────────────────────────────────────────────────


async def test_full_csv_ingest_pipeline_via_api(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    # 1. Upload CSV
    upload_resp = await client.post(
        "/api/v1/connectors/csv/upload",
        files={"file": ("people.csv", SAMPLE_CSV.encode(), "text/csv")},
        data={"name": "full-pipeline-test"},
    )
    assert upload_resp.status_code == 201
    connector_id = upload_resp.json()["id"]

    # 2. Discover schema
    schema_resp = await client.get(f"/api/v1/connectors/{connector_id}/schema")
    assert schema_resp.status_code == 200

    # 3. Create entity type
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"Person-API-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(
                    name="name", data_type="string", required=True
                ),
                PropertyDefinitionCreate(name="age", data_type="integer"),
            ],
        ),
    )
    await db_session.flush()

    # 4. Create field mapping
    mapping_resp = await client.post(
        "/api/v1/mappings",
        json={
            "connector_id": str(connector_id),
            "entity_type_id": str(et.id),
            "mapping_config": {
                "mappings": [
                    {"source_field": "name", "target_property": "name"},
                    {"source_field": "age", "target_property": "age"},
                ]
            },
            "proposed_by": "user",
        },
    )
    assert mapping_resp.status_code == 201

    # 5. Trigger sync (verify endpoint returns 202)
    sync_resp = await client.post(f"/api/v1/connectors/{connector_id}/sync")
    assert sync_resp.status_code == 202
    job_id = sync_resp.json()["id"]

    # Run ingestion inline -- background task can't share the test transaction
    await _execute_ingestion_job(uuid.UUID(job_id), db_session)

    # 6. Check job completed
    job_resp = await client.get(f"/api/v1/ingestion-jobs/{job_id}")
    assert job_resp.status_code == 200
    job_data = job_resp.json()
    assert job_data["status"] == "completed"
    assert job_data["records_processed"] == 2
    assert job_data["records_failed"] == 0


async def test_sync_without_mapping_fails_job(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    upload_resp = await client.post(
        "/api/v1/connectors/csv/upload",
        files={"file": ("people.csv", SAMPLE_CSV.encode(), "text/csv")},
        data={"name": "no-mapping-test"},
    )
    connector_id = upload_resp.json()["id"]

    sync_resp = await client.post(f"/api/v1/connectors/{connector_id}/sync")
    assert sync_resp.status_code == 202
    job_id = sync_resp.json()["id"]

    await _execute_ingestion_job(uuid.UUID(job_id), db_session)

    job_resp = await client.get(f"/api/v1/ingestion-jobs/{job_id}")
    assert job_resp.status_code == 200
    assert job_resp.json()["status"] == "failed"


# ── PATCH /connectors/{id} ────────────────────────────────────────────────────


async def test_patch_connector_name(client: AsyncClient) -> None:
    create_resp = await client.post(
        "/api/v1/connectors",
        json={
            "type": "rest_api",
            "name": "PatchNameTest",
            "config": {
                "base_url": "https://example.com",
                "endpoint": "/api/data",
                "records_jsonpath": "$.results",
                "pagination": {"strategy": "none"},
            },
        },
    )
    conn = create_resp.json()
    response = await client.patch(
        f"/api/v1/connectors/{conn['id']}",
        json={"name": "PatchedName"},
    )
    assert response.status_code == 200
    assert response.json()["name"] == "PatchedName"


async def test_patch_connector_config(client: AsyncClient) -> None:
    create_resp = await client.post(
        "/api/v1/connectors",
        json={
            "type": "rest_api",
            "name": "PatchConfigTest",
            "config": {
                "base_url": "https://example.com",
                "endpoint": "/api/data",
                "records_jsonpath": "$.results",
                "pagination": {"strategy": "none"},
            },
        },
    )
    conn = create_resp.json()
    new_config = {
        "base_url": "https://new.example.com",
        "endpoint": "/api/v2/data",
        "records_jsonpath": "$.items",
        "pagination": {"strategy": "offset", "page_size": 20, "page_param": "p"},
    }
    response = await client.patch(
        f"/api/v1/connectors/{conn['id']}",
        json={"config": new_config},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["config"]["base_url"] == "https://new.example.com"
    assert data["config"]["pagination"]["page_size"] == 20


async def test_patch_connector_not_found(client: AsyncClient) -> None:
    response = await client.patch(
        f"/api/v1/connectors/{uuid.uuid4()}",
        json={"name": "Ghost"},
    )
    assert response.status_code == 404


async def test_patch_connector_no_changes(client: AsyncClient) -> None:
    create_resp = await client.post(
        "/api/v1/connectors",
        json={
            "type": "rest_api",
            "name": "PatchNoOp",
            "config": {
                "base_url": "https://example.com",
                "endpoint": "/data",
                "records_jsonpath": "$.data",
                "pagination": {"strategy": "none"},
            },
        },
    )
    conn = create_resp.json()
    response = await client.patch(
        f"/api/v1/connectors/{conn['id']}",
        json={},
    )
    assert response.status_code == 200
    assert response.json()["name"] == "PatchNoOp"
