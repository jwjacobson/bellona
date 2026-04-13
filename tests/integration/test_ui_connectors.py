"""UI tests for the connectors panel."""

import io
import uuid

import pytest
from httpx import AsyncClient
from unittest.mock import AsyncMock, patch

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_connectors_index(client: AsyncClient) -> None:
    response = await client.get("/ui/connectors")
    assert response.status_code == 200
    assert "Connectors" in response.text


async def test_create_rest_connector_redirects(client: AsyncClient) -> None:
    response = await client.post(
        "/ui/connectors",
        data={
            "name": "UI REST Test",
            "type": "rest_api",
            "base_url": "https://api.example.com/data",
            "auth_type": "none",
            "pagination_strategy": "none",
            "record_path": "$.data",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "/ui/connectors/" in response.headers["location"]


async def test_connector_detail(client: AsyncClient) -> None:
    create = await client.post(
        "/api/v1/connectors",
        json={
            "type": "rest_api",
            "name": "UI Detail Connector",
            "config": {
                "base_url": "https://api.example.com",
                "auth_type": "none",
                "pagination_strategy": "none",
                "record_path": "$.items",
            },
        },
    )
    connector_id = create.json()["id"]

    response = await client.get(f"/ui/connectors/{connector_id}")
    assert response.status_code == 200
    assert "UI Detail Connector" in response.text


async def test_csv_upload_redirects(client: AsyncClient) -> None:
    csv_content = b"name,age\nAlice,30\nBob,25\n"
    response = await client.post(
        "/ui/connectors/csv",
        data={"name": "UI CSV Upload"},
        files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "/ui/connectors/" in response.headers["location"]


# ── POST /ui/connectors/{id}/edit ────────────────────────────────────────────


async def test_ui_edit_connector_redirects(client: AsyncClient) -> None:
    create_resp = await client.post(
        "/api/v1/connectors",
        json={
            "type": "rest_api",
            "name": "UIEditTest",
            "config": {
                "base_url": "https://example.com",
                "endpoint": "/api/data",
                "records_jsonpath": "$.results",
                "pagination": {"strategy": "none"},
            },
        },
    )
    conn = create_resp.json()
    response = await client.post(
        f"/ui/connectors/{conn['id']}/edit",
        data={
            "name": "UIEditedName",
            "base_url": "https://edited.example.com",
            "endpoint": "/api/edited",
            "auth_type": "none",
            "pagination_strategy": "offset",
            "page_size": "25",
            "page_param": "page",
            "records_jsonpath": "$.data",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert f"/ui/connectors/{conn['id']}" in response.headers["location"]


async def test_ui_edit_connector_updates_config(client: AsyncClient) -> None:
    create_resp = await client.post(
        "/api/v1/connectors",
        json={
            "type": "rest_api",
            "name": "UIEditVerify",
            "config": {
                "base_url": "https://example.com",
                "endpoint": "/data",
                "records_jsonpath": "$.data",
                "pagination": {"strategy": "none"},
            },
        },
    )
    conn = create_resp.json()
    await client.post(
        f"/ui/connectors/{conn['id']}/edit",
        data={
            "name": "UIEditVerified",
            "base_url": "https://verified.example.com",
            "endpoint": "/api/v3",
            "auth_type": "bearer",
            "pagination_strategy": "none",
            "page_size": "",
            "page_param": "page",
            "records_jsonpath": "$.results",
        },
    )
    response = await client.get(f"/api/v1/connectors/{conn['id']}")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "UIEditVerified"
    assert data["config"]["base_url"] == "https://verified.example.com"
    assert data["config"]["endpoint"] == "/api/v3"


async def test_ui_edit_connector_not_found(client: AsyncClient) -> None:
    response = await client.post(
        f"/ui/connectors/{uuid.uuid4()}/edit",
        data={
            "name": "Ghost",
            "base_url": "https://x.com",
            "endpoint": "/",
            "auth_type": "none",
            "pagination_strategy": "none",
            "page_size": "",
            "page_param": "page",
            "records_jsonpath": "$.data",
        },
    )
    assert response.status_code == 404


# ── POST /ui/connectors/{id}/propose-schema ──────────────────────────────────


async def test_ui_propose_schema_redirects(client: AsyncClient) -> None:
    create_resp = await client.post(
        "/api/v1/connectors",
        json={
            "type": "rest_api",
            "name": "ProposeSchemaTest",
            "config": {
                "base_url": "https://example.com",
                "endpoint": "/data",
                "records_jsonpath": "$.data",
                "pagination": {"strategy": "none"},
            },
        },
    )
    conn = create_resp.json()

    with patch("bellona.api.ui.connectors.propose_schema") as mock_propose:
        mock_propose.return_value = AsyncMock()
        response = await client.post(
            f"/ui/connectors/{conn['id']}/propose-schema",
            follow_redirects=False,
        )

    assert response.status_code == 303
    assert f"/ui/connectors/{conn['id']}" in response.headers["location"]


async def test_ui_propose_schema_not_found(client: AsyncClient) -> None:
    response = await client.post(
        f"/ui/connectors/{uuid.uuid4()}/propose-schema",
        follow_redirects=False,
    )
    assert response.status_code == 404


# ── POST /ui/connectors/{id}/propose-mapping ─────────────────────────────────


async def test_ui_propose_mapping_redirects(client: AsyncClient) -> None:
    create_resp = await client.post(
        "/api/v1/connectors",
        json={
            "type": "rest_api",
            "name": "ProposeMappingTest",
            "config": {
                "base_url": "https://example.com",
                "endpoint": "/data",
                "records_jsonpath": "$.data",
                "pagination": {"strategy": "none"},
            },
        },
    )
    conn = create_resp.json()
    et_resp = await client.post(
        "/api/v1/entity-types",
        json={
            "name": "ProposeMappingET",
            "properties": [{"name": "name", "data_type": "string"}],
        },
    )
    et = et_resp.json()

    with patch("bellona.api.ui.connectors.propose_mapping") as mock_propose:
        mock_propose.return_value = AsyncMock()
        response = await client.post(
            f"/ui/connectors/{conn['id']}/propose-mapping",
            data={"entity_type_id": et["id"]},
            follow_redirects=False,
        )

    assert response.status_code == 303
    assert f"/ui/connectors/{conn['id']}" in response.headers["location"]


async def test_ui_propose_mapping_not_found(client: AsyncClient) -> None:
    et_resp = await client.post(
        "/api/v1/entity-types",
        json={
            "name": "ProposeMappingETGhost",
            "properties": [{"name": "x", "data_type": "string"}],
        },
    )
    et = et_resp.json()
    response = await client.post(
        f"/ui/connectors/{uuid.uuid4()}/propose-mapping",
        data={"entity_type_id": et["id"]},
        follow_redirects=False,
    )
    assert response.status_code == 404


# ── Connector detail page content ─────────────────────────────────────────────


async def test_connector_detail_shows_edit_button(client: AsyncClient) -> None:
    create_resp = await client.post(
        "/api/v1/connectors",
        json={
            "type": "rest_api",
            "name": "DetailEditTest",
            "config": {
                "base_url": "https://example.com",
                "endpoint": "/data",
                "records_jsonpath": "$.data",
                "pagination": {"strategy": "none"},
            },
        },
    )
    conn = create_resp.json()
    response = await client.get(f"/ui/connectors/{conn['id']}")
    assert response.status_code == 200
    assert "Edit" in response.text or "edit" in response.text


async def test_connector_detail_shows_mapping_status(client: AsyncClient) -> None:
    create_resp = await client.post(
        "/api/v1/connectors",
        json={
            "type": "rest_api",
            "name": "DetailMappingTest",
            "config": {
                "base_url": "https://example.com",
                "endpoint": "/data",
                "records_jsonpath": "$.data",
                "pagination": {"strategy": "none"},
            },
        },
    )
    conn = create_resp.json()
    await client.post(
        "/api/v1/entity-types",
        json={
            "name": "DetailMappingET",
            "properties": [{"name": "x", "data_type": "string"}],
        },
    )
    response = await client.get(f"/ui/connectors/{conn['id']}")
    assert response.status_code == 200
    assert "Mapping" in response.text
    assert "Pipeline Status" in response.text


async def test_connector_detail_shows_propose_schema(client: AsyncClient) -> None:
    create_resp = await client.post(
        "/api/v1/connectors",
        json={
            "type": "rest_api",
            "name": "DetailSchemaTest",
            "config": {
                "base_url": "https://example.com",
                "endpoint": "/data",
                "records_jsonpath": "$.data",
                "pagination": {"strategy": "none"},
            },
        },
    )
    conn = create_resp.json()
    response = await client.get(f"/ui/connectors/{conn['id']}")
    assert response.status_code == 200
    assert "Propose Schema" in response.text
