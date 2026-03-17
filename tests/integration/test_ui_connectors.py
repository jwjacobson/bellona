"""UI tests for the connectors panel."""
import io

import pytest
from httpx import AsyncClient

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
