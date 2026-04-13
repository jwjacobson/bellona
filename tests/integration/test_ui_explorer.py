"""UI tests for the data explorer."""

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_explorer_index(client: AsyncClient) -> None:
    response = await client.get("/ui/explorer")
    assert response.status_code == 200
    assert "Explorer" in response.text


async def test_explorer_entity_type_view(client: AsyncClient) -> None:
    create = await client.post(
        "/api/v1/entity-types",
        json={
            "name": "UIExplorerType",
            "properties": [{"name": "label", "data_type": "string"}],
        },
    )
    entity_type_id = create.json()["id"]

    response = await client.get(f"/ui/explorer/{entity_type_id}")
    assert response.status_code == 200
    assert "UIExplorerType" in response.text


async def test_explorer_unknown_entity_type(client: AsyncClient) -> None:
    response = await client.get("/ui/explorer/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404
