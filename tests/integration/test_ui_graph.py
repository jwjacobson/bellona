"""UI tests for the relationship graph visualization."""

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_graph_index(client: AsyncClient) -> None:
    response = await client.get("/ui/graph")
    assert response.status_code == 200
    assert "cytoscape" in response.text.lower()


async def test_graph_data_returns_json(client: AsyncClient) -> None:
    response = await client.get("/ui/graph/data")
    assert response.status_code == 200
    data = response.json()
    assert "nodes" in data
    assert "edges" in data
    assert isinstance(data["nodes"], list)
    assert isinstance(data["edges"], list)
