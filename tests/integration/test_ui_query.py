"""UI tests for the natural language query interface."""

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_query_index(client: AsyncClient) -> None:
    response = await client.get("/ui/query")
    assert response.status_code == 200
    assert "Query" in response.text


async def test_query_submit_returns_results_page(client: AsyncClient) -> None:
    """POST to /ui/query should render results even with an empty ontology."""
    response = await client.post(
        "/ui/query",
        data={"question": "Show me all companies"},
    )
    assert response.status_code == 200
    assert "Query" in response.text
