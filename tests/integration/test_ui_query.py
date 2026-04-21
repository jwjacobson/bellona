"""UI tests for the natural language query interface."""

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.entities import Entity
from bellona.schemas.agents import QueryAgentResult
from bellona.schemas.ontology import EntityTypeCreate, PropertyDefinitionCreate
from bellona.services.entity_type import create_entity_type

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


async def test_query_submit_renders_synthesized_answer(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"UI-Company-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(name="name", data_type="string", required=True)
            ],
        ),
    )
    db_session.add(
        Entity(entity_type_id=et.id, properties={"name": "Acme"}, schema_version=1)
    )
    await db_session.flush()

    mock_result = QueryAgentResult(
        entity_type_name=et.name,
        filters=None,
        sort=[],
        explanation="All companies.",
        confidence=0.9,
    )

    with patch(
        "bellona.services.agent_service.QueryAgent.translate",
        new=AsyncMock(return_value=mock_result),
    ), patch(
        "bellona.services.agent_service._synthesize_answer",
        new=AsyncMock(return_value="There is one company named Acme."),
    ):
        response = await client.post(
            "/ui/query",
            data={"question": "Which companies exist?"},
        )

    assert response.status_code == 200
    assert "There is one company named Acme." in response.text


async def test_query_submit_renders_agent_error(client: AsyncClient) -> None:
    """Agent failures should render a user-facing error, not a 500."""
    with patch(
        "bellona.services.agent_service.QueryAgent.translate",
        new=AsyncMock(side_effect=RuntimeError("anthropic boom")),
    ):
        response = await client.post(
            "/ui/query",
            data={"question": "Show me everything"},
        )

    assert response.status_code == 200
    assert "query agent encountered an error" in response.text.lower()
    assert "anthropic boom" not in response.text
