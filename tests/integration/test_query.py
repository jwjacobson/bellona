"""Integration tests for the natural language query endpoint."""
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


async def _make_company_type(db: AsyncSession, suffix: str):
    return await create_entity_type(
        db,
        EntityTypeCreate(
            name=f"Company-NL-{suffix}-{uuid.uuid4().hex[:4]}",
            properties=[
                PropertyDefinitionCreate(name="name", data_type="string", required=True),
                PropertyDefinitionCreate(name="founded_year", data_type="integer"),
                PropertyDefinitionCreate(name="status", data_type="string"),
            ],
        ),
    )


# ── POST /api/v1/query/natural ────────────────────────────────────────────────


async def test_nl_query_returns_results(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_company_type(db_session, "basic")
    db_session.add_all([
        Entity(entity_type_id=et.id, properties={"name": "Acme", "founded_year": 2021, "status": "active"}, schema_version=1),
        Entity(entity_type_id=et.id, properties={"name": "OldCo", "founded_year": 2010, "status": "active"}, schema_version=1),
    ])
    await db_session.flush()

    mock_result = QueryAgentResult(
        entity_type_name=et.name,
        filters={"property": "founded_year", "operator": "gte", "value": 2020},
        sort=[],
        explanation="Find companies founded after 2020.",
        confidence=0.9,
    )

    with patch(
        "bellona.services.agent_service.QueryAgent.translate",
        new=AsyncMock(return_value=mock_result),
    ):
        response = await client.post(
            "/api/v1/query/natural",
            json={"question": "Find companies founded after 2020"},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["question"] == "Find companies founded after 2020"
    assert data["explanation"] == "Find companies founded after 2020."
    assert data["total_results"] == 1
    assert data["results"][0]["properties"]["name"] == "Acme"
    assert data["query_used"] is not None


async def test_nl_query_no_filter(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_company_type(db_session, "nofilter")
    db_session.add_all([
        Entity(entity_type_id=et.id, properties={"name": "X"}, schema_version=1),
        Entity(entity_type_id=et.id, properties={"name": "Y"}, schema_version=1),
    ])
    await db_session.flush()

    mock_result = QueryAgentResult(
        entity_type_name=et.name,
        filters=None,
        sort=[],
        explanation="List all companies.",
        confidence=0.95,
    )

    with patch(
        "bellona.services.agent_service.QueryAgent.translate",
        new=AsyncMock(return_value=mock_result),
    ):
        response = await client.post(
            "/api/v1/query/natural",
            json={"question": "List all companies"},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["total_results"] == 2


async def test_nl_query_with_group_filter(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_company_type(db_session, "group")
    db_session.add_all([
        Entity(entity_type_id=et.id, properties={"name": "Match", "founded_year": 2022, "status": "active"}, schema_version=1),
        Entity(entity_type_id=et.id, properties={"name": "NoMatch", "founded_year": 2018, "status": "active"}, schema_version=1),
    ])
    await db_session.flush()

    mock_result = QueryAgentResult(
        entity_type_name=et.name,
        filters={
            "op": "and",
            "conditions": [
                {"property": "founded_year", "operator": "gte", "value": 2020},
                {"property": "status", "operator": "eq", "value": "active"},
            ],
        },
        sort=[],
        explanation="Active companies founded after 2020.",
        confidence=0.88,
    )

    with patch(
        "bellona.services.agent_service.QueryAgent.translate",
        new=AsyncMock(return_value=mock_result),
    ):
        response = await client.post(
            "/api/v1/query/natural",
            json={"question": "Active companies founded after 2020"},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["total_results"] == 1
    assert data["results"][0]["properties"]["name"] == "Match"


async def test_nl_query_unresolvable_entity_type(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """Agent returns unknown entity_type_name → query runs without type filter."""
    mock_result = QueryAgentResult(
        entity_type_name="NonExistentType",
        filters=None,
        sort=[],
        explanation="Could not find matching entity type.",
        confidence=0.1,
    )

    with patch(
        "bellona.services.agent_service.QueryAgent.translate",
        new=AsyncMock(return_value=mock_result),
    ):
        response = await client.post(
            "/api/v1/query/natural",
            json={"question": "Something obscure"},
        )

    assert response.status_code == 422
    assert "NonExistentType" in response.json()["detail"]

async def test_nl_query_with_entity_type_hint(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_company_type(db_session, "hint")
    db_session.add(
        Entity(entity_type_id=et.id, properties={"name": "HintCo"}, schema_version=1)
    )
    await db_session.flush()

    mock_result = QueryAgentResult(
        entity_type_name=None,
        filters=None,
        sort=[],
        explanation="All entities of given type.",
        confidence=0.9,
    )

    with patch(
        "bellona.services.agent_service.QueryAgent.translate",
        new=AsyncMock(return_value=mock_result),
    ):
        response = await client.post(
            "/api/v1/query/natural",
            json={"question": "List all", "entity_type_id": str(et.id)},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["total_results"] == 1


async def test_nl_query_entity_type_not_found(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/query/natural",
        json={"question": "query", "entity_type_id": str(uuid.uuid4())},
    )
    assert response.status_code == 404
