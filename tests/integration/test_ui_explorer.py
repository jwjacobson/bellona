"""UI tests for the data explorer."""

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.entities import Entity
from bellona.schemas.ontology import EntityTypeCreate, PropertyDefinitionCreate
from bellona.services.entity_type import create_entity_type

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


async def test_explorer_puts_display_property_column_first(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"UIExplorerDisplay-{uuid.uuid4().hex[:6]}",
            display_property="code",
            properties=[
                PropertyDefinitionCreate(name="count", data_type="integer"),
                PropertyDefinitionCreate(name="code", data_type="string"),
                PropertyDefinitionCreate(name="note", data_type="string"),
            ],
        ),
    )
    db_session.add(
        Entity(
            entity_type_id=et.id,
            properties={"count": 1, "code": "XYZ", "note": "hello"},
            schema_version=1,
        )
    )
    await db_session.flush()

    response = await client.get(f"/ui/explorer/{et.id}")
    assert response.status_code == 200
    body = response.text
    idx_code = body.find(">code<")
    idx_count = body.find(">count<")
    idx_note = body.find(">note<")
    assert idx_code != -1 and idx_count != -1 and idx_note != -1
    assert idx_code < idx_count
    assert idx_code < idx_note


async def test_explorer_falls_back_to_name_when_no_display_property(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"UIExplorerFallback-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(name="age", data_type="integer"),
                PropertyDefinitionCreate(name="name", data_type="string"),
            ],
        ),
    )
    db_session.add(
        Entity(
            entity_type_id=et.id,
            properties={"age": 30, "name": "Alice"},
            schema_version=1,
        )
    )
    await db_session.flush()

    response = await client.get(f"/ui/explorer/{et.id}")
    assert response.status_code == 200
    body = response.text
    idx_name = body.find(">name<")
    idx_age = body.find(">age<")
    assert idx_name != -1 and idx_age != -1
    assert idx_name < idx_age
