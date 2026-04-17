"""UI tests for the ontology browser."""

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_ontology_index(client: AsyncClient) -> None:
    response = await client.get("/ui/ontology")
    assert response.status_code == 200
    assert "Entity Types" in response.text


async def test_entity_type_detail(client: AsyncClient) -> None:
    create = await client.post(
        "/api/v1/entity-types",
        json={
            "name": "UIDetailType",
            "description": "For detail test",
            "properties": [{"name": "ticker", "data_type": "string"}],
        },
    )
    entity_type_id = create.json()["id"]

    response = await client.get(f"/ui/ontology/entity-types/{entity_type_id}")
    assert response.status_code == 200
    assert "UIDetailType" in response.text
    assert "ticker" in response.text


async def test_relationships_index(client: AsyncClient) -> None:
    response = await client.get("/ui/ontology/relationships")
    assert response.status_code == 200
    assert "Relationship Types" in response.text


async def test_create_relationship_type_redirects(client: AsyncClient) -> None:
    src = await client.post("/api/v1/entity-types", json={"name": "UIRelSrc"})
    tgt = await client.post("/api/v1/entity-types", json={"name": "UIRelTgt"})
    src_id = src.json()["id"]
    tgt_id = tgt.json()["id"]

    response = await client.post(
        "/ui/ontology/relationships",
        data={
            "name": "ui_rel_test",
            "source_entity_type_id": src_id,
            "target_entity_type_id": tgt_id,
            "cardinality": "one-to-many",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/ui/ontology/relationships"
