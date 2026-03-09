import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_create_entity_type(client: AsyncClient):
    response = await client.post(
        "/api/v1/entity-types",
        json={"name": "Company", "description": "A company entity"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "Company"
    assert data["description"] == "A company entity"
    assert data["schema_version"] == 1
    assert "id" in data
    assert "created_at" in data


async def test_create_entity_type_with_properties(client: AsyncClient):
    response = await client.post(
        "/api/v1/entity-types",
        json={
            "name": "Person",
            "properties": [
                {"name": "full_name", "data_type": "string", "required": True},
                {"name": "age", "data_type": "integer", "required": False},
            ],
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "Person"
    assert len(data["property_definitions"]) == 2
    names = {p["name"] for p in data["property_definitions"]}
    assert names == {"full_name", "age"}


async def test_create_entity_type_duplicate_name(client: AsyncClient):
    await client.post("/api/v1/entity-types", json={"name": "Duplicate"})
    response = await client.post("/api/v1/entity-types", json={"name": "Duplicate"})
    assert response.status_code == 409


async def test_list_entity_types(client: AsyncClient):
    await client.post("/api/v1/entity-types", json={"name": "TypeA"})
    await client.post("/api/v1/entity-types", json={"name": "TypeB"})
    response = await client.get("/api/v1/entity-types")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    names = {item["name"] for item in data}
    assert "TypeA" in names
    assert "TypeB" in names


async def test_get_entity_type(client: AsyncClient):
    create_response = await client.post(
        "/api/v1/entity-types",
        json={"name": "GetMe", "description": "For retrieval"},
    )
    entity_id = create_response.json()["id"]

    response = await client.get(f"/api/v1/entity-types/{entity_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == entity_id
    assert data["name"] == "GetMe"


async def test_get_entity_type_not_found(client: AsyncClient):
    import uuid
    response = await client.get(f"/api/v1/entity-types/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_patch_entity_type_add_property(client: AsyncClient):
    create_response = await client.post(
        "/api/v1/entity-types", json={"name": "PatchMe"}
    )
    entity_id = create_response.json()["id"]

    response = await client.patch(
        f"/api/v1/entity-types/{entity_id}",
        json={
            "add_properties": [
                {"name": "founded_year", "data_type": "integer"}
            ]
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["schema_version"] == 2
    prop_names = {p["name"] for p in data["property_definitions"]}
    assert "founded_year" in prop_names


async def test_patch_entity_type_update_description(client: AsyncClient):
    create_response = await client.post(
        "/api/v1/entity-types", json={"name": "DescUpdate"}
    )
    entity_id = create_response.json()["id"]

    response = await client.patch(
        f"/api/v1/entity-types/{entity_id}",
        json={"description": "Updated description"},
    )
    assert response.status_code == 200
    assert response.json()["description"] == "Updated description"
