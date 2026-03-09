import pytest
import pytest_asyncio
from httpx import AsyncClient

pytestmark = pytest.mark.asyncio(loop_scope="session")


@pytest_asyncio.fixture
async def two_entity_types(client: AsyncClient):
    r1 = await client.post("/api/v1/entity-types", json={"name": "Employee"})
    r2 = await client.post("/api/v1/entity-types", json={"name": "Organization"})
    return r1.json()["id"], r2.json()["id"]


async def test_create_relationship_type(client: AsyncClient, two_entity_types):
    source_id, target_id = two_entity_types
    response = await client.post(
        "/api/v1/relationship-types",
        json={
            "name": "employed_by",
            "source_entity_type_id": source_id,
            "target_entity_type_id": target_id,
            "cardinality": "many-to-many",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "employed_by"
    assert data["cardinality"] == "many-to-many"


async def test_list_relationship_types(client: AsyncClient, two_entity_types):
    source_id, target_id = two_entity_types
    await client.post(
        "/api/v1/relationship-types",
        json={
            "name": "manages",
            "source_entity_type_id": source_id,
            "target_entity_type_id": target_id,
            "cardinality": "one-to-many",
        },
    )
    response = await client.get("/api/v1/relationship-types")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


async def test_create_relationship_type_invalid_entity(client: AsyncClient):
    import uuid

    response = await client.post(
        "/api/v1/relationship-types",
        json={
            "name": "bad_rel",
            "source_entity_type_id": str(uuid.uuid4()),
            "target_entity_type_id": str(uuid.uuid4()),
            "cardinality": "one-to-one",
        },
    )
    assert response.status_code == 404
