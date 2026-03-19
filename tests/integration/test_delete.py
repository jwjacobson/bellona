import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.entities import Entity, Relationship
from bellona.models.ontology import RelationshipType
from bellona.schemas.ontology import EntityTypeCreate, PropertyDefinitionCreate
from bellona.services.entity_type import create_entity_type

pytestmark = pytest.mark.asyncio(loop_scope="session")


# ── Helpers ───────────────────────────────────────────────────────────────────


async def create_entity_type_via_api(client: AsyncClient, name: str, **kwargs) -> dict:
    response = await client.post(
        "/api/v1/entity-types", json={"name": name, **kwargs}
    )
    assert response.status_code == 201
    return response.json()


async def create_connector(client: AsyncClient, name: str) -> dict:
    response = await client.post(
        "/api/v1/connectors",
        json={"type": "csv", "name": name, "config": {"file_path": "/tmp/test.csv"}},
    )
    assert response.status_code == 201
    return response.json()


async def create_relationship_type(
    client: AsyncClient, name: str, source_id: str, target_id: str
) -> dict:
    response = await client.post(
        "/api/v1/relationship-types",
        json={
            "name": name,
            "source_entity_type_id": source_id,
            "target_entity_type_id": target_id,
            "cardinality": "one-to-many",
        },
    )
    assert response.status_code == 201
    return response.json()


# ── Entity Type Delete ────────────────────────────────────────────────────────


async def test_delete_entity_type(client: AsyncClient):
    et = await create_entity_type_via_api(client, "DeleteMe")
    response = await client.delete(f"/api/v1/entity-types/{et['id']}")
    assert response.status_code == 204

    # Verify it's gone
    response = await client.get(f"/api/v1/entity-types/{et['id']}")
    assert response.status_code == 404


async def test_delete_entity_type_not_found(client: AsyncClient):
    response = await client.delete(f"/api/v1/entity-types/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_delete_entity_type_blocked_by_relationship_type(client: AsyncClient):
    source = await create_entity_type_via_api(client, "DelSource")
    target = await create_entity_type_via_api(client, "DelTarget")
    await create_relationship_type(client, "blocks_delete", source["id"], target["id"])

    response = await client.delete(f"/api/v1/entity-types/{source['id']}")
    assert response.status_code == 409


async def test_delete_entity_type_cascade(client: AsyncClient):
    et = await create_entity_type_via_api(
        client,
        "CascadeMe",
        properties=[{"name": "val", "data_type": "string"}],
    )
    et2 = await create_entity_type_via_api(client, "CascadeTarget")
    await create_relationship_type(
        client, "cascade_rel", et["id"], et2["id"]
    )

    response = await client.post(
        f"/api/v1/entity-types/{et['id']}/cascade-delete"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["entity_type_deleted"] == "CascadeMe"
    assert data["relationship_types_deleted"] == 1

    response = await client.get(f"/api/v1/entity-types/{et['id']}")
    assert response.status_code == 404


async def test_delete_entity_type_cascade_not_found(client: AsyncClient):
    response = await client.post(
        f"/api/v1/entity-types/{uuid.uuid4()}/cascade-delete"
    )
    assert response.status_code == 404



# ── Relationship Type Delete ──────────────────────────────────────────────────


async def test_delete_relationship_type(client: AsyncClient):
    source = await create_entity_type_via_api(client, "RelDelSrc")
    target = await create_entity_type_via_api(client, "RelDelTgt")
    rt = await create_relationship_type(
        client, "delete_me_rel", source["id"], target["id"]
    )

    response = await client.delete(f"/api/v1/relationship-types/{rt['id']}")
    assert response.status_code == 204


async def test_delete_relationship_type_not_found(client: AsyncClient):
    response = await client.delete(f"/api/v1/relationship-types/{uuid.uuid4()}")
    assert response.status_code == 404


# ── Connector Delete ──────────────────────────────────────────────────────────


async def test_delete_connector(client: AsyncClient):
    conn = await create_connector(client, "DeleteConnector")
    response = await client.delete(f"/api/v1/connectors/{conn['id']}")
    assert response.status_code == 204

    response = await client.get(f"/api/v1/connectors/{conn['id']}")
    assert response.status_code == 404


async def test_delete_connector_not_found(client: AsyncClient):
    response = await client.delete(f"/api/v1/connectors/{uuid.uuid4()}")
    assert response.status_code == 404


# ── Entity Delete ─────────────────────────────────────────────────────────────
# NOTE: These tests need entities in the DB. If you have a helper that creates
# entities via the ingestion pipeline, use that. Otherwise you may need to
# POST directly or set up fixtures. Here's the pattern assuming a direct
# entity creation endpoint or fixture exists.


async def test_delete_entity_not_found(client: AsyncClient):
    response = await client.delete(f"/api/v1/entities/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_bulk_delete_entities_empty_list(client: AsyncClient):
    response = await client.post(
        "/api/v1/entities/bulk-delete", json={"entity_ids": []}
    )
    assert response.status_code == 422  # Pydantic min_length=1 validation


# ── Relationship Delete ───────────────────────────────────────────────────────


async def test_delete_relationship_not_found(client: AsyncClient):
    response = await client.delete(f"/api/v1/relationships/{uuid.uuid4()}")
    assert response.status_code == 404


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _make_entity_type(db: AsyncSession, suffix: str) -> object:
    return await create_entity_type(
        db,
        EntityTypeCreate(
            name=f"Del-{suffix}-{uuid.uuid4().hex[:4]}",
            properties=[
                PropertyDefinitionCreate(name="name", data_type="string", required=True),
            ],
        ),
    )


def _make_entity(entity_type_id: uuid.UUID, **props) -> Entity:
    return Entity(
        entity_type_id=entity_type_id,
        properties=props,
        schema_version=1,
    )


# ── Delete Single Entity ─────────────────────────────────────────────────────


async def test_delete_entity(client: AsyncClient, db_session: AsyncSession) -> None:
    et = await _make_entity_type(db_session, "delent")
    entity = _make_entity(et.id, name="Doomed")
    db_session.add(entity)
    await db_session.flush()

    response = await client.delete(f"/api/v1/entities/{entity.id}")
    assert response.status_code == 204

    response = await client.get(f"/api/v1/entities/{entity.id}")
    assert response.status_code == 404


async def test_delete_entity_also_deletes_relationships(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """Deleting an entity should cascade-delete its relationships."""
    et = await _make_entity_type(db_session, "delentrel")
    e1 = _make_entity(et.id, name="Source")
    e2 = _make_entity(et.id, name="Target")
    db_session.add_all([e1, e2])
    await db_session.flush()

    rel_type = RelationshipType(
        name=f"deltest-{uuid.uuid4().hex[:4]}",
        source_entity_type_id=et.id,
        target_entity_type_id=et.id,
        cardinality="many-to-many",
    )
    db_session.add(rel_type)
    await db_session.flush()

    rel = Relationship(
        relationship_type_id=rel_type.id,
        source_entity_id=e1.id,
        target_entity_id=e2.id,
    )
    db_session.add(rel)
    await db_session.flush()

    response = await client.delete(f"/api/v1/entities/{e1.id}")
    assert response.status_code == 204

    response = await client.get(f"/api/v1/entities/{e2.id}/relationships")
    assert response.status_code == 200
    assert len(response.json()) == 0


# ── Bulk Delete Entities ──────────────────────────────────────────────────────


async def test_bulk_delete_entities(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_entity_type(db_session, "bulkdel")
    entities = [_make_entity(et.id, name=f"Bulk{i}") for i in range(5)]
    for e in entities:
        db_session.add(e)
    await db_session.flush()

    ids_to_delete = [str(entities[i].id) for i in range(3)]
    response = await client.post(
        "/api/v1/entities/bulk-delete",
        json={"entity_ids": ids_to_delete},
    )
    assert response.status_code == 200
    assert response.json()["deleted_count"] == 3

    response = await client.get(
        "/api/v1/entities", params={"entity_type_id": str(et.id)}
    )
    assert response.status_code == 200
    assert response.json()["total"] == 2


async def test_delete_entities_by_type(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_entity_type(db_session, "typedel")
    for i in range(4):
        db_session.add(_make_entity(et.id, name=f"TypeDel{i}"))
    await db_session.flush()

    response = await client.delete(
        "/api/v1/entities", params={"entity_type_id": str(et.id)}
    )
    assert response.status_code == 200
    assert response.json()["deleted_count"] == 4

    response = await client.get(
        "/api/v1/entities", params={"entity_type_id": str(et.id)}
    )
    assert response.status_code == 200
    assert response.json()["total"] == 0


# ── Delete Single Relationship ────────────────────────────────────────────────


async def test_delete_relationship(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_entity_type(db_session, "delrel")
    e1 = _make_entity(et.id, name="A")
    e2 = _make_entity(et.id, name="B")
    db_session.add_all([e1, e2])
    await db_session.flush()

    rel_type = RelationshipType(
        name=f"delrel-{uuid.uuid4().hex[:4]}",
        source_entity_type_id=et.id,
        target_entity_type_id=et.id,
        cardinality="one-to-many",
    )
    db_session.add(rel_type)
    await db_session.flush()

    rel = Relationship(
        relationship_type_id=rel_type.id,
        source_entity_id=e1.id,
        target_entity_id=e2.id,
    )
    db_session.add(rel)
    await db_session.flush()

    response = await client.delete(f"/api/v1/relationships/{rel.id}")
    assert response.status_code == 204

    assert (await client.get(f"/api/v1/entities/{e1.id}")).status_code == 200
    assert (await client.get(f"/api/v1/entities/{e2.id}")).status_code == 200

    response = await client.get(f"/api/v1/entities/{e1.id}/relationships")
    assert response.status_code == 200
    assert len(response.json()) == 0


# ── Delete Entity Type (blocked by dependents) ───────────────────────────────


async def test_delete_entity_type_blocked_by_entities(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_entity_type(db_session, "blocked")
    db_session.add(_make_entity(et.id, name="Blocker"))
    await db_session.flush()

    response = await client.delete(f"/api/v1/entity-types/{et.id}")
    assert response.status_code == 409


async def test_delete_entity_type_after_cleaning_up(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """Delete entities first, then the entity type should succeed."""
    et = await _make_entity_type(db_session, "cleanup")
    db_session.add(_make_entity(et.id, name="Temporary"))
    await db_session.flush()

    # Delete entities of this type
    response = await client.delete(
        "/api/v1/entities", params={"entity_type_id": str(et.id)}
    )
    assert response.status_code == 200

    response = await client.delete(f"/api/v1/entity-types/{et.id}")
    assert response.status_code == 204


# ── Cascade Delete with Actual Data ──────────────────────────────────────────


async def test_cascade_delete_with_entities_and_relationships(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et1 = await _make_entity_type(db_session, "casc1")
    et2 = await _make_entity_type(db_session, "casc2")

    e1 = _make_entity(et1.id, name="CascA")
    e2 = _make_entity(et1.id, name="CascB")
    e3 = _make_entity(et2.id, name="CascC")
    db_session.add_all([e1, e2, e3])
    await db_session.flush()

    rel_type = RelationshipType(
        name=f"cascrel-{uuid.uuid4().hex[:4]}",
        source_entity_type_id=et1.id,
        target_entity_type_id=et2.id,
        cardinality="one-to-many",
    )
    db_session.add(rel_type)
    await db_session.flush()

    rel = Relationship(
        relationship_type_id=rel_type.id,
        source_entity_id=e1.id,
        target_entity_id=e3.id,
    )
    db_session.add(rel)
    await db_session.flush()

    response = await client.post(f"/api/v1/entity-types/{et1.id}/cascade-delete")
    assert response.status_code == 200
    data = response.json()
    assert data["entities_deleted"] == 2
    assert data["relationships_deleted"] == 1
    assert data["relationship_types_deleted"] == 1

    response = await client.get(f"/api/v1/entity-types/{et1.id}")
    assert response.status_code == 404

    response = await client.get(f"/api/v1/entities/{e3.id}")
    assert response.status_code == 200


# ── Delete Connector ──────────────────────────────────────────────────────────


async def test_delete_connector_with_ingested_data(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """Deleting a connector should SET NULL on entity source_connector_id."""
    et = await _make_entity_type(db_session, "conndel")
    await db_session.flush()

    conn_response = await client.post(
        "/api/v1/connectors",
        json={"type": "csv", "name": "doomed-connector", "config": {"file_path": "/tmp/x.csv"}},
    )
    assert conn_response.status_code == 201
    conn_id = conn_response.json()["id"]

    entity = Entity(
        entity_type_id=et.id,
        properties={"name": "Sourced"},
        schema_version=1,
        source_connector_id=uuid.UUID(conn_id),
    )
    db_session.add(entity)
    await db_session.flush()

    response = await client.delete(f"/api/v1/connectors/{conn_id}")
    assert response.status_code == 204

    response = await client.get(f"/api/v1/entities/{entity.id}")
    assert response.status_code == 200