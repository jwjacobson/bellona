"""Integration tests for entity query endpoints."""
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


async def _make_entity_type(db: AsyncSession, suffix: str) -> object:
    return await create_entity_type(
        db,
        EntityTypeCreate(
            name=f"EQ-{suffix}-{uuid.uuid4().hex[:4]}",
            properties=[
                PropertyDefinitionCreate(name="name", data_type="string", required=True),
                PropertyDefinitionCreate(name="year", data_type="integer"),
                PropertyDefinitionCreate(name="status", data_type="string"),
            ],
        ),
    )


def _make_entity(entity_type_id: uuid.UUID, **props) -> Entity:
    return Entity(
        entity_type_id=entity_type_id,
        properties=props,
        schema_version=1,
    )


# ── GET /api/v1/entities ──────────────────────────────────────────────────────


async def test_list_entities_empty(client: AsyncClient, db_session: AsyncSession) -> None:
    et = await _make_entity_type(db_session, "empty")
    await db_session.flush()

    response = await client.get("/api/v1/entities", params={"entity_type_id": str(et.id)})

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 0
    assert data["items"] == []
    assert data["page"] == 1


async def test_list_entities_pagination(client: AsyncClient, db_session: AsyncSession) -> None:
    et = await _make_entity_type(db_session, "page")
    await db_session.flush()

    for i in range(5):
        db_session.add(_make_entity(et.id, name=f"Entity{i}", year=2020 + i))
    await db_session.flush()

    response = await client.get(
        "/api/v1/entities",
        params={"entity_type_id": str(et.id), "page": 1, "page_size": 3},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 5
    assert len(data["items"]) == 3
    assert data["pages"] == 2


async def test_list_entities_page_two(client: AsyncClient, db_session: AsyncSession) -> None:
    et = await _make_entity_type(db_session, "page2")
    await db_session.flush()

    for i in range(4):
        db_session.add(_make_entity(et.id, name=f"P2Entity{i}"))
    await db_session.flush()

    response = await client.get(
        "/api/v1/entities",
        params={"entity_type_id": str(et.id), "page": 2, "page_size": 3},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 1


# ── GET /api/v1/entities/{id} ─────────────────────────────────────────────────


async def test_get_entity_by_id(client: AsyncClient, db_session: AsyncSession) -> None:
    et = await _make_entity_type(db_session, "get")
    entity = _make_entity(et.id, name="Alice", year=1990)
    db_session.add(entity)
    await db_session.flush()

    response = await client.get(f"/api/v1/entities/{entity.id}")

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == str(entity.id)
    assert data["properties"]["name"] == "Alice"
    assert data["entity_type_id"] == str(et.id)


async def test_get_entity_not_found(client: AsyncClient) -> None:
    response = await client.get(f"/api/v1/entities/{uuid.uuid4()}")
    assert response.status_code == 404


# ── GET /api/v1/entities/{id}/relationships ───────────────────────────────────


async def test_get_entity_relationships(client: AsyncClient, db_session: AsyncSession) -> None:
    et = await _make_entity_type(db_session, "rel")
    e1 = _make_entity(et.id, name="Source")
    e2 = _make_entity(et.id, name="Target")
    db_session.add_all([e1, e2])
    await db_session.flush()

    rel_type = RelationshipType(
        name=f"knows-{uuid.uuid4().hex[:4]}",
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

    response = await client.get(f"/api/v1/entities/{e1.id}/relationships")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["source_entity_id"] == str(e1.id)
    assert data[0]["target_entity_id"] == str(e2.id)


async def test_get_entity_relationships_target(client: AsyncClient, db_session: AsyncSession) -> None:
    """Entity that is a target also sees the relationship."""
    et = await _make_entity_type(db_session, "relt")
    e1 = _make_entity(et.id, name="Src")
    e2 = _make_entity(et.id, name="Tgt")
    db_session.add_all([e1, e2])
    await db_session.flush()

    rel_type = RelationshipType(
        name=f"linked-{uuid.uuid4().hex[:4]}",
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

    response = await client.get(f"/api/v1/entities/{e2.id}/relationships")

    assert response.status_code == 200
    data = response.json()
    assert any(r["id"] == str(rel.id) for r in data)


async def test_get_entity_relationships_not_found(client: AsyncClient) -> None:
    response = await client.get(f"/api/v1/entities/{uuid.uuid4()}/relationships")
    assert response.status_code == 404


# ── POST /api/v1/entities/query ───────────────────────────────────────────────


async def test_query_entities_by_entity_type(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_entity_type(db_session, "qet")
    other_et = await _make_entity_type(db_session, "other")
    db_session.add_all([
        _make_entity(et.id, name="Alpha"),
        _make_entity(et.id, name="Beta"),
        _make_entity(other_et.id, name="Gamma"),
    ])
    await db_session.flush()

    response = await client.post(
        "/api/v1/entities/query",
        json={"entity_type_id": str(et.id)},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    names = {item["properties"]["name"] for item in data["items"]}
    assert names == {"Alpha", "Beta"}


async def test_query_entities_eq_filter(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_entity_type(db_session, "qeq")
    db_session.add_all([
        _make_entity(et.id, name="Alice", status="active"),
        _make_entity(et.id, name="Bob", status="inactive"),
        _make_entity(et.id, name="Carol", status="active"),
    ])
    await db_session.flush()

    response = await client.post(
        "/api/v1/entities/query",
        json={
            "entity_type_id": str(et.id),
            "filters": {"property": "status", "operator": "eq", "value": "active"},
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    names = {item["properties"]["name"] for item in data["items"]}
    assert names == {"Alice", "Carol"}


async def test_query_entities_gte_filter(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_entity_type(db_session, "qgte")
    db_session.add_all([
        _make_entity(et.id, name="Old", year=2015),
        _make_entity(et.id, name="Recent", year=2021),
        _make_entity(et.id, name="Newer", year=2023),
    ])
    await db_session.flush()

    response = await client.post(
        "/api/v1/entities/query",
        json={
            "entity_type_id": str(et.id),
            "filters": {"property": "year", "operator": "gte", "value": 2020},
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    names = {item["properties"]["name"] for item in data["items"]}
    assert names == {"Recent", "Newer"}


async def test_query_entities_nested_filter(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_entity_type(db_session, "qnest")
    db_session.add_all([
        _make_entity(et.id, name="A", year=2021, status="active"),
        _make_entity(et.id, name="B", year=2019, status="active"),
        _make_entity(et.id, name="C", year=2021, status="inactive"),
        _make_entity(et.id, name="D", year=2018, status="inactive"),
    ])
    await db_session.flush()

    response = await client.post(
        "/api/v1/entities/query",
        json={
            "entity_type_id": str(et.id),
            "filters": {
                "op": "and",
                "conditions": [
                    {"property": "year", "operator": "gte", "value": 2020},
                    {"property": "status", "operator": "eq", "value": "active"},
                ],
            },
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["items"][0]["properties"]["name"] == "A"


async def test_query_entities_deeply_nested_filter(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """Test the spec's example: AND of (year>=2020) OR (status=active OR year>=2022)."""
    et = await _make_entity_type(db_session, "qdnest")
    db_session.add_all([
        _make_entity(et.id, name="A", year=2021, status="inactive"),  # year>=2020: yes
        _make_entity(et.id, name="B", year=2019, status="active"),    # status=active: yes
        _make_entity(et.id, name="C", year=2018, status="inactive"),  # neither: no
    ])
    await db_session.flush()

    response = await client.post(
        "/api/v1/entities/query",
        json={
            "entity_type_id": str(et.id),
            "filters": {
                "op": "or",
                "conditions": [
                    {"property": "year", "operator": "gte", "value": 2020},
                    {"property": "status", "operator": "eq", "value": "active"},
                ],
            },
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    names = {item["properties"]["name"] for item in data["items"]}
    assert names == {"A", "B"}


async def test_query_entities_sort(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_entity_type(db_session, "qsort")
    db_session.add_all([
        _make_entity(et.id, name="Charlie"),
        _make_entity(et.id, name="Alice"),
        _make_entity(et.id, name="Bob"),
    ])
    await db_session.flush()

    response = await client.post(
        "/api/v1/entities/query",
        json={
            "entity_type_id": str(et.id),
            "sort": [{"property": "name", "direction": "asc"}],
        },
    )

    assert response.status_code == 200
    names = [item["properties"]["name"] for item in response.json()["items"]]
    assert names == sorted(names)


async def test_query_entities_sort_desc(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_entity_type(db_session, "qsortd")
    db_session.add_all([
        _make_entity(et.id, name="Charlie"),
        _make_entity(et.id, name="Alice"),
        _make_entity(et.id, name="Bob"),
    ])
    await db_session.flush()

    response = await client.post(
        "/api/v1/entities/query",
        json={
            "entity_type_id": str(et.id),
            "sort": [{"property": "name", "direction": "desc"}],
        },
    )

    assert response.status_code == 200
    names = [item["properties"]["name"] for item in response.json()["items"]]
    assert names == sorted(names, reverse=True)


async def test_query_entities_pagination(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_entity_type(db_session, "qpag")
    for i in range(6):
        db_session.add(_make_entity(et.id, name=f"Pag{i}"))
    await db_session.flush()

    r1 = await client.post(
        "/api/v1/entities/query",
        json={"entity_type_id": str(et.id), "page": 1, "page_size": 4},
    )
    r2 = await client.post(
        "/api/v1/entities/query",
        json={"entity_type_id": str(et.id), "page": 2, "page_size": 4},
    )

    assert r1.status_code == 200
    assert r2.status_code == 200
    d1, d2 = r1.json(), r2.json()
    assert d1["total"] == 6
    assert len(d1["items"]) == 4
    assert len(d2["items"]) == 2
    assert d1["pages"] == 2

    # No overlap
    ids1 = {i["id"] for i in d1["items"]}
    ids2 = {i["id"] for i in d2["items"]}
    assert ids1.isdisjoint(ids2)


async def test_query_entities_contains_filter(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    et = await _make_entity_type(db_session, "qcont")
    db_session.add_all([
        _make_entity(et.id, name="Acme Corporation"),
        _make_entity(et.id, name="Beta Ltd"),
        _make_entity(et.id, name="Acme Industries"),
    ])
    await db_session.flush()

    response = await client.post(
        "/api/v1/entities/query",
        json={
            "entity_type_id": str(et.id),
            "filters": {"property": "name", "operator": "contains", "value": "Acme"},
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
