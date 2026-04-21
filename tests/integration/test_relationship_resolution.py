"""Integration tests for relationship resolution during ingestion."""

import uuid
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.entities import Entity, Relationship
from bellona.models.ontology import RelationshipType
from bellona.models.system import IngestionJob
from bellona.schemas.ontology import EntityTypeCreate, PropertyDefinitionCreate
from bellona.services.entity_type import create_entity_type
from bellona.services.ingestion import (
    _execute_ingestion_job,
    create_connector,
    create_field_mapping,
)

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def _setup_csv_ingestion(
    db_session: AsyncSession,
    csv_path: Path,
    entity_type_id: uuid.UUID,
    mapping_entries: list[dict],
) -> IngestionJob:
    connector = await create_connector(
        db_session,
        "csv",
        f"rel-ingest-{uuid.uuid4().hex[:6]}",
        {"file_path": str(csv_path)},
    )
    await create_field_mapping(
        db_session,
        connector_id=connector.id,
        entity_type_id=entity_type_id,
        mapping_config={"mappings": mapping_entries},
    )
    job = IngestionJob(connector_id=connector.id, status="pending")
    db_session.add(job)
    await db_session.flush()
    return job


async def test_self_referential_relationship_resolved_during_ingestion(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    """Employee.manager_id → Employee.id should produce Relationship rows."""
    employee = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"Employee-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(
                    name="id", data_type="integer", required=True
                ),
                PropertyDefinitionCreate(name="name", data_type="string"),
                PropertyDefinitionCreate(name="manager_id", data_type="integer"),
            ],
        ),
    )

    rt = RelationshipType(
        name=f"reports_to-{uuid.uuid4().hex[:4]}",
        source_entity_type_id=employee.id,
        target_entity_type_id=employee.id,
        cardinality="many-to-one",
        source_property="manager_id",
        target_property="id",
    )
    db_session.add(rt)
    await db_session.flush()

    csv_file = tmp_path / "employees.csv"
    csv_file.write_text(
        "id,name,manager_id\n"
        "1,Alice,\n"
        "2,Bob,1\n"
        "3,Carol,1\n"
        "4,Dan,2\n"
    )

    job = await _setup_csv_ingestion(
        db_session,
        csv_file,
        employee.id,
        [
            {"source_field": "id", "target_property": "id"},
            {"source_field": "name", "target_property": "name"},
            {"source_field": "manager_id", "target_property": "manager_id"},
        ],
    )

    await _execute_ingestion_job(job.id, db_session)

    ent_result = await db_session.execute(
        select(Entity).where(Entity.entity_type_id == employee.id)
    )
    entities = {e.properties["id"]: e for e in ent_result.scalars().all()}
    assert set(entities.keys()) == {1, 2, 3, 4}

    rel_result = await db_session.execute(
        select(Relationship).where(Relationship.relationship_type_id == rt.id)
    )
    rels = list(rel_result.scalars().all())
    assert len(rels) == 3  # Bob→Alice, Carol→Alice, Dan→Bob

    by_source = {r.source_entity_id: r.target_entity_id for r in rels}
    assert by_source[entities[2].id] == entities[1].id  # Bob → Alice
    assert by_source[entities[3].id] == entities[1].id  # Carol → Alice
    assert by_source[entities[4].id] == entities[2].id  # Dan → Bob


async def test_cross_type_relationship_resolved_during_ingestion(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    """Employee.dept_id → Department.id — targets pre-exist in another entity type."""
    department = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"Department-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(
                    name="id", data_type="integer", required=True
                ),
                PropertyDefinitionCreate(name="name", data_type="string"),
            ],
        ),
    )
    employee = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"Employee-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(
                    name="id", data_type="integer", required=True
                ),
                PropertyDefinitionCreate(name="name", data_type="string"),
                PropertyDefinitionCreate(name="dept_id", data_type="integer"),
            ],
        ),
    )

    # Seed two departments
    dept_eng = Entity(
        entity_type_id=department.id,
        properties={"id": 10, "name": "Engineering"},
        schema_version=department.schema_version,
    )
    dept_sales = Entity(
        entity_type_id=department.id,
        properties={"id": 20, "name": "Sales"},
        schema_version=department.schema_version,
    )
    db_session.add_all([dept_eng, dept_sales])
    await db_session.flush()

    rt = RelationshipType(
        name=f"belongs_to-{uuid.uuid4().hex[:4]}",
        source_entity_type_id=employee.id,
        target_entity_type_id=department.id,
        cardinality="many-to-one",
        source_property="dept_id",
        target_property="id",
    )
    db_session.add(rt)
    await db_session.flush()

    csv_file = tmp_path / "employees.csv"
    csv_file.write_text(
        "id,name,dept_id\n"
        "1,Alice,10\n"
        "2,Bob,20\n"
        "3,Carol,99\n"  # no matching department
        "4,Dan,\n"  # null dept — skip
    )

    job = await _setup_csv_ingestion(
        db_session,
        csv_file,
        employee.id,
        [
            {"source_field": "id", "target_property": "id"},
            {"source_field": "name", "target_property": "name"},
            {"source_field": "dept_id", "target_property": "dept_id"},
        ],
    )

    await _execute_ingestion_job(job.id, db_session)

    rel_result = await db_session.execute(
        select(Relationship).where(Relationship.relationship_type_id == rt.id)
    )
    rels = list(rel_result.scalars().all())
    assert len(rels) == 2  # Alice→Eng, Bob→Sales (Carol & Dan have no match)

    target_ids = {r.target_entity_id for r in rels}
    assert target_ids == {dept_eng.id, dept_sales.id}


async def test_ingestion_succeeds_with_no_relationship_types(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    """Entity types without any relationship types should ingest normally."""
    et = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"Standalone-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(name="name", data_type="string", required=True),
            ],
        ),
    )

    csv_file = tmp_path / "standalone.csv"
    csv_file.write_text("name\nAlice\nBob\n")

    job = await _setup_csv_ingestion(
        db_session,
        csv_file,
        et.id,
        [{"source_field": "name", "target_property": "name"}],
    )

    await _execute_ingestion_job(job.id, db_session)

    job_row = await db_session.get(IngestionJob, job.id)
    assert job_row is not None
    assert job_row.status == "completed"
    assert job_row.records_processed == 2


async def test_ingestion_completes_when_relationship_resolution_errors(
    db_session: AsyncSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Any failure in relationship resolution must not fail the ingestion job."""
    employee = await create_entity_type(
        db_session,
        EntityTypeCreate(
            name=f"Employee-{uuid.uuid4().hex[:6]}",
            properties=[
                PropertyDefinitionCreate(name="id", data_type="integer", required=True),
                PropertyDefinitionCreate(name="name", data_type="string"),
                PropertyDefinitionCreate(name="manager_id", data_type="integer"),
            ],
        ),
    )
    rt = RelationshipType(
        name=f"reports_to-{uuid.uuid4().hex[:4]}",
        source_entity_type_id=employee.id,
        target_entity_type_id=employee.id,
        cardinality="many-to-one",
        source_property="manager_id",
        target_property="id",
    )
    db_session.add(rt)
    await db_session.flush()

    csv_file = tmp_path / "employees.csv"
    csv_file.write_text("id,name,manager_id\n1,Alice,\n2,Bob,1\n")

    from bellona.services import ingestion as ingestion_module

    async def boom(*args, **kwargs):
        raise RuntimeError("simulated relationship resolution failure")

    monkeypatch.setattr(ingestion_module, "_resolve_relationships", boom)

    job = await _setup_csv_ingestion(
        db_session,
        csv_file,
        employee.id,
        [
            {"source_field": "id", "target_property": "id"},
            {"source_field": "name", "target_property": "name"},
            {"source_field": "manager_id", "target_property": "manager_id"},
        ],
    )

    await _execute_ingestion_job(job.id, db_session)

    job_row = await db_session.get(IngestionJob, job.id)
    assert job_row is not None
    assert job_row.status == "completed"
    assert job_row.records_processed == 2
