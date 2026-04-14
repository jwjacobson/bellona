import uuid
from pathlib import Path

import pytest

from bellona.connectors.csv_connector import CSVConnector

pytestmark = pytest.mark.asyncio(loop_scope="session")

SAMPLE_CSV = (
    "name,age,salary,active,joined\n"
    "Alice,30,75000.50,true,2020-01-15\n"
    "Bob,25,55000.00,false,2019-06-01\n"
    "Charlie,,80000.00,yes,2021-03-22\n"
)


@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    f = tmp_path / "sample.csv"
    f.write_text(SAMPLE_CSV)
    return f


@pytest.fixture
def connector(csv_file: Path) -> CSVConnector:
    return CSVConnector(
        connector_id=uuid.uuid4(),
        file_path=str(csv_file),
        name="test-csv",
    )


# ── connect() ─────────────────────────────────────────────────────────────────


async def test_connect_existing_file(connector: CSVConnector) -> None:
    status = await connector.connect()
    assert status.connected is True


async def test_connect_missing_file() -> None:
    c = CSVConnector(uuid.uuid4(), "/nonexistent/file.csv", "test")
    status = await c.connect()
    assert status.connected is False
    assert "not found" in status.message.lower()


# ── discover_schema() ─────────────────────────────────────────────────────────


async def test_discover_schema_field_names(connector: CSVConnector) -> None:
    schema = await connector.discover_schema()
    assert [f.name for f in schema.fields] == [
        "name",
        "age",
        "salary",
        "active",
        "joined",
    ]


async def test_discover_schema_infers_string(connector: CSVConnector) -> None:
    schema = await connector.discover_schema()
    name_field = next(f for f in schema.fields if f.name == "name")
    assert name_field.inferred_type == "string"


async def test_discover_schema_infers_integer(connector: CSVConnector) -> None:
    schema = await connector.discover_schema()
    age_field = next(f for f in schema.fields if f.name == "age")
    assert age_field.inferred_type == "integer"


async def test_discover_schema_infers_float(connector: CSVConnector) -> None:
    schema = await connector.discover_schema()
    salary_field = next(f for f in schema.fields if f.name == "salary")
    assert salary_field.inferred_type == "float"


async def test_discover_schema_infers_boolean(connector: CSVConnector) -> None:
    schema = await connector.discover_schema()
    active_field = next(f for f in schema.fields if f.name == "active")
    assert active_field.inferred_type == "boolean"


async def test_discover_schema_infers_date(connector: CSVConnector) -> None:
    schema = await connector.discover_schema()
    joined_field = next(f for f in schema.fields if f.name == "joined")
    assert joined_field.inferred_type == "date"


async def test_discover_schema_detects_nullable(connector: CSVConnector) -> None:
    schema = await connector.discover_schema()
    age_field = next(f for f in schema.fields if f.name == "age")
    assert age_field.nullable is True


async def test_discover_schema_non_nullable(connector: CSVConnector) -> None:
    schema = await connector.discover_schema()
    salary_field = next(f for f in schema.fields if f.name == "salary")
    assert salary_field.nullable is False


async def test_discover_schema_record_count(connector: CSVConnector) -> None:
    schema = await connector.discover_schema()
    assert schema.record_count_estimate == 3


async def test_discover_schema_sample_values(connector: CSVConnector) -> None:
    schema = await connector.discover_schema()
    name_field = next(f for f in schema.fields if f.name == "name")
    assert "Alice" in name_field.sample_values


# ── fetch_records() ───────────────────────────────────────────────────────────


async def test_fetch_records_count(connector: CSVConnector) -> None:
    records = [r async for r in connector.fetch_records()]
    assert len(records) == 3


async def test_fetch_records_data(connector: CSVConnector) -> None:
    records = [r async for r in connector.fetch_records()]
    assert records[0].data["name"] == "Alice"
    assert records[0].data["age"] == "30"


async def test_fetch_records_empty_to_none(connector: CSVConnector) -> None:
    records = [r async for r in connector.fetch_records()]
    assert records[2].data["age"] is None


async def test_fetch_records_source_metadata(connector: CSVConnector) -> None:
    records = [r async for r in connector.fetch_records()]
    meta = records[0].source_metadata
    assert "connector_id" in meta
    assert "source_identifier" in meta
    assert "fetch_timestamp" in meta


# ── Encoding detection ────────────────────────────────────────────────────────


async def test_utf8_bom(tmp_path: Path) -> None:
    content = "\ufeffname,score\nAlice,100\n"
    f = tmp_path / "bom.csv"
    f.write_bytes(content.encode("utf-8-sig"))

    c = CSVConnector(uuid.uuid4(), str(f), "bom-test")
    schema = await c.discover_schema()

    # BOM must not leak into the first header name
    assert schema.fields[0].name == "name"

    records = [r async for r in c.fetch_records()]
    assert len(records) == 1
    assert records[0].data["name"] == "Alice"


async def test_latin1_encoding(tmp_path: Path) -> None:
    content = "name,city\nRené,Zürich\n"
    f = tmp_path / "latin1.csv"
    f.write_bytes(content.encode("latin-1"))

    c = CSVConnector(uuid.uuid4(), str(f), "latin1-test")
    records = [r async for r in c.fetch_records()]
    assert len(records) == 1
    assert "rich" in records[0].data["city"]


# ── get_metadata() ────────────────────────────────────────────────────────────


async def test_get_metadata(connector: CSVConnector) -> None:
    meta = await connector.get_metadata()
    assert meta.status == "active"
    assert meta.source_name == "test-csv"
    assert meta.last_sync_at is None
