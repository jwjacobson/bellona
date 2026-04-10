import base64
import uuid
from unittest.mock import AsyncMock

import httpx
import pytest

from bellona.connectors.rest_connector import RESTConnector

pytestmark = pytest.mark.asyncio(loop_scope="session")

BASE_CONFIG = {
    "base_url": "https://api.example.com",
    "endpoint": "/users",
    "auth": {"type": "none"},
    "records_jsonpath": "$.data",
    "pagination": {"strategy": "none"},
}


def make_mock_client(*responses: httpx.Response) -> AsyncMock:
    client = AsyncMock(spec=httpx.AsyncClient)
    client.get.side_effect = list(responses)
    return client


_DUMMY_REQUEST = httpx.Request("GET", "https://api.example.com/users")


def make_response(data: dict, status_code: int = 200, headers: dict | None = None) -> httpx.Response:
    return httpx.Response(
        status_code, json=data, headers=headers or {}, request=_DUMMY_REQUEST
    )


# ── connect() ────────────────────────────────────────────────────────────────


async def test_connect_success() -> None:
    client = make_mock_client(make_response({"data": []}))
    connector = RESTConnector(uuid.uuid4(), BASE_CONFIG, "test", _client=client)
    status = await connector.connect()
    assert status.connected is True


async def test_connect_http_error() -> None:
    client = make_mock_client(make_response({}, status_code=403))
    connector = RESTConnector(uuid.uuid4(), BASE_CONFIG, "test", _client=client)
    status = await connector.connect()
    assert status.connected is False
    assert "403" in status.message


async def test_connect_exception() -> None:
    client = AsyncMock(spec=httpx.AsyncClient)
    client.get.side_effect = httpx.ConnectError("connection refused")
    connector = RESTConnector(uuid.uuid4(), BASE_CONFIG, "test", _client=client)
    status = await connector.connect()
    assert status.connected is False


# ── discover_schema() ────────────────────────────────────────────────────────


async def test_discover_schema_fields() -> None:
    client = make_mock_client(
        make_response({"data": [{"id": 1, "name": "Alice", "active": True}]})
    )
    connector = RESTConnector(uuid.uuid4(), BASE_CONFIG, "test", _client=client)
    schema = await connector.discover_schema()
    names = [f.name for f in schema.fields]
    assert names == ["id", "name", "active"]


async def test_discover_schema_type_inference() -> None:
    client = make_mock_client(
        make_response(
            {
                "data": [
                    {
                        "count": 42,
                        "ratio": 3.14,
                        "label": "hello",
                        "flag": False,
                        "created": "2024-01-15",
                        "updated_at": "2024-01-15T10:00:00",
                        "meta": {"k": "v"},
                    }
                ]
            }
        )
    )
    connector = RESTConnector(uuid.uuid4(), BASE_CONFIG, "test", _client=client)
    schema = await connector.discover_schema()
    by_name = {f.name: f.inferred_type for f in schema.fields}
    assert by_name["count"] == "integer"
    assert by_name["ratio"] == "float"
    assert by_name["label"] == "string"
    assert by_name["flag"] == "boolean"
    assert by_name["created"] == "date"
    assert by_name["updated_at"] == "datetime"
    assert by_name["meta"] == "json"


async def test_discover_schema_empty_response() -> None:
    client = make_mock_client(make_response({"data": []}))
    connector = RESTConnector(uuid.uuid4(), BASE_CONFIG, "test", _client=client)
    schema = await connector.discover_schema()
    assert schema.fields == []


# ── discover_schema() — offset multi-page ───────────────────────────────────


async def test_discover_schema_offset_fetches_multiple_pages() -> None:
    config = {
        **BASE_CONFIG,
        "pagination": {
            "strategy": "offset",
            "page_size": 2,
            "page_param": "page",
            "size_param": "per_page",
        },
    }
    client = make_mock_client(
        make_response({"data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}),  # page 1 (full)
        make_response({"data": [{"id": 3, "email": "c@x.com"}]}),  # page 2 (short → stop)
    )
    connector = RESTConnector(uuid.uuid4(), config, "test", _client=client)
    schema = await connector.discover_schema()
    # Should have fields from both pages
    names = {f.name for f in schema.fields}
    assert names == {"id", "name", "email"}
    # Should have made 2 requests
    assert client.get.call_count == 2


async def test_discover_schema_offset_stops_on_short_page() -> None:
    config = {
        **BASE_CONFIG,
        "pagination": {
            "strategy": "offset",
            "page_size": 3,
            "page_param": "page",
            "size_param": "per_page",
        },
    }
    client = make_mock_client(
        make_response({"data": [{"id": 1}, {"id": 2}, {"id": 3}]}),  # full page
        make_response({"data": [{"id": 4}]}),  # short page → stop
        make_response({"data": [{"id": 5}]}),  # should NOT be fetched
    )
    connector = RESTConnector(uuid.uuid4(), config, "test", _client=client)
    schema = await connector.discover_schema()
    # Only 2 requests, not 3
    assert client.get.call_count == 2


async def test_discover_schema_offset_caps_at_3_pages() -> None:
    config = {
        **BASE_CONFIG,
        "pagination": {
            "strategy": "offset",
            "page_size": 2,
            "page_param": "page",
            "size_param": "per_page",
        },
    }
    client = make_mock_client(
        make_response({"data": [{"id": 1}, {"id": 2}]}),  # page 1
        make_response({"data": [{"id": 3}, {"id": 4}]}),  # page 2
        make_response({"data": [{"id": 5}, {"id": 6}]}),  # page 3
        make_response({"data": [{"id": 7}, {"id": 8}]}),  # page 4 — should NOT be fetched
    )
    connector = RESTConnector(uuid.uuid4(), config, "test", _client=client)
    schema = await connector.discover_schema()
    assert client.get.call_count == 3


# ── fetch_records() — no pagination ─────────────────────────────────────────


async def test_fetch_records_no_pagination() -> None:
    records_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    client = make_mock_client(make_response({"data": records_data}))
    connector = RESTConnector(uuid.uuid4(), BASE_CONFIG, "test", _client=client)
    records = [r async for r in connector.fetch_records()]
    assert len(records) == 2
    assert records[0].data == {"id": 1, "name": "Alice"}


async def test_fetch_records_source_metadata() -> None:
    client = make_mock_client(make_response({"data": [{"id": 1}]}))
    connector = RESTConnector(uuid.uuid4(), BASE_CONFIG, "test", _client=client)
    records = [r async for r in connector.fetch_records()]
    meta = records[0].source_metadata
    assert "connector_id" in meta
    assert "source_identifier" in meta
    assert "fetch_timestamp" in meta


# ── fetch_records() — offset pagination ──────────────────────────────────────


async def test_fetch_records_offset_pagination() -> None:
    config = {
        **BASE_CONFIG,
        "pagination": {
            "strategy": "offset",
            "page_size": 2,
            "page_param": "page",
            "size_param": "per_page",
        },
    }
    client = make_mock_client(
        make_response({"data": [{"id": 1}, {"id": 2}]}),
        make_response({"data": [{"id": 3}]}),
        make_response({"data": []}),
    )
    connector = RESTConnector(uuid.uuid4(), config, "test", _client=client)
    records = [r async for r in connector.fetch_records()]
    assert len(records) == 3
    assert records[2].data["id"] == 3


async def test_fetch_records_offset_stops_on_short_page() -> None:
    config = {
        **BASE_CONFIG,
        "pagination": {
            "strategy": "offset",
            "page_size": 3,
            "page_param": "page",
            "size_param": "per_page",
        },
    }
    client = make_mock_client(
        make_response({"data": [{"id": 1}, {"id": 2}, {"id": 3}]}),
        make_response({"data": [{"id": 4}]}),
    )
    connector = RESTConnector(uuid.uuid4(), config, "test", _client=client)
    records = [r async for r in connector.fetch_records()]
    assert len(records) == 4


# ── fetch_records() — cursor pagination ──────────────────────────────────────


async def test_fetch_records_cursor_pagination() -> None:
    config = {
        **BASE_CONFIG,
        "pagination": {
            "strategy": "cursor",
            "cursor_param": "cursor",
            "cursor_path": "$.next_cursor",
            "page_size": 2,
            "size_param": "per_page",
        },
    }
    client = make_mock_client(
        make_response({"data": [{"id": 1}, {"id": 2}], "next_cursor": "abc123"}),
        make_response({"data": [{"id": 3}], "next_cursor": None}),
    )
    connector = RESTConnector(uuid.uuid4(), config, "test", _client=client)
    records = [r async for r in connector.fetch_records()]
    assert len(records) == 3


# ── fetch_records() — link header pagination ─────────────────────────────────


async def test_fetch_records_link_header_pagination() -> None:
    config = {**BASE_CONFIG, "pagination": {"strategy": "link_header"}}
    page2_url = "https://api.example.com/users?page=2"
    client = make_mock_client(
        make_response(
            {"data": [{"id": 1}, {"id": 2}]},
            headers={"link": f'<{page2_url}>; rel="next"'},
        ),
        make_response({"data": [{"id": 3}]}),
    )
    connector = RESTConnector(uuid.uuid4(), config, "test", _client=client)
    records = [r async for r in connector.fetch_records()]
    assert len(records) == 3


# ── auth ─────────────────────────────────────────────────────────────────────


async def test_auth_bearer() -> None:
    config = {
        **BASE_CONFIG,
        "auth": {"type": "bearer", "value": "my-token"},
    }
    client = make_mock_client(make_response({"data": []}))
    connector = RESTConnector(uuid.uuid4(), config, "test", _client=client)
    await connector.connect()
    _, kwargs = client.get.call_args
    headers = kwargs.get("headers", {})
    assert headers.get("Authorization") == "Bearer my-token"


async def test_auth_api_key() -> None:
    config = {
        **BASE_CONFIG,
        "auth": {"type": "api_key", "header": "X-API-Key", "value": "secret"},
    }
    client = make_mock_client(make_response({"data": []}))
    connector = RESTConnector(uuid.uuid4(), config, "test", _client=client)
    await connector.connect()
    _, kwargs = client.get.call_args
    headers = kwargs.get("headers", {})
    assert headers.get("X-API-Key") == "secret"


async def test_auth_basic() -> None:
    config = {
        **BASE_CONFIG,
        "auth": {"type": "basic", "username": "admin", "password": "secret"},
    }
    client = AsyncMock(spec=httpx.AsyncClient)
    client.get.return_value = make_response({"data": []})

    connector = RESTConnector(uuid.uuid4(), config, "test", _client=client)
    await connector.connect()

    _, kwargs = client.get.call_args
    headers = kwargs.get("headers", {})
    assert "Authorization" in headers
    assert headers["Authorization"].startswith("Basic ")

    encoded = headers["Authorization"].split(" ", 1)[1]
    decoded = base64.b64decode(encoded).decode()
    assert decoded == "admin:secret"


async def test_auth_none_sends_no_auth_header() -> None:
    client = make_mock_client(make_response({"data": []}))
    connector = RESTConnector(uuid.uuid4(), BASE_CONFIG, "test", _client=client)
    await connector.connect()
    _, kwargs = client.get.call_args
    headers = kwargs.get("headers", {})
    assert "Authorization" not in headers


# ── get_metadata() ───────────────────────────────────────────────────────────


async def test_get_metadata() -> None:
    connector = RESTConnector(uuid.uuid4(), BASE_CONFIG, "rest-api", _client=AsyncMock())
    meta = await connector.get_metadata()
    assert meta.status == "active"
    assert meta.source_name == "rest-api"
