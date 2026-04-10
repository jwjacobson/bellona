"""Unit tests for Discovery Agent tools."""
import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from bellona.agents.discovery_agent import (
    detect_pagination,
    extract_jsonpath,
    http_get,
    infer_schema,
)


# ── http_get ─────────────────────────────────────────────────────────────────


async def test_http_get_success() -> None:
    mock_response = httpx.Response(
        200,
        json={"results": [{"name": "Luke"}]},
        headers={"content-type": "application/json"},
        request=httpx.Request("GET", "https://example.com/api/"),
    )
    with patch("bellona.agents.discovery_agent.httpx.AsyncClient") as mock_client_cls:
        instance = AsyncMock()
        instance.get = AsyncMock(return_value=mock_response)
        instance.__aenter__ = AsyncMock(return_value=instance)
        instance.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = instance

        result = json.loads(await http_get("https://example.com/api/"))

    assert result["status_code"] == 200
    assert result["body"] == {"results": [{"name": "Luke"}]}
    assert "content-type" in result["headers"]
    assert result["url"] == "https://example.com/api/"


async def test_http_get_auth_required() -> None:
    mock_response = httpx.Response(
        401,
        json={"detail": "Not authenticated"},
        headers={"www-authenticate": "Bearer"},
        request=httpx.Request("GET", "https://example.com/api/"),
    )
    with patch("bellona.agents.discovery_agent.httpx.AsyncClient") as mock_client_cls:
        instance = AsyncMock()
        instance.get = AsyncMock(return_value=mock_response)
        instance.__aenter__ = AsyncMock(return_value=instance)
        instance.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = instance

        result = json.loads(await http_get("https://example.com/api/"))

    assert result["status_code"] == 401


async def test_http_get_timeout() -> None:
    with patch("bellona.agents.discovery_agent.httpx.AsyncClient") as mock_client_cls:
        instance = AsyncMock()
        instance.get = AsyncMock(side_effect=httpx.TimeoutException("timed out"))
        instance.__aenter__ = AsyncMock(return_value=instance)
        instance.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = instance

        result = json.loads(await http_get("https://example.com/api/"))

    assert result["status_code"] == 0
    assert result["error"] == "timeout"


# ── extract_jsonpath ─────────────────────────────────────────────────────────


async def test_extract_jsonpath_valid() -> None:
    data = json.dumps({"results": [{"name": "Luke"}, {"name": "Leia"}]})
    result = json.loads(await extract_jsonpath(data, "$.results"))
    assert result == [[{"name": "Luke"}, {"name": "Leia"}]]


async def test_extract_jsonpath_root_array() -> None:
    data = json.dumps([{"name": "Luke"}, {"name": "Leia"}])
    result = json.loads(await extract_jsonpath(data, "$"))
    assert result == [[{"name": "Luke"}, {"name": "Leia"}]]


async def test_extract_jsonpath_no_match() -> None:
    data = json.dumps({"results": [{"name": "Luke"}]})
    result = json.loads(await extract_jsonpath(data, "$.nonexistent"))
    assert result == []


# ── infer_schema ─────────────────────────────────────────────────────────────


async def test_infer_schema_basic() -> None:
    records = json.dumps([
        {"name": "Luke", "height": 172, "mass": 77.5, "active": True},
        {"name": "Leia", "height": 150, "active": False},
    ])
    result = json.loads(await infer_schema(records))
    assert result["record_count"] == 2

    fields_by_name = {f["name"]: f for f in result["fields"]}
    assert fields_by_name["name"]["type"] == "string"
    assert fields_by_name["name"]["required"] is True
    assert fields_by_name["height"]["type"] == "integer"
    assert fields_by_name["mass"]["type"] == "float"
    assert fields_by_name["mass"]["required"] is False
    assert fields_by_name["active"]["type"] == "boolean"


async def test_infer_schema_empty() -> None:
    result = json.loads(await infer_schema("[]"))
    assert result["fields"] == []
    assert result["record_count"] == 0


# ── detect_pagination ────────────────────────────────────────────────────────


async def test_detect_pagination_offset() -> None:
    response = json.dumps({
        "body": {
            "count": 82,
            "next": "https://swapi.dev/api/people/?page=2",
            "previous": None,
            "results": [{"name": "Luke"}],
        },
        "headers": {},
    })
    result = json.loads(await detect_pagination(response, "https://swapi.dev/api/people/"))
    assert result["detected_strategy"] == "offset"
    assert result["confidence"] == "high"
    assert result["signals"]["has_next_field"] is True
    assert result["signals"]["has_count_field"] is True
    assert result["signals"]["page_param"] == "page"


async def test_detect_pagination_cursor() -> None:
    response = json.dumps({
        "body": {
            "data": [{"id": 1}],
            "next_cursor": "abc123",
            "has_more": True,
        },
        "headers": {},
    })
    result = json.loads(await detect_pagination(response, "https://api.example.com/items"))
    assert result["detected_strategy"] == "cursor"
    assert result["confidence"] == "high"
    assert result["signals"]["has_cursor"] is True


async def test_detect_pagination_link_header() -> None:
    response = json.dumps({
        "body": [{"id": 1}],
        "headers": {
            "link": '<https://api.example.com/items?page=2>; rel="next"',
        },
    })
    result = json.loads(await detect_pagination(response, "https://api.example.com/items"))
    assert result["detected_strategy"] == "link_header"
    assert result["confidence"] == "high"
    assert result["signals"]["has_link_header"] is True


async def test_detect_pagination_none() -> None:
    response = json.dumps({
        "body": [{"id": 1}, {"id": 2}],
        "headers": {},
    })
    result = json.loads(await detect_pagination(response, "https://api.example.com/items"))
    assert result["detected_strategy"] == "none"
