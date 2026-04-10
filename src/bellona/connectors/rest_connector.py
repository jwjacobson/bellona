import base64
import re
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import httpx
from jsonpath_ng import parse as jsonpath_parse

from bellona.connectors.base import (
    BaseConnector,
    ConnectionStatus,
    ConnectorMetadata,
    SchemaDiscovery,
    SchemaField,
    SourceRecord,
)

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")
_NULL_SENTINELS = {"unknown", "n/a", "na", "none", "-", "null", ""}


class RESTConnector(BaseConnector):
    def __init__(
        self,
        connector_id: uuid.UUID,
        config: dict[str, Any],
        name: str,
        _client: httpx.AsyncClient | None = None,
    ) -> None:
        self.connector_id = connector_id
        self.config = config
        self.name = name
        timeout = self.config.get("timeout", 30.0)
        self._client = _client or httpx.AsyncClient(timeout=timeout)

    def _build_headers(self) -> dict[str, str]:
        auth = self.config.get("auth", {})
        match auth.get("type", "none"):
            case "bearer":
                return {"Authorization": f"Bearer {auth['value']}"}
            case "api_key":
                header = auth.get("header", "X-API-Key")
                return {header: auth["value"]}
            case "basic":
                creds = base64.b64encode(
                    f"{auth['username']}:{auth['password']}".encode()
                ).decode()
                return {"Authorization": f"Basic {creds}"}
            case _:
                return {}

    def _extract_records(self, data: Any) -> list[dict[str, Any]]:
        jsonpath = self.config.get("records_jsonpath", "$")
        expression = jsonpath_parse(jsonpath)
        matches = expression.find(data)
        records: list[dict[str, Any]] = []
        for match in matches:
            if isinstance(match.value, list):
                records.extend(match.value)
            elif isinstance(match.value, dict):
                records.append(match.value)
        return records

    def _infer_type_from_samples(self, values: list[Any]) -> str:
        non_null = [v for v in values if v is not None]
        if not non_null:
            return "string"
        # bool must be checked before int — bool is a subclass of int
        if all(isinstance(v, bool) for v in non_null):
            return "boolean"
        if all(isinstance(v, int) for v in non_null):
            return "integer"
        if all(isinstance(v, (int, float)) for v in non_null):
            return "float"
        if all(isinstance(v, (dict, list)) for v in non_null):
            return "json"
        if all(isinstance(v, str) for v in non_null):
            if all(_DATETIME_RE.match(v) for v in non_null):
                return "datetime"
            if all(_DATE_RE.match(v) for v in non_null):
                return "date"
            return "string"
        return "string"

    def _parse_link_next(self, link_header: str) -> str | None:
        for part in link_header.split(","):
            part = part.strip()
            if 'rel="next"' in part:
                m = re.search(r"<([^>]+)>", part)
                if m:
                    return m.group(1)
        return None

    def _build_url(self) -> str:
        base = self.config["base_url"].rstrip("/")
        endpoint = self.config.get("endpoint", "")
        if endpoint and not endpoint.startswith("/"):
            endpoint = "/" + endpoint
        return base + endpoint

    def _detect_sentinels(self, values: list[Any]) -> list[str]:
        """Find string values that look like null placeholders."""
        found = []
        for v in values:
            if isinstance(v, str) and v.lower().strip() in _NULL_SENTINELS:
                if v not in found:
                    found.append(v)
        return found


    async def connect(self) -> ConnectionStatus:
        url = self._build_url()
        try:
            response = await self._client.get(url, headers=self._build_headers())
            if response.status_code < 400:
                return ConnectionStatus(connected=True)
            return ConnectionStatus(
                connected=False, message=f"HTTP {response.status_code}"
            )
        except Exception as exc:
            return ConnectionStatus(connected=False, message=str(exc))

    async def discover_schema(self, sample_size: int = 100) -> SchemaDiscovery:
        url = self._build_url()
        headers = self._build_headers()
        pagination = self.config.get("pagination", {})
        strategy = pagination.get("strategy", "none")

        if strategy == "offset":
            page_size = pagination.get("page_size", 100)
            page_param = pagination.get("page_param", "page")
            size_param = pagination.get("size_param", "per_page")
            max_pages = 3
            records: list[dict[str, Any]] = []
            for page in range(1, max_pages + 1):
                params = {page_param: page, size_param: page_size}
                response = await self._client.get(url, headers=headers, params=params)
                response.raise_for_status()
                page_records = self._extract_records(response.json())
                records.extend(page_records)
                if len(records) >= sample_size or len(page_records) < page_size:
                    break
            records = records[:sample_size]
        else:
            response = await self._client.get(url, headers=headers)
            response.raise_for_status()
            records = self._extract_records(response.json())[:sample_size]

        if not records:
            return SchemaDiscovery(fields=[])

        # Preserve key order; union of all keys across sample records
        all_keys: list[str] = list(dict.fromkeys(k for r in records for k in r))

        fields = []
        for key in all_keys:
            values = [r[key] for r in records if key in r]
            nullable = any(v is None for v in values) or len(values) < len(records)
            sentinels = self._detect_sentinels(values)
            clean_values = [v for v in values if not (isinstance(v, str) and v.lower().strip() in _NULL_SENTINELS)]
            fields.append(
                SchemaField(
                    name=key,
                    inferred_type=self._infer_type_from_samples(clean_values),
                    nullable=nullable or bool(sentinels),
                    sample_values=[v for v in values[:5] if v is not None],
                    null_sentinels=sentinels
                )
            )
        return SchemaDiscovery(fields=fields)

    async def fetch_records(self) -> AsyncIterator[SourceRecord]:  # type: ignore[override]
        url = self._build_url()
        headers = self._build_headers()
        pagination = self.config.get("pagination", {})
        strategy = pagination.get("strategy", "none")
        fetch_time = datetime.now(UTC).isoformat()
        source_id = url

        def _make_record(data: dict[str, Any]) -> SourceRecord:
            return SourceRecord(
                data=data,
                source_metadata={
                    "connector_id": str(self.connector_id),
                    "source_identifier": source_id,
                    "fetch_timestamp": fetch_time,
                },
            )

        if strategy == "none":
            response = await self._client.get(url, headers=headers)
            response.raise_for_status()
            for record in self._extract_records(response.json()):
                yield _make_record(record)

        elif strategy == "offset":
            page_size = pagination.get("page_size", 100)
            page_param = pagination.get("page_param", "page")
            size_param = pagination.get("size_param", "per_page")
            page = 1
            while True:
                params = {page_param: page, size_param: page_size}
                response = await self._client.get(url, headers=headers, params=params)
                if response.status_code == 404:
                    break
                response.raise_for_status()
                records = self._extract_records(response.json())
                if not records:
                    break
                for record in records:
                    yield _make_record(record)
                if len(records) < page_size:
                    break
                page += 1

        elif strategy == "cursor":
            cursor_param = pagination.get("cursor_param", "cursor")
            cursor_path = pagination.get("cursor_path", "$.next_cursor")
            size_param = pagination.get("size_param", "per_page")
            page_size = pagination.get("page_size", 100)
            cursor: str | None = None
            while True:
                params: dict[str, Any] = {size_param: page_size}
                if cursor:
                    params[cursor_param] = cursor
                response = await self._client.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                records = self._extract_records(data)
                if not records:
                    break
                for record in records:
                    yield _make_record(record)
                cursor_matches = jsonpath_parse(cursor_path).find(data)
                cursor = cursor_matches[0].value if cursor_matches else None
                if not cursor:
                    break

        elif strategy == "link_header":
            next_url: str | None = url
            while next_url:
                response = await self._client.get(next_url, headers=headers)
                response.raise_for_status()
                for record in self._extract_records(response.json()):
                    yield _make_record(record)
                link = response.headers.get("link", "")
                next_url = self._parse_link_next(link)

    async def get_metadata(self) -> ConnectorMetadata:
        return ConnectorMetadata(
            connector_id=self.connector_id,
            source_name=self.name,
            status="active",
        )
