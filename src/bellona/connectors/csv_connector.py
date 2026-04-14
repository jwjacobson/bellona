import asyncio
import csv
import io
import re
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import chardet

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
_BOOL_VALUES = {"true", "false", "yes", "no", "1", "0"}


class CSVConnector(BaseConnector):
    def __init__(self, connector_id: uuid.UUID, file_path: str, name: str) -> None:
        self.connector_id = connector_id
        self.file_path = Path(file_path)
        self.name = name

    async def connect(self) -> ConnectionStatus:
        if not self.file_path.exists():
            return ConnectionStatus(
                connected=False, message=f"File not found: {self.file_path}"
            )
        if not self.file_path.is_file():
            return ConnectionStatus(
                connected=False, message=f"Not a file: {self.file_path}"
            )
        return ConnectionStatus(connected=True)

    async def _read_content(self) -> str:
        def _read() -> str:
            raw = self.file_path.read_bytes()
            detected = chardet.detect(raw)
            encoding = detected.get("encoding") or "utf-8"
            return raw.decode(encoding).lstrip("\ufeff")

        return await asyncio.to_thread(_read)

    def _infer_type(self, values: list[str]) -> str:
        non_null = [v for v in values if v.strip()]
        if not non_null:
            return "string"
        if all(v.lower() in _BOOL_VALUES for v in non_null):
            return "boolean"
        try:
            [int(v) for v in non_null]
            return "integer"
        except ValueError:
            pass
        try:
            [float(v) for v in non_null]
            return "float"
        except ValueError:
            pass
        if all(_DATETIME_RE.match(v) for v in non_null):
            return "datetime"
        if all(_DATE_RE.match(v) for v in non_null):
            return "date"
        return "string"

    async def discover_schema(self, sample_size: int = 100) -> SchemaDiscovery:
        content = await self._read_content()
        reader = csv.DictReader(io.StringIO(content), dialect=csv.excel)

        headers: list[str] = []
        samples: dict[str, list[str]] = {}
        record_count = 0

        for i, row in enumerate(reader):
            if i == 0:
                headers = list(row.keys())
                samples = {h: [] for h in headers}
            if i < sample_size:
                for h in headers:
                    samples[h].append(row.get(h) or "")
            record_count += 1

        fields = [
            SchemaField(
                name=name,
                inferred_type=self._infer_type(samples[name]),
                nullable=any(v.strip() == "" for v in samples[name]),
                sample_values=[v for v in samples[name][:5] if v],
            )
            for name in headers
        ]
        return SchemaDiscovery(fields=fields, record_count_estimate=record_count)

    async def fetch_records(self) -> AsyncIterator[SourceRecord]:  # type: ignore[override]
        content = await self._read_content()
        fetch_time = datetime.now(UTC).isoformat()
        connector_id = str(self.connector_id)
        source_identifier = str(self.file_path)

        def _parse() -> list[SourceRecord]:
            reader = csv.DictReader(io.StringIO(content), dialect=csv.excel)
            return [
                SourceRecord(
                    data={
                        k: (v if v and v.strip() else None)
                        for k, v in row.items()
                        if k is not None
                    },
                    source_metadata={
                        "connector_id": connector_id,
                        "source_identifier": source_identifier,
                        "fetch_timestamp": fetch_time,
                    },
                )
                for row in reader
            ]

        for record in await asyncio.to_thread(_parse):
            yield record

    async def get_metadata(self) -> ConnectorMetadata:
        return ConnectorMetadata(
            connector_id=self.connector_id,
            source_name=self.name,
            status="active",
        )
