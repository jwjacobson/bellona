import uuid
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class ConnectionStatus:
    connected: bool
    message: str = ""


@dataclass
class SchemaField:
    name: str
    inferred_type: str
    nullable: bool
    sample_values: list[Any] = field(default_factory=list)
    null_sentinels: list[str] = field(default_factory=list)


@dataclass
class SchemaDiscovery:
    fields: list[SchemaField]
    record_count_estimate: int | None = None


@dataclass
class ConnectorMetadata:
    connector_id: uuid.UUID
    source_name: str
    status: str
    last_sync_at: datetime | None = None
    record_count: int | None = None


@dataclass
class SourceRecord:
    data: dict[str, Any]
    source_metadata: dict[str, Any]


class BaseConnector(ABC):
    @abstractmethod
    async def connect(self) -> ConnectionStatus: ...

    @abstractmethod
    async def discover_schema(self) -> SchemaDiscovery: ...

    @abstractmethod
    def fetch_records(self) -> AsyncIterator[SourceRecord]:
        """Implementations must be async generator functions."""
        ...

    @abstractmethod
    async def get_metadata(self) -> ConnectorMetadata: ...
