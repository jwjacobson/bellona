import uuid

from pydantic import BaseModel, Field


class BulkDeleteRequest(BaseModel):
    entity_ids: list[uuid.UUID] = Field(min_length=1)


class BulkDeleteResult(BaseModel):
    deleted_count: int


class CascadeDeleteResult(BaseModel):
    relationships_deleted: int
    entities_deleted: int
    relationship_types_deleted: int
    entity_type_deleted: str
