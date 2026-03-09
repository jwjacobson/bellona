import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from bellona.models.base import Base


class EntityType(Base):
    __tablename__ = "entity_types"

    id: Mapped[uuid.UUID] = mapped_column(default=uuid.uuid4, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    schema_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    property_definitions: Mapped[list["PropertyDefinition"]] = relationship(
        back_populates="entity_type", cascade="all, delete-orphan"
    )
    entities: Mapped[list["Entity"]] = relationship(back_populates="entity_type")


class PropertyDefinition(Base):
    __tablename__ = "property_definitions"

    id: Mapped[uuid.UUID] = mapped_column(default=uuid.uuid4, primary_key=True)
    entity_type_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("entity_types.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    data_type: Mapped[str] = mapped_column(String(50), nullable=False)
    required: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    constraints: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    schema_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    entity_type: Mapped["EntityType"] = relationship(
        back_populates="property_definitions"
    )


class RelationshipType(Base):
    __tablename__ = "relationship_types"

    id: Mapped[uuid.UUID] = mapped_column(default=uuid.uuid4, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    source_entity_type_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("entity_types.id", ondelete="RESTRICT"), nullable=False
    )
    target_entity_type_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("entity_types.id", ondelete="RESTRICT"), nullable=False
    )
    cardinality: Mapped[str] = mapped_column(String(20), nullable=False)
    properties: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    source_entity_type: Mapped["EntityType"] = relationship(
        foreign_keys=[source_entity_type_id]
    )
    target_entity_type: Mapped["EntityType"] = relationship(
        foreign_keys=[target_entity_type_id]
    )
