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
    display_property: Mapped[str | None] = mapped_column(String(255), nullable=True)
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

    def resolve_display_property(
        self, available: "list[str] | set[str] | None" = None
    ) -> str | None:
        """Pick the property to use as a human-readable label.

        Order: explicit `display_property`, then `name`, then `title`, then the
        first string-typed property. Candidates must exist as a property
        definition and, if `available` is given, must appear in it too
        (used when only the keys on an Entity row are known).
        """
        prop_names = {p.name for p in self.property_definitions}

        def _ok(candidate: str | None) -> bool:
            if candidate is None or candidate not in prop_names:
                return False
            return available is None or candidate in available

        if _ok(self.display_property):
            return self.display_property
        for fallback in ("name", "title"):
            if _ok(fallback):
                return fallback
        for p in self.property_definitions:
            if p.data_type == "string" and _ok(p.name):
                return p.name
        return None


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
    source_property: Mapped[str | None] = mapped_column(String(255), nullable=True)
    target_property: Mapped[str | None] = mapped_column(String(255), nullable=True)
    properties: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    source_entity_type: Mapped["EntityType"] = relationship(
        foreign_keys=[source_entity_type_id]
    )
    target_entity_type: Mapped["EntityType"] = relationship(
        foreign_keys=[target_entity_type_id]
    )
