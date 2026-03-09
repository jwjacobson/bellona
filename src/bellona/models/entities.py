import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from bellona.models.base import Base


class Entity(Base):
    __tablename__ = "entities"

    id: Mapped[uuid.UUID] = mapped_column(default=uuid.uuid4, primary_key=True)
    entity_type_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("entity_types.id", ondelete="RESTRICT"), nullable=False
    )
    properties: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    source_connector_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("connectors.id", ondelete="SET NULL"), nullable=True
    )
    source_record_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    entity_type: Mapped["EntityType"] = relationship(back_populates="entities")  # type: ignore[name-defined]
    source_connector: Mapped["Connector | None"] = relationship()  # type: ignore[name-defined]


class Relationship(Base):
    __tablename__ = "relationships"

    id: Mapped[uuid.UUID] = mapped_column(default=uuid.uuid4, primary_key=True)
    relationship_type_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("relationship_types.id", ondelete="RESTRICT"), nullable=False
    )
    source_entity_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("entities.id", ondelete="CASCADE"), nullable=False
    )
    target_entity_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("entities.id", ondelete="CASCADE"), nullable=False
    )
    properties: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    relationship_type: Mapped["RelationshipType"] = relationship()  # type: ignore[name-defined]
    source_entity: Mapped["Entity"] = relationship(foreign_keys=[source_entity_id])
    target_entity: Mapped["Entity"] = relationship(foreign_keys=[target_entity_id])
