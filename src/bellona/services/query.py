"""Query service: translates EntityQuery into SQLAlchemy and executes it."""
from __future__ import annotations

import math
import uuid

import structlog
from sqlalchemy import and_, cast, func, or_, select
from sqlalchemy.exc import DataError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.types import Numeric

from bellona.models.entities import Entity, Relationship
from bellona.schemas.query import (
    EntityPage,
    EntityQuery,
    EntityRead,
    FilterCondition,
    FilterGroup,
    RelationshipRead,
)

logger = structlog.get_logger()


# ── Filter translation ────────────────────────────────────────────────────────


def _filter_condition_to_clause(cond: FilterCondition):
    col = Entity.properties[cond.property].astext

    match cond.operator:
        case "eq":
            return col == str(cond.value) if cond.value is not None else col.is_(None)
        case "neq":
            return col != str(cond.value) if cond.value is not None else col.isnot(None)
        case "gt":
            return cast(col, Numeric) > cond.value
        case "gte":
            return cast(col, Numeric) >= cond.value
        case "lt":
            return cast(col, Numeric) < cond.value
        case "lte":
            return cast(col, Numeric) <= cond.value
        case "contains":
            escaped = str(cond.value).replace("%", r"\%").replace("_", r"\_")
            return col.ilike(f"%{escaped}%", escape="\\")
        case "in":
            values = cond.value if isinstance(cond.value, list) else [cond.value]
            return col.in_([str(v) for v in values])
        case "is_null":
            return col.is_(None)
        case "not_null":
            return col.isnot(None)
        case _:
            raise ValueError(f"Unknown filter operator: {cond.operator!r}")


def _filter_node_to_clause(node: FilterGroup | FilterCondition):
    if isinstance(node, FilterGroup):
        sub = [_filter_node_to_clause(c) for c in node.conditions]
        return and_(*sub) if node.op == "and" else or_(*sub)
    return _filter_condition_to_clause(node)


# ── Query execution ───────────────────────────────────────────────────────────


async def query_entities(db: AsyncSession, query: EntityQuery) -> EntityPage:
    stmt = select(Entity)

    if query.entity_type_id is not None:
        stmt = stmt.where(Entity.entity_type_id == query.entity_type_id)

    if query.filters is not None:
        stmt = stmt.where(_filter_node_to_clause(query.filters))

    # Count total before pagination
    try:
        count_stmt = select(func.count()).select_from(stmt.subquery())
        total: int = (await db.execute(count_stmt)).scalar_one()
    except DataError as exc:
        raise ValueError(
            f"Filter type mismatch (e.g. numeric operator on a string property): {exc.orig}"
        ) from exc

    # Sort
    for sort_clause in query.sort:
        col = Entity.properties[sort_clause.property].astext
        if sort_clause.data_type == "numeric":
            col = cast(col, Numeric)
        elif sort_clause.data_type == "date":
            col = cast(col, Date)
        stmt = stmt.order_by(col.desc() if sort_clause.direction == "desc" else col.asc())

    if not query.sort:
        stmt = stmt.order_by(Entity.created_at.desc())

    # Paginate
    offset = (query.page - 1) * query.page_size
    stmt = stmt.offset(offset).limit(query.page_size)

    result = await db.execute(stmt)
    entities = list(result.scalars().all())

    pages = max(1, math.ceil(total / query.page_size))

    logger.info(
        "entity query executed",
        total=total,
        page=query.page,
        page_size=query.page_size,
        returned=len(entities),
    )
    return EntityPage(
        items=[EntityRead.model_validate(e) for e in entities],
        total=total,
        page=query.page,
        page_size=query.page_size,
        pages=pages,
    )


async def get_entity(db: AsyncSession, entity_id: uuid.UUID) -> Entity | None:
    return await db.get(Entity, entity_id)


async def get_entity_relationships(
    db: AsyncSession, entity_id: uuid.UUID
) -> list[Relationship]:
    result = await db.execute(
        select(Relationship)
        .where(
            or_(
                Relationship.source_entity_id == entity_id,
                Relationship.target_entity_id == entity_id,
            )
        )
        .order_by(Relationship.created_at.desc())
    )
    return list(result.scalars().all())
