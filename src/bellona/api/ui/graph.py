import structlog
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.api.ui.templates import templates
from bellona.db.session import get_db
from bellona.models.entities import Entity, Relationship
from bellona.models.ontology import EntityType, RelationshipType

logger = structlog.get_logger()
router = APIRouter(prefix="/graph")

_MAX_ENTITIES = 200


@router.get("")
async def graph_index(request: Request):
    return templates.TemplateResponse(request, "graph/index.html", {})


@router.get("/data")
async def graph_data(db: AsyncSession = Depends(get_db)):
    """Return Cytoscape.js-compatible node/edge data."""
    et_result = await db.execute(select(EntityType).order_by(EntityType.name))
    entity_types = {et.id: et for et in et_result.scalars().all()}

    ent_result = await db.execute(
        select(Entity).order_by(Entity.created_at.desc()).limit(_MAX_ENTITIES)
    )
    entities = list(ent_result.scalars().all())

    entity_ids = {e.id for e in entities}
    rel_result = await db.execute(
        select(Relationship).where(Relationship.source_entity_id.in_(entity_ids))
    )
    relationships = list(rel_result.scalars().all())

    rel_type_result = await db.execute(select(RelationshipType))
    rel_types = {rt.id: rt for rt in rel_type_result.scalars().all()}

    nodes = []
    for entity in entities:
        et = entity_types.get(entity.entity_type_id)
        label = (
            str(next(iter(entity.properties.values()), entity.id))
            if entity.properties
            else str(entity.id)[:8]
        )
        nodes.append(
            {
                "data": {
                    "id": str(entity.id),
                    "label": str(label)[:40],
                    "entity_type": et.name if et else "unknown",
                    "entity_type_id": str(entity.entity_type_id),
                }
            }
        )

    edges = []
    for rel in relationships:
        if rel.target_entity_id not in entity_ids:
            continue
        rt = rel_types.get(rel.relationship_type_id)
        edges.append(
            {
                "data": {
                    "id": str(rel.id),
                    "source": str(rel.source_entity_id),
                    "target": str(rel.target_entity_id),
                    "label": rt.name if rt else "",
                }
            }
        )

    return JSONResponse({"nodes": nodes, "edges": edges})
