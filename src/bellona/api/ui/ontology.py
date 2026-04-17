import uuid

import structlog
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.api.ui.templates import templates
from bellona.db.session import get_db
from bellona.schemas.ontology import (
    EntityTypeCreate,
    EntityTypePatch,
    PropertyDefinitionCreate,
    RelationshipTypeCreate,
)
from bellona.services.entity_type import (
    create_entity_type,
    get_entity_type,
    list_entity_types,
    patch_entity_type,
)
from bellona.services.relationship_type import (
    create_relationship_type,
    list_relationship_types,
)

logger = structlog.get_logger()
router = APIRouter(prefix="/ontology")


@router.get("")
async def ontology_index(request: Request, db: AsyncSession = Depends(get_db)):
    entity_types = await list_entity_types(db)
    return templates.TemplateResponse(
        request, "ontology/index.html", {"entity_types": entity_types}
    )


@router.get("/entity-types/{entity_type_id}")
async def entity_type_detail(
    request: Request,
    entity_type_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    entity_type = await get_entity_type(db, entity_type_id)
    if entity_type is None:
        return templates.TemplateResponse(request, "404.html", {}, status_code=404)
    return templates.TemplateResponse(
        request, "ontology/entity_type.html", {"entity_type": entity_type}
    )


@router.get("/relationships")
async def relationships_index(request: Request, db: AsyncSession = Depends(get_db)):
    rel_types = await list_relationship_types(db)
    entity_types = await list_entity_types(db)
    return templates.TemplateResponse(
        request,
        "ontology/relationships.html",
        {"rel_types": rel_types, "entity_types": entity_types},
    )


@router.post("/relationships")
async def create_relationship_type_ui(
    request: Request,
    name: str = Form(...),
    source_entity_type_id: uuid.UUID = Form(...),
    target_entity_type_id: uuid.UUID = Form(...),
    cardinality: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    try:
        data = RelationshipTypeCreate(
            name=name,
            source_entity_type_id=source_entity_type_id,
            target_entity_type_id=target_entity_type_id,
            cardinality=cardinality,  # type: ignore[arg-type]
        )
        await create_relationship_type(db, data)
        await db.commit()
    except (IntegrityError, ValueError) as exc:
        await db.rollback()
        rel_types = await list_relationship_types(db)
        entity_types = await list_entity_types(db)
        return templates.TemplateResponse(
            request,
            "ontology/relationships.html",
            {
                "rel_types": rel_types,
                "entity_types": entity_types,
                "error": str(exc),
            },
            status_code=422,
        )
    return RedirectResponse(url="/ui/ontology/relationships", status_code=303)
