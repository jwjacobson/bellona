import uuid

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.api.ui.templates import templates
from bellona.db.session import get_db
from bellona.schemas.query import EntityQuery
from bellona.services.entity_type import get_entity_type, list_entity_types
from bellona.services.query import query_entities

router = APIRouter(prefix="/explorer")

_PAGE_SIZE = 50


@router.get("")
async def explorer_index(request: Request, db: AsyncSession = Depends(get_db)):
    entity_types = await list_entity_types(db)
    return templates.TemplateResponse(
        request, "explorer/index.html", {"entity_types": entity_types, "selected": None}
    )


@router.get("/{entity_type_id}")
async def explorer_entity_type(
    request: Request,
    entity_type_id: uuid.UUID,
    page: int = 1,
    db: AsyncSession = Depends(get_db),
):
    entity_type = await get_entity_type(db, entity_type_id)
    if entity_type is None:
        return templates.TemplateResponse(request, "404.html", {}, status_code=404)

    entity_types = await list_entity_types(db)
    offset = (page - 1) * _PAGE_SIZE
    q = EntityQuery(entity_type_id=entity_type_id, limit=_PAGE_SIZE, offset=offset)
    page_result = await query_entities(db, q)

    return templates.TemplateResponse(
        request,
        "explorer/index.html",
        {
            "entity_types": entity_types,
            "selected": entity_type,
            "page": page,
            "page_size": _PAGE_SIZE,
            "entities": page_result.items,
            "total": page_result.total,
        },
    )
