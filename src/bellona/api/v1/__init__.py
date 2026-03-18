from fastapi import APIRouter

from bellona.api.v1.agents import router as agents_router
from bellona.api.v1.connectors import router as connectors_router
from bellona.api.v1.entities import router as entities_router
from bellona.api.v1.entity_types import router as entity_types_router
from bellona.api.v1.query import router as query_router
from bellona.api.v1.relationship_types import router as relationship_types_router
from bellona.api.v1.relationships import router as relationships_router

router = APIRouter(prefix="/api/v1")
router.include_router(entity_types_router)
router.include_router(relationship_types_router)
router.include_router(connectors_router)
router.include_router(agents_router)
router.include_router(entities_router)
router.include_router(query_router)
router.include_router(relationships_router)