from fastapi import APIRouter

from bellona.api.v1.entity_types import router as entity_types_router
from bellona.api.v1.relationship_types import router as relationship_types_router

router = APIRouter(prefix="/api/v1")
router.include_router(entity_types_router)
router.include_router(relationship_types_router)
