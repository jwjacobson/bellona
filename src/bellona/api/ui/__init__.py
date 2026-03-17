from fastapi import APIRouter
from fastapi.responses import RedirectResponse

from bellona.api.ui.connectors import router as connectors_router
from bellona.api.ui.explorer import router as explorer_router
from bellona.api.ui.graph import router as graph_router
from bellona.api.ui.ontology import router as ontology_router
from bellona.api.ui.proposals import router as proposals_router
from bellona.api.ui.query import router as query_router

router = APIRouter(prefix="/ui")
router.include_router(ontology_router)
router.include_router(explorer_router)
router.include_router(connectors_router)
router.include_router(proposals_router)
router.include_router(query_router)
router.include_router(graph_router)


@router.get("")
async def ui_root():
    return RedirectResponse(url="/ui/ontology", status_code=302)
