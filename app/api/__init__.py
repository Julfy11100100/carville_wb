from fastapi import APIRouter
from app.api.categories import router as router_categories
from app.api.wildberries import router as router_wildberries
from app.api.default import router as router_default
from app.api.feedbacks import router as router_feedback

router = APIRouter(prefix="/api/v1")
router.include_router(router_default)
router.include_router(router_wildberries)
router.include_router(router_categories, prefix="/wb-types")
router.include_router(router_feedback)
