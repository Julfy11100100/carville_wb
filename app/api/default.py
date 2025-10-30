from datetime import datetime, timezone

from fastapi import APIRouter

from app.config import settings

router = APIRouter(prefix="/api", tags=["default"])


@router.get("/info")
async def service_info():
    return {
        "service": settings.APP_NAME,
        "description": settings.DESCRIPTION,
    }


@router.get("/health")
async def health_check():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc)}
