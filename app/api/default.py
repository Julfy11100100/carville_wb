from datetime import datetime, timezone

from fastapi import APIRouter

from config import settings

router = APIRouter(tags=["default"])


@router.get(
    "/",
    summary="Информация о сервисе",
    description="Возвращает основную информацию о сервисе, включая имя и описание"
)
async def service_info():
    return {
        "service": settings.APP_NAME,
        "description": settings.DESCRIPTION,
    }


@router.get(
    "/health",
    summary="Проверка здоровья сервиса",
    description="Проверяет доступность и работоспособность сервиса. Возвращает статус 'ok' если сервис работает нормально"
)
async def health_check():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc)}
