from dependency_injector.wiring import Provide, inject
from fastapi import HTTPException, Depends, APIRouter, Header

from app.containers import Container
from app.schemas.reviews import ReviewByValueRequest, ReviewByValueResponse
from app.services.review_service import ReviewService
from app.utils.logging import get_logger
from config import settings

logger = get_logger()
router = APIRouter(prefix="/reviews")


def get_wb_admin_token(x_wb_token: str = Header(..., description="WB API токен")) -> str:
    """Зависимость для получения WB API токена из заголовка запроса + сверка с админским токеном"""
    if not x_wb_token:
        logger.warning("Токен WB API отсутствует в запросе")
        raise HTTPException(
            status_code=401,
            detail="Требуется токен WB API REVIEWS в заголовке X-WB-Token"
        )
    if x_wb_token != settings.REVIEWS_WB_TOKEN:
        logger.warning(f"Токен WB API не является админским")
        raise HTTPException(
            status_code=403,
            detail="Требуется админский токен WB API REVIEWS"
        )
    return x_wb_token


@router.post(
    "/by-value",
    tags=["reviews"],
    summary="Получить отзывы по значению",
    description="Админский эндпоинт. Возвращает список отзывов по полям barcode|vendor_code",
    response_model=ReviewByValueResponse
)
@inject
async def get_reviews_by_value(
        request: ReviewByValueRequest,
        token: str = Depends(get_wb_admin_token),
        review_service: ReviewService = Depends(Provide[Container.review_service])
):
    try:
        return await review_service.get_reviews_by_value(
            token=token,
            vendor_code=request.vendor_code,
            barcode=request.barcode
        )

    except Exception as e:
        logger.error(f"Неожиданная ошибка при получении примера товара: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера"
        )
