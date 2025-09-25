from dependency_injector.wiring import Provide
from fastapi import HTTPException, Depends, APIRouter, Header

from app.containers import Container
from app.core.wb_client import WildberriesClient
from app.schemas.wildberries import ProductFilters

router = APIRouter(prefix="/wb", tags=["wb"])


def get_wb_token(x_wb_token: str = Header(..., description="WB API токен")) -> str:
    """Dependency для получения WB API токена из заголовка"""
    if not x_wb_token:
        raise HTTPException(status_code=401, detail="WB API token required")
    return x_wb_token


@router.get("/health", tags=["Health"])
async def health_check():
    """Health check эндпоинт"""
    return {"status": "healthy", "service": "wb-api-proxy"}


@router.get("/products", tags=["Products"])
async def get_products(
        filters: ProductFilters = Depends(),
        token: str = Depends(get_wb_token),
        wb_client: WildberriesClient = Depends(Provide[Container.wildberries_client])
):
    """
    Получение списка товаров с фильтрами.

    Args:
        search: Поиск по названию товара
        limit: Количество товаров (1-1000)
        offset: Смещение для пагинации
        X-WB-Token: WB API токен в заголовке

    Returns:
        Список товаров в формате WB API
    """

    return await wb_client.get_products(
        token=token,
        search=filters.search,
        limit=filters.limit,
        offset=filters.offset
    )

#
# @router.post("/products", tags=["Products"])
# async def create_products(
#         products: List[ProductCreateItem],
#         token: str = Depends(get_wb_token),
#         limiter: RedisLimiter = Depends(get_rate_limiter)
# ):
#     """
#     Создание товаров (batch операция).
#
#     Args:
#         products: Список товаров для создания
#         X-WB-Token: WB API токен в заголовке
#
#     Returns:
#         Результат создания товаров
#     """
#     if len(products) > 100:
#         raise HTTPException(
#             status_code=400,
#             detail="Максимум 100 товаров за один запрос"
#         )
#
#     async with WildberriesClient(limiter) as client:
#         return await client.create_products(token=token, products=products)
#
#
# @router.put("/products/{product_id}", tags=["Products"])
# async def update_product(
#         product_id: int,
#         product_data: ProductUpdate,
#         token: str = Depends(get_wb_token),
#         limiter: RedisLimiter = Depends(get_rate_limiter)
# ):
#     """
#     Обновление товара по ID.
#
#     Args:
#         product_id: Артикул WB (nmID)
#         product_data: Данные для обновления товара
#         X-WB-Token: WB API токен в заголовке
#
#     Returns:
#         Результат обновления товара
#     """
#     async with WildberriesClient(limiter) as client:
#         return await client.update_product(
#             token=token,
#             product_id=product_id,
#             product_data=product_data
#         )
#
#
# @router.get("/limits", response_model=Dict[str, Any], tags=["API Info"])
# async def get_api_limits(
#         token: str = Depends(get_wb_token),
#         limiter: RedisLimiter = Depends(get_rate_limiter)
# ):
#     """
#     Получение текущих лимитов API.
#
#     Args:
#         X-WB-Token: WB API токен в заголовке
#
#     Returns:
#         Информация о текущих лимитах и статусе API
#     """
#     async with WildberriesClient(limiter) as client:
#         return await client.get_api_limits(token=token)
