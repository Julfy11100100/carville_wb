import asyncio

from dependency_injector.wiring import Provide, inject
from fastapi import HTTPException, Depends, APIRouter, Header

from app.containers import Container
from app.core.task_manager import TaskManager
from app.core.wb_client import WildberriesClient
from app.schemas.task import GetTaskRequest, TaskType
from app.utils.jwt import is_valid_token

router = APIRouter(prefix="/api", )


def get_wb_token(x_wb_token: str = Header(..., description="WB API токен")) -> str:
    """Dependency для получения WB API токена из заголовка"""
    if not x_wb_token:
        raise HTTPException(status_code=401, detail="WB API token required")
    return x_wb_token


@router.get("/auth/check_token", tags=["authentication"])
async def check_token(
        token: str = Depends(get_wb_token)
):
    """
    Проверяем валидность токена
    :arg
        token: токен wb

    :return
        Параметры токена
    """
    return is_valid_token(token)


@router.get("/product/sample", tags=["products"])
@inject
async def get_product(
        token: str = Depends(get_wb_token),
        wb_client: WildberriesClient = Depends(Provide[Container.wildberries_client])
):
    """
    Получение 1 товара.

    :arg
        token: токен wb в заголовке

    :return
        1 товар
    """

    return await wb_client.get_product(token=token)


@router.post("/product/info", tags=["products"])
@inject
async def get_or_create_products_task(
        token: str = Depends(get_wb_token),
        wb_client: WildberriesClient = Depends(Provide[Container.wildberries_client]),
        task_manager: TaskManager = Depends(Provide[Container.task_manager])
):
    """
    Создает асинхронную задачу для получения детальной информации о товарах. Результат сохраняется в файл.

    :arg
        token: токен wb в заголовке

    :return
        id операции по которой можем пинговать статус
    """
    task = await task_manager.get_id_task_by_token(wb_token=token)
    if not task:
        task = await task_manager.create_task(wb_token=token, task_type=TaskType.GET_PRODUCTS)
        asyncio.create_task(wb_client.collect_products_background(token=token, task_info=task))
    return task.to_front()


@router.post("/product/task/status", tags=["products"])
@inject
async def get_task(
        task_info: GetTaskRequest,
        token: str = Depends(get_wb_token),
        task_manager: TaskManager = Depends(Provide[Container.task_manager])
):
    tasks = await task_manager.get_tasks_by_token(wb_token=token, task_info=task_info)
    if tasks:
        return [task.to_front() for task in tasks]
    return []


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
