from typing import List

from app.schemas.task import TaskInfo
from app.services.task_manager import TaskManager
from app.services.wb_client import WildberriesClient
from app.utils.logging import get_logger

logger = get_logger()


async def run_collect_products_background(
        token: str,
        task_info: TaskInfo,
        task_manager: TaskManager
):
    """
    Обёртка для фоновой задачи сбора товаров с правильным управлением ресурсами

    Args:
        token: WB API токен
        task_info: Информация о задаче
        task_manager: Менеджер задач
    """
    # Создаём отдельный клиент для фоновой задачи
    async with WildberriesClient(task_manager=task_manager) as wb_client:
        try:
            await wb_client.collect_products_background(token, task_info)
        except Exception as e:
            logger.error(
                "Фоновая задача сбора товаров завершилась с ошибкой",
                extra={
                    "task_id": task_info.task_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )


async def run_update_products_background(
        token: str,
        products: List[dict],
        task_info: TaskInfo,
        task_manager: TaskManager
):
    """
    Обёртка для фоновой задачи обновления товаров с правильным управлением ресурсами

    Args:
        token: WB API токен
        products: Список товаров для обновления
        task_info: Информация о задаче
        task_manager: Менеджер задач
    """
    async with WildberriesClient(task_manager=task_manager) as wb_client:
        try:
            await wb_client.update_products_background(token, products, task_info)
        except Exception as e:
            logger.error(
                "Фоновая задача обновления товаров завершилась с ошибкой",
                extra={
                    "task_id": task_info.task_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )
