from typing import List, AsyncGenerator

from dependency_injector.wiring import Provide, inject
from fastapi import HTTPException, Depends, APIRouter, Header, BackgroundTasks

from app.containers import Container
from app.core.task_manager import TaskManager
from app.core.wb_client import WildberriesClient, WildberriesAPIError
from app.exceptions.task import TaskAlreadyExistsError, TaskDatabaseError
from app.schemas.task import GetTaskRequest, TaskType, TaskInfo
from app.utils.jwt import is_valid_token
from app.utils.logging import get_logger, set_task_id

logger = get_logger("api")
router = APIRouter(prefix="/api")


# ============================================================================
# Dependencies
# ============================================================================

def get_wb_token(x_wb_token: str = Header(..., description="WB API токен")) -> str:
    """
    Dependency для получения WB API токена из заголовка

    Args:
        x_wb_token: WB API токен из заголовка

    Returns:
        Валидный токен

    Raises:
        HTTPException: Если токен отсутствует
    """
    if not x_wb_token:
        logger.warning("Missing WB API token in request")
        raise HTTPException(
            status_code=401,
            detail="WB API token required in X-WB-Token header"
        )
    return x_wb_token


@inject
async def get_wb_client(
        task_manager: TaskManager = Depends(Provide[Container.task_manager])
) -> AsyncGenerator[WildberriesClient, None]:
    """
    Dependency для получения WildberriesClient с автоматическим закрытием

    Использует yield для гарантированного закрытия после обработки запроса
    """
    async with WildberriesClient(task_manager=task_manager) as client:
        yield client


# ============================================================================
# Background Task Wrappers
# ============================================================================

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
                "Background task failed",
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
                "Background update task failed",
                extra={
                    "task_id": task_info.task_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )


# ============================================================================
# Routes
# ============================================================================

@router.get("/auth/check_token", tags=["authentication"])
async def check_token(token: str = Depends(get_wb_token)):
    """
    Проверяет валидность WB API токена

    Args:
        token: WB API токен из заголовка

    Returns:
        Валидность токена

    Raises:
        HTTPException: Если токен невалиден
    """
    try:
        token_data = is_valid_token(token)

        if not token_data:
            logger.warning("Invalid WB token provided")
            raise HTTPException(
                status_code=401,
                detail="Invalid WB API token"
            )

        logger.info(
            "Token validation successful",
            extra={"token_valid": True}
        )

        return token_data

    except Exception as e:
        logger.error(
            "Token validation failed",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=401,
            detail="Token validation failed"
        )


@router.get("/product/sample", tags=["products"])
async def get_product(
        token: str = Depends(get_wb_token),
        wb_client: WildberriesClient = Depends(get_wb_client)
):
    """
    Получает один товар для тестирования API

    Args:
        token: WB API токен из заголовка
        wb_client: Клиент для работы с WB API

    Returns:
        Данные одного товара

    Raises:
        HTTPException: При ошибках WB API
    """
    try:
        logger.info("Fetching sample product")

        result = await wb_client.get_product(token=token)

        logger.info(
            "Sample product fetched successfully",
            extra={
                "cards_count": len(result.get("cards", []))
            }
        )

        return result

    except WildberriesAPIError as e:
        logger.error(
            "Failed to fetch sample product",
            extra={
                "error": str(e),
                "status_code": e.status_code
            },
            exc_info=True
        )
        raise HTTPException(
            status_code=e.status_code or 500,
            detail=f"WB API error: {e.message}"
        )
    except Exception as e:
        logger.error(
            "Unexpected error while fetching sample product",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )


@router.post("/product/collect", tags=["products"])
@inject
async def create_products_collection_task(
        background_tasks: BackgroundTasks,
        token: str = Depends(get_wb_token),
        task_manager: TaskManager = Depends(Provide[Container.task_manager])
):
    """
    Создаёт асинхронную задачу для получения всех товаров.
    Результат сохраняется в файл.

    Args:
        background_tasks: FastAPI background tasks
        token: WB API токен из заголовка
        task_manager: Менеджер задач

    Returns:
        Информация о созданной задаче

    Raises:
        HTTPException: При ошибках создания задачи
    """
    try:
        logger.info("Checking for active collection task")

        # Проверяем, есть ли уже активная задача для этого токена
        active_task = await task_manager.get_active_task_by_token(token)

        if active_task:
            logger.info(
                "Active task already exists",
                extra={
                    "task_id": active_task.task_id,
                    "status": active_task.status.value,
                    "progress": f"{active_task.processed_items}/{active_task.total_items}"
                }
            )
            return {
                "task_id": active_task.task_id,
                "status": active_task.status.value,
                "message": "Task already in progress",
                "created_at": active_task.created_at.isoformat(),
                "progress": {
                    "total": active_task.total_items,
                    "processed": active_task.processed_items
                }
            }

        # Создаём новую задачу
        logger.info("Creating new collection task")

        task = await task_manager.create_task(
            wb_token=token,
            task_type=TaskType.COLLECT_PRODUCTS
        )

        # Устанавливаем task_id в контекст для логирования
        set_task_id(task.task_id)

        # Запускаем фоновую задачу
        background_tasks.add_task(
            run_collect_products_background,
            token,
            task,
            task_manager
        )

        logger.info(
            "Collection task created and started",
            extra={
                "task_id": task.task_id,
                "task_type": task.task_type.value
            }
        )

        return {
            "task_id": task.task_id,
            "status": task.status.value,
            "message": "Task created successfully",
            "created_at": task.created_at.isoformat()
        }

    except TaskAlreadyExistsError as e:
        logger.warning(
            "Task already exists",
            extra={"error": str(e)}
        )
        raise HTTPException(
            status_code=409,
            detail=e.message
        )
    except TaskDatabaseError as e:
        logger.error(
            "Database error while creating task",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=503,
            detail="Database service unavailable"
        )
    except Exception as e:
        logger.error(
            "Unexpected error while creating task",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )


@router.post("/product/update", tags=["products"])
@inject
async def create_products_update_task(
        products: List[dict],
        background_tasks: BackgroundTasks,
        token: str = Depends(get_wb_token),
        task_manager: TaskManager = Depends(Provide[Container.task_manager])
):
    """
    Создаёт асинхронную задачу для обновления товаров.

    Args:
        products: Список товаров для обновления
        background_tasks: FastAPI background tasks
        token: WB API токен из заголовка
        task_manager: Менеджер задач

    Returns:
        Информация о созданной задаче

    Raises:
        HTTPException: При ошибках создания задачи
    """
    try:
        if not products:
            raise HTTPException(
                status_code=400,
                detail="Products list cannot be empty"
            )

        logger.info(
            "Creating products update task",
            extra={"products_count": len(products)}
        )

        # Создаём задачу
        task = await task_manager.create_task(
            wb_token=token,
            task_type=TaskType.UPDATE_PRODUCTS
        )

        set_task_id(task.task_id)

        # Запускаем фоновую задачу
        background_tasks.add_task(
            run_update_products_background,
            token,
            products,
            task,
            task_manager
        )

        logger.info(
            "Update task created and started",
            extra={
                "task_id": task.task_id,
                "products_count": len(products)
            }
        )

        return {
            "task_id": task.task_id,
            "status": task.status.value,
            "message": "Update task created successfully",
            "created_at": task.created_at.isoformat(),
            "total_items": len(products)
        }

    except TaskDatabaseError as e:
        logger.error(
            "Database error while creating update task",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=503,
            detail="Database service unavailable"
        )
    except Exception as e:
        logger.error(
            "Unexpected error while creating update task",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )


@router.get("/tasks/{task_id}", tags=["tasks"])
@inject
async def get_task_status(
        task_id: str,
        token: str = Depends(get_wb_token),
        task_manager: TaskManager = Depends(Provide[Container.task_manager])
):
    """
    Получает статус конкретной задачи по её ID

    Args:
        task_id: ID задачи
        token: WB API токен из заголовка (для авторизации)
        task_manager: Менеджер задач

    Returns:
        Детальная информация о задаче

    Raises:
        HTTPException: Если задача не найдена или ошибка БД
    """
    try:
        logger.debug(
            "Fetching task status",
            extra={"task_id": task_id}
        )

        task = await task_manager.get_task_by_id(task_id)

        if not task:
            logger.warning(
                "Task not found",
                extra={"task_id": task_id}
            )
            raise HTTPException(
                status_code=404,
                detail=f"Task with ID {task_id} not found"
            )

        logger.debug(
            "Task status retrieved",
            extra={
                "task_id": task_id,
                "status": task.status.value
            }
        )

        return {
            "task_id": task.task_id,
            "task_type": task.task_type.value,
            "status": task.status.value,
            "created_at": task.created_at.isoformat(),
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
            "progress": {
                "total": task.total_items,
                "processed": task.processed_items,
                "percentage": (
                    (task.processed_items / task.total_items * 100)
                    if task.total_items > 0 else 0
                )
            },
            "file_path": task.file_path,
            "category_ids": task.category_ids,
            "error": task.error,
            "metadata": task.metadata
        }

    except TaskDatabaseError as e:
        logger.error(
            "Database error while fetching task status",
            extra={
                "task_id": task_id,
                "error": str(e)
            },
            exc_info=True
        )
        raise HTTPException(
            status_code=503,
            detail="Database service unavailable"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Unexpected error while fetching task status",
            extra={
                "task_id": task_id,
                "error": str(e)
            },
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )


@router.post("/tasks/search", tags=["tasks"])
@inject
async def search_tasks(
        filters: GetTaskRequest,
        token: str = Depends(get_wb_token),
        task_manager: TaskManager = Depends(Provide[Container.task_manager])
):
    """
    Получает список задач по токену с фильтрацией

    Args:
        filters: Фильтры для поиска задач
        token: WB API токен из заголовка
        task_manager: Менеджер задач

    Returns:
        Список задач, соответствующих фильтрам

    Raises:
        HTTPException: При ошибке БД
    """
    try:
        logger.info(
            "Searching tasks",
            extra={
                "filters": {
                    "task_id": filters.task_id,
                    "task_type": filters.task_type.value if filters.task_type else None,
                    "status": filters.status.value if filters.status else None
                }
            }
        )

        tasks = await task_manager.get_tasks_by_token(
            wb_token=token,
            filters=filters
        )

        logger.info(
            "Tasks search completed",
            extra={"found_count": len(tasks)}
        )

        if not tasks:
            return []

        return [
            {
                "task_id": task.task_id,
                "task_type": task.task_type.value,
                "status": task.status.value,
                "created_at": task.created_at.isoformat(),
                "completed_at": task.completed_at.isoformat() if task.completed_at else None,
                "progress": {
                    "total": task.total_items,
                    "processed": task.processed_items
                },
                "error": task.error
            }
            for task in tasks
        ]

    except TaskDatabaseError as e:
        logger.error(
            "Database error while searching tasks",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=503,
            detail="Database service unavailable"
        )
    except Exception as e:
        logger.error(
            "Unexpected error while searching tasks",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )


# @router.get("/tasks/statistics", tags=["tasks"])
# @inject
# async def get_tasks_statistics(
#         token: str = Depends(get_wb_token),
#         task_manager: TaskManager = Depends(Provide[Container.task_manager])
# ):
#     """
#     Получает статистику по задачам для текущего токена
#
#     Args:
#         token: WB API токен из заголовка
#         task_manager: Менеджер задач
#
#     Returns:
#         Статистика по количеству задач в разных статусах
#
#     Raises:
#         HTTPException: При ошибке БД
#     """
#     try:
#         logger.info("Fetching tasks statistics")
#
#         statistics = await task_manager.get_task_statistics(wb_token=token)
#
#         logger.info(
#             "Tasks statistics retrieved",
#             extra={"statistics": statistics}
#         )
#
#         return {
#             "statistics": statistics,
#             "total_tasks": sum(statistics.values())
#         }
#
#     except TaskDatabaseError as e:
#         logger.error(
#             "Database error while fetching statistics",
#             extra={"error": str(e)},
#             exc_info=True
#         )
#         raise HTTPException(
#             status_code=503,
#             detail="Database service unavailable"
#         )
#     except Exception as e:
#         logger.error(
#             "Unexpected error while fetching statistics",
#             extra={"error": str(e)},
#             exc_info=True
#         )
#         raise HTTPException(
#             status_code=500,
#             detail="Internal server error"
#         )
