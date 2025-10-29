from typing import List, AsyncGenerator

from dependency_injector.wiring import Provide, inject
from fastapi import HTTPException, Depends, APIRouter, Header, BackgroundTasks

from app.containers import Container
from app.exceptions.task import TaskAlreadyExistsError, TaskDatabaseError
from app.schemas.task import GetTaskRequest, TaskType
from app.services.task_manager import TaskManager
from app.services.wb_client import WildberriesClient, WildberriesAPIError
from app.utils.background_operations import run_update_products_background, run_collect_products_background
from app.utils.jwt import is_valid_token
from app.utils.logging import get_logger, set_task_id

logger = get_logger("api")
router = APIRouter(prefix="/api")


def get_wb_token(x_wb_token: str = Header(..., description="WB API токен")) -> str:
    """
    Зависимость для получения WB API токена из заголовка запроса

    Args:
        x_wb_token: WB API токен из заголовка

    Returns:
        Валидный токен

    Raises:
        HTTPException: Если токен отсутствует
    """
    if not x_wb_token:
        logger.warning("Токен WB API отсутствует в запросе")
        raise HTTPException(
            status_code=401,
            detail="Требуется токен WB API в заголовке X-WB-Token"
        )
    return x_wb_token


@inject
async def get_wb_client(
        task_manager: TaskManager = Depends(Provide[Container.task_manager])
) -> AsyncGenerator[WildberriesClient, None]:
    """
    Зависимость для получения WildberriesClient с автоматическим закрытием соединения

    Использует yield для гарантированного закрытия ресурсов после обработки запроса
    """
    async with WildberriesClient(task_manager=task_manager) as client:
        yield client


@router.get("/auth/check_token", tags=["authentication"])
async def check_token(token: str = Depends(get_wb_token)):
    """
    Проверяет валидность WB API токена

    Args:
        token: WB API токен из заголовка

    Returns:
        Данные о валидности токена

    Raises:
        HTTPException: Если токен невалиден
    """
    try:
        token_data = is_valid_token(token)

        if not token_data:
            logger.warning("Предоставлен невалидный WB токен")
            raise HTTPException(
                status_code=401,
                detail="Невалидный токен WB API"
            )

        logger.info(
            "Проверка токена успешна",
            extra={"token_valid": True}
        )

        return token_data

    except Exception as e:
        logger.error(
            "Ошибка при проверке токена",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=401,
            detail="Ошибка при проверке токена"
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
        logger.info("Запрос примера товара")

        result = await wb_client.get_product(token=token)

        logger.info(
            "Пример товара успешно получен",
            extra={
                "cards_count": len(result.get("cards", []))
            }
        )

        return result

    except WildberriesAPIError as e:
        logger.error(
            "Ошибка при получении примера товара",
            extra={
                "error": str(e),
                "status_code": e.status_code
            },
            exc_info=True
        )
        raise HTTPException(
            status_code=e.status_code or 500,
            detail=f"Ошибка WB API: {e.message}"
        )
    except Exception as e:
        logger.error(
            "Неожиданная ошибка при получении примера товара",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера"
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
        background_tasks: Фоновые задачи FastAPI
        token: WB API токен из заголовка
        task_manager: Менеджер задач

    Returns:
        Информация о созданной задаче

    Raises:
        HTTPException: При ошибках создания задачи
    """
    try:
        logger.info("Проверка наличия активной задачи сбора")

        # Проверяем, есть ли уже активная задача для этого токена
        active_task = await task_manager.get_active_task_by_token(token)

        if active_task:
            logger.info(
                "Активная задача уже существует",
                extra={
                    "task_id": active_task.task_id,
                    "status": active_task.status.value,
                    "progress": f"{active_task.processed_items}/{active_task.total_items}"
                }
            )
            return {
                "task_id": active_task.task_id,
                "status": active_task.status.value,
                "message": "Задача уже выполняется",
                "created_at": active_task.created_at.isoformat(),
                "progress": {
                    "total": active_task.total_items,
                    "processed": active_task.processed_items
                }
            }

        # Создаём новую задачу
        logger.info("Создание новой задачи сбора товаров")

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
            "Задача сбора товаров успешно создана и запущена",
            extra={
                "task_id": task.task_id,
                "task_type": task.task_type.value
            }
        )

        return {
            "task_id": task.task_id,
            "status": task.status.value,
            "message": "Задача успешно создана",
            "created_at": task.created_at.isoformat()
        }

    except TaskAlreadyExistsError as e:
        logger.warning(
            "Задача уже существует",
            extra={"error": str(e)}
        )
        raise HTTPException(
            status_code=409,
            detail=e.message
        )
    except TaskDatabaseError as e:
        logger.error(
            "Ошибка базы данных при создании задачи",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=503,
            detail="Сервис базы данных недоступен"
        )
    except Exception as e:
        logger.error(
            "Неожиданная ошибка при создании задачи",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера"
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
        background_tasks: Фоновые задачи FastAPI
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
                detail="Список товаров не может быть пустым"
            )

        logger.info(
            "Создание задачи обновления товаров",
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
            "Задача обновления товаров успешно создана и запущена",
            extra={
                "task_id": task.task_id,
                "products_count": len(products)
            }
        )

        return {
            "task_id": task.task_id,
            "status": task.status.value,
            "message": "Задача обновления успешно создана",
            "created_at": task.created_at.isoformat(),
            "total_items": len(products)
        }

    except TaskDatabaseError as e:
        logger.error(
            "Ошибка базы данных при создании задачи обновления",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=503,
            detail="Сервис базы данных недоступен"
        )
    except Exception as e:
        logger.error(
            "Неожиданная ошибка при создании задачи обновления",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера"
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
            "Получение статуса задачи",
            extra={"task_id": task_id}
        )

        task = await task_manager.get_task_by_id(task_id)

        if not task:
            logger.warning(
                "Задача не найдена",
                extra={"task_id": task_id}
            )
            raise HTTPException(
                status_code=404,
                detail=f"Задача с ID {task_id} не найдена"
            )

        logger.debug(
            "Статус задачи получен",
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
            "Ошибка базы данных при получении статуса задачи",
            extra={
                "task_id": task_id,
                "error": str(e)
            },
            exc_info=True
        )
        raise HTTPException(
            status_code=503,
            detail="Сервис базы данных недоступен"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Неожиданная ошибка при получении статуса задачи",
            extra={
                "task_id": task_id,
                "error": str(e)
            },
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера"
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
            "Поиск задач",
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
            "Поиск задач завершён",
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
            "Ошибка базы данных при поиске задач",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=503,
            detail="Сервис базы данных недоступен"
        )
    except Exception as e:
        logger.error(
            "Неожиданная ошибка при поиске задач",
            extra={"error": str(e)},
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера"
        )
