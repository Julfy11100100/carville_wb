from typing import List

from dependency_injector.wiring import Provide, inject
from fastapi import HTTPException, Depends, APIRouter, Header, BackgroundTasks

from app.containers import Container
from app.exceptions.task import TaskAlreadyExistsError, TaskDatabaseError
from app.exceptions.wb_api import WildberriesAPIError
from app.schemas.task import GetTaskRequest, TaskType
from app.services.task_manager import TaskManager
from app.services.wb_client import WildberriesClient
from app.utils.jwt import is_valid_token
from app.utils.logging import get_logger

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

        logger.info("Проверка токена успешна")

        return token_data

    except Exception as e:
        logger.error(f"Ошибка при проверке токена: {e}", exc_info=True)

        raise HTTPException(
            status_code=401,
            detail="Ошибка при проверке токена"
        )


@router.get("/product/sample", tags=["products"])
@inject
async def get_product(
        token: str = Depends(get_wb_token),
        wb_client: WildberriesClient = Depends(Provide[Container.wildberries_client])
):
    """
    Получает один товар для тестирования API

    Args:
        token: WB API токен из заголовка
        wb_client: Клиент для работы с WB

    Returns:
        Данные одного товара

    Raises:
        HTTPException: При ошибках WB API
    """
    try:
        logger.info("Запрос примера товара")
        result = await wb_client.get_product(token=token)

        cards_count = len(result.get("cards", []))
        logger.info(f"Пример товара успешно получен: {cards_count} товаров")

        return result

    except WildberriesAPIError as e:
        logger.error(
            f"Ошибка при получении примера товара: {e.message} (код: {e.status_code})",
            exc_info=True
        )

        raise HTTPException(
            status_code=e.status_code or 500,
            detail=f"Ошибка WB API: {e.message}"
        )

    except Exception as e:
        logger.error(f"Неожиданная ошибка при получении примера товара: {e}", exc_info=True)

        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера"
        )


@router.post("/product/collect", tags=["products"])
@inject
async def create_products_collection_task(
        background_tasks: BackgroundTasks,
        token: str = Depends(get_wb_token),
        task_manager: TaskManager = Depends(Provide[Container.task_manager]),
        wb_client: WildberriesClient = Depends(Provide[Container.wildberries_client])

):
    """
    Создаёт асинхронную задачу для получения всех товаров.
    Результат сохраняется в файл.

    Args:
        background_tasks: Фоновые задачи FastAPI
        token: WB API токен из заголовка
        task_manager: Менеджер задач
        wb_client: Клиент для работы с WB

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
                f"Активная задача уже существует: {active_task.task_id}, "
                f"статус={active_task.status.value}, "
                f"прогресс={active_task.processed_items}/{active_task.total_items}"
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

        # Запускаем фоновую задачу
        background_tasks.add_task(
            wb_client.collect_products_background,
            token,
            task
        )

        logger.info(f"Задача сбора товаров успешно создана: {task.task_id}")

        return {
            "task_id": task.task_id,
            "status": task.status.value,
            "message": "Задача успешно создана",
            "created_at": task.created_at.isoformat()
        }

    except TaskAlreadyExistsError as e:
        logger.warning(f"Задача уже существует: {e.message}")

        raise HTTPException(
            status_code=409,
            detail=e.message
        )

    except TaskDatabaseError as e:
        logger.error(f"Ошибка базы данных при создании задачи: {e}", exc_info=True)

        raise HTTPException(
            status_code=503,
            detail="Сервис базы данных недоступен"
        )

    except Exception as e:
        logger.error(f"Неожиданная ошибка при создании задачи: {e}", exc_info=True)

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
        task_manager: TaskManager = Depends(Provide[Container.task_manager]),
        wb_client: WildberriesClient = Depends(Provide[Container.wildberries_client])
):
    """
    Создаёт асинхронную задачу для обновления товаров.

    Args:
        products: Список товаров для обновления
        background_tasks: Фоновые задачи FastAPI
        token: WB API токен из заголовка
        task_manager: Менеджер задач
        wb_client: Клиент для работы с WB

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

        logger.info(f"Создание задачи обновления товаров: {len(products)} товаров")

        # Создаём задачу
        task = await task_manager.create_task(
            wb_token=token,
            task_type=TaskType.UPDATE_PRODUCTS
        )

        # Запускаем фоновую задачу
        background_tasks.add_task(
            wb_client.update_products_background,
            token,
            products,
            task
        )

        logger.info(f"Задача обновления товаров успешно создана: {task.task_id}")

        return {
            "task_id": task.task_id,
            "status": task.status.value,
            "message": "Задача обновления успешно создана",
            "created_at": task.created_at.isoformat(),
            "total_items": len(products)
        }

    except TaskDatabaseError as e:
        logger.error(f"Ошибка базы данных при создании задачи обновления: {e}", exc_info=True)

        raise HTTPException(
            status_code=503,
            detail="Сервис базы данных недоступен"
        )

    except Exception as e:
        logger.error(f"Неожиданная ошибка при создании задачи обновления: {e}", exc_info=True)

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
        logger.debug(f"Получение статуса задачи: {task_id}")

        task = await task_manager.get_task_by_id(token, task_id)

        if not task:
            logger.warning(f"Задача не найдена: {task_id}")

            raise HTTPException(
                status_code=404,
                detail=f"Задача с ID {task_id} не найдена"
            )

        logger.debug(f"Статус задачи получен: {task_id}, статус={task.status.value}")

        return {
            "task_id": task.task_id,
            "task_type": task.task_type.value,
            "status": task.status.value,
            "created_at": task.created_at.isoformat(),
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
            "progress": {
                "total": task.total_items,
                "processed": task.processed_items,
            },
            "file_path": task.file_path,
            "category_ids": task.category_ids,
            "error": task.error,
            "metadata": task.metadata
        }

    except TaskDatabaseError as e:
        logger.error(f"Ошибка базы данных при получении статуса задачи {task_id}: {e}", exc_info=True)

        raise HTTPException(
            status_code=503,
            detail="Сервис базы данных недоступен"
        )

    except HTTPException:
        raise

    except Exception as e:
        logger.error(f"Неожиданная ошибка при получении статуса задачи {task_id}: {e}", exc_info=True)

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
        task_type = filters.task_type.value if filters.task_type else None
        status = filters.status.value if filters.status else None

        logger.info(
            f"Поиск задач: task_id={filters.task_id}, "
            f"task_type={task_type}, status={status}"
        )

        tasks = await task_manager.get_tasks_by_token(
            wb_token=token,
            filters=filters
        )

        logger.info(f"Поиск завершён, найдено {len(tasks)} задач")

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
        logger.error(f"Ошибка базы данных при поиске задач: {e}", exc_info=True)

        raise HTTPException(
            status_code=503,
            detail="Сервис базы данных недоступен"
        )

    except Exception as e:
        logger.error(f"Неожиданная ошибка при поиске задач: {e}", exc_info=True)

        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера"
        )
