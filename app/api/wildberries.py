from typing import List, Union

from dependency_injector.wiring import Provide, inject
from fastapi import HTTPException, Depends, APIRouter, Header, BackgroundTasks

from app.containers import Container
from app.exceptions.task import TaskDatabaseError
from app.exceptions.wb_api import WildberriesAPIError
from app.schemas.product_analyze import ProductAnalyzeResponse, ProductAnalyzeRequest
from app.schemas.product_create import ProductCreateRequest
from app.schemas.product_match import ProductMatchRequest, ProductListMatchResponse, ProductMatchResponse
from app.schemas.product_update import ProductUpdateRequest
from app.schemas.task import TaskStatusRequest, TaskType, TaskCreateResponse, TaskInfoResponse
from app.services.product_analyze_service import ProductAnalyzeService
from app.services.product_match_service import ProductMatchService
from app.services.sql_category_service import SqlCategoryService
from app.services.task_manager import TaskManager
from app.services.wb_client_service import WildberriesClient
from app.utils.jwt import is_valid_token
from app.utils.logging import get_logger
from app.utils.token import hash_token

logger = get_logger()
router = APIRouter()


def get_wb_token(x_wb_token: str = Header(..., description="WB API токен")) -> str:
    """Зависимость для получения WB API токена из заголовка запроса"""
    if not x_wb_token:
        logger.warning("Токен WB API отсутствует в запросе")
        raise HTTPException(
            status_code=401,
            detail="Требуется токен WB API в заголовке X-WB-Token"
        )
    return x_wb_token


@router.get(
    "/auth/check-token",
    tags=["authentication"],
    summary="Проверка валидности токена",
    description="Проверяет валидность WB API токена, предоставленного в заголовке X-WB-Token"
)
async def check_token(token: str = Depends(get_wb_token)):
    try:
        token_data = is_valid_token(token)
        if token_data.status != "success":
            logger.warning("Предоставлен невалидный WB токен")
            raise HTTPException(
                status_code=403,
                detail="Невалидный токен WB API"
            )

        logger.info("Проверка токена успешна")
        return token_data

    except Exception as e:
        logger.error(f"Ошибка при проверке токена: {e}", exc_info=True)
        raise HTTPException(
            status_code=403,
            detail="Ошибка при проверке токена"
        )


@router.get(
    "/product/sample",
    tags=["products"],
    summary="Получить пример товара",
    description="Получает один товар для тестирования API и проверки подключения к WB"
)
@inject
async def get_product(
        token: str = Depends(get_wb_token),
        wb_client: WildberriesClient = Depends(Provide[Container.wildberries_client])
):
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


@router.post(
    "/product/info",
    tags=["products"],
    summary="Создать задачу сбора товаров",
    description="Создаёт асинхронную задачу для получения всех товаров. Результат сохраняется в файл и индексируется. "
                "Если активная задача уже существует для этого токена, возвращается информация о ней.",
    response_model=TaskCreateResponse
)
@inject
async def create_products_collection_task(
        background_tasks: BackgroundTasks,
        token: str = Depends(get_wb_token),
        task_manager: TaskManager = Depends(Provide[Container.task_manager]),
        wb_client: WildberriesClient = Depends(Provide[Container.wildberries_client])
):
    try:
        logger.info("Проверка наличия активной задачи сбора")

        active_task = await task_manager.get_active_task_by_token(
            wb_token=token,
            task_type=TaskType.PRODUCTS_INFO
        )
        if active_task:
            logger.info(
                f"Активная задача уже существует: {active_task.task_id}, "
                f"статус={active_task.status.value}, "
                f"прогресс={active_task.processed_items}/{active_task.total_items}"
            )
            return TaskCreateResponse(
                task_type=active_task.task_type,
                task_id=active_task.task_id,
                status=active_task.status,
                message="Активная задача уже существует"
            )

        logger.info("Создание новой задачи сбора товаров")
        task = await task_manager.create_task(
            wb_token=token,
            task_type=TaskType.PRODUCTS_INFO
        )

        background_tasks.add_task(
            wb_client.collect_products_background,
            token,
            task
        )

        logger.info(f"Задача сбора товаров успешно создана: {task.task_id}")

        return TaskCreateResponse(
            task_id=task.task_id,
            task_type=task.task_type,
            status=task.status,
            message="Задача сбора товаров успешно создана"
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


@router.post(
    "/product/update",
    tags=["products"],
    summary="Создать задачу обновления товаров",
    description="Создаёт асинхронную задачу для обновления товаров. Поддерживает массовое обновление по nm_id. "
                "Возвращается информация о задаче.",
    response_model=TaskCreateResponse
)
@inject
async def create_products_update_task(
        request: ProductUpdateRequest,
        background_tasks: BackgroundTasks,
        token: str = Depends(get_wb_token),
        task_manager: TaskManager = Depends(Provide[Container.task_manager]),
        wb_client: WildberriesClient = Depends(Provide[Container.wildberries_client])
):
    """
    **Параметры запроса:**

    - `update_field`: Имя поля для обновления (обязательно)
    - `products`: Список товаров с nm_id и новыми значениями (обязательно, не может быть пустым)
    """
    try:
        hash_wb_token = hash_token(token)
        # Проверяем, не идет ли индексация
        if await task_manager.is_client_indexing(token):
            logger.warning(f"Идет индексирование для клиента {hash_wb_token} или администратора")
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "Идет индексация",
                    "details": "В данный момент происходит индексация товаров. Пожалуйста, подождите немного и попробуйте снова."
                }
            )

        if not request or not request.products:
            raise HTTPException(
                status_code=400,
                detail="Список товаров не может быть пустым"
            )

        if not request.update_field:
            raise HTTPException(
                status_code=400,
                detail="Поле для обновления не может быть пустым"
            )

        logger.info(
            f"Создание задачи обновления товаров: {len(request.products)} товаров, "
            f"поле={request.update_field}"
        )

        updates = {int(item.nm_id): item.value for item in request.products}

        task = await task_manager.create_task(
            wb_token=token,
            task_type=TaskType.PRODUCT_UPDATE
        )

        background_tasks.add_task(
            wb_client.update_products_background,
            token,
            request.update_field,
            updates,
            task
        )

        logger.info(
            f"Задача обновления товаров успешно создана: {task.task_id}, "
            f"товаров={len(updates)}, поле={request.update_field}"
        )

        return TaskCreateResponse(
            task_id=task.task_id,
            task_type=task.task_type,
            status=task.status,
            message="Задача обновления товаров успешно создана"
        )

    except TaskDatabaseError as e:
        logger.error(f"Ошибка базы данных при создании задачи обновления: {e}", exc_info=True)
        raise HTTPException(
            status_code=503,
            detail="Сервис базы данных недоступен"
        )

    except ValueError as e:
        logger.error(f"Ошибка валидации данных: {e}", exc_info=True)
        raise HTTPException(
            status_code=400,
            detail=f"Ошибка в данных запроса: {str(e)}"
        )

    except Exception as e:
        logger.error(f"Неожиданная ошибка при создании задачи обновления: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера"
        )


@router.post(
    "/product/task/status",
    tags=["tasks"],
    summary="Получить список задач",
    description="""Получает список задач по токену с опциональной фильтрацией\n
    **Параметры фильтра:**\n

    - `task_id`: Фильтр по ID задачи (опционально)\n
    - `task_type`: Фильтр по типу задачи (обязательно)\n
    - `status`: Фильтр по статусу (опционально)\n
    - `period`: Период в формате: 1h, 2d, 3w (опционально). Примеры: '1h' (последний час), '2d' (последние 2 дня), '3w' (последние 3 недели)
    """,
    response_model=Union[List[TaskInfoResponse], TaskInfoResponse]
)
@inject
async def search_tasks(
        filters: TaskStatusRequest,
        token: str = Depends(get_wb_token),
        task_manager: TaskManager = Depends(Provide[Container.task_manager])
):
    try:
        logger.info(
            f"Поиск задач: task_id={filters.task_id}, "
            f"task_type={filters.task_type}, status={filters.status}"
        )

        tasks = await task_manager.get_tasks_by_token(
            wb_token=token,
            filters=filters
        )

        logger.info(f"Поиск завершён, найдено {len(tasks)} задач")

        if not tasks:
            return []

        if len(tasks) > 1:
            return [TaskInfoResponse.from_task_info(task) for task in tasks]

        return TaskInfoResponse.from_task_info(tasks[0])

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


@router.post(
    "/product/match",
    tags=["products"],
    summary="Сопоставить товары",
    description="""Сопостовляет товары WB с товарами из БД по указанным полям для сравнения значений\n
     **Параметры запроса:**\n
    - `wb_match_field`: Поле по которому будем матчить полученные товары из API с нашими (обязательно)\n
    - `carville_match_field`: Поле в нашей БД с которым будем матчить продукты (обязательно)\n
    - `comparison_field`: Имя поля которое будем сравнивать у сматченных объектов (обязательно)\n
    - `brand`: Бренд для фильтрации товаров из WB API (опционально)\n
    - `filter`: Фильтр по категориям вида {root_category: [category1, category2]}
    """,
    response_model=ProductListMatchResponse
)
@inject
async def match_products(
        request: ProductMatchRequest,
        token: str = Depends(get_wb_token),
        sql_categories_service: SqlCategoryService = Depends(Provide[Container.sql_category_service]),
        product_match_service: ProductMatchService = Depends(Provide[Container.product_match_service])
):
    try:
        # Обрабатываем категории
        categories = []
        for parent_category_id, subcategories in request.filter.items():
            if not subcategories:
                # берём все категории по родительской категории
                categories_by_parent_id = await sql_categories_service.get_categories_by_parent_id(
                    parent_id=int(parent_category_id)
                )
                categories += categories_by_parent_id
            else:
                categories += subcategories

        result = await product_match_service.match_products(
            token=hash_token(token),
            match_field=request.wb_match_field,
            carville_match_field=request.carville_match_field,
            categories=categories,
            comparison_field=request.comparison_field,
            brand=request.brand
        )

        result_status = result.get("status", "error")
        result_message = result.get("message", None)
        result_products = [
            ProductMatchResponse.model_validate(product)
            for product in result.get("products", [])
        ]
        total_products_count = result.get("total_products")
        matched_products_count = result.get("matched_products")
        sample_count = min(3, len(result_products))

        logger.info(f"Статус сопоставления товаров: {result_status}")
        logger.info(f"Результат сопоставления товаров: {result_message}")
        logger.info(f"Количество сопоставленных товаров: {len(result_products)}")
        logger.info(f"Примеры сопоставленных товаров ({sample_count}): {result_products[:sample_count]}")

        if result_status != "success":
            error_msg = result_message
            logger.error(f"Ошибка сопоставления продуктов: {error_msg}")
            raise HTTPException(
                status_code=500,
                detail={"error": "Ошибка сопоставления продуктов", "details": error_msg}
            )

        logger.info(f"Успешно сопоставили товары для токена {hash_token(token)}")

        return ProductListMatchResponse(
            **request.model_dump(),
            products=result_products,
            total_products=total_products_count,
            matched_products=matched_products_count
        )

    except Exception as e:
        logger.error(f"Неожиданная ошибка при сопоставлении продуктов: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера"
        )


@router.post(
    "/product/analyze",
    tags=["products"],
    response_model=ProductAnalyzeResponse,
    summary="Анализ данных товаров",
    description="Анализ данных товаров по указанному бренду",
)
@inject
async def analyze_products(
        request: ProductAnalyzeRequest,
        token: str = Depends(get_wb_token),
        task_manager: TaskManager = Depends(Provide[Container.task_manager]),
        product_analyze_service: ProductAnalyzeService = Depends(Provide[Container.product_analyze_service])
):
    try:
        hash_wb_token = hash_token(token)
        # Проверяем, не идет ли индексация
        if await task_manager.is_client_indexing(token):
            logger.warning(f"Идет индексирование для клиента {hash_wb_token} или администратора")
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "Идет индексация",
                    "details": "В данный момент происходит индексация товаров. Пожалуйста, подождите немного и попробуйте снова."
                }
            )

        logger.info(f"Начало анализа продуктов для: {hash_wb_token}, бренд: {request.brand}")

        # Вызываем метод сервиса для подготовки данных
        products = await product_analyze_service.prepare_data(
            request.brand, token, request.carville_match_field, request.client_match_field
        )

        logger.info(f"Анализ продукта успешно завершен для {hash_wb_token}")

        return ProductAnalyzeResponse(
            products=products["products"],
            category_ids=products["category_ids"],
            total_products=products["total_products"]
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при анализе продуктов: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера"
        )


@router.post(
    "/product/create",
    tags=["products"],
    summary="Создать задачу создания товаров",
    description="Создаёт асинхронную задачу для создания товаров."
                "Возвращается информация о задаче.",
    response_model=TaskCreateResponse
)
@inject
async def create_products_create_task(
        request: ProductCreateRequest,
        background_tasks: BackgroundTasks,
        token: str = Depends(get_wb_token),
        task_manager: TaskManager = Depends(Provide[Container.task_manager]),
        wb_client: WildberriesClient = Depends(Provide[Container.wildberries_client])
):
    """
    **Параметры запроса:**

    - `brand`: Брэнд товаров (опционально)
    - `products`: Список товаров с nm_id и новыми значениями (обязательно, не может быть пустым)
    """
    try:
        hash_wb_token = hash_token(token)
        # Проверяем, не идет ли индексация
        if await task_manager.is_client_indexing(token):
            logger.warning(f"Идет индексирование для клиента {hash_wb_token} или администратора")
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "Идет индексация",
                    "details": "В данный момент происходит индексация товаров. Пожалуйста, подождите немного и попробуйте снова."
                }
            )

        if not request or not request.products:
            raise HTTPException(
                status_code=400,
                detail="Список товаров не может быть пустым"
            )

        logger.info(
            f"Создание задачи создания товаров: {len(request.products)} товаров, "
            f"brand={request.brand}"
        )

        task = await task_manager.create_task(
            wb_token=token,
            task_type=TaskType.PRODUCT_CREATE
        )

        background_tasks.add_task(
            wb_client.create_products_background,
            token,
            request.products,
            request.brand,
            task
        )

        logger.info(
            f"Задача создания товаров успешно создана: {task.task_id}, "
            f"товаров={len(request.products)}, brand={request.brand}"
        )

        return TaskCreateResponse(
            task_id=task.task_id,
            task_type=task.task_type,
            status=task.status,
            message="Задача создания товаров успешно создана"
        )

    except TaskDatabaseError as e:
        logger.error(f"Ошибка базы данных при создании задачи создания: {e}", exc_info=True)
        raise HTTPException(
            status_code=503,
            detail="Сервис базы данных недоступен"
        )

    except ValueError as e:
        logger.error(f"Ошибка валидации данных: {e}", exc_info=True)
        raise HTTPException(
            status_code=400,
            detail=f"Ошибка в данных запроса: {str(e)}"
        )

    except Exception as e:
        logger.error(f"Неожиданная ошибка при создании задачи создания: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера"
        )
