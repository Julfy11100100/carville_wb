from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, HTTPException, status, Depends

from app.containers import Container
from app.exceptions.sql_database import DatabaseError
from app.services.sql_category_service import SqlCategoryService
from app.utils.logging import get_logger

logger = get_logger()

router = APIRouter()


@router.get(
    "/tree",
    tags=["categories"],
    summary="Получить дерево категорий",
    description="Возвращает полное иерархическое дерево категорий с взаимосвязями родитель-потомок"
)
@inject
async def get_types_tree(
        sql_category_service: SqlCategoryService = Depends(Provide[Container.sql_category_service])
):
    try:
        return await sql_category_service.get_types_tree()
    except DatabaseError as e:
        logger.warning(f"База данных недоступна для дерева типов wb: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail={"error": "База данных недоступна", "детали": str(e)}
        )

    except Exception as e:
        logger.error(f"Не удалось получить дерево типов: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={"error": "Внутренняя ошибка сервера", "details": str(e)}
        )
