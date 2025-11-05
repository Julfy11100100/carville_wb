import asyncio

from app.services.api_category_service import ApiCategoryService
from app.services.sql_category_service import SqlCategoryService
from app.services.sql_repository import SQLDatabaseRepository
from app.services.wb_api import WildberriesAPI
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


async def insert_categories():
    """
    Сначала получаем категории, затем загружаем их в бд
    """
    # Создаем экземпляры сервисов
    wb_api = WildberriesAPI()
    sql_repository = SQLDatabaseRepository()
    api_category_service = ApiCategoryService(wb_api)
    sql_category_service = SqlCategoryService(sql_repository)

    try:
        logger.info("Начинаем получение дерева категорий")

        categories_tree = await api_category_service.create_categories_tree(
            token=settings.DEFAULT_WB_TOKEN
        )

        logger.info(
            f"Получено категорий: {len(categories_tree.get('categories', {}))}. "
            f"Начинаем синхронизацию с БД"
        )

        result = await sql_category_service.sync_categories_tree_to_db(
            tree_data=categories_tree
        )

        logger.info(f"Результат синхронизации категорий: {result}")
        return result

    except Exception as e:
        logger.error(
            f"Ошибка при синхронизации категорий: {str(e)}",
            exc_info=True
        )
        raise
    finally:
        await wb_api.close()
        await sql_repository.close_pool()


if __name__ == "__main__":
    asyncio.run(insert_categories())
