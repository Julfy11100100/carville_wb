import asyncio

from app.services.wb_api import WildberriesAPI
from app.services.category_service import CategoryService
from app.services.sql_repository import MSSQLDatabaseService
from app.utils.logging import get_logger
from app.config import settings

logger = get_logger()


async def insert_categories():
    """
    Сначала получаем категории, затем загружаем их в бд
    """
    # Создаем экземпляры сервисов
    wb_api = WildberriesAPI()
    category_service = CategoryService(wb_api)
    ms_sql_client = MSSQLDatabaseService()

    try:
        async with wb_api:
            logger.info("Начинаем получение дерева категорий")

            categories_tree = await category_service.create_categories_tree(
                token=settings.DEFAULT_WB_TOKEN
            )

            logger.info(
                f"Получено категорий: {len(categories_tree.get('categories', {}))}. "
                f"Начинаем синхронизацию с БД"
            )

            result = await ms_sql_client.sync_categories_tree_to_db(
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


if __name__ == "__main__":
    asyncio.run(insert_categories())
