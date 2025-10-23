import asyncio

from app.core.category_service import CategoryService
from app.core.sql_repository import MSSQLDatabaseService
from app.utils.logging import get_logger

logger = get_logger()


async def insert_categories():
    """
    Сначала получаем категории, затем загружаем их в бд
    """
    category_service = CategoryService()
    ms_sql_client = MSSQLDatabaseService()
    categories_tree = await category_service.create_categories_tree()
    await category_service.close()
    result = await ms_sql_client.sync_categories_tree_to_db(tree_data=categories_tree)
    logger.info(f"Результат синхронизации категорий: {result}")
    return result


if __name__ == "__main__":
    asyncio.run(insert_categories())
