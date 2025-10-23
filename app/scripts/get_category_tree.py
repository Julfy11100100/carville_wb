import asyncio

from app.utils.logging import get_logger
from app.core.wb_client import WildberriesClient
from app.core.sql_repository import MSSQLDatabaseService

logger = get_logger()


async def insert_categories():
    """
    Сначала получаем категории, затем загружаем их в бд
    """
    wb_client = WildberriesClient()
    ms_sql_client = MSSQLDatabaseService()
    categories_tree = await wb_client.create_categories_tree()
    result = await ms_sql_client.sync_categories_tree_to_db(tree_data=categories_tree)
    logger.info(f"Результат синхронизации категорий: {result}")
    return result


if __name__ == "__main__":
    asyncio.run(insert_categories())
