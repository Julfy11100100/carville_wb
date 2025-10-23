from typing import List, Dict, Any

from app.core.wb_client import WildberriesClient
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


class CategoryService:
    """
    Сервис для работы с категориями товаров WB

    Выделен в отдельный класс по принципу Single Responsibility
    """

    def __init__(self, wb_client: WildberriesClient = None):
        self.wb_client = wb_client if wb_client else WildberriesClient()

    async def close(self):
        await self.wb_client.close()

    async def get_parent_categories(self, token: str) -> List[Dict[str, Any]]:
        """Получает родительские категории"""
        response = await self.wb_client._make_request(
            "GET",
            "/content/v2/object/parent/all",
            token
        )
        logger.info(f"Получили родительские категории. count: {len(response.get('data', []))}")
        return response.get("data", [])

    async def get_children_categories(
            self,
            token: str,
            parent_id: str
    ) -> List[Dict[str, Any]]:
        """Получает дочерние категории по parent_id"""
        response = await self.wb_client._make_request(
            "GET",
            "/content/v2/object/all",
            token,
            params={"parentID": parent_id, "limit": 1000, "offset": 0}
        )
        logger.info(
            f"Получили дочерние категории. parent_id: {parent_id} count: {len(response.get('data', []))}",
        )
        return response.get("data", [])

    async def create_categories_tree(self) -> Dict[str, Any]:
        """Создаёт полное дерево категорий"""
        logger.info("Строим дерево категорий")

        token = settings.DEFAULT_WB_TOKEN

        parent_categories = await self.get_parent_categories(token)
        tree = {
            "root_categories": {},
            "categories": {}
        }

        for parent in parent_categories:
            parent_id = parent["id"]
            tree["root_categories"][parent_id] = parent["name"]

            children = await self.get_children_categories(token, parent_id)
            for child in children:
                tree["categories"][child["subjectID"]] = {
                    "name": child["subjectName"],
                    "parent_id": parent_id
                }

        logger.info(
            f"Построили дерево категорий: root_categories: "
            f"{len(tree['root_categories'])} categories: {len(tree['categories'])}",
        )

        return tree
