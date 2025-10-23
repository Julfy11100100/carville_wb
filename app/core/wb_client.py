import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
from aiohttp_retry import RetryClient, ExponentialRetry
from fastapi import HTTPException

from app.core.task_manager import TaskManager
from app.schemas.task import TaskStatus, TaskInfo, TaskType
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


class WildberriesClient:
    """
    HTTP клиент для работы с Wildberries API.
    """

    def __init__(
            self,
            task_manager: Optional[TaskManager] = None,
    ):
        self.task_manager = task_manager

        self.retry_options = ExponentialRetry(
            attempts=settings.MAX_RETRIES,
            start_timeout=1,
            max_timeout=30,
            factor=2,
            statuses={429},
        )

    def _get_headers(self, token: str) -> Dict[str, str]:
        """Создает заголовки для WB API"""
        return {
            "Authorization": token,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    async def _make_request(
            self,
            method: str,
            url: str,
            token: str,
            **kwargs
    ) -> dict:
        """Выполняет HTTP запрос с retry-логикой"""

        headers = self._get_headers(token)
        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))

        async with RetryClient(
                retry_options=self.retry_options,
                raise_for_status=False
        ) as retry_client:
            try:
                async with retry_client.request(
                        method, url, headers=headers, **kwargs
                ) as response:

                    if response.status >= 400:
                        error_detail = f"WB API error: {response.status}"
                        try:
                            error_data = await response.json()
                            error_detail = error_data.get('errorText', error_detail)
                        except Exception:
                            pass
                        raise HTTPException(
                            status_code=response.status,
                            detail=error_detail
                        )

                    return await response.json() if response.content_length else {}

            except httpx.RequestError as e:
                raise HTTPException(
                    status_code=503,
                    detail=f"Service unavailable: {e}"
                )
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(
                    status_code=500,
                    detail="Internal server error"
                )

    async def get_product(
            self,
            token: str,
    ) -> Dict[str, Any]:
        """
        Получение 1 товара.
        Использует POST /content/v2/get/cards/list
        """
        url = f"{settings.WB_CONTENT_API_URL}/content/v2/get/cards/list"

        # Формируем тело запроса согласно документации WB API
        body = {
            "settings": {
                "cursor": {
                    "limit": 1,
                    "offset": 0
                }
            }
        }

        return await self._make_request("POST", url, token, json=body)

    @staticmethod
    async def _save_products_to_file(task_id: str, products: list) -> str:
        """
        Сохраняем товары в файл
        """
        output_dir = Path("data/products")
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / f"products_{task_id}.json"

        def default_datetime_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat() + 'Z'
            raise TypeError(f"Type {type(obj)} not serializable")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump({
                "task_id": task_id,
                "collected_at": datetime.now(),
                "total_count": len(products),
                "products": products
            }, f, ensure_ascii=False, indent=2, default=default_datetime_serializer)
        return str(file_path)

    async def collect_products_background(self, token: str, task_info: TaskInfo):
        """
        Функция для получения всех товаров (сохраняет в файл)
        """
        try:
            task_info.status = TaskStatus.RUNNING
            await self.task_manager.save_task(task_info)

            all_products = []
            category_id = set()
            cursor = {}  # Начальное значение курсора

            while True:
                response = await self._make_request(
                    method="POST",
                    url=f"{settings.WB_CONTENT_API_URL}/content/v2/get/cards/list",
                    token=token,
                    json={
                        "settings": {
                            "cursor": {
                                "limit": 100,
                                **cursor
                            },
                            "filter": {
                                "withPhoto": -1
                            }
                        }
                    }
                )
                cards = response.get("cards", [])
                logger.info(f"task_id: {task_info.task_id} получили {len(cards)} карточек")
                if not cards:
                    break

                # Достаём id категорий
                category_id.update([card.get("subjectID") for card in cards])
                all_products.extend(cards)

                # Обновляем курсор для следующего запроса
                cursor = response.get("cursor", {})
                if not cursor or len(cards) < cursor.get("limit", 100):
                    break

                task_info.total_items = len(all_products)
                await self.task_manager.save_task(task_info)
                await asyncio.sleep(0.1)

            file_path = await self._save_products_to_file(task_info.task_id, all_products)
            task_info.status = TaskStatus.COMPLETED
            task_info.completed_at = datetime.now()
            task_info.total_items = len(all_products)
            task_info.category_ids = list(category_id)
            task_info.file_path = file_path
            logger.info(
                f"Успешно загрузили карточки товаров в файл {file_path}. Количество товаров: {len(all_products)}")

        except Exception as e:
            task_info.status = TaskStatus.FAILED
            task_info.completed_at = datetime.now()
            task_info.error = str(e)

        finally:
            await self.task_manager.save_task(task_info)

    async def _get_parents_categories(self) -> list:
        """
        Возвращает родительские категории
        """
        parents_categories = await self._make_request(
            method="GET",
            url=f"{settings.WB_CONTENT_API_URL}/content/v2/object/parent/all",
            token=settings.DEFAULT_WB_TOKEN,
        )
        logger.info(f"Получили родительские категории товаров: {parents_categories}")
        return parents_categories["data"]

    async def _get_children_categories(self, parent_id: str) -> list:
        """
        Возвращает дочернюю категорию по parent_id
        """
        children_categories = await self._make_request(
            method="GET",
            url=f"{settings.WB_CONTENT_API_URL}/content/v2/object/all",
            params={"parentID": parent_id, "limit": 1000, "offset": 0},
            token=settings.DEFAULT_WB_TOKEN
        )
        logger.info(f"Получили дочерние категории {children_categories} по parent_id {parent_id}")
        return children_categories["data"]

    async def create_categories_tree(self):
        """
        Получаем дерево категорий
        """
        parent_categories = await self._get_parents_categories()
        tree = {
            "root_categories": {},
            "categories": {}
        }
        for parent in parent_categories:
            parent_id = parent["id"]
            tree["root_categories"].update({
                parent_id: parent["name"]
            })

            children = await self._get_children_categories(parent_id)
            for child in children:
                tree["categories"].update({
                    child["subjectID"]: {"name": child["subjectName"], "parent_id": parent_id}
                })

        return tree
