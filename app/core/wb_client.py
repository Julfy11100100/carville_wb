import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from fastapi import HTTPException
from unicodedata import category

from app.core.task_manager import TaskManager
from app.schemas.task import TaskStatus, TaskInfo, TaskType
from app.schemas.wildberries import ProductCreateItem, ProductUpdate
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


class WildberriesClient:
    """
    HTTP клиент для работы с Wildberries API.
    - Автоматический retry при 429
    """

    def __init__(self, task_manager: TaskManager):
        self.task_manager = task_manager
        self.client: Optional[httpx.AsyncClient] = None
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.REQUEST_TIMEOUT),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            self.client.aclose()

    def _get_headers(self, token: str) -> Dict[str, str]:
        """Создает заголовки для WB API (без Bearer!)"""
        return {
            "Authorization": token,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    async def _make_single_request(
            self,
            method: str,
            url: str,
            headers: Dict,
            data: Optional[Dict]
    ):
        """Функция одиночного запроса"""
        async with self.client as client:
            response = await client.request(
                method=method, url=url, headers=headers, json=data
            )
            response.raise_for_status()
            return response.json()

    async def _make_request(
            self,
            method: str,
            url: str,
            token: str,
            **kwargs
    ) -> Dict[str, Any]:
        """
        Выполняет HTTP запрос с обработкой специфики WB API.

        Args:
            method: HTTP метод
            url: URL запроса
            token: WB API токен
            **kwargs: Дополнительные параметры запроса

        Returns:
            Dict с ответом API
        """
        if not self.client:
            raise RuntimeError("HTTP client not initialized")

        headers = self._get_headers(token)
        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))

        retries = 0
        while retries <= settings.MAX_RETRIES:
            try:
                logger.info(f"{method} запрос на {url} с параметрами {kwargs}")

                response = await self.client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    **kwargs
                )

                if response.status_code == 429:
                    # Получаем время ожидания из заголовка
                    retry_after = int(response.headers.get('X-Ratelimit-Retry', 60))
                    logger.warning(f"429 Too Many Requests, waiting {retry_after}s")

                    if retries < settings.MAX_RETRIES:
                        await asyncio.sleep(retry_after)
                        retries += 1
                        continue
                    else:
                        raise HTTPException(
                            status_code=429,
                            detail="Rate limit exceeded, max retries reached",
                            headers={"Retry-After": str(retry_after)}
                        )

                # Логируем результат запроса
                logger.info(f"Статус: {response.status_code}")

                if response.status_code >= 400:
                    error_detail = f"WB API error: {response.status_code}"
                    try:
                        error_data = response.json()
                        error_detail = error_data.get('errorText', error_detail)
                    except:
                        pass

                    logger.info(f"Получили ошибку {error_detail}")
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=error_detail
                    )

                return response.json() if response.content else {}

            except httpx.RequestError as e:
                logger.error(f"Request error: {e}")
                raise HTTPException(status_code=503, detail=f"Service unavailable: {e}")

            except HTTPException:
                raise

            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                raise HTTPException(status_code=500, detail="Internal server error")

    async def get_product(
            self,
            token: str,
    ) -> Dict[str, Any]:
        """
        Получение товара.
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

    async def create_or_get_task_for_all_products(
            self,
            token: str
    ) -> Dict:
        task = await self.task_manager.get_id_task_by_token(wb_token=token)
        if not task:
            # Тут логика запуска бэкграунда
            task = await self.task_manager.create_task(wb_token=token, task_type=TaskType.GET_PRODUCTS)
            asyncio.create_task(self._collect_products_background(token=token, task_info=task))
        return task.to_front()

    @staticmethod
    async def _save_products_to_file(task_id: str, products: list) -> str:
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

    async def _collect_products_background(self, token: str, task_info: TaskInfo):
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
                await asyncio.sleep(0.1)  # Учитываем лимиты по запросам

            file_path = await self._save_products_to_file(task_info.task_id, all_products)
            task_info.status = TaskStatus.COMPLETED
            task_info.completed_at = datetime.now()
            task_info.total_items = len(all_products)
            task_info.category_ids = list(category_id)
            task_info.file_path = file_path

        except Exception as e:
            task_info.status = TaskStatus.FAILED
            task_info.completed_at = datetime.now()
            task_info.error = str(e)

        finally:
            await self.task_manager.save_task(task_info)

    async def get_all_products(
            self,
            token: str
    ):
        pass

    async def create_products(
            self,
            token: str,
            products: List[ProductCreateItem]
    ) -> Dict[str, Any]:
        """
        Создание товаров (batch операция).
        Использует POST /content/v2/cards/upload
        """
        url = f"{settings.WB_CONTENT_API_URL}/content/v2/cards/upload"

        # Преобразуем данные в формат WB API
        wb_products = []
        for product in products:
            wb_product = {
                "subjectID": product.subject_id,
                "variants": [{
                    "vendorCode": product.vendor_code,
                    "title": product.title,
                    "description": product.description,
                    "brand": product.brand,
                    "dimensions": product.dimensions,
                    "characteristics": product.characteristics,
                    "sizes": product.sizes
                }]
            }
            wb_products.append(wb_product)

        return await self._make_request("POST", url, token, json=wb_products)

    async def update_product(
            self,
            token: str,
            product_id: int,
            product_data: ProductUpdate
    ) -> Dict[str, Any]:
        """
        Обновление товара по ID.
        Использует PUT /content/v2/cards/update
        """
        url = f"{settings.WB_CONTENT_API_URL}/content/v2/cards/update"

        # Формируем данные для обновления
        update_data = {
            "nmID": product_id,
            "vendorCode": product_data.vendor_code
        }

        # Добавляем только заполненные поля
        if product_data.title:
            update_data["title"] = product_data.title
        if product_data.description:
            update_data["description"] = product_data.description
        if product_data.brand:
            update_data["brand"] = product_data.brand
        if product_data.dimensions:
            update_data["dimensions"] = product_data.dimensions
        if product_data.characteristics:
            update_data["characteristics"] = product_data.characteristics
        if product_data.sizes:
            update_data["sizes"] = product_data.sizes

        return await self._make_request("PUT", url, token, json=[update_data])

    async def get_api_limits(self, token: str) -> Dict[str, Any]:
        """
        Получение текущих лимитов API через ping.
        Использует GET /ping для common-api
        """
        url = f"{settings.WB_COMMON_API_URL}/ping"

        try:
            response = await self._make_request("GET", url, token)

            # Получаем информацию о лимитах из Redis
            key = f"wb:{hash(token)}:GET"
            rate_info = await self.rate_limiter.redis.hmget(
                key, 'tokens', 'capacity'
            )

            remaining = int(rate_info[0]) if rate_info[0] else None
            limit = int(rate_info[1]) if rate_info[1] else None

            return {
                "status": response.get("Status", "Unknown"),
                "timestamp": response.get("TS"),
                "rate_limit": {
                    "remaining": remaining,
                    "limit": limit,
                    "reset": None,
                    "retry_after": None
                }
            }

        except Exception as e:
            logger.error(f"Error getting API limits: {e}")
            raise
