import asyncio
from typing import Any, Dict, List, Optional

import httpx
from fastapi import HTTPException

from app.core.rate_limiter import RedisLimiter
from app.schemas.wildberries import ProductCreateItem, ProductUpdate
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


class WildberriesClient:
    """
    HTTP клиент для работы с Wildberries API.
    Обрабатывает специфичные особенности WB API:
    - Авторизация без Bearer
    - Автоматический retry при 429
    - Обработка 409 как 5 запросов
    """

    def __init__(self, rate_limiter: RedisLimiter):
        self.rate_limiter = rate_limiter
        self.client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.REQUEST_TIMEOUT),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            headers={"User-Agent": "WB-Proxy/1.0"}
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    def _get_headers(self, token: str) -> Dict[str, str]:
        """Создает заголовки для WB API (без Bearer!)"""
        return {
            "Authorization": token,  # БЕЗ Bearer!
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

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

        # Проверяем rate limit перед запросом
        tokens_needed = 1
        allowed, remaining = await self.rate_limiter.check_rate_limit(
            token, method, requested_tokens=tokens_needed
        )

        if not allowed:
            logger.warning(f"Rate limit exceeded for token {hash(token)}")
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded",
                headers={"Retry-After": "60"}
            )

        retries = 0
        while retries <= settings.MAX_RETRIES:
            try:
                logger.info(f"Making {method} request to {url}")

                response = await self.client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    **kwargs
                )

                # Обновляем rate limiter на основе заголовков ответа
                await self.rate_limiter.update_from_headers(
                    token, method, dict(response.headers)
                )

                # Обрабатываем специфичные статус-коды WB API
                if response.status_code == 409:
                    # 409 считается как 5 запросов в WB API
                    logger.warning(f"409 Conflict received, counting as 5 requests")
                    await self.rate_limiter.check_rate_limit(
                        token, method, requested_tokens=5
                    )

                elif response.status_code == 429:
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
                logger.info(f"Request completed: {response.status_code}")

                if response.status_code >= 400:
                    error_detail = f"WB API error: {response.status_code}"
                    try:
                        error_data = response.json()
                        error_detail = error_data.get('errorText', error_detail)
                    except:
                        pass

                    raise HTTPException(
                        status_code=response.status_code,
                        detail=error_detail
                    )

                return response.json() if response.content else {}

            except httpx.RequestError as e:
                logger.error(f"Request error: {e}")
                if retries < settings.MAX_RETRIES:
                    retries += 1
                    await asyncio.sleep(2 ** retries)  # Exponential backoff
                    continue
                raise HTTPException(status_code=503, detail=f"Service unavailable: {e}")

            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                raise HTTPException(status_code=500, detail="Internal server error")

    async def get_products(
            self,
            token: str,
            search: Optional[str] = None,
            limit: int = 100,
            offset: int = 0
    ) -> Dict[str, Any]:
        """
        Получение списка товаров с фильтрами.
        Использует POST /content/v2/get/cards/list
        """
        url = f"{settings.WB_CONTENT_API_URL}/content/v2/get/cards/list"

        # Формируем тело запроса согласно документации WB API
        body = {
            "settings": {
                "sort": {"ascending": False},
                "filter": {
                    "withPhoto": -1,  # Все товары
                    "allowedCategoriesOnly": True
                },
                "cursor": {
                    "limit": limit,
                    "offset": offset
                }
            }
        }

        # Добавляем поиск если указан
        if search:
            body["settings"]["filter"]["textSearch"] = search

        return await self._make_request("POST", url, token, json=body)

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
