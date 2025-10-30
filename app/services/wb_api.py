import asyncio
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from aiohttp_retry import RetryClient, ExponentialRetry

from app.exceptions.wb_api import WildberriesRateLimitError, WildberriesAPIError
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


class WildberriesAPI:
    """
    HTTP-клиент для работы с Wildberries API.
    """

    def __init__(
        self,
        base_url: str = None,
        max_retries: int = None,
        timeout: int = 30,
        max_connections: int = 100,
    ):
        """
        Args:
            base_url: Базовый URL для WB API
            max_retries: Максимальное количество повторных попыток
            timeout: Таймаут для HTTP-запросов в секундах
            max_connections: Максимальное количество одновременных соединений
        """
        self.base_url = base_url or settings.WB_CONTENT_API_URL
        self.max_retries = max_retries or settings.MAX_RETRIES
        self.timeout = ClientTimeout(total=timeout)

        # Настройка connection pooling
        self.connector = TCPConnector(
            limit=max_connections,
            limit_per_host=30,
            ttl_dns_cache=300
        )

        # Настройка retry логики
        self.retry_options = ExponentialRetry(
            attempts=self.max_retries,
            start_timeout=1,
            max_timeout=30,
            factor=2,
            statuses={429, 500, 502, 503, 504},
        )

        self._session: Optional[aiohttp.ClientSession] = None
        self._retry_client: Optional[RetryClient] = None

    async def __aenter__(self):
        """Создание сессии при входе в контекстный менеджер"""
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие сессии при выходе из контекстного менеджера"""
        await self.close()

    async def _ensure_session(self):
        """Создаёт сессию, если она ещё не создана"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                connector=self.connector,
                connector_owner=False
            )

            self._retry_client = RetryClient(
                client_session=self._session,
                retry_options=self.retry_options,
                raise_for_status=False
            )

            logger.info(
                "Сессия WB API создана",
                extra={
                    "base_url": self.base_url,
                    "max_retries": self.max_retries,
                    "max_connections": self.connector.limit
                }
            )

    async def close(self):
        """Закрывает все активные соединения"""
        if self._retry_client:
            await self._retry_client.close()
        if self._session and not self._session.closed:
            await self._session.close()
        if self.connector:
            await self.connector.close()
        logger.info("Сессия WB API закрыта")

    def _get_headers(self, token: str) -> Dict[str, str]:
        """Создаёт заголовки для WB API"""
        return {
            "Authorization": token,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    async def make_request(
        self,
        method: str,
        endpoint: str,
        token: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Выполняет HTTP-запрос с retry-логикой и обработкой ошибок

        Args:
            method: HTTP метод (GET, POST, etc.)
            endpoint: API endpoint (например, /content/v2/get/cards/list)
            token: API токен
            **kwargs: Дополнительные параметры для запроса

        Returns:
            Распарсенный JSON ответ

        Raises:
            WildberriesAPIError: При ошибках API
            WildberriesRateLimitError: При превышении rate limit
        """
        await self._ensure_session()

        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers(token)

        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))

        logger.debug(
            "Выполнение запроса к WB API",
            extra={
                "method": method,
                "endpoint": endpoint,
                "has_body": "json" in kwargs or "data" in kwargs
            }
        )

        try:
            async with self._retry_client.request(
                method, url, headers=headers, **kwargs
            ) as response:
                # Обработка различных статус-кодов
                if response.status == 429:
                    error_msg = "Превышен лимит запросов"
                    logger.warning(
                        error_msg,
                        extra={
                            "endpoint": endpoint,
                            "retry_after": response.headers.get("Retry-After")
                        }
                    )
                    raise WildberriesRateLimitError(
                        error_msg,
                        status_code=response.status
                    )

                if response.status >= 400:
                    error_detail = f"Ошибка WB API: {response.status}"
                    try:
                        error_data = await response.json()
                        error_detail = error_data.get('errorText', error_detail)
                        logger.error(
                            "Ошибочный ответ от WB API",
                            extra={
                                "status_code": response.status,
                                "error_detail": error_detail,
                                "endpoint": endpoint,
                                "error_data": error_data
                            }
                        )
                    except Exception:
                        pass

                    raise WildberriesAPIError(
                        error_detail,
                        status_code=response.status,
                        response_data=error_data if 'error_data' in locals() else None
                    )

                # Успешный ответ
                result = await response.json() if response.content_length else {}
                logger.debug(
                    "Запрос к WB API выполнен успешно",
                    extra={
                        "status_code": response.status,
                        "endpoint": endpoint
                    }
                )
                return result

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(
                "Ошибка сети при запросе к WB API",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "endpoint": endpoint
                },
                exc_info=True
            )
            raise WildberriesAPIError(
                f"Ошибка сети: {str(e)}",
                response_data={"original_error": str(e)}
            )
        except WildberriesAPIError:
            raise
        except Exception as e:
            logger.error(
                "Неожиданная ошибка при запросе к WB API",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "endpoint": endpoint
                },
                exc_info=True
            )
            raise WildberriesAPIError(
                f"Неожиданная ошибка: {str(e)}",
                response_data={"original_error": str(e)}
            )

    async def get_product(self, token: str) -> Dict[str, Any]:
        """
        Получение одного товара
        """
        body = {
            "settings": {
                "cursor": {
                    "limit": 1,
                    "offset": 0
                }
            }
        }
        return await self.make_request(
            "POST",
            "/content/v2/get/cards/list",
            token,
            json=body
        )

    async def get_products_page(
        self,
        token: str,
        limit: int = 100,
        cursor: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Получение страницы товаров с пагинацией

        Args:
            token: API токен
            limit: Количество товаров на странице (макс 100)
            cursor: Курсор для пагинации

        Returns:
            Ответ с товарами и новым курсором
        """
        body = {
            "settings": {
                "cursor": {
                    "limit": min(limit, 100),
                    **(cursor or {})
                },
                "filter": {
                    "withPhoto": -1
                }
            }
        }
        return await self.make_request(
            "POST",
            "/content/v2/get/cards/list",
            token,
            json=body
        )

    async def update_products(
        self,
        token: str,
        products: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Обновление карточек товаров (до 3000 за раз)

        Args:
            token: API токен
            products: Список карточек для обновления

        Returns:
            Ответ от API

        Raises:
            ValueError: Если передано больше 3000 товаров
        """
        if len(products) > 3000:
            raise ValueError(
                f"Невозможно обновить больше 3000 товаров одновременно. "
                f"Передано: {len(products)}. Используйте update_products_chunked()."
            )

        logger.info(
            "Обновление товаров",
            extra={"count": len(products)}
        )

        return await self.make_request(
            "POST",
            "/content/v2/cards/update",
            token,
            json=products
        )

    async def update_products_chunked(
        self,
        token: str,
        products: List[Dict[str, Any]],
        chunk_size: int = 3000,
        delay_between_chunks: float = 6.0
    ) -> List[Dict[str, Any]]:
        """
        Обновление большого количества товаров с автоматическим разбиением на чанки

        Args:
            token: API токен
            products: Список всех карточек для обновления
            chunk_size: Размер одного чанка (макс 3000)
            delay_between_chunks: Задержка между чанками в секундах (учёт rate limit)

        Returns:
            Список ответов от API для каждого чанка
        """
        chunk_size = min(chunk_size, 3000)
        results = []
        total_chunks = (len(products) - 1) // chunk_size + 1

        logger.info(
            "Начало обновления товаров с разбиением на чанки",
            extra={
                "total_products": len(products),
                "chunk_size": chunk_size,
                "total_chunks": total_chunks
            }
        )

        for i in range(0, len(products), chunk_size):
            chunk = products[i:i + chunk_size]
            chunk_number = i // chunk_size + 1

            logger.info(
                "Обновление чанка товаров",
                extra={
                    "chunk": f"{chunk_number}/{total_chunks}",
                    "chunk_size": len(chunk)
                }
            )

            try:
                result = await self.update_products(token, chunk)
                results.append(result)

                # Задержка между чанками для соблюдения rate limit
                if i + chunk_size < len(products):
                    await asyncio.sleep(delay_between_chunks)

            except WildberriesAPIError as e:
                logger.error(
                    "Ошибка при обновлении чанка товаров",
                    extra={
                        "chunk": f"{chunk_number}/{total_chunks}",
                        "error": str(e)
                    }
                )
                raise

        logger.info(
            "Обновление товаров с разбиением на чанки завершено",
            extra={"total_chunks_processed": len(results)}
        )

        return results

    async def get_all_errors_for_update(self, token: str, max_batches: int = 10000) -> List[Dict[str, Any]]:
        """
        Получает все пакеты ошибок для полноценного мониторинга.
        """
        all_error_batches = []
        cursor = {"limit": 100}
        iteration = 0

        logger.info("Начало сбора всех ошибок обновления")

        while iteration < max_batches:
            body = {
                "cursor": cursor,
                "order": {"ascending": True}
            }

            response = await self.make_request(
                "POST",
                "/content/v2/cards/error/list",
                token,
                json=body
            )

            data = response.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            all_error_batches.extend(items)

            response_cursor = data.get("cursor", {})
            if not response_cursor.get("next", False):
                break

            cursor = {
                "limit": 100,
                "updatedAt": response_cursor.get("updatedAt"),
                "batchUUID": response_cursor.get("batchUUID")
            }

            iteration += 1
            await asyncio.sleep(6)  # rate limit

        logger.info("Сбор ошибок завершен", extra={"total_batches": len(all_error_batches)})
        return all_error_batches
