import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from aiohttp_retry import RetryClient, ExponentialRetry

from app.core.product_file_service import ProductFileService
from app.core.task_manager import TaskManager
from app.exceptions.wb_api import WildberriesRateLimitError, WildberriesAPIError
from app.schemas.task import TaskStatus, TaskInfo
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


class WildberriesClient:
    """
    Асинхронный HTTP-клиент для работы с Wildberries API.

    Использует connection pooling и автоматический retry для надёжности.
    Должен использоваться как async context manager для правильного управления ресурсами.

    Example:
        async with WildberriesClient() as client:
            product = await client.get_product(token)
    """

    def __init__(
            self,
            task_manager: Optional[TaskManager] = None,
            base_url: str = None,
            max_retries: int = None,
            timeout: int = 30,
            max_connections: int = 100,
    ):
        """
        Args:
            task_manager: Менеджер задач для фоновых операций
            base_url: Базовый URL для WB API
            max_retries: Максимальное количество повторных попыток
            timeout: Таймаут для HTTP-запросов в секундах
            max_connections: Максимальное количество одновременных соединений
        """
        self.task_manager = task_manager
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
                "WB API session created",
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
        logger.info("WB API session closed")

    def _get_headers(self, token: str) -> Dict[str, str]:
        """Создаёт заголовки для WB API"""
        return {
            "Authorization": token,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    async def _make_request(
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
            "Making WB API request",
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
                    error_msg = "Rate limit exceeded"
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
                    error_detail = f"WB API error: {response.status}"
                    try:
                        error_data = await response.json()
                        error_detail = error_data.get('errorText', error_detail)

                        logger.error(
                            "WB API error response",
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
                    "WB API request successful",
                    extra={
                        "status_code": response.status,
                        "endpoint": endpoint
                    }
                )

                return result

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(
                "Network error during WB API request",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "endpoint": endpoint
                },
                exc_info=True
            )
            raise WildberriesAPIError(
                f"Network error: {str(e)}",
                response_data={"original_error": str(e)}
            )
        except WildberriesAPIError:
            raise
        except Exception as e:
            logger.error(
                "Unexpected error during WB API request",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "endpoint": endpoint
                },
                exc_info=True
            )
            raise WildberriesAPIError(
                f"Unexpected error: {str(e)}",
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
        return await self._make_request(
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
        return await self._make_request(
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
                f"Cannot update more than 3000 products at once. "
                f"Got {len(products)}. Use update_products_chunked() instead."
            )

        logger.info(
            "Updating products",
            extra={"count": len(products)}
        )

        return await self._make_request(
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
            "Starting chunked products update",
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
                "Updating products chunk",
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
                    "Failed to update products chunk",
                    extra={
                        "chunk": f"{chunk_number}/{total_chunks}",
                        "error": str(e)
                    }
                )
                raise

        logger.info(
            "Chunked products update completed",
            extra={"total_chunks_processed": len(results)}
        )

        return results

    async def get_cards_errors(
            self,
            token: str,
            limit: int = 100,
            updated_at: Optional[str] = None,
            batch_uuid: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Получение списка карточек с ошибками после обновления
        """
        body = {
            "cursor": {
                "limit": min(limit, 1000)
            },
            "order": {
                "ascending": True
            }
        }

        if updated_at and batch_uuid:
            body["cursor"]["updatedAt"] = updated_at
            body["cursor"]["batchUUID"] = batch_uuid

        return await self._make_request(
            "POST",
            "/content/v2/cards/error/list",
            token,
            json=body
        )

    async def get_product_full_data(
            self,
            token: str,
            nm_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Получение полных данных карточки по nmID
        """
        body = {
            "settings": {
                "cursor": {
                    "limit": 1
                },
                "filter": {
                    "withPhoto": -1,
                    "textSearch": str(nm_id)
                }
            }
        }

        response = await self._make_request(
            "POST",
            "/content/v2/get/cards/list",
            token,
            json=body
        )

        cards = response.get("cards", [])
        return cards[0] if cards else None

    async def collect_products_background(self, token: str, task_info: TaskInfo):
        """
        Фоновая задача для получения всех товаров
        """
        try:
            task_info.status = TaskStatus.RUNNING
            await self.task_manager.save_task(task_info)

            all_products = []
            category_ids: Set[int] = set()
            cursor = {}

            logger.info(
                "Starting products collection",
                extra={"task_id": task_info.task_id}
            )

            while True:
                try:
                    response = await self.get_products_page(token, limit=100, cursor=cursor)
                    cards = response.get("cards", [])

                    if not cards:
                        break

                    # Извлекаем ID категорий
                    category_ids.update(
                        card.get("subjectID")
                        for card in cards
                        if card.get("subjectID")
                    )
                    all_products.extend(cards)

                    # Обновляем курсор
                    cursor = response.get("cursor", {})
                    if not cursor or len(cards) < cursor.get("limit", 100):
                        break

                    # Обновляем прогресс
                    task_info.total_items = len(all_products)
                    await self.task_manager.save_task(task_info)

                    logger.debug(
                        "Products collection progress",
                        extra={
                            "task_id": task_info.task_id,
                            "collected": len(all_products),
                            "batch_size": len(cards)
                        }
                    )

                    await asyncio.sleep(0.1)

                except WildberriesRateLimitError:
                    logger.warning(
                        "Rate limit hit during collection, waiting",
                        extra={"task_id": task_info.task_id}
                    )
                    await asyncio.sleep(60)
                    continue

            # Сохраняем результаты
            file_path = await ProductFileService.save_products_to_file(
                task_info.task_id,
                all_products
            )

            task_info.status = TaskStatus.COMPLETED
            task_info.completed_at = datetime.now()
            task_info.total_items = len(all_products)
            task_info.category_ids = list(category_ids)
            task_info.file_path = file_path

            logger.info(
                "Products collection completed",
                extra={
                    "task_id": task_info.task_id,
                    "total_products": len(all_products),
                    "file_path": file_path,
                    "categories_count": len(category_ids)
                }
            )

        except Exception as e:
            task_info.status = TaskStatus.FAILED
            task_info.completed_at = datetime.now()
            task_info.error = str(e)

            logger.error(
                "Products collection failed",
                extra={
                    "task_id": task_info.task_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )

        finally:
            await self.task_manager.save_task(task_info)

    async def update_products_background(
            self,
            token: str,
            products: List[Dict[str, Any]],
            task_info: TaskInfo
    ):
        """
        Фоновая задача для обновления карточек товаров
        """
        try:
            task_info.status = TaskStatus.RUNNING
            task_info.total_items = len(products)
            await self.task_manager.save_task(task_info)

            logger.info(
                "Starting products update",
                extra={
                    "task_id": task_info.task_id,
                    "total_products": len(products)
                }
            )

            # Обновляем с автоматическим разбиением на чанки
            await self.update_products_chunked(token, products, delay_between_chunks=6)

            # Обновляем прогресс
            task_info.processed_items = len(products)
            await self.task_manager.save_task(task_info)

            # Ждём обработки на стороне WB
            await asyncio.sleep(10)

            # Проверяем ошибки
            errors_response = await self.get_cards_errors(token, limit=100)
            errors_list = errors_response.get("data", {}).get("items", [])

            if errors_list:
                logger.warning(
                    "Products update completed with errors",
                    extra={
                        "task_id": task_info.task_id,
                        "error_batches": len(errors_list)
                    }
                )

            # Завершаем задачу
            task_info.status = TaskStatus.COMPLETED
            task_info.completed_at = datetime.now()
            task_info.processed_items = len(products)

            if errors_list:
                task_info.error = f"Updated with errors. Found {len(errors_list)} error batches"
                task_info.metadata = {"errors": errors_list}

            logger.info(
                "Products update completed",
                extra={
                    "task_id": task_info.task_id,
                    "updated_products": len(products),
                    "error_batches": len(errors_list)
                }
            )

        except Exception as e:
            task_info.status = TaskStatus.FAILED
            task_info.completed_at = datetime.now()
            task_info.error = str(e)

            logger.error(
                "Products update failed",
                extra={
                    "task_id": task_info.task_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )

        finally:
            await self.task_manager.save_task(task_info)
