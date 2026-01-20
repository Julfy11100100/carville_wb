import asyncio
import time
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import ClientTimeout, TCPConnector

from app.constants.wb_api import ResponseStatus
from app.exceptions.wb_api import WildberriesAPIError
from app.services.retry_service import RetryService
from app.services.wb_error_handler import WBErrorHandler
from app.services.wb_rate_limiter import wb_rate_limiter
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


class WildberriesAPI:
    """HTTP-клиент для работы с Wildberries API."""

    def __init__(
            self,
            base_url: str = None,
            max_retries: int = None,
            timeout: int = 30,
            max_connections: int = 100,
    ):
        self.base_url = base_url or settings.WB_CONTENT_API_URL
        self.max_retries = max_retries or settings.MAX_RETRIES

        self.timeout = ClientTimeout(
            total=timeout,
            connect=10.0,
            sock_read=timeout,
            sock_connect=10.0
        )

        self.connector = TCPConnector(
            limit=max_connections,
            limit_per_host=30,
            ttl_dns_cache=300
        )

        self.rate_limiter = wb_rate_limiter
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self):
        """Создаёт сессию, если она ещё не создана."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                connector=self.connector,
                connector_owner=False
            )
            logger.info(f"WB API сессия создана: base_url={self.base_url}")

    async def close(self):
        """Закрывает все активные соединения."""
        if self._session and not self._session.closed:
            await self._session.close()
        if self.connector:
            await self.connector.close()
        logger.info("WB API сессия закрыта")

    def _get_headers(self, token: str) -> Dict[str, str]:
        """Создаёт заголовки для WB API."""
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
            base_url_override: Optional[str] = None,
            **kwargs
    ) -> Dict[str, Any]:
        """Выполняет HTTP-запрос с rate limiting и retry-логикой."""
        await self._ensure_session()

        url = f"{base_url_override}{endpoint}" if base_url_override else f"{self.base_url}{endpoint}"
        headers = self._get_headers(token)

        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))

        async def make_single_request():
            wait_time = await self.rate_limiter.acquire()

            start_time = time.time()
            async with self._session.request(
                    method, url, headers=headers, **kwargs
            ) as response:
                duration_ms = int((time.time() - start_time) * 1000)
                logger.info(
                    f"WB API {method} {endpoint} -> {response.status} "
                    f"in {duration_ms}ms (rate_limit_wait={wait_time:.3f}s)"
                )

                if response.status >= 400:
                    error_detail = f"Ошибка WB API: {response.status}"
                    error_data = None
                    try:
                        error_data = await response.json()
                        error_detail = error_data.get('errorText', error_detail)
                    except Exception:
                        pass

                    error = WildberriesAPIError(
                        error_detail,
                        status_code=response.status,
                        response_data=error_data
                    )
                    error.status_code = response.status
                    error.is_client_error = (400 <= response.status < 500)

                    if error.is_client_error:
                        logger.error(
                            f"WB API {method} {endpoint} returned {response.status}: {error_detail}"
                        )

                    raise error

                result = await response.json() if response.status == 200 else {}
                logger.debug(f"Запрос выполнен успешно: {endpoint}, статус={response.status}")
                return result

        try:
            result = await RetryService.execute_with_retry(
                make_single_request,
                max_retries=self.max_retries,
                base_delay=1.0,
                operation_name=f"WB API {method} {endpoint}",
                retryable_statuses={408, 429, 500, 502, 503, 504}
            )
            return result

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(
                f"Ошибка сети при запросе к WB API {endpoint}: {e}",
                exc_info=True
            )
            raise WildberriesAPIError(
                f"Ошибка сети: {e}",
                response_data={"original_error": str(e)}
            )
        except WildberriesAPIError as e:
            if hasattr(e, 'is_client_error') and e.is_client_error:
                raise
            logger.error(f"WB API ошибка: {e}")
            raise
        except Exception as e:
            logger.error(
                f"Неожиданная ошибка при запросе к WB API {endpoint}: {e}",
                exc_info=True
            )
            raise WildberriesAPIError(
                f"Неожиданная ошибка: {e}",
                response_data={"original_error": str(e)}
            )

    async def get_product(self, token: str) -> Dict[str, Any]:
        """Получение одного товара с обработкой ошибок """
        return await WBErrorHandler.safe_api_call(
            operation_name="WB get_product",
            api_call=lambda: self.make_request(
                "POST",
                "/content/v2/get/cards/list",
                token,
                json={
                    "settings": {
                        "cursor": {"limit": 1},
                        "filter": {"withPhoto": -1}
                    }
                }
            ),
            success_transform=lambda r: WBErrorHandler.create_success_response(
                data=r.get("cards")
            )
        )

    async def get_products_page(
            self,
            token: str,
            limit: int = 100,
            cursor: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Получение страницы товаров с пагинацией """
        return await WBErrorHandler.safe_api_call(
            operation_name="WB get_products_page",
            api_call=lambda: self.make_request(
                "POST",
                "/content/v2/get/cards/list",
                token,
                json={
                    "settings": {
                        "cursor": {
                            "limit": min(limit, 100),
                            **(cursor or {})
                        },
                        "filter": {"withPhoto": -1}
                    }
                }
            ),
            success_transform=lambda r: WBErrorHandler.create_success_response(
                cards=r.get("cards"),
                cursor=r.get("cursor", {})
            )
        )

    async def update_products(
            self,
            token: str,
            products: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Обновление карточек товаров """
        if len(products) > 3000:
            raise ValueError(
                f"Невозможно обновить больше 3000 товаров одновременно. "
                f"Передано: {len(products)}."
            )

        logger.info(f"Обновление товаров: {len(products)} товаров")

        return await WBErrorHandler.safe_api_call(
            operation_name="WB update_products",
            api_call=lambda: self.make_request(
                "POST",
                "/content/v2/cards/update",
                token,
                json=products
            ),
            success_transform=lambda r: WBErrorHandler.create_success_response(
                data=r.get("data"),
                errors=r.get("errors", [])
            )
        )

    async def update_products_chunked(
            self,
            token: str,
            products: List[Dict[str, Any]],
            chunk_size: int = 3000,
            delay_between_chunks: float = 1.0
    ) -> Dict[str, Any]:
        """Обновление товаров с разбиением на чанки."""
        chunk_size = min(chunk_size, 3000)
        results = []
        total_chunks = (len(products) - 1) // chunk_size + 1

        logger.info(
            f"Начало обновления товаров с разбиением: "
            f"всего={len(products)}, размер_чанка={chunk_size}, чанков={total_chunks}"
        )

        for i in range(0, len(products), chunk_size):
            chunk = products[i:i + chunk_size]
            chunk_number = i // chunk_size + 1

            logger.info(f"Обновление чанка {chunk_number}/{total_chunks}: {len(chunk)} товаров")

            try:
                result = await self.update_products(token, chunk)
                results.append(result)

                if i + chunk_size < len(products) and delay_between_chunks > 0:
                    await asyncio.sleep(delay_between_chunks)

            except WildberriesAPIError as e:
                logger.error(f"Ошибка при обновлении чанка {chunk_number}/{total_chunks}: {e}")
                return WBErrorHandler.create_error_response(
                    e,
                    status_code=getattr(e, 'status_code', None),
                    response_data=getattr(e, 'response_data', None),
                    chunk_number=chunk_number,
                    processed_chunks=len([r for r in results if r.get("status") == ResponseStatus.SUCCESS])
                )

        logger.info(f"Обновление товаров завершено: обработано {len(results)} чанков")

        return WBErrorHandler.create_success_response(
            chunks_processed=len(results),
            results=results
        )

    async def create_products(
            self,
            token: str,
            cards: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Создание новых карточек товаров """
        logger.info(f"Создание товаров: {len(cards)} карточек")

        return await WBErrorHandler.safe_api_call(
            operation_name="WB create_products",
            api_call=lambda: self.make_request(
                "POST",
                "/content/v2/cards/upload",
                token,
                json=cards
            ),
            success_transform=lambda r: WBErrorHandler.create_success_response(
                data=r.get("data"),
                errors=r.get("errors", [])
            )
        )

    async def get_all_errors_for_update(self, token: str, max_batches: int = 10000) -> Dict[str, Any]:
        """Получает все пакеты ошибок для обновления товаров """
        all_error_batches = []
        cursor = {"limit": 100}
        iteration = 0

        logger.info("Начало сбора всех ошибок обновления")

        while iteration < max_batches:
            body = {
                "cursor": cursor,
                "order": {"ascending": True}
            }

            result = await WBErrorHandler.safe_api_call(
                operation_name=f"WB get_all_errors_for_update (iteration {iteration})",
                api_call=lambda: self.make_request(
                    "POST",
                    "/content/v2/cards/error/list",
                    token,
                    json=body
                )
            )

            if result.get("status") == ResponseStatus.ERROR:
                return result

            data = result.get("data", {}) if "data" in result else result.get("data", {})
            items = data.get("items", []) if isinstance(data, dict) else []

            if not items:
                break

            all_error_batches.extend(items)

            response_cursor = data.get("cursor", {}) if isinstance(data, dict) else {}
            if not response_cursor.get("next", False):
                break

            cursor = {
                "limit": 100,
                "updatedAt": response_cursor.get("updatedAt"),
                "batchUUID": response_cursor.get("batchUUID")
            }

            iteration += 1
            await asyncio.sleep(0.5)

        logger.info(
            f"Сбор ошибок завершен: найдено {len(all_error_batches)} пакетов ошибок "
            f"за {iteration + 1} итераций"
        )

        return WBErrorHandler.create_success_response(
            error_batches=all_error_batches,
            total_batches=len(all_error_batches),
            iterations=iteration + 1
        )

    async def get_all_errors_for_create(
            self,
            token: str,
            max_batches: int = 10000
    ) -> Dict[str, Any]:
        """Получает все пакеты ошибок создания товаров"""
        all_error_batches = []
        cursor = {"limit": 100}
        iteration = 0

        logger.info("Начало сбора всех ошибок создания товаров")

        while iteration < max_batches:
            body = {
                "cursor": cursor,
                "order": {"ascending": True}
            }

            result = await WBErrorHandler.safe_api_call(
                operation_name=f"WB get_all_errors_for_create (iteration {iteration})",
                api_call=lambda: self.make_request(
                    "POST",
                    "/content/v2/cards/error/list",
                    token,
                    json=body
                )
            )

            if result.get("status") == ResponseStatus.ERROR:
                return result

            data = result.get("data", {}) if "data" in result else result.get("data", {})
            items = data.get("items", []) if isinstance(data, dict) else []

            if not items:
                logger.debug(f"Итерация {iteration}: нет ошибок")
                break

            all_error_batches.extend(items)

            logger.debug(
                f"Итерация {iteration}: получено {len(items)} пакетов, "
                f"всего={len(all_error_batches)}"
            )

            response_cursor = data.get("cursor", {}) if isinstance(data, dict) else {}
            if not response_cursor.get("next", False):
                logger.debug("Флаг next=false, конец пагинации")
                break

            cursor = {
                "limit": 100,
                "updatedAt": response_cursor.get("updatedAt"),
                "batchUUID": response_cursor.get("batchUUID")
            }

            iteration += 1
            await asyncio.sleep(0.5)

        logger.info(
            f"Сбор ошибок завершен: найдено {len(all_error_batches)} пакетов ошибок "
            f"за {iteration + 1} итераций"
        )

        return WBErrorHandler.create_success_response(
            error_batches=all_error_batches,
            total_batches=len(all_error_batches),
            iterations=iteration + 1
        )

    async def save_product_images(
            self,
            token: str,
            nm_id: int,
            upload_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Загружает/обновляет картинки товара через API v3/media/save.

        Args:
            token: API токен
            nm_id: Номенклатура ID товара
            upload_data: {"nmId": 123, "data": ["url1", "url2"]}

        Returns:
            {status: "success"|"error", data: {...}, errors: [...]}
        """
        logger.info(
            f"Загрузка картинок: nm_id={nm_id}, "
            f"картинок={len(upload_data.get('data', []))}"
        )

        return await WBErrorHandler.safe_api_call(
            operation_name=f"WB save_product_images (nmId={nm_id})",
            api_call=lambda: self.make_request(
                "POST",
                "/content/v3/media/save",
                token,
                json=upload_data
            ),
            success_transform=lambda r: WBErrorHandler.create_success_response(
                data=r.get("data"),
                errors=r.get("errors", [])
            )
        )

    async def upload_prices(
            self,
            token: str,
            prices_data: Dict[str, List[Dict[str, int | None]]]
    ) -> Dict[str, Any]:
        """
        Загружает цены товаров через Prices API.

        Args:
            token: API токен
            prices_data: [{"nmId": 123, "price": 1999}, ...]

        Returns:
            {status: "success"|"error", data: {"uploadId": ...}, errors: [...]}
        """

        logger.info(f"Загрузка цен: {len(prices_data['data'])} товаров")

        prices_api_url = settings.WB_PRICES_API_URL

        return await WBErrorHandler.safe_api_call(
            operation_name="WB upload_prices",
            api_call=lambda: self.make_request(
                "POST",
                "/api/v2/upload/task",
                token,
                json=prices_data,
                base_url_override=prices_api_url
            ),
            success_transform=lambda r: WBErrorHandler.create_success_response(
                data=r.get("data"),
                errors=r.get("errors", [])
            )
        )

    async def get_create_limits(
            self,
            token: str
    ) -> Dict[str, Any]:
        """Получение лимитов на создание товаров"""
        return await WBErrorHandler.safe_api_call(
            operation_name="WB get_create_limits",
            api_call=lambda: self.make_request(
                "GET",
                "/content/v2/cards/limits",
                token
            ),
            success_transform=lambda r: WBErrorHandler.create_success_response(
                data=r.get("data")
            )
        )
