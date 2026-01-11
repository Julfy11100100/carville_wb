import asyncio
from typing import Dict, Any, List, Optional

from elasticsearch import AsyncElasticsearch

from app.services.base_reconnectable import ReconnectableService
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


class ElasticsearchService(ReconnectableService):
    """Сервис для работы с Elasticsearch"""

    def __init__(self):
        super().__init__("Elasticsearch")
        self.client: Optional[AsyncElasticsearch] = None
        self._hosts = [settings.ELASTICSEARCH_HOST]
        self._timeout = 120
        self._client_loop: Optional[asyncio.AbstractEventLoop] = None
        self._basic_auth = (
            settings.ELASTICSEARCH_USERNAME,
            settings.ELASTICSEARCH_PASSWORD,
        )

    def record_failure(self):
        self._record_failure()

    async def _connect(self):
        """Подключение к Elasticsearch"""
        if self.client:
            await self._disconnect()

        self.client = AsyncElasticsearch(
            hosts=self._hosts,
            timeout=self._timeout,
            max_retries=2,
            retry_on_timeout=True,
            basic_auth=self._basic_auth,
        )
        # Запоминаем event loop, в котором создан клиент
        self._client_loop = asyncio.get_event_loop()
        logger.info("Клиент Elasticsearch успешно подключен")

    async def _disconnect(self):
        """Отключение от Elasticsearch"""
        if self.client:
            try:
                await self.client.close()
            except Exception as e:
                logger.warning(f"Ошибка при закрытии клиента Elasticsearch: {str(e)}")
            self.client = None
            self._client_loop = None

    async def _health_check(self) -> bool:
        """Проверка здоровья Elasticsearch"""
        if not self.client:
            return False

        # Проверяем event loop
        if not self._is_client_loop_valid():
            return False

        try:
            await self.client.ping()
            return True
        except Exception:
            return False

    def _is_client_loop_valid(self) -> bool:
        """Проверить, что event loop клиента ещё актуален"""
        if not self._client_loop:
            return False

        try:
            current_loop = asyncio.get_event_loop()
        except RuntimeError:
            return False

        # Проверяем, что это тот же loop и он не закрыт
        if self._client_loop != current_loop:
            logger.warning("Event loop клиента Elasticsearch изменился, требуется переподключение")
            return False

        if self._client_loop.is_closed():
            logger.warning("Event loop клиента Elasticsearch закрыт, требуется переподключение")
            return False

        return True

    async def _ensure_valid_client(self):
        """Убедиться, что клиент валиден для текущего event loop"""
        if not self.client or not self._is_client_loop_valid():
            logger.info("Пересоздание клиента Elasticsearch для текущего event loop")
            self._is_connected = False
            await self.ensure_connection()

    @staticmethod
    def _get_index_name(token: str) -> str:
        """Получить имя индекса для клиента"""
        return f"wb_products_{token}"

    async def create_index(self, token: str) -> Dict[str, Any]:
        """Создать индекс для клиента"""
        try:
            await self._ensure_valid_client()
        except Exception as e:
            logger.error(f"Не удалось подключиться к Elasticsearch: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": f"Ошибка подключения: {str(e)}",
                "message": f"Не удалось создать индекс для клиента {token}"
            }

        try:
            index_name = self._get_index_name(token)

            # Проверяем существует ли индекс
            exists = await self.client.indices.exists(index=index_name)
            if exists:
                await self.delete_client_data(token)

            # Создаем индекс с оптимизированными настройками для WildBerries
            await self.client.indices.create(
                index=index_name,
                body={
                    "settings": {
                        "index": {
                            "max_result_window": 100000,  # Увеличиваем лимит до 100,000
                            "number_of_shards": 1,  # Один шард для эффективности
                            "number_of_replicas": 0,  # Нет реплик для экономии памяти
                            "refresh_interval": "60s"  # Реже обновляем индекс для скорости
                        }
                    },
                    "mappings": {
                        "dynamic": False,  # Отключаем динамическую индексацию
                        "properties": {
                            # Идентификаторы WB
                            "nmID": {"type": "long"},  # Артикул WB (основной идентификатор)
                            "vendorCode": {"type": "keyword"},  # Артикул продавца

                            # Категоризация
                            "subjectID": {"type": "long"},  # ID предмета/категории
                            "subjectName": {"type": "text", "analyzer": "standard"},  # Название предмета

                            # Основные данные товара
                            "brand": {"type": "keyword"},  # Бренд
                            "title": {"type": "text", "analyzer": "standard"},  # Название товара
                            "description": {"type": "text", "analyzer": "standard"},  # Описание

                            # Баркоды (массив строк) добавляем вручную.
                            "barcode": {"type": "keyword"}  # Хранит массив баркодов
                        }
                    }
                }
            )

            return {
                "status": "success",
                "message": f"Индекс {index_name} успешно создан"
            }

        except Exception as e:
            logger.error(f"Не удалось создать индекс для клиента {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": str(e),
                "message": f"Не удалось создать индекс для клиента {token}"
            }

    async def index_products(self, token: str, products: List[Dict[str, Any]]) -> bool:
        """Индексировать товары в Elasticsearch (автоматически вызывается при синхронизации)"""
        try:
            await self._ensure_valid_client()
        except Exception as e:
            logger.error(f"Не удалось подключиться к Elasticsearch для клиента {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return False

        try:
            index_name = self._get_index_name(token)

            # Подготавливаем данные для индексации
            actions = []
            for product in products:
                # Добавляем action метаданные
                action_meta = {
                    "index": {
                        "_index": index_name,
                        "_id": product.get("nmID")  # Используем nmId как ID документа
                    }
                }
                # Добавляем баркоды из size, если есть
                barcodes = []
                for size in product.get("sizes", []):
                    barcodes.extend(size.get("skus", []))
                if barcodes:
                    product["barcode"] = barcodes

                actions.append(action_meta)
                # Добавляем данные документа
                actions.append(product)

            # Индексируем батчами по 1000 документов
            batch_size = 2000  # 1000 пар (метаданные + документ)
            for i in range(0, len(actions), batch_size):
                batch = actions[i:i + batch_size]
                await self.client.bulk(body=batch)
                logger.info(
                    f"Проиндексирован батч {i // batch_size + 1} для клиента {token}: {len(batch) // 2} товаров")

            logger.info(f"Успешно проиндексировано {len(products)} товаров для клиента {token}")

            # Принудительно обновляем индекс для немедленной доступности данных
            await self.client.indices.refresh(index=index_name)
            logger.info(f"Индекс {index_name} успешно обновлен")

            return True

        except Exception as e:
            logger.error(f"Не удалось проиндексировать товары для клиента {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return False

    async def delete_client_data(self, token: str) -> bool:
        """Удалить все данные клиента из Elasticsearch (вызывается при новой синхронизации)"""
        try:
            await self._ensure_valid_client()
        except Exception as e:
            logger.warning(f"Не удалось подключиться к Elasticsearch для клиента {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return False

        try:
            index_name = self._get_index_name(token)
            exists = await self.client.indices.exists(index=index_name)

            if exists:
                await self.client.indices.delete(index=index_name)
                logger.info(f"Удален индекс Elasticsearch для клиента {token}")

            return True

        except Exception as e:
            logger.error(f"Не удалось удалить данные для клиента {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return False

    async def search_products(self, token: str, filters: Dict[str, Any] = None,
                              limit: int = 100, offset: int = 0, fields: List[str] = None) -> Dict[str, Any]:
        """Поиск товаров с фильтрацией"""
        try:
            await self._ensure_valid_client()
        except Exception as e:
            logger.error(f"Не удалось подключиться к Elasticsearch: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": f"Ошибка подключения: {str(e)}",
                "products": [],
                "total": 0
            }

        try:
            index_name = self._get_index_name(token)

            # Проверяем существует ли индекс
            exists = await self.client.indices.exists(index=index_name)
            if not exists:
                return {
                    "status": "error",
                    "error": f"Данные для клиента {token} не найдены. Пожалуйста, сначала запустите синхронизацию данных.",
                    "products": [],
                    "total": 0
                }

            # Строим запрос с фильтрами
            query = self._build_search_query(filters or {})

            import time
            es_start = time.time()

            # Настройки запроса
            search_body = {
                "query": query,
                "size": limit,
                "from": offset
            }

            # Добавляем фильтр полей если указан
            if fields:
                search_body["_source"] = fields

            logger.info(f"Ищем товары для индекса {index_name} по запросу:{search_body}")
            response = await self.client.search(
                index=index_name,
                body=search_body
            )
            es_time = time.time() - es_start

            # Оптимизированное извлечение данных
            products = []
            hits = response["hits"]["hits"]
            for hit in hits:
                products.append(hit["_source"])

            return {
                "status": "success",
                "products": products,
                "total": response["hits"]["total"]["value"],
                "limit": limit,
                "offset": offset,
                "elasticsearch_time": f"{es_time:.4f}"
            }

        except Exception as e:
            logger.error(f"Не удалось найти товары для клиента {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": str(e),
                "products": [],
                "total": 0
            }

    async def get_product_by_nm_id(self, token: str, nm_id: int) -> Dict[str, Any]:
        """Получить товар по nmId (артикулу WB) со всеми полями"""
        try:
            await self._ensure_valid_client()
        except Exception as e:
            logger.error(f"Не удалось подключиться к Elasticsearch: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": f"Ошибка подключения: {str(e)}",
                "product": None
            }

        try:
            index_name = self._get_index_name(token)

            # Проверяем существует ли индекс
            exists = await self.client.indices.exists(index=index_name)
            if not exists:
                return {
                    "status": "error",
                    "error": f"Данные для клиента {token} не найдены. Пожалуйста, сначала запустите синхронизацию данных.",
                    "product": None
                }

            # Получаем документ по ID (nmId)
            response = await self.client.get(
                index=index_name,
                id=str(nm_id)
            )

            return {
                "status": "success",
                "product": response["_source"]
            }

        except Exception as e:
            if "not_found" in str(e).lower():
                return {
                    "status": "error",
                    "error": f"Товар с nmId {nm_id} не найден",
                    "product": None
                }
            logger.error(f"Не удалось получить товар с nmId {nm_id} для клиента {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": str(e),
                "product": None
            }

    def _build_search_query(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Построить Elasticsearch запрос из фильтров"""
        must_clauses = []

        for field, value in filters.items():
            if value is not None and value != "":
                if isinstance(value, list):
                    # Для списков используем terms
                    must_clauses.append({"terms": {field: value}})
                else:
                    # Для одиночных значений используем term
                    must_clauses.append({"term": {field: value}})

        return {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}

    async def find_products_with_different_value(
            self,
            token: str,
            field: str,
            expected_values: Dict[int, Any]
    ) -> List[Dict[str, Any]]:
        """
        Найти полные карточки товаров по nm_id, у которых значение поля отличается от ожидаемого.

        Args:
            token: Хешированный токен клиента
            field: Поле для проверки
            expected_values: Словарь {nm_id: expected_value}

        Returns:
            Список полных карточек товаров с отличающимся значением поля
        """
        try:
            await self._ensure_valid_client()
        except Exception as e:
            logger.error(f"Не удалось подключиться к Elasticsearch: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return []

        try:
            index_name = self._get_index_name(token)

            # Проверяем существует ли индекс
            exists = await self.client.indices.exists(index=index_name)
            if not exists:
                logger.warning(f"Индекс {index_name} не существует")
                return []

            nm_ids = list(expected_values.keys())

            if not nm_ids:
                return []

            # Строим запрос для поиска товаров по nmID
            query = {
                "terms": {
                    "nmID": nm_ids
                }
            }

            # Выполняем поиск
            response = await self.client.search(
                index=index_name,
                body={
                    "query": query,
                    "size": len(nm_ids)
                }
            )

            # Извлекаем результаты и фильтруем только товары с отличающимся значением
            products = []
            for hit in response["hits"]["hits"]:
                source = hit["_source"]
                nm_id = source.get("nmID")
                current_value = source.get(field)
                expected_value = expected_values.get(nm_id)

                # Сравниваем текущее значение с ожидаемым
                if current_value != expected_value:
                    products.append(source)  # Добавляем полную карточку

                    logger.debug(
                        f"Найдено отличие для nmID {nm_id}: "
                        f"поле '{field}' = {current_value} (ожидалось {expected_value})"
                    )

            logger.info(
                f"Найдено {len(products)} товаров с отличающимся значением поля '{field}' "
                f"из {len(nm_ids)} проверенных для клиента {token}"
            )

            return products

        except Exception as e:
            logger.error(f"Не удалось найти товары для клиента {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return []

    async def bulk_update_products(
            self,
            token: str,
            field: str,
            updates: Dict[int, Any]  # {nm_id: new_value, ...}
    ) -> Dict[str, Any]:
        """
        Массовое обновление значений поля для списка товаров

        Args:
            token: Токен клиента
            field: Поле для обновления
            updates: Словарь {nm_id: new_value}

        Returns:
            Dict с результатами обновления
        """
        try:
            await self._ensure_valid_client()
        except Exception as e:
            logger.error(f"Не удалось подключиться к Elasticsearch: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": f"Ошибка подключения: {str(e)}",
                "updated": 0,
                "failed": 0
            }

        try:
            from elasticsearch.helpers import async_streaming_bulk

            index_name = self._get_index_name(token)

            # Проверяем существует ли индекс
            exists = await self.client.indices.exists(index=index_name)
            if not exists:
                return {
                    "status": "error",
                    "error": f"Данные для клиента {token} не найдены. Пожалуйста, сначала запустите синхронизацию данных.",
                    "updated": 0,
                    "failed": 0
                }

            # Подготавливаем действия для bulk update
            async def generate_actions():
                for nm_id, value in updates.items():
                    yield {
                        "_op_type": "update",
                        "_index": index_name,
                        "_id": str(nm_id),
                        "doc": {
                            field: value
                        }
                    }

            # Выполняем bulk update
            success_count = 0
            failed_count = 0
            errors = []

            async for ok, result in async_streaming_bulk(
                    client=self.client,
                    actions=generate_actions(),
                    chunk_size=500,
                    raise_on_error=False
            ):
                if ok:
                    success_count += 1
                else:
                    failed_count += 1
                    action, result_data = result.popitem()
                    error_info = {
                        "nm_id": result_data.get("_id"),
                        "error": result_data.get("error", {}).get("reason", "Unknown error")
                    }
                    errors.append(error_info)
                    logger.warning(f"Не удалось обновить товар {error_info['nm_id']}: {error_info['error']}")

            logger.info(
                f"Обновление завершено для клиента {token}: "
                f"успешно={success_count}, ошибок={failed_count}"
            )

            # Принудительно обновляем индекс для немедленной доступности данных
            await self.client.indices.refresh(index=index_name)

            return {
                "status": "success" if failed_count == 0 else "partial_success",
                "updated": success_count,
                "failed": failed_count,
                "errors": errors if errors else None,
                "field": field,
                "total_requested": len(updates)
            }

        except Exception as e:
            logger.error(f"Не удалось обновить товары для клиента {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": str(e),
                "updated": 0,
                "failed": len(updates)
            }

    # ЛОГИКА ДЛЯ ИНДЕКСАЦИИ ОТЗЫВОВ
    # =========================================================================

    @staticmethod
    def _get_review_index_name(token: str) -> str:
        """Получить имя индекса для отзывов клиента"""
        return f"wb_reviews_{token}"

    async def create_review_index(self, token: str) -> Dict[str, Any]:
        """Создать индекс для отзывов с оптимальной маппингом"""
        try:
            await self._ensure_valid_client()
        except Exception as e:
            logger.error(f"Не удалось подключиться к Elasticsearch: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": f"Ошибка подключения: {str(e)}",
                "message": f"Не удалось создать индекс для отзывов клиента {token}"
            }

        try:
            index_name = self._get_review_index_name(token)

            # Проверяем существует ли индекс
            exists = await self.client.indices.exists(index=index_name)
            if exists:
                return {
                    "status": "success",
                    "message": f"Индекс {index_name} уже есть"
                }

            # Создаём индекс с маппингом для отзывов
            await self.client.indices.create(
                index=index_name,
                body={
                    "settings": {
                        "index": {
                            "max_result_window": 100000,
                            "number_of_shards": 1,
                            "number_of_replicas": 0,
                            "refresh_interval": "60s"
                        }
                    },

                    "mappings": {
                        "dynamic": False,
                        "properties": {
                            "id_review": {"type": "keyword"},
                            "sku": {"type": "integer"},
                            "text": {"type": "text", "analyzer": "standard"},
                            "published_at": {"type": "date"},
                            "rating": {"type": "byte"},
                            "comments_amount": {"type": "byte"},
                            "photos_amount": {"type": "integer"},
                            "videos_amount": {"type": "integer"},
                            "is_rating_participant": {"type": "byte"},
                            "offer_id": {"type": "keyword"},  # supplierArticle он же vendor_code
                            "product_name": {"type": "text", "analyzer": "standard"},
                            "barcodes": {"type": "keyword"},
                        }
                    }
                }
            )

            logger.info(f"Индекс отзывов {index_name} успешно создан")
            return {
                "status": "success",
                "message": f"Индекс {index_name} успешно создан"
            }

        except Exception as e:
            logger.error(f"Не удалось создать индекс отзывов для {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": str(e),
                "message": f"Не удалось создать индекс отзывов для {token}"
            }

    async def index_reviews(
            self,
            token: str,
            reviews: List[Dict[str, Any]]
    ) -> bool:
        """Индексировать отзывы в Elasticsearch"""
        try:
            await self._ensure_valid_client()
        except Exception as e:
            logger.error(f"Не удалось подключиться к Elasticsearch для {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return False

        try:
            index_name = self._get_review_index_name(token)

            # Подготавливаем bulk действия
            actions = []
            for review in reviews:
                # Action metadata
                action_meta = {
                    "index": {
                        "_index": index_name,
                        "_id": review.get("id")
                    }
                }
                actions.append(action_meta)
                actions.append(review)

            # Индексируем батчами
            batch_size = 2000
            for i in range(0, len(actions), batch_size):
                batch = actions[i:i + batch_size]
                response = await self.client.bulk(body=batch)

                if response.get("errors"):
                    error_count = len([e for e in response.get('items', [])
                                       if e.get('index', {}).get('error')])
                    logger.warning(
                        f"Ошибки при индексации батча отзывов для {token}: "
                        f"{error_count} документов"
                    )

                logger.debug(
                    f"Проиндексирован батч отзывов {i // batch_size + 1} "
                    f"для {token}: {len(batch) // 2} отзывов"
                )

            # Обновляем индекс для немедленной доступности
            await self.client.indices.refresh(index=index_name)

            logger.info(f"Успешно проиндексировано {len(reviews)} отзывов для {token}")
            return True

        except Exception as e:
            logger.error(f"Не удалось проиндексировать отзывы для {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return False

    async def get_last_indexed_review_date(self, token: str) -> Optional[str]:
        """Получить дату последнего индексированного отзыва"""

        try:
            await self._ensure_valid_client()
        except Exception as e:
            logger.error(f"Не удалось подключиться к Elasticsearch для {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return None

        try:
            index_name = self._get_review_index_name(token)

            response = await self.client.search(
                index=index_name,
                body={
                    "query": {"match_all": {}},
                    "size": 1,
                    "sort": [{"published_at": {"order": "desc"}}]
                }
            )

            hits = response.get("hits", {}).get("hits", [])
            if hits:
                last_date = hits[0]["_source"].get("published_at")
                logger.debug(f"Последняя дата отзыва для {token}: {last_date}")
                return last_date

            logger.info(f"Индекс отзывов для {token} пуст")
            return None

        except Exception as e:
            logger.warning(f"Не удалось получить последнюю дату для {token}: {str(e)}")
            return None

    async def search_reviews(
            self,
            token: str,
            filters: Dict[str, Any] = None,
            limit: int = 100,
            offset: int = 0
    ) -> Dict[str, Any]:
        """Поиск отзывов с фильтрацией"""
        try:
            await self._ensure_valid_client()
        except Exception as e:
            logger.error(f"Не удалось подключиться к Elasticsearch: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": f"Ошибка подключения: {str(e)}",
                "reviews": [],
                "total": 0
            }

        try:
            index_name = self._get_review_index_name(token)

            # Проверяем существует ли индекс
            exists = await self.client.indices.exists(index=index_name)
            if not exists:
                return {
                    "status": "error",
                    "error": f"Данные для клиента {token} не найдены",
                    "reviews": [],
                    "total": 0
                }

            # Строим запрос с фильтрами
            query = self._build_search_query(filters or {})

            search_body = {
                "query": query,
                "size": limit,
                "from": offset
            }

            logger.info(f"Ищем отзывы для индекса {index_name}")
            response = await self.client.search(
                index=index_name,
                body=search_body
            )

            # Извлекаем результаты
            reviews = [hit["_source"] for hit in response["hits"]["hits"]]

            return {
                "status": "success",
                "reviews": reviews,
                "total": response["hits"]["total"]["value"],
                "limit": limit,
                "offset": offset
            }

        except Exception as e:
            logger.error(f"Не удалось найти отзывы для {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": str(e),
                "reviews": [],
                "total": 0
            }

    async def search_reviews_by_value(
            self,
            token: str,
            vendor_code: Optional[str] = None,
            barcode: Optional[int] = None,
            size: int = 10000
    ) -> Dict[str, Any]:
        """Получить отзывы по vendor_code (offer_id) или barcode (barcodes)"""

        try:
            await self._ensure_valid_client()
        except Exception as e:
            logger.error(f"Не удалось подключиться к Elasticsearch: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": f"Ошибка подключения: {str(e)}",
                "reviews": [],
                "total": 0
            }

        if not vendor_code and not barcode:
            return {
                "reviews": [],
                "total": 0,
                "avg_rating": None,
                "ratings": {},
                "error": "Необходимо указать vendor_code или barcode"
            }

        try:
            index_name = self._get_review_index_name(token)

            # Строим query
            must_clauses = []
            if vendor_code:
                must_clauses.append({"term": {"offer_id": vendor_code}})
            if barcode:
                must_clauses.append({"term": {"barcodes": barcode}})

            query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}

            response = await self.client.search(
                index=index_name,
                body={
                    "query": query,
                    "size": min(size, 10000),
                    "sort": [{"published_at": {"order": "desc"}}]
                }
            )

            hits = response.get("hits", {})
            docs = [hit["_source"] for hit in hits.get("hits", [])]

            # Считаем статистику рейтингов
            ratings = {}
            ratings_sum = 0
            ratings_count = 0

            for doc in docs:
                val = doc.get("product_valuation")
                if val and isinstance(val, (int, float)):
                    ratings[str(int(val))] = ratings.get(str(int(val)), 0) + 1
                    ratings_sum += val
                    ratings_count += 1

            avg_rating = (ratings_sum / ratings_count) if ratings_count > 0 else None

            total = hits.get("total", {}).get("value", 0)

            logger.info(
                f"Получены отзывы для {token}: "
                f"vendor_code={vendor_code}, barcode={barcode}, найдено={total}"
            )

            return {
                "reviews": docs,
                "total": total,
                "avg_rating": round(avg_rating, 2) if avg_rating else None,
                "ratings": ratings
            }

        except Exception as e:
            logger.error(f"Ошибка при поиске отзывов для {token}: {str(e)}")
            return {
                "reviews": [],
                "total": 0,
                "avg_rating": None,
                "ratings": {},
                "error": str(e)
            }

    async def search_reviews_by_period(
            self,
            token: str,
            date_from: Optional[str] = None,
            date_to: Optional[str] = None,
            size: int = 1000
    ) -> Dict[str, Any]:
        """Получить отзывы за период"""
        try:
            index_name = self._get_review_index_name(token)

            # Строим query с фильтром по датам
            must_clauses = []
            if date_from or date_to:
                range_filter = {}
                if date_from:
                    range_filter["gte"] = date_from
                if date_to:
                    range_filter["lte"] = date_to
                must_clauses.append({"range": {"published_at": range_filter}})

            query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}

            # Сортировка для search_after пагинации
            sort = [
                {"published_at": {"order": "desc"}},
                {"id": {"order": "desc"}}
            ]

            body = {
                "query": query,
                "size": min(size, 1000),
                "sort": sort
            }

            response = await self.client.search(
                index=index_name,
                body=body
            )

            hits = response.get("hits", {})
            docs = [hit["_source"] for hit in hits.get("hits", [])]

            total = hits.get("total", {}).get("value", 0)
            has_next = len(docs) == min(size, 1000)

            # Генерируем следующий курсор
            next_cursor = None
            if has_next and docs:
                import json
                import base64
                last_doc = response["hits"]["hits"][-1]
                search_after = last_doc["sort"]
                next_cursor = base64.b64encode(
                    json.dumps(search_after).encode()
                ).decode()

            logger.info(
                f"Получены отзывы за период для {token}: "
                f"найдено={total}, на странице={len(docs)}, has_next={has_next}"
            )

            return {
                "reviews": docs,
                "total": total,
                "has_next": has_next,
                "next_cursor": next_cursor
            }

        except Exception as e:
            logger.error(f"Ошибка при получении отзывов за период для {token}: {str(e)}")
            return {
                "reviews": [],
                "total": 0,
                "has_next": False,
                "next_cursor": None,
                "error": str(e)
            }
