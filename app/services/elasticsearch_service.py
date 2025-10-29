import asyncio
from typing import Dict, Any, List, Optional

from elasticsearch import AsyncElasticsearch

from app.services.base_reconnectable import ReconnectableService
from app.utils.logging import get_logger

logger = get_logger()


class ElasticsearchService(ReconnectableService):
    """Сервис для работы с Elasticsearch"""

    def __init__(self):
        super().__init__("Elasticsearch")
        self.client: Optional[AsyncElasticsearch] = None
        self._hosts = ["http://127.0.0.1:9200"]
        self._timeout = 120
        self._client_loop: Optional[asyncio.AbstractEventLoop] = None

    async def _connect(self):
        """Подключение к Elasticsearch"""
        if self.client:
            await self._disconnect()

        self.client = AsyncElasticsearch(
            hosts=self._hosts,
            timeout=self._timeout,
            max_retries=2,
            retry_on_timeout=True
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

    def _get_index_name(self, token: str) -> str:
        """Получить имя индекса для клиента"""
        return f"wb_products_{token}"

    async def create_index(self, token: str) -> Dict[str, Any]:
        """Создать индекс для клиента"""
        try:
            await self._ensure_valid_client()
        except RuntimeError as e:
            # Circuit breaker открыт
            logger.warning(f"Elasticsearch недоступен для клиента {token}: {str(e)}")
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": "Сервис Elasticsearch временно недоступен",
                "message": f"Не удалось создать индекс для клиента {token}"
            }
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
                            "nmId": {"type": "long"},  # Артикул WB (основной идентификатор)
                            "vendorCode": {"type": "keyword"},  # Артикул продавца

                            # Категоризация
                            "subjectId": {"type": "long"},  # ID предмета/категории
                            "subjectName": {"type": "text", "analyzer": "standard"},  # Название предмета
                            "parentId": {"type": "long"},  # ID родительской категории

                            # Основные данные товара
                            "brand": {"type": "keyword"},  # Бренд
                            "title": {"type": "text", "analyzer": "standard"},  # Название товара
                            "description": {"type": "text", "analyzer": "standard"},  # Описание

                            # Штрихкоды
                            "barcode": {"type": "keyword"},  # Основной баркод
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
        except RuntimeError as e:
            # Circuit breaker открыт
            logger.warning(f"Elasticsearch недоступен для индексации клиента {token}: {str(e)}")
            self._schedule_background_reconnect()
            return False
        except Exception as e:
            logger.error(f"Не удалось подключиться к Elasticsearch для клиента {token}: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            return False

        try:
            index_name = self._get_index_name(token)

            # Создаем индекс если не существует
            await self.create_index(token)

            # Подготавливаем данные для индексации
            actions = []
            for product in products:
                # Добавляем action метаданные
                action_meta = {
                    "index": {
                        "_index": index_name,
                        "_id": product.get("nmId")  # Используем nmId как ID документа
                    }
                }
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
        except RuntimeError as e:
            # Circuit breaker открыт
            logger.warning(f"Elasticsearch недоступен для удаления данных клиента {token}: {str(e)}")
            self._schedule_background_reconnect()
            return False
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
        except RuntimeError as e:
            # Circuit breaker открыт
            logger.warning(f"Elasticsearch недоступен для поиска клиента {token}: {str(e)}")
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": "Сервис Elasticsearch временно недоступен",
                "products": [],
                "total": 0
            }
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
        except RuntimeError as e:
            # Circuit breaker открыт
            logger.warning(f"Elasticsearch недоступен для получения товара клиента {token}: {str(e)}")
            self._schedule_background_reconnect()
            return {
                "status": "error",
                "error": "Сервис Elasticsearch временно недоступен",
                "product": None
            }
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

        # Динамическое формирование фильтров для любого поля
        for field, value in filters.items():
            if value is not None and value != "":
                must_clauses.append({"term": {field: value}})

        return {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}
