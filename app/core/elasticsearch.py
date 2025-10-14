import json
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import NotFoundError, RequestError

logger = logging.getLogger(__name__)


class ElasticsearchClient:
    def __init__(self, host="localhost", port=9200, index_name="wb-products"):
        self.es = Elasticsearch([f"http://{host}:{port}"])
        self.index = index_name

    def ping(self) -> bool:
        """Проверка подключения к Elasticsearch"""
        try:
            return self.es.ping()
        except Exception as e:
            logger.error(f"Ошибка подключения к Elasticsearch: {e}")
            return False

    def create_index(self, mappings: Optional[Dict] = None) -> bool:
        """Создание индекса с маппингами"""
        try:
            if self.es.indices.exists(index=self.index):
                logger.info(f"Индекс {self.index} уже существует")
                return True

            # Базовые маппинги для карточек товаров WB
            default_mappings = {
                "properties": {
                    "id": {"type": "keyword"},
                    "nmID": {"type": "long"},
                    "vendorCode": {"type": "keyword"},
                    "name": {"type": "text", "analyzer": "standard"},
                    "brand": {"type": "keyword"},
                    "description": {"type": "text"},
                    "category": {"type": "keyword"},
                    "price": {"type": "double"},
                    "discount": {"type": "integer"},
                    "rating": {"type": "float"},
                    "feedbacks": {"type": "integer"},
                    "photos": {"type": "keyword"},
                    "video": {"type": "keyword"},
                    "sizes": {"type": "nested"},
                    "colors": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"}
                }
            }

            body = {"mappings": mappings or default_mappings}
            self.es.indices.create(index=self.index, body=body)
            logger.info(f"Индекс {self.index} создан успешно")
            return True

        except RequestError as e:
            logger.error(f"Ошибка создания индекса: {e}")
            return False

    def delete_index(self) -> bool:
        """Удаление индекса"""
        try:
            if self.es.indices.exists(index=self.index):
                self.es.indices.delete(index=self.index)
                logger.info(f"Индекс {self.index} удален")
                return True
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления индекса: {e}")
            return False

    def load_from_json_file(self, file_path: str) -> bool:
        """Загрузка данных из JSON файла с карточками товаров"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Ожидаем структуру типа {"products": [...], "task_id": "...", ...}
            products = data.get("products", [])
            if not products:
                logger.warning("Нет продуктов для загрузки")
                return False

            return self.bulk_index(products)

        except Exception as e:
            logger.error(f"Ошибка загрузки файла {file_path}: {e}")
            return False

    def bulk_index(self, documents: List[Dict], chunk_size: int = 1000) -> bool:
        """Массовая индексация документов"""
        try:
            def generate_docs():
                for doc in documents:
                    yield {
                        "_index": self.index,
                        "_id": doc.get("id") or doc.get("nmID"),
                        "_source": doc
                    }

            success, failed = helpers.bulk(
                self.es,
                generate_docs(),
                chunk_size=chunk_size,
                request_timeout=60
            )

            logger.info(f"Проиндексировано {success} документов")
            if failed:
                logger.warning(f"Не удалось проиндексировать {len(failed)} документов")

            return success > 0

        except Exception as e:
            logger.error(f"Ошибка массовой индексации: {e}")
            return False

    def search(self, query: Dict, size: int = 10, from_: int = 0) -> Dict:
        """Поиск документов"""
        try:
            body = {
                "query": query,
                "size": size,
                "from": from_
            }
            return self.es.search(index=self.index, body=body)
        except Exception as e:
            logger.error(f"Ошибка поиска: {e}")
            return {"hits": {"hits": [], "total": {"value": 0}}}

    def search_by_text(self, text: str, fields: List[str] = None, size: int = 10) -> List[Dict]:
        """Полнотекстовый поиск"""
        fields = fields or ["name", "description", "brand"]
        query = {
            "multi_match": {
                "query": text,
                "fields": fields,
                "type": "best_fields"
            }
        }

        result = self.search(query, size=size)
        return [hit["_source"] for hit in result["hits"]["hits"]]

    def filter_products(self, filters: Dict, size: int = 100) -> List[Dict]:
        """Фильтрация товаров по параметрам"""
        bool_query = {"bool": {"must": []}}

        for field, value in filters.items():
            if isinstance(value, list):
                bool_query["bool"]["must"].append({"terms": {field: value}})
            elif isinstance(value, dict) and ("gte" in value or "lte" in value):
                bool_query["bool"]["must"].append({"range": {field: value}})
            else:
                bool_query["bool"]["must"].append({"term": {field: value}})

        result = self.search(bool_query, size=size)
        return [hit["_source"] for hit in result["hits"]["hits"]]

    def get_aggregations(self, agg_config: Dict) -> Dict:
        """Получение агрегаций"""
        try:
            body = {
                "aggs": agg_config,
                "size": 0
            }
            result = self.es.search(index=self.index, body=body)
            return result.get("aggregations", {})
        except Exception as e:
            logger.error(f"Ошибка агрегации: {e}")
            return {}

    def get_stats(self) -> Dict:
        """Статистика по индексу"""
        try:
            stats = self.es.indices.stats(index=self.index)
            doc_count = stats["indices"][self.index]["total"]["docs"]["count"]
            size_mb = stats["indices"][self.index]["total"]["store"]["size_in_bytes"] / (1024 * 1024)

            return {
                "document_count": doc_count,
                "size_mb": round(size_mb, 2),
                "index_name": self.index
            }
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {}
