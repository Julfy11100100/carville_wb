from typing import Any, Dict, List, Optional

from app.services.elasticsearch_service import ElasticsearchService
from app.utils.logging import get_logger
from app.utils.normalize import get_normalize_identifier
from config import settings

logger = get_logger()


class NprProductService:
    """Сервис чтения NPR товаров из Elasticsearch."""

    def __init__(self, elasticsearch_service: ElasticsearchService):
        self.elasticsearch_service = elasticsearch_service

    INDEX_NAME = settings.NPR_PRODUCTS_INDEX
    SOURCE_FIELDS = ["norm_code", "cross_codes_string", "oem_string", "ozon_price_clear"]

    async def get_analyze_npr_data(
            self,
            offer_ids: List[Any],
            brand: str,
    ) -> Dict[str, Optional[Dict[str, Optional[str]]]]:
        """
        Данные для анализа (без цены).

        Возвращает словарь {оригинальный offer_id: {"oem": ..., "cross_num": ...} | None}.
        """
        prepared = self._prepare_payload(offer_ids, brand)
        if prepared is None:
            return {}

        originals_map, norm_map, norm_brand_lower = prepared
        if not norm_map or not norm_brand_lower:
            return originals_map

        documents = await self._fetch_documents(
            list(norm_map.keys()),
            norm_brand_lower,
            exclude_rlz=True,
        )
        return self._merge_documents(originals_map, norm_map, documents, include_price=False)

    async def get_create_npr_data(
            self,
            offer_ids: List[Any],
            brand: str,
    ) -> Dict[str, Optional[Dict[str, Optional[str]]]]:
        """
        Данные для создания товаров (с ценой).

        Возвращает словарь {оригинальный offer_id: {"oem": ..., "cross_num": ..., "price": ...} | None}.
        """
        prepared = self._prepare_payload(offer_ids, brand)
        if prepared is None:
            return {}

        originals_map, norm_map, norm_brand_lower = prepared
        if not norm_map or not norm_brand_lower:
            return originals_map

        documents = await self._fetch_documents(
            list(norm_map.keys()),
            norm_brand_lower,
            exclude_rlz=False,
        )
        return self._merge_documents(originals_map, norm_map, documents, include_price=True)

    def _prepare_payload(
            self,
            offer_ids: Optional[List[Any]],
            brand: Optional[str] = None,
    ) -> Optional[
        tuple[
            Dict[str, Optional[Dict[str, Optional[str]]]],
            Dict[str, List[str]],
            str,
        ]
    ]:
        """Готовим структуры для ответа и нормализуем входные данные."""
        if not offer_ids:
            return None

        originals_map: Dict[str, Optional[Dict[str, Optional[str]]]] = {}
        norm_map: Dict[str, List[str]] = {}

        for raw_offer in offer_ids:
            original_value = str(raw_offer)
            if original_value not in originals_map:
                originals_map[original_value] = None

            norm_code = get_normalize_identifier(original_value)
            if not norm_code:
                continue

            norm_map.setdefault(norm_code, []).append(original_value)

        norm_brand_lower = get_normalize_identifier(brand or "").lower()

        return originals_map, norm_map, norm_brand_lower

    async def _fetch_documents(
            self,
            norm_codes: List[str],
            norm_brand_lower: str,
            *,
            exclude_rlz: bool,
    ) -> Dict[str, Dict[str, Optional[str]]]:
        """
        Чтение документов из индекса web_api_products_<CLIENT_ID>.

        Параметр exclude_rlz управляет исключением товаров со статусом RLZ.
        """
        if not norm_codes:
            return {}

        try:
            await self.elasticsearch_service.ensure_connection()
        except Exception as exc:
            logger.error("Не удалось подключиться к Elasticsearch: %s", exc)
            self.elasticsearch_service.record_failure()
            self.elasticsearch_service.schedule_background_reconnect()
            return {}

        client = self.elasticsearch_service.client
        if client is None:
            return {}

        try:
            exists = await client.indices.exists(index=self.INDEX_NAME)
            if not exists:
                logger.warning("Индекс %s отсутствует", self.INDEX_NAME)
                return {}

            query: Dict[str, Any] = {
                "bool": {
                    "must": [
                        {"terms": {"norm_code": norm_codes}},
                        {"term": {"norm_brand_lower": norm_brand_lower}},
                    ],
                },
            }
            if exclude_rlz:
                query["bool"]["must_not"] = [{"term": {"status_abc": "RLZ"}}]

            search_body = {
                "query": query,
                "size": len(norm_codes),
                "_source": self.SOURCE_FIELDS,
            }

            response = await client.search(index=self.INDEX_NAME, body=search_body)
            documents = {}
            hits = response.get("hits", {}).get("hits", [])

            for hit in hits:
                source = hit.get("_source") or {}
                norm_code = source.get("norm_code")
                if not norm_code:
                    continue
                documents[norm_code] = {
                    "norm_code": norm_code,
                    "cross_codes_string": source.get("cross_codes_string"),
                    "oem_string": source.get("oem_string"),
                    "ozon_price_clear": source.get("ozon_price_clear"),
                }

            return documents
        except Exception as exc:
            logger.error("Ошибка поиска в индексе %s: %s", self.INDEX_NAME, exc)
            self.elasticsearch_service.record_failure()
            return {}

    def _merge_documents(
            self,
            originals_map: Dict[str, Optional[Dict[str, Optional[str]]]],
            norm_map: Dict[str, List[str]],
            documents: Dict[str, Dict[str, Optional[str]]],
            include_price: bool,
    ) -> Dict[str, Optional[Dict[str, Optional[str]]]]:
        """Соотнесение найденных документов с оригинальными offer_id."""
        for norm_code, originals in norm_map.items():
            document = documents.get(norm_code)
            if not document:
                continue

            base_payload = {
                "oem": document.get("oem_string"),
                "cross_num": document.get("cross_codes_string"),
            }

            if include_price:
                base_payload["price"] = document.get("ozon_price_clear")

            for original_offer in originals:
                originals_map[original_offer] = dict(base_payload)

        return originals_map
