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
    SOURCE_FIELDS = ["norm_code", "cross_codes", "oem", "wb_price_clear", "bar_code"]

    async def get_analyze_npr_data(
            self,
            vendor_codes: List[Any],
    ) -> Dict[str, Optional[Dict[str, Optional[str]]]]:
        """
        Данные для анализа (без цены).

        Возвращает словарь {оригинальный vendor_code: {"oem": ..., "cross_num": ...} | None}.
        """
        prepared = self._prepare_payload(vendor_codes)
        if prepared is None:
            return {}

        originals_map, norm_map = prepared
        if not norm_map:
            return originals_map

        documents = await self._fetch_documents(
            list(norm_map.keys()),
            exclude_rlz=True,
        )
        return self._merge_documents(originals_map, norm_map, documents, include_price=False)

    async def get_create_npr_data(
            self,
            vendor_codes: List[Any],
    ) -> Dict[str, Optional[Dict[str, Optional[str]]]]:
        """
        Данные для создания товаров (с ценой).

        Возвращает словарь {оригинальный vendor_code: {"oem": ..., "cross_num": ..., "price": ...} | None}.
        """
        prepared = self._prepare_payload(vendor_codes)
        if prepared is None:
            return {}

        originals_map, norm_map = prepared
        if not norm_map:
            return originals_map

        documents = await self._fetch_documents(
            list(norm_map.keys()),
            exclude_rlz=False,
        )
        return self._merge_documents(originals_map, norm_map, documents, include_price=True)

    def _prepare_payload(
            self,
            vendor_codes: Optional[List[Any]],
    ) -> Optional[
        tuple[
            Dict[str, Optional[Dict[str, Optional[str]]]],
            Dict[str, List[str]],
        ]
    ]:
        """Готовим структуры для ответа и нормализуем входные данные."""
        if not vendor_codes:
            return None

        originals_map: Dict[str, Optional[Dict[str, Optional[str]]]] = {}
        norm_map: Dict[str, List[str]] = {}

        for raw_vendor_code in vendor_codes:
            original_value = str(raw_vendor_code)
            if original_value not in originals_map:
                originals_map[original_value] = None

            norm_code = get_normalize_identifier(original_value)
            if not norm_code:
                continue

            norm_map.setdefault(norm_code, []).append(original_value)

        return originals_map, norm_map

    async def _fetch_documents(
            self,
            norm_codes: List[str],
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
                    "cross_codes": source.get("cross_codes"),
                    "oem": source.get("oem"),
                    "wb_price_clear": source.get("wb_price_clear"),
                    "bar_code": source.get("bar_code")
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
        """Соотнесение найденных документов с оригинальными vendor_codes."""
        for norm_code, originals in norm_map.items():
            document = documents.get(norm_code)
            if not document:
                continue

            oem = document.get("oem") or []
            cross_codes = document.get("cross_codes") or []

            base_payload = {
                "oem": [doc.get("OEM_CODE") for doc in oem if doc and doc.get("OEM_CODE")],
                "cross_num": [doc.get("CROSS_CODE") for doc in cross_codes if doc and doc.get("CROSS_CODE")],
            }

            if include_price:
                base_payload["price"] = document.get("wb_price_clear")
                base_payload["bar_code"] = document.get("bar_code")

            for original_vendor_code in originals:
                originals_map[original_vendor_code] = base_payload

        return originals_map
