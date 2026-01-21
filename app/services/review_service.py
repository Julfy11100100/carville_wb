import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from dateutil import parser as date_parser

from app.services.elasticsearch_service import ElasticsearchService
from app.services.sql_review_service import SqlReviewService
from app.services.wb_api_service import WildberriesAPI
from app.utils.logging import get_logger
from app.utils.token import hash_token
from config import settings

logger = get_logger()


class ReviewService:
    """
    Сервис для работы с отзывами Wildberries.
    Занимается загрузкой с WB API и трансформацией данных c последующей индексацией через ElasticsearchService.
    """

    # Константы
    REVIEW_ENDPOINT = settings.REVIEWS_ENDPOINT
    DEFAULT_PAGE_SIZE = 5000

    def __init__(
            self,
            api_client: WildberriesAPI,
            elasticsearch_service: ElasticsearchService,
            sql_service: Optional[SqlReviewService] = None
    ):
        """
        Args:
            api_client: WildberriesAPI клиент
            elasticsearch_service: ElasticsearchService для работы с ES
            sql_service: Сервис по записи в MSSQL
        """
        self.api_client = api_client
        self.es = elasticsearch_service
        self.sql_service = sql_service if sql_service else SqlReviewService()

    @staticmethod
    def _transform_vendor_code(vendor_code: str) -> str:
        """Преобразуем vendor_code: убираем всё после "/", пробелы и "-" """
        if not vendor_code:
            return ""

        # Убираем всё после "/"
        vendor_code = vendor_code.split("/")[0]

        # Убираем пробелы и "-"
        vendor_code = vendor_code.replace(" ", "").replace("-", "")

        return vendor_code.strip()

    def _transform_review_to_doc(self, review: Dict[str, Any]) -> Dict[str, Any]:
        """
        Преобразовать отзыв WB в документ для индексации и записи в бд.

        Args:
            review: Отзыв с WB API

        Returns:
            Документ для ES и Бд
        """
        product_details = review.get("productDetails", {}) or {}
        text_value = review.get("text") or None
        if isinstance(text_value, str) and len(text_value) > 3000:
            text_value = text_value[:3000]
        published_at = self._parse_datetime(review.get("createdDate"))

        return {
            "id_review": review.get("id"),
            "sku": self._safe_int(product_details.get("nmId")),
            "text": text_value,
            "published_at": self._format_datetime(published_at),
            "rating": self._safe_int(review.get("productValuation")),
            "comments_amount": 1,
            "photos_amount": len(review.get("photoLinks") or []),
            "videos_amount": 1 if review.get("video", None) else 0,
            "is_rating_participant": 1,
            "offer_id": self._transform_vendor_code(product_details.get("supplierArticle")),
            "product_name": product_details.get("productName"),
            "barcodes": review.get("lastOrderShkId")
        }

    async def create_review_index(self, token: str) -> Dict[str, Any]:
        """Создать индекс для отзывов"""
        hashed_token = hash_token(token)
        return await self.es.create_review_index(hashed_token)

    async def get_last_review_date_from_db(self) -> Optional[int]:
        """
        Получить Unix timestamp последнего отзыва из бд.
        Возвращает None если дата пустая.
        """
        try:
            dt = await self.sql_service.get_last_date()
            if not dt:
                return None
            return int(dt.timestamp())
        except Exception as e:
            logger.warning(f"Не удалось получить дату, ошибка: {str(e)}")
            return None

    async def fetch_and_index_all_reviews(
            self,
            token: str,
            resume_from_date: Optional[int] = None,
            take: int = DEFAULT_PAGE_SIZE
    ) -> Dict[str, Any]:
        """
        Получить ВСЕ отзывы (до 350k) с WB и индексировать в ES.
        Поддерживает возобновление с последнего сохранённого отзыва.

        Использует пагинацию с skip (0, 5000, 10000, ...)

        Args:
            token: API токен
            resume_from_date: Unix timestamp, с которого начать (для возобновления)
            take: Размер батча (макс 5000)

        Returns:
            Dict с статистикой загрузки
        """
        hashed_token = hash_token(token)

        # Если не указана дата, пытаемся получить последнюю сохранённую
        start_timestamp = await self.get_last_review_date_from_db() if not resume_from_date else resume_from_date

        skip = 0
        total_fetched = 0
        total_indexed = 0
        total_inserted = 0
        failed_batches = 0
        last_error = None
        batch_count = 0

        take = min(take, self.DEFAULT_PAGE_SIZE)

        logger.info(
            f"Начинаю загрузку всех отзывов для {hashed_token} "
            f"(начало с: {datetime.fromtimestamp(start_timestamp) if start_timestamp else 'нуля'})"
        )

        try:
            # Создаём индекс перед загрузкой
            result = await self.create_review_index(token)
            if result.get("status") == "error":
                logger.warning(f"Ошибка при создании индекса: {result.get('error')}")

            while True:
                try:
                    params = {
                        "take": take,
                        "skip": skip,
                        "isAnswered": "true",
                    }

                    # Если указана дата начала, фильтруем по ней
                    if start_timestamp:
                        params["dateFrom"] = start_timestamp

                    # Сортировка по дате (от старых к новым для логичного порядка)
                    params["order"] = "dateAsc"

                    logger.debug(
                        f"Запрашиваю отзывы: skip={skip}, take={take} "
                        f"{'(dateFrom=' + str(start_timestamp) + ')' if start_timestamp else ''}"
                    )

                    response = await self.api_client.get_reviews(
                        token,
                        params=params
                    )

                    reviews = response.get("feedbacks", [])
                    if not reviews:
                        logger.info(
                            f"Нет больше отзывов (skip={skip}). Загрузка завершена"
                        )
                        break

                    # Трансформируем и индексируем батч
                    docs = [self._transform_review_to_doc(fb) for fb in reviews]

                    # Индексируем
                    indexed = await self.es.index_reviews(hashed_token, docs)

                    # Сохраняем в бд
                    saved = await self.sql_service.bulk_insert_product_wb_reviews(docs)

                    if indexed:
                        total_indexed += len(reviews)
                    else:
                        failed_batches += 1

                    if saved:
                        total_inserted += saved

                    total_fetched += len(reviews)
                    batch_count += 1

                    logger.info(
                        f"Батч {batch_count}: получено {len(reviews)}, "
                        f"всего: {total_fetched}, skip={skip}"
                    )

                    # Если получилось меньше, чем запросили, это последняя страница
                    if len(reviews) < take:
                        logger.info(
                            f"Получено меньше отзывов ({len(reviews)} < {take}), "
                            f"это последняя страница"
                        )
                        break

                    skip += take
                    await asyncio.sleep(1)

                    if skip >= 195_000:
                        # Тут у нас условие по api что скип не может быть больше 195к
                        # Поэтому обнуляем skip и берём start_timestamp с последнего загруженного отзыва
                        skip = 0
                        total_fetched -= 1
                        total_indexed -= 1
                        total_inserted -= 1
                        last_review = docs[-1]
                        dt = date_parser.isoparse(last_review.get("published_at"))
                        start_timestamp = int(dt.timestamp())
                        logger.info(
                            f"Превысили лимит по skip в 195к,"
                            f"обнуляем и выставляем новый start_timestamp = {start_timestamp}")

                except Exception as e:
                    logger.exception(f"Ошибка при загрузке батча (skip={skip})")
                    last_error = str(e)
                    failed_batches += 1

                    if skip <= 100000:
                        logger.info(f"Повторная попытка загрузки (skip={skip})")
                        await asyncio.sleep(2)
                        continue
                    else:
                        skip += take
                        await asyncio.sleep(2)

            # Производим оставшиеся действия с бд
            # удаляем дубли
            await self.sql_service.delete_duplicates()
            # первое обогащение id_product
            await self.sql_service.update_product_ids_after_reviews_migration()
            # второе обогащение id_product
            await self.sql_service.update_product_ids_from_additional_names()

        except Exception as e:
            logger.error(f"Критическая ошибка при загрузке для {hashed_token}: {str(e)}")
            last_error = str(e)

        finally:
            await self.sql_service.close_pool()

        return {
            "status": "completed" if failed_batches == 0 else "partially_completed",
            "total_fetched": total_fetched,
            "total_indexed": total_indexed,
            "total_inserted": total_inserted,
            "failed_batches": failed_batches,
            "batches_processed": batch_count,
            "last_skip": skip,
            "last_error": last_error,
            "message": f"Загружено {total_fetched} отзывов, "
                       f"успешно индексировано {total_indexed}, "
                       f"успешно сохранено в бд {total_inserted}"
        }

    async def get_reviews_by_value(
            self,
            token: str,
            vendor_code: Optional[str] = None,
            barcode: Optional[int] = None,
            size: int = 10000
    ) -> Dict[str, Any]:
        """Получить отзывы по vendor_code или bar_code"""
        return await self.es.search_reviews_by_value(
            token=hash_token(token),
            vendor_code=vendor_code,
            barcode=barcode,
            size=size
        )

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        if not value:
            return None
        if isinstance(value, datetime):
            # приводим к UTC без tzinfo для MSSQL
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                dt = dt.astimezone(timezone.utc)
                return dt.replace(tzinfo=None)
            except ValueError:
                return None
        return None

    @staticmethod
    def _format_datetime(value: Optional[datetime]) -> Optional[str]:
        if not value:
            return None
        return value.strftime("%Y-%m-%dT%H:%M:%S")
