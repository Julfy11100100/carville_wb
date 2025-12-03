import asyncio
import base64
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple

from dateutil import parser as date_parser

from app.services.elasticsearch_service import ElasticsearchService
from app.services.wb_api import WildberriesAPI
from app.utils.logging import get_logger
from app.utils.token import hash_token
from config import settings

logger = get_logger()


class FeedbackService:
    """
    Сервис для работы с отзывами Wildberries.
    Занимается загрузкой с WB API и трансформацией данных c последующей индексацией через ElasticsearchService.
    """

    # Константы
    FEEDBACKS_ENDPOINT = settings.FEEDBACKS_ENDPOINT
    DEFAULT_PAGE_SIZE = 5000

    def __init__(
            self,
            api_client: WildberriesAPI,
            elasticsearch_service: ElasticsearchService,
    ):
        """
        Args:
            api_client: WildberriesAPI клиент
            elasticsearch_service: ElasticsearchService для работы с ES
        """
        self.api_client = api_client
        self.es = elasticsearch_service

    @staticmethod
    def _transform_feedback_to_doc(feedback: Dict[str, Any]) -> Dict[str, Any]:
        """
        Преобразовать отзыв WB в документ для индексации.

        Args:
            feedback: Отзыв с WB API

        Returns:
            Документ для ES
        """
        product_details = feedback.get("productDetails", {}) or {}

        return {
            "id": feedback.get("id"),
            "text": feedback.get("text"),
            "pros": feedback.get("pros"),
            "cons": feedback.get("cons"),
            "product_valuation": feedback.get("productValuation"),
            "created_date": feedback.get("createdDate"),
            "product_name": product_details.get("productName"),
            "vendor_code": product_details.get("supplierArticle"),
            "brand_name": product_details.get("brandName"),
            "subject_id": feedback.get("subjectId"),
            "barcode": feedback.get("lastOrderShkId"),
            "photos_amount": len(feedback.get("photoLinks") or []),
            "videos_amount": 1,
            "status": feedback.get("state"),
        }

    async def create_feedback_index(self, token: str) -> Dict[str, Any]:
        """Создать индекс для отзывов"""
        hashed_token = hash_token(token)
        return await self.es.create_feedback_index(hashed_token)

    async def get_last_indexed_feedback_date(self, token: str) -> Optional[int]:
        """
        Получить Unix timestamp последнего индексированного отзыва.
        Возвращает None если индекс пуст или не существует.
        """
        hashed_token = hash_token(token)
        date_str = await self.es.get_last_indexed_feedback_date(hashed_token)

        if not date_str:
            return None

        try:
            dt = date_parser.isoparse(date_str)
            return int(dt.timestamp())
        except Exception as e:
            logger.warning(f"Не удалось парсить дату: {date_str}, ошибка: {str(e)}")
            return None

    async def fetch_and_index_all_feedbacks(
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
        start_timestamp = await self.get_last_indexed_feedback_date(token) if not resume_from_date else resume_from_date

        skip = 0
        total_fetched = 0
        total_indexed = 0
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
            result = await self.create_feedback_index(token)
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

                    # Сортировка по дате (от новых к старым для логичного порядка)
                    params["order"] = "dateAsc"

                    logger.debug(
                        f"Запрашиваю отзывы: skip={skip}, take={take} "
                        f"{'(dateFrom=' + str(start_timestamp) + ')' if start_timestamp else ''}"
                    )

                    response = await self.api_client.make_request(
                        "GET",
                        self.FEEDBACKS_ENDPOINT,
                        token,
                        params=params
                    )

                    feedbacks = response.get("data", {}).get("feedbacks", [])

                    if not feedbacks:
                        logger.info(
                            f"Нет больше отзывов (skip={skip}). Загрузка завершена"
                        )
                        break

                    # Трансформируем и индексируем батч
                    docs = [self._transform_feedback_to_doc(fb) for fb in feedbacks]
                    indexed = await self.es.index_feedbacks(hashed_token, docs)

                    if indexed:
                        total_indexed += len(feedbacks)
                    else:
                        failed_batches += 1

                    total_fetched += len(feedbacks)
                    batch_count += 1

                    logger.info(
                        f"Батч {batch_count}: получено {len(feedbacks)}, "
                        f"всего: {total_fetched}, skip={skip}"
                    )

                    # Если получилось меньше, чем запросили, это последняя страница
                    if len(feedbacks) < take:
                        logger.info(
                            f"Получено меньше отзывов ({len(feedbacks)} < {take}), "
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
                        last_feedback = docs[-1]
                        dt = date_parser.isoparse(last_feedback.get("created_date"))
                        start_timestamp = int(dt.timestamp())
                        logger.info(
                            f"Превысили лимит по skip в 195к,"
                            f"обнуляем и выставляем новый start_timestamp = {start_timestamp}")


                except Exception as e:
                    logger.error(f"Ошибка при загрузке батча (skip={skip}): {str(e)}")
                    last_error = str(e)
                    failed_batches += 1

                    if skip <= 100000:
                        logger.info(f"Повторная попытка загрузки (skip={skip})")
                        await asyncio.sleep(2)
                        continue
                    else:
                        skip += take
                        await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"Критическая ошибка при загрузке для {hashed_token}: {str(e)}")
            last_error = str(e)

        return {
            "status": "completed" if failed_batches == 0 else "partially_completed",
            "total_fetched": total_fetched,
            "total_indexed": total_indexed,
            "failed_batches": failed_batches,
            "batches_processed": batch_count,
            "last_skip": skip,
            "last_error": last_error,
            "message": f"Загружено {total_fetched} отзывов, "
                       f"успешно индексировано {total_indexed}"
        }

    async def get_feedbacks_by_value(
            self,
            token: str,
            vendor_code: Optional[str] = None,
            bar_code: Optional[int] = None,
            size: int = 10000
    ) -> Dict[str, Any]:
        """Получить отзывы по vendor_code или bar_code"""
        return await self.es.search_feedbacks_by_value(
            token=hash_token(token),
            vendor_code=vendor_code,
            bar_code=bar_code,
            size=size
        )

    async def get_feedbacks_by_period(
            self,
            token: str,
            period: Optional[str] = None,
            date_from: Optional[str] = None,
            date_to: Optional[str] = None,
            cursor: Optional[str] = None,
            size: int = 1000
    ) -> Dict[str, Any]:
        """Получить отзывы за период с постраничной навигацией"""
        # Преобразуем период в даты если нужно
        if period:
            date_from, date_to = self._parse_relative_period(period)

        elif date_from or date_to:
            # Парсим абсолютные даты
            if date_from:
                dt = self._parse_date_string(date_from)
                if "T" not in date_from:
                    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                date_from = dt.isoformat() + "Z"

            if date_to:
                dt = self._parse_date_string(date_to)
                if "T" not in date_to:
                    dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                date_to = dt.isoformat() + "Z"

        # Обработка курсора
        if cursor:
            try:
                search_after = json.loads(base64.b64decode(cursor))
                # Добавляем search_after в поиск ES
                # Это требует модификации метода в ElasticsearchService
                # Для простоты, пока просто передаём даты
            except Exception as e:
                logger.warning(f"Ошибка парсинга курсора: {str(e)}")
                return {
                    "reviews": [],
                    "total": 0,
                    "has_next": False,
                    "next_cursor": None,
                    "error": f"Неверный формат курсора: {str(e)}"
                }

        return await self.es.search_feedbacks_by_period(
            token=hash_token(token),
            date_from=date_from,
            date_to=date_to,
            size=size
        )

    @staticmethod
    def _parse_relative_period(period: str) -> Tuple[str, str]:
        """Парсить относительный период (15h, 2d, 3w) в абсолютные даты"""
        now = datetime.utcnow()

        if period.endswith("h"):
            delta = timedelta(hours=int(period[:-1]))
        elif period.endswith("d"):
            delta = timedelta(days=int(period[:-1]))
        elif period.endswith("w"):
            delta = timedelta(weeks=int(period[:-1]))
        else:
            raise ValueError(f"Неизвестный формат периода: {period}")

        date_from = (now - delta).replace(microsecond=0)
        date_to = now.replace(microsecond=0)

        return (
            date_from.isoformat() + "Z",
            date_to.isoformat() + "Z"
        )

    @staticmethod
    def _parse_date_string(date_str: str) -> datetime:
        """Парсить дату в разных форматах"""
        try:
            return date_parser.isoparse(date_str)
        except Exception:
            raise ValueError(f"Не удалось парсить дату: {date_str}")
