from datetime import datetime
from typing import Optional

from app.exceptions.sql_database import DatabaseError
from app.services.sql_repository import SQLDatabaseRepository
from app.utils.logging import get_logger

logger = get_logger()


class SqlReviewService:
    """Сервис для работы отзывами в MS SQL"""

    def __init__(self,
                 sql_repository: SQLDatabaseRepository,
                 table: str = "product_wb_review"
                 ):
        self.sql = sql_repository
        self.table = table

    async def get_last_date(self) -> Optional[datetime]:
        """Получаем последнюю (максимальную) дату published_at из всей таблицы"""
        query = f"""
            SELECT TOP 1
                published_at
            FROM 
                {self.table}
            ORDER BY 
                published_at DESC;
        """

        try:
            async with self.sql.get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query)
                    row = await cursor.fetchone()

                    # Проверяем, что строка есть и в ней не NULL
                    result = row[0] if row and row[0] else None

                    logger.info(f"Последняя дата отзыва в базе: {result}")
                    return result

        except DatabaseError:
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при извлечении последней даты: {str(e)}")
            raise DatabaseError(f"Ошибка извлечения последней даты: {str(e)}")

    async def bulk_insert_reviews(self, reviews: list[dict], batch_size: int = 100) -> dict:
        """
        Массовая запись отзывов в таблицу батчами по batch_size записей

        Args:
            reviews: Список словарей с полями отзывов
            batch_size: Размер батча (по умолчанию 100)

        Returns:
            dict с статистикой: {'inserted': int, 'failed': int, 'errors': list}
        """
        if not reviews:
            logger.warning("Пустой список отзывов для записи")
            return {'inserted': 0, 'failed': 0, 'errors': []}

        stats = {'inserted': 0, 'failed': 0, 'errors': []}

        # Разбиваем на батчи
        batches = [reviews[i:i + batch_size] for i in range(0, len(reviews), batch_size)]

        for batch_num, batch in enumerate(batches, 1):
            try:
                async with self.sql.get_connection() as conn:
                    async with conn.cursor() as cursor:
                        # Подготавливаем INSERT запрос
                        insert_query = f"""
                            INSERT INTO {self.table} 
                            (id_product, id_review, sku, [text], published_at, rating, 
                             comments_amount, photos_amount, videos_amount, 
                             is_rating_participant, offer_id, product_name, barcodes)
                            VALUES 
                            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """

                        # Подготавливаем данные для вставки
                        values_list = []
                        for review in batch:
                            values_list.append((
                                review.get('id_product'),
                                review.get('id_review'),
                                review.get('sku'),
                                review.get('text'),
                                review.get('published_at'),
                                review.get('rating'),
                                review.get('comments_amount', 0),
                                review.get('photos_amount', 0),
                                review.get('videos_amount', 0),
                                review.get('is_rating_participant', 0),
                                review.get('offer_id'),
                                review.get('product_name'),
                                review.get('barcodes')
                            ))

                        # Executemany для группировки
                        await cursor.executemany(insert_query, values_list)
                        await conn.commit()

                        inserted_count = len(batch)
                        stats['inserted'] += inserted_count
                        logger.info(f"Батч {batch_num}: успешно записано {inserted_count} отзывов")

            except DatabaseError as e:
                stats['failed'] += len(batch)
                error_msg = f"Батч {batch_num}: ошибка БД - {str(e)}"
                stats['errors'].append(error_msg)
                logger.error(error_msg)

            except Exception as e:
                stats['failed'] += len(batch)
                error_msg = f"Батч {batch_num}: неожиданная ошибка - {str(e)}"
                stats['errors'].append(error_msg)
                logger.error(error_msg)

        logger.info(f"Завершена массовая запись. Статистика: {stats}")
        return stats
