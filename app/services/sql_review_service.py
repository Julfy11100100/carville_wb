import json
from datetime import datetime
from typing import Optional, List, Dict, Any

from app.exceptions.sql_database import DatabaseError
from app.services.sql_repository import SQLDatabaseRepository
from app.utils.logging import get_logger

logger = get_logger()


class SqlReviewService:
    """Сервис для работы отзывами в MS SQL"""

    def __init__(self,
                 sql_repository: Optional[SQLDatabaseRepository] = None,
                 table: str = "product_wb_review"
                 ):
        self.sql = sql_repository if sql_repository else SQLDatabaseRepository()
        self.table = table

    async def close_pool(self):
        """Закрывает пул соединений"""
        await self.sql.close_pool()

    async def get_last_date(self) -> Optional[datetime]:
        """Получаем последнюю (максимальную) дату published_at из всей таблицы"""
        select_sql = f"""
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
                    await cursor.execute(select_sql)
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

    async def delete_duplicates(self):
        """Удаляем дубликаты отзывов"""
        delete_sql = f"""
            DELETE FROM {self.table}
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM {self.table}
                GROUP BY id_review
            );
        """

        try:
            async with self.sql.get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(delete_sql)
                    deleted_count = cursor.rowcount
                    logger.info(f"Успешно удалили {deleted_count} дубликатов отзывов")
        except DatabaseError:
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при удалении дублей: {str(e)}")
            raise DatabaseError(f"Ошибка удаления дублей: {str(e)}")

    async def bulk_insert_product_wb_reviews(self, rows: List[Dict[str, Any]]) -> int:
        """Пакетная вставка отзывов в таблицу table с помощью OPENJSON."""
        if not rows:
            logger.info("Нет строк отзывов для вставки в таблицу product_wb_review")
            return 0

        insert_sql = f"""
        DECLARE @payload nvarchar(max) = ?;

        INSERT INTO {self.table} WITH (TABLOCK)
        (
            id_product,
            id_review,
            sku,
            [text],
            published_at,
            rating,
            comments_amount,
            photos_amount,
            videos_amount,
            is_rating_participant,
            offer_id,
            product_name,
            barcodes
        )
        SELECT
            0 AS id_product,
            j.id_review,
            j.sku,
            j.[text],
            j.published_at,
            j.rating,
            j.comments_amount,
            j.photos_amount,
            j.videos_amount,
            j.is_rating_participant,
            j.offer_id,
            j.product_name,
            j.barcodes
        FROM OPENJSON(@payload)
        WITH (
            id_review varchar(36) '$.id_review',
            sku bigint '$.sku',
            [text] nvarchar(3000) '$.text',
            published_at datetime '$.published_at',
            rating tinyint '$.rating',
            comments_amount int '$.comments_amount',
            photos_amount int '$.photos_amount',
            videos_amount int '$.videos_amount',
            is_rating_participant bit '$.is_rating_participant',
            offer_id varchar(60) '$.offer_id',
            product_name varchar(200) '$.product_name',
            barcodes varchar(1000) '$.barcodes'
        ) AS j;
        """

        payload = json.dumps(rows, ensure_ascii=False, default=str)

        async with self.sql.get_connection() as conn:
            conn.autocommit = False
            async with conn.cursor() as cursor:
                await cursor.execute("SET ARITHABORT ON")
                await cursor.execute(insert_sql, (payload,))
            await conn.commit()

        logger.info(f"Вставлено {len(rows)} отзывов в {self.table}")
        return len(rows)

    async def update_product_ids_after_reviews_migration(self) -> None:
        """Финальное обновление id_product в table по NormalizeString и code_ex."""
        update_sql = f"""
        UPDATE pwr
        SET pwr.id_product = p.id
        FROM {self.table} pwr
        JOIN products p ON [dbo].[NormalizeString](
            CASE
                WHEN pwr.offer_id LIKE '%-X[0-9]' THEN LEFT(pwr.offer_id, LEN(pwr.offer_id) - 3)
                WHEN pwr.offer_id LIKE '%-X[0-9][0-9]' THEN LEFT(pwr.offer_id, LEN(pwr.offer_id) - 4)
                ELSE pwr.offer_id
            END,
            '%[^a-zA-Zа-ЯА-Я0-9]%'
        ) = p.code_ex
        WHERE pwr.id_product = 0;
        """

        async with self.sql.get_connection() as conn:
            conn.autocommit = False
            async with conn.cursor() as cursor:
                await cursor.execute("SET ARITHABORT ON")
                await cursor.execute(update_sql)
            await conn.commit()

        logger.info(f"Значения id_product обновлены в {self.table} с использованием сопоставления NormalizeString.")

    async def update_product_ids_from_additional_names(self) -> None:
        """Обновление id_product в table по NormalizeString через product_additional_names."""
        update_sql = f"""
        UPDATE pwr
        SET pwr.id_product = p.id_product
        FROM {self.table} pwr
        JOIN NPR.dbo.product_additional_names p ON 
            [dbo].[NormalizeString](pwr.offer_id, '%[^a-zA-Zа-ЯА-Я0-9]%') = [dbo].[NormalizeString](p.sub_code, '%[^a-zA-Zа-ЯА-Я0-9]%')
        WHERE pwr.id_product = 0;
        """

        async with self.sql.get_connection() as conn:
            conn.autocommit = False
            async with conn.cursor() as cursor:
                await cursor.execute("SET ARITHABORT ON")
                await cursor.execute(update_sql)
            await conn.commit()

        logger.info(
            f"Значения id_product обновляются в {self.table} с использованием сопоставления product_additional_names.")
