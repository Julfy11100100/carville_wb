import json
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from app.exceptions.sql_database import DatabaseError
from app.schemas.wb_types import WbTypesTreeResponse, CategoryResponse, WbTypeResponse
from app.services.sql_repository import SQLDatabaseRepository
from app.utils.logging import get_logger
from config import settings, project_root

logger = get_logger()


class SqlFeedbackService:
    """Сервис для работы отзывами в MS SQL"""

    def __init__(self,
                 sql_repository: SQLDatabaseRepository,
                 table: str = "product_wb_review"
    ):
        self.sql = sql_repository
        self.table = table


    async def get_last_date(self) -> Optional[datetime]:
        """Получаем из table последнюю запись по полю published_at"""
        query = f"""
                SELECT 
                    c.type_id
                FROM 
                    wb_type c
                LEFT JOIN 
                    wb_type p ON c.parent_id = p.id
                WHERE 
                    p.type_id = {parent_id}
                ORDER BY 
                    p.type_id;           
            """

        try:
            # Используем query_timeout=5 секунд для защиты от длинных блокировок
            async with self.sql.get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query)
                    rows = await cursor.fetchall()
                    result = [row[0] for row in rows if row]
                    logger.info(f"Получили список категорий по родительской {parent_id}: {result}")
                    return result
        except DatabaseError:
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при извлечении родительских типов из бд: {str(e)}")
            raise DatabaseError(f"Ошибка извлечения родительских типов из бд: {str(e)}")
