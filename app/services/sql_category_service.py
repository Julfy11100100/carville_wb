from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List

from app.exceptions.sql_database import DatabaseError
from app.schemas.wb_types import WbTypesTreeResponse, CategoryResponse, WbTypeResponse
from app.services.sql_repository import SQLDatabaseRepository
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


class SqlCategoryService:
    """Сервис для работы с базой данных MS SQL"""

    def __init__(self,
                 sql_repository: SQLDatabaseRepository,
                 table: str = "wb_type",
                 categories_file: str = "categories.json"):

        self.sql = sql_repository

        self.table = table
        self.schemes_path = Path("data/static/categories") / categories_file

    async def sync_categories_tree_to_db(self, tree_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Синхронизация структурированного дерева категорий с БД
        """
        if not tree_data or not any(tree_data.values()):
            logger.warning("Отсутствуют данные для синхронизации")
            return {
                "status": "error",
                "message": "Отсутствуют данные для синхронизации",
                "statistics": {}
            }

        logger.info(f"Стартуем синхронизацию: {len(tree_data.get('root_categories', {}))} родительских категорий, "
                    f"{len(tree_data.get('categories', {}))} категорий")

        try:
            # Подключаемся к БД
            async with self.sql.get_connection() as conn:
                conn.autocommit = False

                async with conn.cursor() as cursor:
                    # Инициализируем статистику
                    statistics = {
                        'updated_roots': 0,
                        'updated_categories': 0,
                        'created_roots': 0,
                        'created_categories': 0,
                        'name_changes_roots': 0,
                        'name_changes_categories': 0,
                        # Детальная статистика
                        'new_roots': [],  # список новых корневых категорий
                        'new_categories': [],  # список новых категорий
                        'renamed_roots': [],  # список переименованных корневых категорий
                        'renamed_categories': [],  # список переименованных категорий
                    }

                    # Словари для маппинга ID на новые ID записей в БД
                    root_id_mapping = {}
                    category_id_mapping = {}

                    # 1. Синхронизация корневых категорий
                    logger.info("Синхронизация родительских категорий")
                    start_time = datetime.now()
                    await self._sync_root_categories(cursor, tree_data['root_categories'], statistics, root_id_mapping)
                    logger.info(
                        f"Синхронизация родительских категорий завершена за {(datetime.now() - start_time).total_seconds():.1f}s")

                    # 2. Синхронизация подкатегорий
                    logger.info("Синхронизация категорий")
                    start_time = datetime.now()
                    await self._sync_categories(cursor, tree_data['categories'], tree_data['root_categories'],
                                                statistics, category_id_mapping, root_id_mapping)
                    logger.info(
                        f"Синхронизация категорий завершена за {(datetime.now() - start_time).total_seconds():.1f}s")

                    logger.info(f"Завершили синхронизацию")

                    await conn.commit()

                    # Подсчитываем общее количество операций (только числа, не списки)
                    total_operations = (
                            statistics['updated_roots'] + statistics['updated_categories'] +
                            statistics['created_roots'] + statistics['created_categories']
                    )

                    return {
                        "status": "success",
                        "message": f"Выполнена синхронизация категорий: {total_operations} операций",
                        "statistics": statistics
                    }

        except Exception as e:
            logger.error(f"Ошибка синхронизации категорий: {str(e)}", exc_info=True)

            return {
                "status": "error",
                "error": str(e),
                "statistics": {}
            }

    async def _sync_root_categories(self, cursor, root_categories: Dict[int, str], statistics: Dict,
                                    root_id_mapping: Dict):
        """Синхронизация корневых категорий"""
        total_roots = len(root_categories)
        logger.info(f"Обработка {total_roots} родительских категорий")

        for i, (root_id, root_name) in enumerate(root_categories.items(), 1):
            root_id = int(root_id)
            # Проверяем существование записи
            await cursor.execute(f"SELECT id, type_name FROM {self.table} WHERE type_id = ? AND parent_id IS NULL",
                                 (root_id,))
            existing = await cursor.fetchone()

            if existing:
                existing_db_id, existing_name = existing
                root_id_mapping[root_id] = existing_db_id

                # Проверяем, нужно ли обновить имя
                if existing_name != root_name:
                    await cursor.execute(f"""
                        UPDATE {self.table} 
                        SET type_name = ?, last_updated = GETDATE()
                        WHERE id = ?
                    """, (root_name, existing_db_id))

                    statistics['updated_roots'] += 1
                    statistics['name_changes_roots'] += 1
                    statistics['renamed_roots'].append({
                        'id': root_id,
                        'old_name': existing_name,
                        'new_name': root_name
                    })
                    logger.info(f"Обновили родительскую категорию {root_id}: '{existing_name}' → '{root_name}'")
                else:
                    # Обновляем только last_updated
                    await cursor.execute(f"UPDATE {self.table} SET last_updated = GETDATE() WHERE id = ?",
                                         (existing_db_id,))
                    logger.info(f"Для родительской категории {root_id} - {root_name} обновили только дату last_updated")
            else:
                # Создаем новую запись
                await cursor.execute(f"""
                    INSERT INTO {self.table} (type_id, type_name, parent_id, carville_cat, last_updated)
                    VALUES (?, ?, NULL, NULL, GETDATE())
                """, (root_id, root_name,))

                await cursor.execute("SELECT @@IDENTITY")
                new_id = (await cursor.fetchone())[0]
                root_id_mapping[root_id] = new_id
                statistics['created_roots'] += 1
                statistics['new_roots'].append({
                    'id': root_id,
                    'name': root_name
                })
                logger.info(f"Создана родительская категория {root_id} - {root_name} (ID: {new_id})")

            # Логируем прогресс каждые 10 записей или в конце
            if i % 10 == 0 or i == total_roots:
                logger.info(f"Обновление родительских категорий: {i}/{total_roots} ({i / total_roots * 100:.1f}%)")

    async def _sync_categories(self, cursor, categories: Dict[int, Dict], root_categories: Dict[int, str],
                               statistics: Dict, category_id_mapping: Dict, root_id_mapping: Dict):
        """Синхронизация подкатегорий"""
        total_categories = len(categories)
        logger.info(f"Обработка {total_categories} категорий")

        for i, (category_id, category_data) in enumerate(categories.items(), 1):
            category_id = int(category_id)
            category_name = category_data['name']
            parent_root_id = category_data['parent_id']

            # Получаем parent_id из маппинга корневых категорий
            parent_db_id = root_id_mapping.get(parent_root_id)
            if not parent_db_id:
                logger.warning(f"Родительская категория {parent_root_id} не найдена для {category_id}")
                continue

            # Проверяем существование записи с правильным parent_id
            await cursor.execute(f"""
                SELECT id, type_name FROM {self.table} 
                WHERE type_id = ? AND parent_id = ?
            """, (category_id, parent_db_id))
            existing = await cursor.fetchone()

            if existing:
                existing_db_id, existing_name = existing
                category_id_mapping[category_id] = existing_db_id

                # Проверяем, нужно ли обновить имя
                if existing_name != category_name:
                    await cursor.execute(f"""
                        UPDATE {self.table} 
                        SET type_name = ?, last_updated = GETDATE()
                        WHERE id = ?
                    """, (category_name, existing_db_id))

                    statistics['updated_categories'] += 1
                    statistics['name_changes_categories'] += 1
                    # Получаем имя корневой категории
                    root_name = root_categories.get(parent_root_id, f"Unknown ({parent_root_id})")
                    statistics['renamed_categories'].append({
                        'id': category_id,
                        'old_name': existing_name,
                        'new_name': category_name,
                        'parent_root_id': parent_root_id,
                        'parent_root_name': root_name
                    })
                    logger.info(f"Обновили категорию {category_id}: '{existing_name}' → '{category_name}'")
                else:
                    # Обновляем только last_updated
                    await cursor.execute(f"UPDATE {self.table} SET last_updated = GETDATE() WHERE id = ?",
                                         (existing_db_id,))
                    logger.info(
                        f"Для категории {category_id} - {category_name} обновили только дату last_updated")
            else:
                # Создаем новую запись
                await cursor.execute(f"""
                    INSERT INTO {self.table} (type_id, type_name, parent_id, carville_cat, last_updated)
                    VALUES (?, ?, ?, NULL, GETDATE())
                """, (category_id, category_name, parent_db_id))

                await cursor.execute("SELECT @@IDENTITY")
                new_id = (await cursor.fetchone())[0]
                category_id_mapping[category_id] = new_id
                statistics['created_categories'] += 1
                # Получаем имя корневой категории
                root_name = root_categories.get(parent_root_id, f"Unknown ({parent_root_id})")
                statistics['new_categories'].append({
                    'id': category_id,
                    'name': category_name,
                    'parent_root_id': parent_root_id,
                    'parent_root_name': root_name
                })
                logger.info(
                    f"Создали категорию {category_id} - {category_name} (ID: {new_id}, ID родительской: {parent_db_id})")

            # Логируем прогресс каждые 50 записей или в конце
            if i % 50 == 0 or i == total_categories:
                logger.info(f"Обновление категорий: {i}/{total_categories} ({i / total_categories * 100:.1f}%)")

    async def get_outdated_records(self) -> Dict[str, Any]:
        """Получает записи, которые не обновлялись более указанного количества часов"""
        try:
            async with self.sql.get_connection() as conn:
                async with conn.cursor() as cursor:
                    # Записи старше указанного количества часов и минут
                    outdated_hours = settings.OUTDATED_RECORDS_HOURS
                    outdated_minutes = settings.OUTDATED_RECORDS_MINUTES
                    cutoff_date = datetime.now() - timedelta(hours=outdated_hours, minutes=outdated_minutes)

                    # Корневые категории (parent_id IS NULL)
                    await cursor.execute(f"""
                        SELECT type_id, type_name FROM {self.table} 
                        WHERE parent_id IS NULL AND last_updated < ?
                    """, (cutoff_date,))
                    outdated_roots_data = await cursor.fetchall()
                    outdated_roots = len(outdated_roots_data)

                    # Подкатегории с полным путем (parent_id ссылается на корневую категорию)
                    await cursor.execute(f"""
                        SELECT c.type_id, c.type_name, r.type_id as root_id, r.type_name as root_name
                        FROM {self.table} c
                        INNER JOIN {self.table} r ON c.parent_id = r.id
                        WHERE r.parent_id IS NULL AND c.last_updated < ?
                    """, cutoff_date)
                    outdated_categories_data = await cursor.fetchall()
                    outdated_categories = len(outdated_categories_data)

                    statistics = {
                        'outdated_roots': outdated_roots,
                        'outdated_categories': outdated_categories,
                        'outdated_roots_list': [{
                            'id': type_id,
                            'name': type_name
                        } for type_id, type_name in outdated_roots_data],
                        'outdated_categories_list': [{
                            'id': cat_id,
                            'name': cat_name,
                            'parent_root_id': root_id,
                            'parent_root_name': root_name
                        } for cat_id, cat_name, root_id, root_name in outdated_categories_data],
                    }

                    # Формируем строку времени для лога
                    time_str = f"{outdated_hours}h" if outdated_minutes == 0 else f"{outdated_hours}h {outdated_minutes}m"
                    logger.info(
                        f"Проверка устаревших записей (>{time_str}, {cutoff_date}): {outdated_roots} родительских, {outdated_categories} категорий")

                    return {
                        "status": "success",
                        "statistics": statistics
                    }

        except Exception as e:
            logger.error(f"Ошибка подсчета устаревших записей: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "statistics": {}
            }

    async def get_types_tree(self) -> WbTypesTreeResponse:
        """
        Получить дерево категорий и типов товаров Wb

        Returns:
            WbTypesTreeResponse - структурированное дерево категорий
        """
        try:
            logger.info("Получаем дерево категорий Wb с Базы данных Carville")

            # Получаем все записи из БД
            raw_data = await self._fetch_all_types()

            # Строим дерево в памяти
            tree = self._build_tree(raw_data)

            logger.info(f"Успешно построили дерево категорий: {len(tree.root_categories)} корневых категорий")
            return tree

        except Exception as e:
            logger.error(f"Оштбка построения дерева категорий: {str(e)}")
            raise

    async def _fetch_all_types(self) -> List[Dict[str, Any]]:
        """Получить все записи из таблицы, исключая устаревшие записи"""
        max_age_hours = settings.WB_TYPES_MAX_AGE_HOURS

        query = f"""
        SELECT 
            id,
            type_id,
            type_name,
            parent_id,
            carville_cat,
            last_updated
        FROM {self.table}
        WHERE last_updated >= DATEADD(HOUR, -{max_age_hours}, GETDATE())
        ORDER BY parent_id, type_name
        """

        try:
            # Используем query_timeout=5 секунд для защиты от длинных блокировок
            async with self.sql.get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query)
                    rows = await cursor.fetchall()

                    # Преобразуем в список словарей
                    result = []
                    for row in rows:
                        result.append({
                            'id': row[0],
                            'type_id': row[1],
                            'type_name': row[2],
                            'parent_id': row[3],
                            'carville_cat': bool(row[4]) if row[4] is not None else False,
                            'last_updated': row[5]
                        })

                    logger.info(
                        f"Извлечено {len(result)} записей из таблицы {self.table} (отфильтровано: last_updated >= {max_age_hours} часов)")
                    return result
        except DatabaseError:
            # Пробрасываем DatabaseError наверх
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при извлечении типов из бд: {str(e)}")
            raise DatabaseError(f"Ошибка извлечения типов из бд: {str(e)}")

    def _build_tree(self, raw_data: List[Dict[str, Any]]) -> WbTypesTreeResponse:
        """Построить дерево категорий из плоских данных"""

        # Группируем по parent_id
        by_parent = defaultdict(list)
        for item in raw_data:
            parent_id = item['parent_id']
            by_parent[parent_id].append(item)

        # Строим дерево
        root_categories = []

        # Корневые категории (parent_id = None)
        root_items = by_parent.get(None, [])
        for root_item in root_items:
            root_category = CategoryResponse(
                type_id=root_item['type_id'],
                name=root_item['type_name'],
                categories=[]
            )

            type_items = by_parent.get(root_item['id'], [])
            for type_item in type_items:
                wb_type = WbTypeResponse(
                    type_id=type_item['type_id'],
                    name=type_item['type_name'],
                    carville_cat=type_item['carville_cat']
                )
                root_category.categories.append(wb_type)

            root_categories.append(root_category)

        return WbTypesTreeResponse(root_categories=root_categories)
