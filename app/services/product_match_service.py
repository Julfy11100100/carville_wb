import json
from typing import Dict, Any, List, Optional

from app.exceptions.sql_database import DatabaseError
from app.services.elasticsearch_service import ElasticsearchService
from app.services.sql_repository import SQLDatabaseRepository
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


class ProductMatchService:
    """Сервис для сопоставления товаров с товарами в БД"""
    COMPARISON_FIELDS_MAP = {
        "title": "name"
    }  # процедура в бд ожидает другие имена

    def __init__(self, elasticsearch_service: ElasticsearchService, database_service: SQLDatabaseRepository):
        self.elasticsearch_service = elasticsearch_service
        self.database_service = database_service

    async def match_products(
            self,
            token: str,
            match_field: str,
            carville_match_field: str,
            categories: List[int],
            comparison_field: str,
            brand: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Сопоставить товары WB с товарами в БД

        Args:
            token: token клиента
            match_field: Поле для сопоставления в WB (например, 'VendorCode')
            carville_match_field: Поле для сопоставления в БД (например, 'code', 'bar_code')
            categories: Список категорий
            comparison_field: Поле для сравнения (например, 'name')
            brand: ID бренда для фильтрации (опционально, преобразуется в строку для процедуры)
        Returns:
            Dict с результатами сопоставления
        """
        try:
            logger.info(f"Начало сопоставления товаров для клиента {token}...")
            logger.info(
                f"Параметры: match_field='{match_field}', db_field='{carville_match_field}', "
                f"categories={categories}, comparison_field='{comparison_field}', brand='{brand}'"
            )

            # 1. Получаем товары из Elasticsearch
            logger.info("Получение товаров из Elasticsearch...")
            products = await self._get_products_from_elasticsearch(
                token=token,
                categories=categories,
                match_field=match_field,
                comparison_field=comparison_field
            )

            total_products_count = len(products)

            if not products:
                logger.warning(f"Товары не найдены в категориях {categories}")
                return {
                    "status": "success",
                    "message": "No products found in the specified category",
                    "products": [],
                    "total_products": 0,
                    "matched_products": 0,
                }

            logger.info(f"Найдено {len(products)} товаров в Elasticsearch")

            # 2. Подготавливаем список
            logger.info("Подготовка списка значений...")
            values = self._prepare_values(products, match_field)

            if not values:
                logger.warning("Уникальные значения для сопоставления не найдены")
                return {
                    "status": "success",
                    "message": "No unique values found for matching",
                    "products": [],
                    "total_products": total_products_count,
                    "matched_products": 0,
                }

            logger.info(f"Подготовлено {len(values)} уникальных значений для сопоставления")

            # 3. Вызываем процедуру сопоставления
            logger.info("Вызов процедуры сопоставления в БД...")
            # Маппинг поля сравнения для процедуры: 'name' -> 'name'
            comparison_field_for_proc = self.COMPARISON_FIELDS_MAP[comparison_field]
            if comparison_field_for_proc != comparison_field:
                logger.info(
                    f"Поле сравнения '{comparison_field}' преобразовано в '{comparison_field_for_proc}' "
                    "для вызова процедуры"
                )

            procedure_results = await self._call_matching_procedure(
                brand=brand,
                carville_match_field=carville_match_field,
                comparison_field=comparison_field_for_proc,
                values=values
            )

            # Тестовый вывод первых 10 записей результата процедуры
            if procedure_results:
                sample_len = min(10, len(procedure_results))
                logger.info(f"Пример результатов процедуры ({sample_len} из {len(procedure_results)}):")
                for i in range(sample_len):
                    logger.info(f"   {i + 1}. {procedure_results[i]}")

            result_products = []

            logger.info(f"Процедура вернула {len(procedure_results)} результатов")

            # Построим индекс по значению поля сопоставления из Elasticsearch: {match_value -> [product, ...]}
            # Храним списком на случай дубликатов (штрихкоды и т.п.)
            es_index: Dict[str, List[Dict[str, Any]]] = {}
            for product in products:
                values = self._get_match_values(product, match_field)
                if values is None:
                    continue

                for value in values:
                    key = str(value).strip()
                    if not key:
                        continue
                    es_index.setdefault(key, []).append(product)

            # Пройдём по результатам процедуры и сопоставим с ES по Ident_tov
            matched_count = 0
            for row in procedure_results:
                try:
                    ident_val_raw = row.get("Ident_tov")
                    recommend_val = row.get("Recommend")
                    recommend_type = row.get("Recommend_name_type", None)
                    # Отфильтруем пустые рекомендации, чтобы удовлетворять схеме ответа
                    if ident_val_raw is None or recommend_val is None:
                        continue
                    ident_key = str(ident_val_raw).strip()
                    if not ident_key:
                        continue

                    es_products = es_index.get(ident_key)
                    if not es_products:
                        continue

                    for es_prod in es_products:
                        nmid_val = es_prod.get("nmID")
                        # Берём текущее значение по полю сравнения из Elasticsearch
                        comparison_val = es_prod.get(comparison_field)
                        if comparison_val is None:
                            # Пропускаем позиции без текущего значения по полю сравнения
                            continue

                        # Нормализуем строки для сравнения
                        def _norm(v):
                            if isinstance(v, str):
                                return v.strip().lower()
                            return v

                        is_equal = _norm(comparison_val) == _norm(recommend_val)

                        if not is_equal:
                            result_products.append({
                                "nm_id": nmid_val,
                                "identifier_value": ident_key,
                                "carville_value": recommend_val,
                                "wb_value": comparison_val,
                                "recommend_type": recommend_type
                            })
                            matched_count += 1

                except Exception as map_err:
                    logger.error(f"Ошибка при обработке строки: {map_err}")
                    continue

            logger.info(f"Сопоставление завершено успешно: {matched_count} сопоставленных товаров")

            return {
                "status": "success",
                "message": f"Procedure returned {len(procedure_results)} results; matched {matched_count}",
                "products": result_products,
                "total_products": total_products_count,
                "matched_products": matched_count,
            }

        except Exception as e:
            logger.error(f"Ошибка при сопоставлении товаров: {str(e)}")
            raise

    async def _get_products_from_elasticsearch(
            self,
            token: str,
            categories: List[int],
            match_field: str,
            comparison_field: str
    ) -> List[Dict[str, Any]]:
        """Получить товары из Elasticsearch по категории"""

        # Определяем поля для запроса
        fields_to_fetch = ["nmID", comparison_field]

        # Добавляем match_field\characteristics
        if match_field.startswith("characteristics"):
            fields_to_fetch.append("characteristics")
        else:
            fields_to_fetch.append(match_field)
        # Убираем дубликаты полей
        fields_to_fetch = list(set(fields_to_fetch))

        try:
            result = await self.elasticsearch_service.search_products(
                token=token,
                filters={"subjectID": categories},
                limit=50000,  # Получаем все товары в категории
                fields=fields_to_fetch
            )

            if result["status"] == "error":
                error_msg = result.get("error", "Unknown Elasticsearch error")
                logger.error(f"Ошибка Elasticsearch: {error_msg}")
                raise DatabaseError(f"Failed to fetch products from Elasticsearch: {error_msg}")

            products = result.get("products", [])
            logger.info(f"Elasticsearch вернул {len(products)} товаров")

            return products

        except Exception as e:
            logger.error(f"Ошибка при получении товаров из Elasticsearch: {str(e)}")
            raise DatabaseError(f"Elasticsearch operation failed: {str(e)}")

    def _prepare_values(self, products: List[Dict[str, Any]], match_field: str) -> List[Dict[str, str]]:
        """Подготовить список уникальных значений для процедуры"""

        unique_values = set()

        for product in products:
            values = self._get_match_values(product, match_field)
            for value in values:
                if value is not None:
                    unique_values.add(value)

        # Формируем список в формате [{"val": value}, ...]
        values = [{"val": value} for value in sorted(unique_values)]

        logger.info(f"Подготовлено {len(values)} уникальных значений из {len(products)} товаров")

        return values

    async def _call_matching_procedure(
            self,
            brand: Optional[int],
            carville_match_field: str,
            comparison_field: str,
            values: List[Dict[str, str]]
    ) -> List[Dict[str, Any]]:
        """Вызвать процедуру сопоставления в БД"""

        # Преобразуем список в JSON строку
        values_json = json.dumps(values, ensure_ascii=False)

        # Преобразуем brand из int в str для процедуры (если указан)
        brand_str = str(brand) if brand is not None else None

        # Формируем вызов процедуры
        # Явно указываем базу данных из конфига, чтобы избежать контекстных несоответствий
        procedure_call = f"""
        EXEC [{settings.DB_NAME}]..[aip_market_comparation]
            @Site = 'wb',
            @Brand = NULL,
            @Carville_match_field = ?,
            @Comparison_field = ?,
            @Values = ?
        """

        try:
            # Используем query_timeout=30 секунд для процедуры
            async with self.database_service.get_connection(query_timeout=30) as conn:
                async with conn.cursor() as cursor:
                    logger.info("Выполнение хранимой процедуры...")
                    logger.info("Параметры процедуры:")
                    logger.info(f"   Brand: {brand_str}")
                    logger.info(f"   Carville_match_field: {carville_match_field}")
                    logger.info(f"   Comparison_field: {comparison_field}")
                    logger.info(f"   Values count: {len(values)}")
                    logger.info(f"   Values example: {values[:3]}")

                    await cursor.execute(
                        procedure_call,
                        (
                            carville_match_field,
                            comparison_field,
                            values_json
                        )
                    )

                    # Переходим к первому результирующему набору (после возможных сообщений о количестве строк)
                    # В SQL Server без SET NOCOUNT ON могут приходить пустые наборы результатов
                    attempts = 0
                    while cursor.description is None:
                        attempts += 1
                        has_more = await cursor.nextset()
                        if not has_more:
                            logger.warning("Хранимая процедура не вернула наборы результатов")
                            return []
                        if attempts > 10:
                            logger.warning("Слишком много пустых наборов результатов от процедуры, прерывание разбора")
                            return []

                    # Теперь cursor.description описывает текущий набор — читаем строки
                    rows = await cursor.fetchall()

                    # Получаем названия колонок
                    columns = [column[0] for column in cursor.description] if cursor.description else []

                    # Преобразуем в список словарей
                    results = []
                    for row in rows:
                        row_dict = {}
                        for i, value in enumerate(row):
                            if i < len(columns):
                                row_dict[columns[i]] = value
                        results.append(row_dict)

                    logger.info(f"Процедура выполнена успешно, возвращено {len(results)} строк")

                    return results

        except Exception as e:
            logger.error(f"Ошибка при выполнении хранимой процедуры: {str(e)}")
            raise DatabaseError(f"Database procedure execution failed: {str(e)}")

    @staticmethod
    def _get_match_values(product: Dict[str, Any], match_field: str) -> Optional[List[str]]:
        """
        Получить значение для матчинга из документа Elasticsearch.
        """
        try:
            # Матчинг по плоскому полю
            if not isinstance(match_field, str) or not match_field.startswith("characteristics_"):
                field = product.get(match_field)
                if field:
                    if isinstance(field, List):
                        return [str(item).strip() for item in field if item]
                    return [str(field).strip()]
                return None

            # Матчинг по атрибуту: ожидаем формат 'characteristics_<id>'
            parts = match_field.split("_", 1)
            if len(parts) != 2:
                return None
            characteristic_id_str = parts[1]
            if not characteristic_id_str.isdigit():
                return None
            target_id = int(characteristic_id_str)

            characteristics = product.get("characteristics") or []
            for characteristic in characteristics:
                if not isinstance(characteristic, dict):
                    continue
                if characteristic.get("id") == target_id:
                    values = characteristic.get("value") or []
                    if values:
                        return [str(val).strip() for val in values if val]

            return None
        except Exception:
            return None
