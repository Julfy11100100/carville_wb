import json

from app.exceptions.sql_database import DatabaseError
from app.services.sql_repository import SQLDatabaseRepository
from app.utils.logging import get_logger
from app.utils.normalize import get_normalize_identifier
from config import settings

logger = get_logger()


class RecommendationService:

    def __init__(
            self,
            sql_repository: SQLDatabaseRepository,
    ):
        self.sql = sql_repository

    """Сервис для работы с рекомендациями из БД"""

    async def get_name_recommendations(self, vendor_codes: list[str]) -> dict:
        procedure_call = f"""
        EXEC [{settings.DB_NAME}]..[aip_market_comparation]
            @Site = 'wb',
            @Brand = ?,
            @Carville_match_field = ?,
            @Comparison_field = ?,
            @Values = ?
        """

        values_json = self._prepare_values_json(vendor_codes)

        return await self._get_recommendations(
            vendor_codes=vendor_codes,
            procedure_call=procedure_call,
            procedure_params=(None, 'code', 'name', values_json),
            id_field="Ident_tov",
            value_field="Recommend",
            recommendation_type="name"
        )

    async def get_description_recommendations(self, vendor_codes: list[str]) -> dict:
        procedure_call = f"""
        EXEC [{settings.DB_NAME}]..[aip_prod_description]
            @Brand = ?,
            @Match_field = ?,
            @Values = ?
        """

        values_json = self._prepare_values_json(vendor_codes)

        return await self._get_recommendations(
            vendor_codes=vendor_codes,
            procedure_call=procedure_call,
            procedure_params=(None, 'code', values_json),
            id_field="code",
            value_field="description",
            recommendation_type="description"
        )

    def _prepare_values_json(self, vendor_codes: list[str]) -> str:  # noqa
        """Подготовка JSON для значений"""
        unique_values = set()
        for vendor_code in vendor_codes:
            if vendor_code is not None and str(vendor_code).strip():
                unique_values.add(str(vendor_code).strip())

        values = [{"val": value} for value in sorted(unique_values)]
        return json.dumps(values, ensure_ascii=False)

    async def _get_recommendations(
            self,
            vendor_codes: list[str],
            procedure_call: str,
            procedure_params: tuple,
            id_field: str,  # Ident_tov | code
            value_field: str,  # Recommend | description
            recommendation_type: str  # name | description
    ) -> dict:
        """Общий метод для получения рекомендаций"""
        total_vendor_codes = len(vendor_codes)
        logger.info(f"Получение рекомендаций {recommendation_type} для {total_vendor_codes} vendor_codes")

        # Инициализируем результаты
        recommendations = {vendor_code: None for vendor_code in vendor_codes}

        # Подготовка данных
        unique_values, vendor_code_index = self._prepare_vendor_codes(vendor_codes)

        if not unique_values:
            logger.warning(f"Нет валидных vendor_codes для рекомендаций {recommendation_type}")
            return recommendations

        # Вызов процедуры
        procedure_results = await self._execute_recommendation_procedure(procedure_call, procedure_params)

        # Построение индексов
        if procedure_results:
            procedure_index = self._build_recommendations_index(procedure_results, id_field, value_field)
        else:
            procedure_index = {}

        # Формирование итогового словаря
        found_count = 0
        not_found_count = 0
        not_found_count_offers_ids = []

        for normalized_vendor_code, vendor_code in vendor_code_index.items():
            if normalized_vendor_code in procedure_index:
                recommendations[vendor_code] = procedure_index[normalized_vendor_code]
                found_count += 1
            else:
                not_found_count += 1
                not_found_count_offers_ids.append(vendor_code)

        logger.info(
            f"Статистика рекомендаций {recommendation_type}: "
            f"всего={total_vendor_codes}, найдено={found_count}, не_найдено={not_found_count}"
        )
        logger.info(
            f"Первые 10 offer ids без рекомендаций {recommendation_type}: "
            f"{not_found_count_offers_ids[:10]}"
        )

        return recommendations

    async def _execute_recommendation_procedure(self, procedure_name: str, params: tuple) -> list[dict]:  # noqa
        """Общая логика выполнения процедуры БД"""

        procedure_short_name = procedure_name.split("\n")[1]
        logger.info(f"Выполнение хранимой процедуры {procedure_short_name}")
        try:
            async with self.sql.get_connection(query_timeout=40) as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        procedure_name,
                        params
                    )

                    # Общая логика обработки результатов
                    attempts = 0
                    while cursor.description is None:
                        attempts += 1
                        has_more = await cursor.nextset()
                        if not has_more:
                            logger.warning("Хранимая процедура не вернула наборы результатов")
                            return []
                        if attempts > 10:
                            logger.warning(
                                "Слишком много пустых наборов результатов от хранимой процедуры, прерываем парсинг")
                            return []

                    rows = await cursor.fetchall()
                    columns = [column[0] for column in cursor.description] if cursor.description else []

                    # Преобразуем в список словарей
                    procedure_results = []
                    for row in rows:
                        row_dict = {}
                        for i, value in enumerate(row):
                            if i < len(columns):
                                row_dict[columns[i]] = value
                        procedure_results.append(row_dict)

                    procedure_short_name = procedure_name.split("\n")[1]
                    msg = f"Процедура {procedure_short_name} выполнена успешно, возвращено {len(procedure_results)} строк"
                    logger.info(msg)
                    return procedure_results

        except Exception as e:
            logger.error(f"Ошибка выполнения хранимой процедуры {procedure_name}: {str(e)}")
            raise DatabaseError(f"Не удалось выполнить процедуру базы данных: {str(e)}")

    def _prepare_vendor_codes(self, vendor_codes: list[str]) -> tuple[set, dict]:  # noqa
        """Подготовка vendor_codes и создание индекса для нормализации"""
        unique_values = set()
        vendor_code_index = {}

        for vendor_code in vendor_codes:
            if vendor_code is not None and str(vendor_code).strip():
                stripped_id = str(vendor_code).strip()
                unique_values.add(stripped_id)

                # Создаем индекс normalized -> original
                normalized = get_normalize_identifier(vendor_code)
                if normalized:
                    vendor_code_index[normalized] = vendor_code

        return unique_values, vendor_code_index

    @staticmethod
    def _build_recommendations_index(procedure_results: list[dict], id_field: str,
                                     value_field: str) -> dict:  # noqa
        """Построение индекса рекомендаций из результатов процедуры"""
        index = {}
        for row in procedure_results:
            ident_raw = row.get(id_field)
            value = row.get(value_field)
            if ident_raw is not None and value is not None and str(value).strip():
                normalized_ident = get_normalize_identifier(ident_raw)
                if normalized_ident:
                    index[normalized_ident] = value
        return index
