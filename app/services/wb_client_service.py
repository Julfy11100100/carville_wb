import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Set

from app.constants.brands import BRANDS
from app.exceptions.wb_api import WildberriesRateLimitError
from app.schemas.product_create import ProductCreateItem
from app.schemas.task import TaskStatus, TaskInfo
from app.services.elasticsearch_service import ElasticsearchService
from app.services.npr_product_service import NprProductService
from app.services.recommendation_service import RecommendationService
from app.services.sql_category_service import SqlCategoryService
from app.services.task_manager import TaskManager
from app.services.wb_api_service import WildberriesAPI
from app.utils.logging import get_logger
from app.utils.token import hash_token
from config import settings

logger = get_logger()


class WildberriesClient:
    """
    Клиент для работы с Wildberries.
    Объединяет WildberriesAPI, TaskManager, ElasticsearchService и бизнес-логику.
    """

    def __init__(
            self,
            task_manager: TaskManager,
            elasticsearch_service: ElasticsearchService,
            api_client: WildberriesAPI,
            sql_category_service: SqlCategoryService,
            recommendation_service: RecommendationService,
            npr_product_service: NprProductService,
    ):
        """
        Args:
            task_manager: Менеджер задач для фоновых операций
            elasticsearch_service: Сервис для работы с Elasticsearch
            api_client: Сервис запросов к WB
        """
        # Бизнес-сервисы
        self.task_manager = task_manager
        self.elasticsearch_service = elasticsearch_service
        self.recommendation_service = recommendation_service
        self.npr_product_service = npr_product_service

        # HTTP API клиент
        self.api = api_client

        # SQL сервис
        self.category_service = sql_category_service

    async def get_product(self, token: str) -> Dict[str, Any]:
        """Получение одного товара"""
        return await self.api.get_product(token)

    async def collect_products_background(self, token: str, task_info: TaskInfo):
        """Фоновая задача для получения всех товаров с батч-индексацией"""
        try:
            task_info.status = TaskStatus.RUNNING
            await self.task_manager.save_task(task_info)

            batch_products = []
            category_ids: Set[int] = set()
            cursor = {}
            total_collected = 0
            batch_size = 1000  # Размер батча для индексации

            logger.info(f"Начало сбора товаров: {task_info.task_id}")
            # Создаём индекс в эластике
            await self.elasticsearch_service.create_index(hash_token(token))
            while True:
                try:
                    response = await self.api.get_products_page(token, limit=100, cursor=cursor)
                    cards = response.get("cards", [])

                    cards = [card for card in cards if card.get("brand", None) in BRANDS]

                    if not cards:
                        break

                    # Извлекаем ID категорий
                    category_ids.update(
                        card.get("subjectID")
                        for card in cards
                        if card.get("subjectID")
                    )

                    batch_products.extend(cards)
                    total_collected += len(cards)

                    # Индексируем батч при достижении лимита
                    if len(batch_products) >= batch_size:
                        await self.elasticsearch_service.index_products(
                            token=hash_token(token),
                            products=batch_products
                        )
                        logger.info(
                            f"Проиндексирован батч: {len(batch_products)} товаров, "
                            f"всего обработано: {total_collected}"
                        )
                        batch_products.clear()  # Очищаем память

                    # Обновляем курсор
                    cursor = response.get("cursor", {})
                    if not cursor or len(cards) < cursor.get("limit", 100):
                        break

                    # Обновляем прогресс
                    task_info.total_items = total_collected
                    await self.task_manager.save_task(task_info)

                    logger.debug(
                        f"Прогресс сбора: {task_info.task_id}, "
                        f"собрано={total_collected}, батч={len(cards)}, "
                        f"в буфере={len(batch_products)}"
                    )

                    await asyncio.sleep(0.1)

                except WildberriesRateLimitError:
                    logger.warning(f"Rate limit для {task_info.task_id}, ожидание 60с...")
                    await asyncio.sleep(60)
                    continue

            # Индексируем остатки, если есть
            if batch_products:
                await self.elasticsearch_service.index_products(
                    token=hash_token(token),
                    products=batch_products
                )
                logger.info(
                    f"Проиндексирован финальный батч: {len(batch_products)} товаров, "
                    f"всего обработано: {total_collected}"
                )
                batch_products.clear()

            # Получаем из бд список [{id родительской: id категории}]
            full_category_ids = await self.category_service.get_parents_category_by_id_categories(list(category_ids))

            task_info.status = TaskStatus.COMPLETED
            task_info.completed_at = datetime.now()
            task_info.total_items = total_collected
            task_info.categories_count = len(category_ids)
            task_info.category_ids = full_category_ids

            logger.info(
                f"Сбор товаров завершён: {task_info.task_id}, "
                f"всего={total_collected}, категорий={len(category_ids)}"
            )

        except Exception as e:
            task_info.status = TaskStatus.FAILED
            task_info.completed_at = datetime.now()
            task_info.error = str(e)

            logger.error(f"Ошибка при сборе товаров {task_info.task_id}: {e}", exc_info=True)

        finally:
            await self.task_manager.save_task(task_info)

    async def update_products_background(
            self,
            token: str,
            field: str,
            updates: Dict[int, Any],
            task_info: TaskInfo
    ):
        """
        Фоновая задача для обновления карточек товаров с гарантированной проверкой.
        1. Извлекает карточки из Elasticsearch пакетами по 1000 (только с отличающимся значением поля)
        2. Обновляет поле на новое значение
        3. Сразу отправляет каждый пакет на обновление в WB
        4. Проверяет результаты с поллингом
        5. Обновляет только успешно обновленные карточки в Elasticsearch

        Args:
            token: API токен
            field: Поле для обновления
            updates: Словарь {nm_id: new_value}
            task_info: Информация о задаче
        """
        try:
            task_info.status = TaskStatus.RUNNING
            task_info.total_items = len(updates)

            nm_ids = list(updates.keys())
            hashed_token = hash_token(token)

            # Маппинг vendorCode -> nmID (1:1)
            vendor_code_to_nm_id = {}
            nm_id_to_vendor_code = {}

            task_info.metadata = {
                "nm_ids": nm_ids,
                "update_field": field,
                "update_started_at": datetime.now().isoformat(),
                "update_completed_at": None,
                "check_results": None,
                "polling_attempts": 0,
                "final_error_count": None,
                "final_success_count": None,
                "elasticsearch_update_count": 0,
                "batches_sent": 0,
                "skipped_without_changes": 0,
                "vendor_code_to_nm_id": {}  # Добавляем маппинг
            }

            await self.task_manager.save_task(task_info)

            logger.info(
                f"Начало обновления товаров: {task_info.task_id}, "
                f"всего={len(updates)}, поле={field}"
            )

            # 1. Извлекаем и отправляем пакетами по 3000
            batch_size = 3000
            batches_sent = 0
            skipped_count = 0

            for i in range(0, len(nm_ids), batch_size):
                batch_nm_ids = nm_ids[i:i + batch_size]

                # Создаем словарь ожидаемых значений для этого батча
                batch_updates = {nm_id: updates[nm_id] for nm_id in batch_nm_ids}

                logger.info(
                    f"Загрузка пакета {batches_sent + 1}: nmID {batch_nm_ids[0]}-{batch_nm_ids[-1]} "
                    f"({len(batch_nm_ids)} товаров)"
                )

                # Загружаем только товары с отличающимся значением поля
                batch_products = await self.elasticsearch_service.find_products_with_different_value(
                    token=hashed_token,
                    field=field,
                    expected_values=batch_updates
                )

                # Учитываем пропущенные товары (без изменений)
                batch_skipped = len(batch_nm_ids) - len(batch_products)
                if batch_skipped > 0:
                    skipped_count += batch_skipped
                    logger.info(
                        f"Пакет {batches_sent + 1}: {batch_skipped} товаров уже имеют нужное значение, пропущены"
                    )

                if not batch_products:
                    logger.warning(
                        f"Пакет {batches_sent + 1} пуст (все товары имеют актуальное значение), пропускаем отправку"
                    )
                    await asyncio.sleep(0.1)
                    continue

                # Обновляем поле на новое значение в каждой карточке
                for product in batch_products:
                    nm_id = product.get("nmID")
                    vendor_code = product.get("vendorCode")

                    if nm_id in batch_updates:
                        old_value = product.get(field)
                        new_value = batch_updates[nm_id]
                        product[field] = new_value

                        # Строим маппинг vendorCode <-> nmID (1:1)
                        if vendor_code:
                            vendor_code_to_nm_id[vendor_code] = nm_id
                            nm_id_to_vendor_code[nm_id] = vendor_code

                        logger.debug(
                            f"Обновлено поле '{field}' для nmID {nm_id} (vendorCode={vendor_code}): "
                            f"{old_value} -> {new_value}"
                        )

                logger.info(
                    f"Отправка пакета {batches_sent + 1} в WB: {len(batch_products)} товаров "
                    f"(из {len(batch_nm_ids)} в пакете)"
                )

                # Отправляем пакет в WB
                await self.api.update_products_chunked(
                    token,
                    batch_products,
                )

                batches_sent += 1
                task_info.metadata["batches_sent"] = batches_sent
                task_info.metadata["update_sent_at"] = datetime.now().isoformat()
                task_info.metadata["skipped_without_changes"] = skipped_count
                task_info.metadata["vendor_code_to_nm_id"] = vendor_code_to_nm_id
                await self.task_manager.save_task(task_info)

                logger.info(
                    f"Пакет {batches_sent} успешно отправлен: {len(batch_products)} товаров"
                )

                # Небольшая задержка между пакетами для избежания перегрузки
                await asyncio.sleep(1)

            logger.info(
                f"Все {batches_sent} пакетов отправлены в WB: {task_info.task_id}, "
                f"пропущено без изменений={skipped_count}, "
                f"уникальных vendorCode'ов={len(vendor_code_to_nm_id)}"
            )

            # 2. Проверяем результаты с поллингом
            result = await self._check_update_results_with_polling(
                token=token,
                task_info=task_info,
                vendor_code_to_nm_id=vendor_code_to_nm_id,
                max_attempts=5,
                initial_delay=10,
                retry_delay=15
            )

            # Сохраняем финальные результаты
            task_info.metadata["check_results"] = result
            task_info.metadata["update_completed_at"] = datetime.now().isoformat()
            task_info.processed_items = result.get("success_count", 0)

            error_nm_ids = set(result.get("error_nm_ids", []))
            success_nm_ids = set(nm_ids) - error_nm_ids

            logger.info(
                f"Результаты проверки: {task_info.task_id}, "
                f"успешно={len(success_nm_ids)}, ошибок={len(error_nm_ids)}, "
                f"попыток={result.get('polling_attempts', 0)}"
            )

            # 3. Обновляем в Elasticsearch только успешно обновленные карточки
            if success_nm_ids:
                # Создаем словарь успешных обновлений
                success_updates = {
                    nm_id: updates[nm_id]
                    for nm_id in success_nm_ids
                }

                # Отправляем пакетами по 1000 на обновление в Elasticsearch
                es_batch_size = 1000
                es_updated_total = 0
                es_failed_total = 0

                success_nm_ids_list = list(success_nm_ids)
                for es_i in range(0, len(success_nm_ids_list), es_batch_size):
                    es_batch_nm_ids = success_nm_ids_list[es_i:es_i + es_batch_size]
                    es_batch_updates = {
                        nm_id: success_updates[nm_id]
                        for nm_id in es_batch_nm_ids
                    }

                    elasticsearch_result = await self.elasticsearch_service.bulk_update_products(
                        token=hashed_token,
                        field=field,
                        updates=es_batch_updates
                    )

                    es_updated_total += elasticsearch_result.get("updated", 0)
                    es_failed_total += elasticsearch_result.get("failed", 0)

                    logger.info(
                        f"Elasticsearch пакет {es_i // es_batch_size + 1}: "
                        f"обновлено={elasticsearch_result.get('updated', 0)}, "
                        f"ошибок={elasticsearch_result.get('failed', 0)}"
                    )

                    await asyncio.sleep(0.1)

                task_info.metadata["elasticsearch_update_count"] = es_updated_total
                task_info.metadata["elasticsearch_update_errors"] = es_failed_total

                logger.info(
                    f"Elasticsearch обновлены: {task_info.task_id}, "
                    f"успешно={es_updated_total}, ошибок={es_failed_total}"
                )

            # 4. Определяем финальный статус
            if error_nm_ids:
                task_info.status = TaskStatus.COMPLETED_WITH_ERRORS
                task_info.metadata["final_error_count"] = len(error_nm_ids)
                task_info.metadata["final_success_count"] = len(success_nm_ids)

                logger.warning(
                    f"Обновление {task_info.task_id} завершено с ошибками: "
                    f"успешно={len(success_nm_ids)}, ошибок={len(error_nm_ids)}, "
                    f"попыток={result.get('polling_attempts', 0)}, "
                    f"пакетов_отправлено={batches_sent}, "
                    f"пропущено_без_изменений={skipped_count}, "
                    f"elasticsearch_обновлено={task_info.metadata['elasticsearch_update_count']}"
                )
            else:
                task_info.status = TaskStatus.COMPLETED
                task_info.metadata["final_error_count"] = 0
                task_info.metadata["final_success_count"] = len(success_nm_ids)

                logger.info(
                    f"Обновление {task_info.task_id} успешно: "
                    f"успешно={len(success_nm_ids)}, попыток={result.get('polling_attempts', 0)}, "
                    f"пакетов_отправлено={batches_sent}, "
                    f"пропущено_без_изменений={skipped_count}, "
                    f"elasticsearch_обновлено={task_info.metadata['elasticsearch_update_count']}"
                )

        except Exception as e:
            task_info.status = TaskStatus.FAILED
            task_info.completed_at = datetime.now()
            task_info.error = str(e)

            logger.error(
                f"Ошибка при обновлении товаров {task_info.task_id}: {e}",
                exc_info=True
            )

        finally:
            task_info.completed_at = datetime.now()
            await self.task_manager.save_task(task_info)

    async def _check_update_results_with_polling(
            self,
            token: str,
            task_info: TaskInfo,
            vendor_code_to_nm_id: Dict[str, int],
            max_attempts: int = 5,
            initial_delay: int = 10,
            retry_delay: int = 15,
            timeout_minutes: int = 5
    ) -> Dict[str, Any]:
        """
        Проверяет результаты обновления с поллингом.
        Вызывает проверку несколько раз, чтобы гарантировать все ошибки.

        Args:
            token: API токен
            task_info: Информация о задаче
            vendor_code_to_nm_id: Маппинг vendorCode -> nmID (1:1)
            max_attempts: Максимальное количество попыток
            initial_delay: Первая задержка (сек)
            retry_delay: Задержка между попытками (сек)
            timeout_minutes: Максимальное время ожидания (мин)

        Returns:
            Результаты с информацией об ошибках
        """
        vendor_codes = set(vendor_code_to_nm_id.keys())
        total_products = task_info.total_items or 0

        if not vendor_codes:
            return {
                "checked": False,
                "reason": "Нет vendorCode в маппинге",
                "polling_attempts": 0
            }

        previous_error_count = -1
        stable_count = 0
        attempt = 0
        timeout_deadline = datetime.now() + timedelta(minutes=timeout_minutes)

        await asyncio.sleep(initial_delay)

        logger.info(
            f"Начало поллинга результатов: {task_info.task_id}, "
            f"макс_попыток={max_attempts}, отслеживаем {len(vendor_codes)} vendorCode'ов"
        )

        while attempt < max_attempts:
            if datetime.now() > timeout_deadline:
                logger.warning(f"Таймаут поллинга {task_info.task_id} ({timeout_minutes} мин)")
                break

            attempt += 1

            try:
                logger.debug(f"Проверка результатов {task_info.task_id}: попытка {attempt}/{max_attempts}")

                all_errors_result = await self.api.get_all_errors_for_update(token)
                all_errors = all_errors_result.get("error_batches")

                stats = self._calculate_error_statistics(
                    all_errors,
                    vendor_code_to_nm_id,
                    total_products
                )

                current_error_count = stats["error_count"]

                logger.info(
                    f"Результаты проверки {task_info.task_id}: "
                    f"попытка={attempt}, ошибок={current_error_count}, успешно={stats['success_count']}"
                )

                if current_error_count == previous_error_count:
                    stable_count += 1

                    if stable_count >= 2:
                        logger.info(
                            f"Результаты стабильны {task_info.task_id}: "
                            f"попытка={attempt}, ошибок={current_error_count}"
                        )

                        return {
                            "checked": True,
                            "success_count": stats["success_count"],
                            "error_count": current_error_count,
                            "success_rate": stats["success_rate"],
                            "error_nm_ids": list(stats["error_nm_ids"]),
                            "error_vendor_codes": list(stats["error_vendor_codes"]),
                            "error_details": stats["error_details"],
                            "error_batches_count": len(stats["error_batches"]),
                            "polling_attempts": attempt,
                            "stable": True
                        }
                else:
                    stable_count = 0
                    previous_error_count = current_error_count

                if attempt == max_attempts:
                    logger.warning(
                        f"Исчерпаны все попытки {task_info.task_id}: ошибок={current_error_count}"
                    )

                    return {
                        "checked": True,
                        "success_count": stats["success_count"],
                        "error_count": current_error_count,
                        "success_rate": stats["success_rate"],
                        "error_nm_ids": list(stats["error_nm_ids"]),
                        "error_vendor_codes": list(stats["error_vendor_codes"]),
                        "error_details": stats["error_details"],
                        "error_batches_count": len(stats["error_batches"]),
                        "polling_attempts": attempt,
                        "stable": False,
                        "warning": "Результаты могут быть неполными (достигнут таймаут)"
                    }

                await asyncio.sleep(retry_delay)

            except Exception as e:
                logger.error(f"Ошибка при проверке {task_info.task_id} (попытка {attempt}): {e}", exc_info=True)

                if attempt == max_attempts:
                    return {
                        "checked": False,
                        "reason": f"Ошибка после {attempt} попыток: {e}",
                        "polling_attempts": attempt
                    }

                await asyncio.sleep(retry_delay)

        return {
            "checked": False,
            "reason": "Цикл поллинга исчерпан",
            "polling_attempts": attempt
        }

    @staticmethod
    def _filter_relevant_errors(
            all_errors: List[Dict[str, Any]],
            vendor_codes: Set[str]
    ) -> List[Dict[str, Any]]:
        """Фильтрует пакеты ошибок по vendorCode"""
        relevant = []
        for batch in all_errors:
            # Пытаемся получить vendorCodes разными способами
            batch_vendor_codes = batch.get("vendorCodes", [])

            if not batch_vendor_codes:
                # Если нет поля vendorCodes, берем ключи из errors
                batch_vendor_codes = list(batch.get("errors", {}).keys())

            if not batch_vendor_codes:
                # Если и там нет, берем из subjects
                batch_vendor_codes = list(batch.get("subjects", {}).keys())

            batch_vendor_codes_set = set(batch_vendor_codes)

            # Проверяем пересечение с нашими vendorCode'ами
            if batch_vendor_codes_set & vendor_codes:
                relevant.append(batch)

        return relevant

    def _calculate_error_statistics(
            self,
            all_errors: List[Dict[str, Any]],
            vendor_code_to_nm_id: Dict[str, int],
            total_products: int
    ) -> Dict[str, Any]:
        """
        Подсчитывает статистику ошибок по vendorCode с маппингом на nmID.

        Так как vendorCode уникален, каждый vendorCode -> ровно один nmID.
        """
        vendor_codes = set(vendor_code_to_nm_id.keys())
        relevant_errors = self._filter_relevant_errors(all_errors, vendor_codes)

        error_nm_ids = set()
        error_vendor_codes = set()
        error_details_by_vendor_codes = {}

        for batch in relevant_errors:
            batch_errors = batch.get("errors", {})
            batch_subjects = batch.get("subjects", {})

            for vendor_code, errors in batch_errors.items():
                # Получаем соответствующий nmID
                nm_id = vendor_code_to_nm_id.get(vendor_code)

                if nm_id is None:
                    # Если vendorCode не в нашем маппинге, пропускаем (не наша карточка)
                    logger.debug(f"Пропущен vendorCode={vendor_code} (не в списке отправленных)")
                    continue

                error_nm_ids.add(nm_id)
                error_vendor_codes.add(vendor_code)

                subject_info = batch_subjects.get(vendor_code, {})

                # Логируем детали
                logger.warning(
                    f"Ошибка для vendorCode={vendor_code} (nmID={nm_id}): {errors}"
                )

                # Сохраняем детали по vendor code
                error_details_by_vendor_codes[vendor_code] = {
                    "nm_id": str(nm_id),
                    "subject_id": subject_info.get("id"),
                    "subject_name": subject_info.get("name"),
                    "errors": errors
                }

        error_count = len(error_nm_ids)
        success_count = total_products - error_count
        success_rate = success_count / total_products if total_products > 0 else 0

        logger.info(
            f"Статистика ошибок: всего={total_products}, "
            f"успешно={success_count}, ошибок={error_count}, "
            f"уникальных vendorCode'ов с ошибками={len(error_vendor_codes)}"
        )

        return {
            "success_count": success_count,
            "error_count": error_count,
            "success_rate": success_rate,
            "error_nm_ids": error_nm_ids,
            "error_vendor_codes": error_vendor_codes,
            "error_details": error_details_by_vendor_codes,
            "error_batches": relevant_errors
        }

    async def get_all_errors_for_update(self, token: str, max_batches: int = 10000) -> List[Dict[str, Any]]:
        """Получает все пакеты ошибок для полноценного мониторинга."""
        all_error_batches = []
        cursor = {"limit": 100}
        iteration = 0

        logger.info("Начало сбора всех ошибок обновления")

        while iteration < max_batches:
            body = {
                "cursor": cursor,
                "order": {"ascending": True}
            }

            response = await self.api.make_request(
                "POST",
                "/content/v2/cards/error/list",
                token,
                json=body
            )

            data = response.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            all_error_batches.extend(items)

            response_cursor = data.get("cursor", {})
            if not response_cursor.get("next", False):
                break

            cursor = {
                "limit": 100,
                "updatedAt": response_cursor.get("updatedAt"),
                "batchUUID": response_cursor.get("batchUUID")
            }

            iteration += 1
            await asyncio.sleep(6)  # rate limit

        logger.info(f"Сбор ошибок завершен: найдено {len(all_error_batches)} пакетов ошибок")
        return all_error_batches

    async def create_products_background(
            self,
            token: str,
            products: List[ProductCreateItem],
            brand,
            task_info: TaskInfo
    ):
        """
        Фоновая задача для создания новых карточек товаров в WB.

        Процесс:
        1. Извлекает товары из Elasticsearch по списку vendor_code
        2. Трансформирует в формат WB (subjectID, vendorCode, sizes, skus)
        3. Отправляет пакеты вариантов в WB
        4. Проверяет результаты создания с поллингом
        5. Загружает картинки для успешно созданных товаров
        6. Сохраняет статус выполнения

        Args:
            token: API токен Wildberries
            products: Список ProductCreateItem с vendor_code товаров для создания
            brand: Бренд
            task_info: Информация о задаче для отслеживания прогресса
        """
        try:
            task_info.status = TaskStatus.RUNNING

            # Извлекаем vendor_codes из ProductCreateItem
            vendor_codes = [p.vendor_code for p in products]
            task_info.total_items = len(vendor_codes)
            task_info.metadata = {
                "vendor_codes": vendor_codes,
                "creation_started_at": datetime.now().isoformat(),
                "creation_completed_at": None,
                "check_started_at": None,
                "check_completed_at": None,
                "batches_sent": 0,
                "total_products_sent": 0,
                "elasticsearch_fetch_count": 0,
                "transformation_errors": 0,
                "check_results": None,
                "images_upload_status": None,
                "images_upload_started_at": None,
                "images_upload_completed_at": None,
                "images_upload_stats": None,
                "images_upload_summary": None,
                "images_upload_errors": None,
            }
            await self.task_manager.save_task(task_info)
            logger.info(
                f"[{task_info.task_id}] Начало создания товаров, "
                f"всего товаров={len(vendor_codes)}"
            )

            # ШАГ 1: ЗАГРУЗКА ТОВАРОВ ИЗ ELASTICSEARCH
            logger.info(f"[{task_info.task_id}] Загрузка товаров из Elasticsearch")
            es_products = await self._get_products_for_vendor_codes(vendor_codes)
            if not es_products:
                logger.warning(f"[{task_info.task_id}] Товары не найдены в Elasticsearch")
                task_info.status = TaskStatus.FAILED
                task_info.metadata["creation_completed_at"] = datetime.now().isoformat()
                await self.task_manager.save_task(task_info)
                return

            task_info.metadata["elasticsearch_fetch_count"] = len(es_products)
            logger.info(
                f"[{task_info.task_id}] Загружено из Elasticsearch: "
                f"запрашивали={len(vendor_codes)}, получили={len(es_products)}"
            )
            await self.task_manager.save_task(task_info)
            # Получаем картинки
            images = await self._get_images_for_vendor_codes(es_products)
            # Получаем рекомендации по именам и описанию
            rec_names = await self.recommendation_service.get_name_recommendations(vendor_codes)
            rec_descriptions = await self.recommendation_service.get_description_recommendations(vendor_codes)
            # Получаем кросы, оемы, bar_codes, prices
            npr_data = await self.npr_product_service.get_create_npr_data(vendor_codes)

            # Подготавливаем данные
            failed_clean_product = {}  # сбор ошибок данных
            prepare_products = []  # подготовленные продукты
            for vendor_code in vendor_codes:
                try:
                    clean_product = self._clean_product_document(
                        vendor_code, es_products[vendor_code],
                        rec_names[vendor_code], rec_descriptions[vendor_code],
                        npr_data.get(vendor_code, {}), images[vendor_code]
                    )
                    prepare_products.append(clean_product)
                except Exception as err:
                    msg = f"Товар {vendor_code} имеет неполные данные: {err}"

                    failed_clean_product[vendor_code] = msg

            # логируем товары по которым не удалось подготовить данные для отправки
            if failed_clean_product:
                msg = (
                    f"Товары, для которых было недостаточно данных {len(failed_clean_product)}: "
                    f"{failed_clean_product}"
                )
                logger.warning(msg)
                task_info.metadata["transformation_errors"] = len(failed_clean_product)

            if not prepare_products:
                task_info.status = TaskStatus.FAILED
                task_info.error = "Нет корректных карточек на отправку"
                await self.task_manager.save_task(task_info)
                return

            # ШАГ 2: Подготовка карточек в формате WB
            logger.info(f"[{task_info.task_id}] Группировка товаров по subjectID")

            # Группируем по subjectID
            wb_cards = self.group_by_subject_id(prepare_products)

            logger.info(
                f"[{task_info.task_id}] Сгруппировано: "
                f"{len(prepare_products)} товаров → {len(wb_cards)} карточек"
            )

            # Разбиваем на батчи с учётом лимита вариантов
            wb_batches = self.split_cards_by_variants(wb_cards, max_variants=100)

            logger.info(
                f"[{task_info.task_id}] Создано {len(wb_batches)} батчей для отправки"
            )

            # ШАГ 3: Отправка на WB
            wb_batches_sent = 0
            total_products_sent = 0

            logger.info(f"[{task_info.task_id}] Начало отправки в WB")

            for batch_number, batch in enumerate(wb_batches, start=1):
                # Считаем количество товаров в батче
                batch_variants_count = sum(len(card["variants"]) for card in batch)

                logger.debug(
                    f"[{task_info.task_id}] Пакет {batch_number}/{len(wb_batches)}: "
                    f"карточек={len(batch)}, товаров={batch_variants_count}"
                )

                # Отправляем пакет в WB
                create_result = await self.api.create_products(
                    token=token,
                    cards=batch
                )

                # Проверяем статус
                if create_result["status"] == "error":
                    logger.error(
                        f"[{task_info.task_id}] Пакет {batch_number} ОШИБКА: "
                        f"{create_result['error']} (статус: {create_result.get('status_code')})"
                    )
                    task_info.status = TaskStatus.FAILED
                    task_info.error = create_result["error"]
                    await self.task_manager.save_task(task_info)
                    return

                wb_batches_sent += 1
                total_products_sent += batch_variants_count
                task_info.processed_items = total_products_sent

                logger.info(
                    f"[{task_info.task_id}] Пакет {batch_number}/{len(wb_batches)}: "
                    f"отправлено успешно, карточек={len(batch)}, товаров={batch_variants_count}"
                )

                # Сохраняем прогресс
                task_info.metadata["batches_sent"] = wb_batches_sent
                task_info.metadata["total_products_sent"] = total_products_sent
                task_info.metadata["cards_sent"] = sum(
                    len(b) for b in wb_batches[:batch_number]
                )
                await self.task_manager.save_task(task_info)

                # Задержка между пакетами
                if batch_number < len(wb_batches):
                    logger.debug(f"[{task_info.task_id}] Пакет {batch_number}: ждём 6 сек")
                    await asyncio.sleep(6)

            logger.info(
                f"[{task_info.task_id}] Все пакеты отправлены: "
                f"батчей={wb_batches_sent}, карточек={len(wb_cards)}, товаров={total_products_sent}"
            )

            # ШАГ 3: ПРОВЕРКА РЕЗУЛЬТАТОВ СОЗДАНИЯ С ПОЛЛИНГОМ

            task_info.metadata["check_started_at"] = datetime.now().isoformat()
            await self.task_manager.save_task(task_info)

            logger.info(f"[{task_info.task_id}] Начало проверки результатов создания")

            check_result = await self._check_create_results_with_polling(
                token=token,
                task_info=task_info,
                vendor_codes=vendor_codes,
                max_attempts=5,
                initial_delay=10,
                retry_delay=15
            )

            task_info.metadata["check_results"] = check_result
            task_info.metadata["check_completed_at"] = datetime.now().isoformat()

            # ШАГ 4: ОПРЕДЕЛЯЕМ УСПЕШНЫЕ ТОВАРЫ
            if not check_result.get("checked"):
                task_info.status = TaskStatus.COMPLETED_WITH_ERRORS
                logger.warning(
                    f"[{task_info.task_id}] Проверка не завершена: "
                    f"причина={check_result.get('reason')}"
                )
                return

            error_count = check_result.get("error_count", 0)
            success_count = check_result.get("success_count", 0)

            logger.info(
                f"[{task_info.task_id}] Проверка завершена: "
                f"успешно={success_count}, ошибок={error_count}, "
                f"попыток={check_result.get('polling_attempts', 0)}"
            )

            # Определяем успешные товары (БЕЗ ошибок)
            error_vendor_codes = set(check_result.get("error_vendor_codes", []))
            successful_vendor_codes = [
                vc for vc in vendor_codes
                if vc not in error_vendor_codes
            ]

            logger.info(
                f"[{task_info.task_id}] Успешно созданы товары: "
                f"количество={len(successful_vendor_codes)}"
            )
            # Получаем данные по загруженным карточкам {vendor_code: nm_id}
            vendor_codes_to_nm_id = await self.get_recent_nmids_for_vendor_codes(
                token=token,
                successful_vendor_codes=successful_vendor_codes,
                hours=1,
                limit=len(successful_vendor_codes)
            )

            # ШАГ 5: ЗАГРУЖАЕМ КАРТИНКИ ЕСЛИ ЕСТЬ УСПЕШНЫЕ
            if successful_vendor_codes:
                await self._upload_images_for_successful_products(
                    token=token,
                    task_id=task_info.task_id,
                    vendor_codes_to_nm_id=vendor_codes_to_nm_id,
                    images=images,
                    task_info=task_info
                )

            # ШАГ 6: ЗАГРУЖАЕМ ЦЕНЫ
            if successful_vendor_codes:
                await self._upload_prices_for_successful_products(
                    token=token,
                    task_id=task_info.task_id,
                    vendor_codes_to_nm_id=vendor_codes_to_nm_id,
                    npr_data=npr_data,
                    task_info=task_info
                )

            # ШАГ 7: ФИНАЛЬНЫЙ СТАТУС
            if error_count > 0:
                task_info.status = TaskStatus.COMPLETED_WITH_ERRORS
                logger.warning(
                    f"[{task_info.task_id}] Создание завершено с ошибками: "
                    f"пакетов={wb_batches_sent}, товаров={total_products_sent}, "
                    f"ошибок={error_count}"
                )
            else:
                task_info.status = TaskStatus.COMPLETED
                logger.info(
                    f"[{task_info.task_id}] Создание завершено успешно: "
                    f"пакетов={wb_batches_sent}, товаров={total_products_sent}"
                )

            task_info.metadata["creation_completed_at"] = datetime.now().isoformat()

        except Exception as e:
            task_info.status = TaskStatus.FAILED
            task_info.completed_at = datetime.now()
            task_info.error = str(e)

            logger.error(
                f"[{task_info.task_id}] ОШИБКА при создании товаров: {e}",
                exc_info=True
            )

        finally:
            task_info.completed_at = datetime.now()
            await self.task_manager.save_task(task_info)

    async def _get_products_for_vendor_codes(self, vendor_codes: List[str]) -> dict[str, dict]:
        """
        Получить товары для списка vendor_codes из Elasticsearch

        Args:
            vendor_codes: Список vendor_codes для получения товаров
        """

        response = await self.elasticsearch_service.search_products(
            hash_token(settings.ADMIN_WB_TOKEN),
            filters={"vendorCode": vendor_codes},
            limit=1000,
            offset=0
        )
        # Обработка ответа от search_products
        products = (
            response.get("products", [])
            if isinstance(response, dict)
            else response
        )
        logger.info(f"Получено {len(products)} товаров из Elasticsearch")

        # Формируем словарь: vendor_code -> тело документа
        products_dict = {}
        found_vendor_codes = set()

        for product_doc in products:
            vendor_code = product_doc.get("vendorCode")
            found_vendor_codes.add(vendor_code)
            products_dict[vendor_code] = product_doc
            found_vendor_codes.add(vendor_code)

        # Определяем vendor_codes, для которых не нашлись товары
        not_found_vendor_codes = list(set(vendor_codes) - found_vendor_codes)
        found_count = len(found_vendor_codes)
        not_found_count = len(not_found_vendor_codes)
        logger.info(
            f"Найдены товары для {found_count} vendor_codes, не найдены для {not_found_count} vendor_codes"
        )

        # Выводим первые 10 vendor_codes без товаров
        if not_found_vendor_codes:
            sample_size = min(10, len(not_found_vendor_codes))
            sample_not_found = not_found_vendor_codes[:sample_size]
            logger.info(f"Первые {sample_size} offer_ids без товаров: {sample_not_found}")

        # подставляем пустое тело, что бы упасть далее
        for vendor_code in not_found_vendor_codes:
            products_dict[vendor_code] = {}

        return products_dict

    @staticmethod
    def group_by_subject_id(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Группирует товары по subjectID в формат WB API

        Args:
            products: Список подготовленных товаров

        Returns:
            Список карточек в формате WB [{subjectID, variants: [...]}]
        """
        # Группируем по subjectID
        grouped = defaultdict(list)

        for product in products:
            subject_id = product.get("subjectID")
            if not subject_id:
                continue

            # Создаём variant (убираем subjectID из товара)
            variant = {k: v for k, v in product.items() if k != "subjectID"}
            grouped[subject_id].append(variant)

        # Формируем карточки в формате WB
        cards = []
        for subject_id, variants in grouped.items():
            cards.append({
                "subjectID": subject_id,
                "variants": variants
            })

        return cards

    @staticmethod
    def split_cards_by_variants(cards: List[Dict], max_variants: int = 100) -> List[List[Dict]]:
        """
        Разбивает карточки на батчи с учётом лимита вариантов

        Args:
            cards: Список карточек [{subjectID, variants: [...]}]
            max_variants: Максимум вариантов в одном батче

        Returns:
            Список батчей карточек
        """
        batches = []
        current_batch = []
        current_variants_count = 0

        for card in cards:
            variants_count = len(card["variants"])

            # Если одна карточка содержит больше max_variants
            if variants_count > max_variants:
                # Разбиваем её на несколько карточек
                for i in range(0, variants_count, max_variants):
                    chunk_variants = card["variants"][i:i + max_variants]

                    # Сохраняем предыдущий батч если есть
                    if current_batch:
                        batches.append(current_batch)
                        current_batch = []
                        current_variants_count = 0

                    # Добавляем как отдельный батч
                    batches.append([{
                        "subjectID": card["subjectID"],
                        "variants": chunk_variants
                    }])

            # Если добавление карточки превысит лимит
            elif current_variants_count + variants_count > max_variants:
                # Сохраняем текущий батч
                batches.append(current_batch)
                # Начинаем новый батч с этой карточки
                current_batch = [card]
                current_variants_count = variants_count

            # Добавляем в текущий батч
            else:
                current_batch.append(card)
                current_variants_count += variants_count

        # Добавляем последний батч
        if current_batch:
            batches.append(current_batch)

        return batches

    async def _check_create_results_with_polling(
            self,
            token: str,
            task_info: TaskInfo,
            vendor_codes: List[str],
            max_attempts: int = 5,
            initial_delay: int = 10,
            retry_delay: int = 15,
            timeout_minutes: int = 5
    ) -> Dict[str, Any]:
        """
        Проверяет результаты создания товаров с поллингом.

        Вызывает /content/v2/cards/error/list несколько раз,
        чтобы гарантировать все ошибки загрузились.

        Args:
            token: API токен
            task_info: Информация о задаче
            vendor_codes: Список vendor_codes
            max_attempts: Максимальное количество попыток
            initial_delay: Первая задержка (сек)
            retry_delay: Задержка между попытками (сек)
            timeout_minutes: Максимальное время ожидания (мин)

        Returns:
            Результаты с информацией об ошибках создания
        """

        total_products = task_info.total_items or 0

        if not vendor_codes:
            return {
                "checked": False,
                "reason": "Нет vendorCode в маппинге",
                "polling_attempts": 0
            }

        previous_error_count = -1
        stable_count = 0
        attempt = 0
        timeout_deadline = datetime.now() + timedelta(minutes=timeout_minutes)

        await asyncio.sleep(initial_delay)

        logger.info(
            f"[{task_info.task_id}] Начало поллинга результатов создания: "
            f"макс_попыток={max_attempts}, отслеживаем {len(vendor_codes)} vendorCode'ов"
        )

        while attempt < max_attempts:
            if datetime.now() > timeout_deadline:
                logger.warning(
                    f"[{task_info.task_id}] Таймаут поллинга ({timeout_minutes} мин)"
                )
                break

            attempt += 1

            try:
                logger.debug(
                    f"[{task_info.task_id}] Проверка результатов: "
                    f"попытка {attempt}/{max_attempts}"
                )

                # Получаем все ошибки создания
                all_errors_result = await self.api.get_all_errors_for_create(token)

                # Проверяем статус API
                if all_errors_result["status"] == "error":
                    logger.error(
                        f"[{task_info.task_id}] Ошибка получения ошибок: "
                        f"{all_errors_result['error']}"
                    )
                    # Продолжаем попытки, это может быть временная ошибка
                    await asyncio.sleep(retry_delay)
                    continue

                all_errors = all_errors_result.get("error_batches", [])

                stats = self._calculate_create_error_statistics(
                    all_errors,
                    vendor_codes,
                    total_products
                )

                current_error_count = stats["error_count"]

                logger.info(
                    f"[{task_info.task_id}] Результаты проверки: "
                    f"попытка={attempt}, ошибок={current_error_count}, "
                    f"успешно={stats['success_count']}"
                )

                if current_error_count == previous_error_count:
                    stable_count += 1

                    if stable_count >= 2:
                        logger.info(
                            f"[{task_info.task_id}] Результаты стабильны: "
                            f"попытка={attempt}, ошибок={current_error_count}"
                        )

                        return {
                            "checked": True,
                            "success_count": stats["success_count"],
                            "error_count": current_error_count,
                            "success_rate": stats["success_rate"],
                            "error_vendor_codes": list(stats["error_vendor_codes"]),
                            "error_details": stats["error_details"],
                            "error_batches_count": len(stats["error_batches"]),
                            "polling_attempts": attempt,
                            "stable": True
                        }
                else:
                    stable_count = 0
                    previous_error_count = current_error_count

                if attempt == max_attempts:
                    logger.warning(
                        f"[{task_info.task_id}] Исчерпаны все попытки: "
                        f"ошибок={current_error_count}"
                    )

                    return {
                        "checked": True,
                        "success_count": stats["success_count"],
                        "error_count": current_error_count,
                        "success_rate": stats["success_rate"],
                        "error_vendor_codes": list(stats["error_vendor_codes"]),
                        "error_details": stats["error_details"],
                        "error_batches_count": len(stats["error_batches"]),
                        "polling_attempts": attempt,
                        "stable": False,
                        "warning": "Результаты могут быть неполными (достигнут лимит попыток)"
                    }

                await asyncio.sleep(retry_delay)

            except Exception as e:
                logger.error(
                    f"[{task_info.task_id}] Ошибка при проверке (попытка {attempt}): {e}",
                    exc_info=True
                )

                if attempt == max_attempts:
                    return {
                        "checked": False,
                        "reason": f"Ошибка после {attempt} попыток: {e}",
                        "polling_attempts": attempt
                    }

                await asyncio.sleep(retry_delay)

        return {
            "checked": False,
            "reason": "Цикл поллинга исчерпан",
            "polling_attempts": attempt
        }

    @staticmethod
    def _filter_relevant_create_errors(
            all_errors: List[Dict[str, Any]],
            vendor_codes: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Фильтрует пакеты ошибок создания по vendor_code.
        Ищет vendorCodes в ошибках WB API.
        """

        relevant = []
        vendor_codes_set = set(vendor_codes)
        for batch in all_errors:
            batch_vendor_codes = batch.get("vendorCodes", [])

            if not batch_vendor_codes:
                batch_vendor_codes = list(batch.get("errors", {}).keys())

            if not batch_vendor_codes:
                batch_vendor_codes = list(batch.get("subjects", {}).keys())

            batch_vendor_codes_set = set(batch_vendor_codes)

            # Пересечение - есть ли наши vendor_codes в этом пакете?
            if batch_vendor_codes_set & vendor_codes_set:
                relevant.append(batch)

        return relevant

    def _calculate_create_error_statistics(
            self,
            all_errors: List[Dict[str, Any]],
            vendor_codes: List[str],
            total_products: int
    ) -> Dict[str, Any]:
        """
        Подсчитывает статистику ошибок создания товаров по vendor_code.
        """
        relevant_errors = self._filter_relevant_create_errors(all_errors, vendor_codes)

        error_vendor_codes = set()
        error_details_by_vendor_codes = {}

        for batch in relevant_errors:
            batch_errors = batch.get("errors", {})
            batch_subjects = batch.get("subjects", {})

            for vendor_code, errors in batch_errors.items():
                if vendor_code not in vendor_codes:
                    logger.debug(
                        f"Пропущен vendor_code={vendor_code} (не в списке отправленных)"
                    )
                    continue

                error_vendor_codes.add(vendor_code)

                subject_info = batch_subjects.get(vendor_code, {})

                logger.warning(
                    f"Ошибка создания: vendor_code={vendor_code}, ошибки={errors}"
                )

                error_details_by_vendor_codes[vendor_code] = {
                    "subject_name": subject_info.get("name"),
                    "errors": errors
                }

        error_count = len(error_vendor_codes)
        success_count = total_products - error_count
        success_rate = success_count / total_products if total_products > 0 else 0

        logger.info(
            f"Статистика ошибок создания: всего={total_products}, "
            f"успешно={success_count}, ошибок={error_count}, "
            f"уникальных vendor_codes с ошибками={len(error_vendor_codes)}"
        )

        return {
            "success_count": success_count,
            "error_count": error_count,
            "success_rate": success_rate,
            "error_vendor_codes": error_vendor_codes,
            "error_details": error_details_by_vendor_codes,
            "error_batches": relevant_errors
        }

    async def get_recent_nmids_for_vendor_codes(
            self,
            token: str,
            successful_vendor_codes: List[str],
            hours: int = 1,
            limit: int = 100,
    ) -> Dict[str, int]:
        """
        Возвращает {vendorCode: nmID} для карточек,
        обновлённых за последние `hours` часов
        и присутствующих в successful_vendor_codes.
        """
        # 1. Считаем порог по времени (UTC)
        now_utc = datetime.now(timezone.utc)
        updated_from = now_utc - timedelta(hours=hours)

        logger.info(
            f"Запуск get_recent_nmids_for_vendor_codes: "
            f"hours={hours}, limit={limit}, updated_from={updated_from.isoformat()}, "
            f"successful_vendor_codes={len(successful_vendor_codes)}"
        )

        target_codes = set(successful_vendor_codes)
        result = {}

        cursor_updated_at = None
        cursor_nmid = None
        page = 0

        while True:
            cursor: Dict[str, Any] = {"limit": limit}
            if cursor_updated_at is not None and cursor_nmid is not None:
                cursor["updatedAt"] = cursor_updated_at
                cursor["nmID"] = cursor_nmid
                logger.debug(
                    f"Следующая страница: cursor.updatedAt={cursor_updated_at}, "
                    f"cursor.nmID={cursor_nmid}, limit={limit}"
                )
            else:
                logger.debug(f"Первая страница: limit={limit}, без cursor.updatedAt/nmID")

            page += 1
            logger.info(f"Запрос страницы {page} списка актуальных карточек (limit={limit})")

            response = await self.api.get_products_page(
                token=token,
                cursor=cursor
            )

            if response["status"] == "error":
                logger.warning(
                    f"Получили статус error при запросе списка актуальных карточек на странице {page}: "
                    f"{response.get('error')}"
                )
                return result

            cards = response.get("cards") or []
            cursor_resp = response.get("cursor") or {}

            logger.info(
                f"Получено карточек: {len(cards)} на странице {page}, "
                f"уже найдено совпадений: {len(result)} из {len(target_codes)}"
            )

            # 2. Обрабатываем карточки
            matched_on_page = 0
            for card in cards:
                vendor_code = card.get("vendorCode")
                nmid = card.get("nmID")
                if not vendor_code or nmid is None:
                    continue

                if vendor_code in target_codes and vendor_code not in result:
                    result[vendor_code] = nmid
                    matched_on_page += 1

            if matched_on_page:
                logger.info(
                    f"На странице {page} найдено {matched_on_page} новых совпадений vendorCode→nmID, "
                    f"всего найдено: {len(result)}"
                )

            # 3. Обновляем курсор
            cursor_updated_at = cursor_resp.get("updatedAt")
            cursor_nmid = cursor_resp.get("nmID")
            total = cursor_resp.get("total", 0)

            logger.debug(
                f"Ответ cursor: updatedAt={cursor_updated_at}, nmID={cursor_nmid}, "
                f"total={total} (page={page})"
            )

            # 4. Остановка по размеру страницы
            if total < limit:
                logger.info(
                    f"Остановка пагинации: total ({total}) < limit ({limit}) на странице {page}"
                )
                break

            # 5. Остановка по времени (если ушли за пределы окна hours)
            if cursor_updated_at:
                try:
                    current_dt = datetime.fromisoformat(cursor_updated_at.replace("Z", "+00:00"))
                    if current_dt < updated_from:
                        logger.info(
                            f"Остановка пагинации по времени: cursor.updatedAt={current_dt.isoformat()} < "
                            f"updated_from={updated_from.isoformat()} (page={page})"
                        )
                        break
                except ValueError:
                    logger.warning(
                        f"Не удалось распарсить cursor.updatedAt='{cursor_updated_at}', "
                        f"продолжаем только по total/limit (page={page})"
                    )

            # 6. Если нашли все коды — тоже можно завершить
            if len(result) == len(target_codes):
                logger.info(
                    f"Найдены все vendorCode ({len(result)} из {len(target_codes)}), "
                    f"завершаем на странице {page}"
                )
                break

        logger.info(
            f"Завершение get_recent_nmids_for_vendor_codes: найдено {len(result)} соответствий "
            f"из {len(target_codes)} целевых кодов"
        )
        return result

    @staticmethod
    def _clean_product_document(
            vendor_code: str,
            es_product: Dict[str, Any],
            recommendation_name: str,
            recommendation_description: str,
            npr_data: Dict,
            image: List

    ) -> Dict[str, Any]:
        """
        Трансформирует товары в формат WB для создания карточек.

        Args:
            vendor_code: VendorCode
            es_product: Карточка из Elasticsearch
            recommendation_name: Рекомендация по имени
            recommendation_description: Рекомендация по описанию
            npr_data: Данные из нпр
            image: инфо по картинке

        Returns:
            Очищенный подготовленный документ, warnings
        """
        if (npr_data is None or not isinstance(npr_data, dict)
                or npr_data.get("price", None) is None
                or npr_data.get("bar_code", None) is None
        ):
            raise AttributeError(f"У товара {vendor_code} отсутствуют данные NPR.")
        if not image:
            raise AttributeError(f"У товара {vendor_code} отсутствуют изображения.")
        if not es_product:
            raise AttributeError(f"У товара {vendor_code} нет данных в базе данных.")
        if not recommendation_name:
            raise AttributeError(f"У товара {vendor_code} нет рекомендуемого названия.")
        if not recommendation_description:
            raise AttributeError(f"Для товара {vendor_code} отсутствует описание рекомендаций.")

        # заполняем верхне-уровневые поля
        # если нет какого то главного поля нет и смысла отправлять товар на создание
        try:
            clean_product = {  # noqa
                "subjectID": es_product.get("subjectID"),
                "subjectName": es_product.get("subjectName"),
                "brand": es_product.get("brand"),
                "vendorCode": vendor_code,
                "title": recommendation_name,
                "description": recommendation_description
            }

        except KeyError as err:
            msg = f"У товара {vendor_code} отсутствует поле первого уровня: {err}"
            logger.warning(msg)
            raise AttributeError(msg)

        # Заполняем dimensions
        if "dimensions" in es_product.keys():
            clean_product["dimensions"] = es_product.get("dimensions")

        # вытаскиваем характеристики из нашего товара
        characteristics = es_product.get("characteristics")
        if characteristics:
            # Удаляем ОЕМ характеристику 4572490 и характеристику артикула 5522881
            delete_characteristics = (4572490, 5522881)
            clean_product["characteristics"] = [char for char in characteristics if
                                                char.get("id") not in delete_characteristics]
        else:
            clean_product["characteristics"] = []

        # Заполняем характеристику артикула 5522881
        clean_product["characteristics"].append({
            "id": 5522881,
            "name": "Артикул производителя",
            "value": [
                vendor_code,
                vendor_code.replace(" ", "")
            ]
        })

        # Заполняем данные из НПР если есть
        oem_list = npr_data.get("oem", None)
        if oem_list:
            clean_product["characteristics"].append({
                "id": 4572490,
                "name": "ОЕМ номер",
                "value": oem_list
            })

        # Добавляем поле 'sizes'
        bar_code = npr_data.get("bar_code")
        sizes = es_product.get("sizes")
        if bar_code and sizes:
            sizes[0]["skus"] = [bar_code]
            clean_product["sizes"] = sizes

        return clean_product

    async def _get_images_for_vendor_codes(
            self,
            products: Dict[str, Any],
    ) -> Dict[str, List]:
        """
        Получить изображения для списка vendor_codes из Elasticsearch.
        Затем составить массив изображений, если есть.

        Args:
            products: Список карточек из Elasticsearch

        Returns:
            {'vendor_code': ["links"]}
        """
        vendor_codes = [vendor_code for vendor_code in products.keys()]
        logger.info(f"Получение изображений для {len(vendor_codes)} vendor_codes")

        # Получаем изображения из Elasticsearch
        result = await self.elasticsearch_service.get_images_by_vendor_codes(
            vendor_codes=vendor_codes
        )

        if result.get("status") != "success":
            error = result.get("error", "Unknown error")
            msg = f"Ошибка получения изображений: {error}"
            logger.error(msg)
            raise AttributeError(msg)

        images = result.get("images", {})
        logger.info(f"Получено {len(images)} изображений из Elasticsearch")

        # Преобразуем: vendor_code → список ссылок (LINK) для TYPE == 'FORMAT34'
        images_dict = {}
        found_vendor_codes = set()

        for vendor_code, images_list in images.items():
            # Собираем ссылки
            format34_links = [
                img.get("LINK")
                for img in images_list
                if img.get("LINK")
            ]

            if format34_links:
                images_dict[vendor_code] = format34_links
                found_vendor_codes.add(vendor_code)

        # Статистика
        not_found_vendor_codes = list(set(vendor_codes) - found_vendor_codes)
        found_count = len(found_vendor_codes)
        not_found_count = len(not_found_vendor_codes)

        logger.info(
            f"Статистика: найдено изображений для {found_count} vendor_codes, "
            f"не найдено для {not_found_count} vendor_codes"
        )

        if not_found_vendor_codes:
            sample_size = min(10, len(not_found_vendor_codes))
            sample_not_found = not_found_vendor_codes[:sample_size]
            logger.info(
                f"Первые {sample_size} vendor_codes без изображений: {sample_not_found}"
            )

        # подставляем пустое тело, что бы упасть далее
        for vendor_code in not_found_vendor_codes:
            images_dict[vendor_code] = {}

        return images_dict

    async def _upload_images_for_successful_products(
            self,
            token: str,
            task_id: str,
            vendor_codes_to_nm_id: Dict[str, int],
            images: Dict[str, List],
            task_info: TaskInfo
    ) -> None:
        """
        Загружает картинки для успешно созданных товаров.
        """
        try:
            logger.info(
                f"[{task_id}] Начало загрузки картинок: "
                f"товаров={len(vendor_codes_to_nm_id.keys())}"
            )

            # ШАГ 1: Проверяем необходимые картинки
            images_vendor_codes = images.keys()
            images_by_nm_id = {}
            for vendor_code, nm_id in vendor_codes_to_nm_id.items():
                if vendor_code not in images_vendor_codes:
                    continue

                if not images_by_nm_id.get(nm_id, None):
                    images_by_nm_id[nm_id] = []

                images_by_nm_id[nm_id].extend(images[vendor_code])

            total_images = sum(len(imgs) for imgs in images_by_nm_id.values())
            logger.info(
                f"[{task_id}] Получены картинки: "
                f"всего={total_images}, товаров={len(images_by_nm_id)}"
            )

            # ШАГ 2: Подготавливаем данные для API
            images_upload_summary = {}
            upload_data_list = []

            for nm_id, links in images_by_nm_id.items():
                if not links:
                    logger.debug(f"[{task_id}] Пропуск: нет изображений для nm_id={nm_id}")
                    continue

                upload_data = {
                    "nmId": nm_id,
                    "data": links
                }

                logger.debug(
                    f"[{task_id}] Подготовка к загрузке: "
                    f"nm_id={nm_id}, картинок={len(links)}"
                )

                upload_data_list.append(upload_data)
                images_upload_summary[str(nm_id)] = {
                    "images_count": len(links),
                    "status": "PENDING"
                }

            if not upload_data_list:
                logger.warning(
                    f"[{task_id}] Не удалось подготовить картинки для загрузки"
                )
                task_info.metadata["images_upload_status"] = "PREPARATION_FAILED"
                await self.task_manager.save_task(task_info)
                return

            # ШАГ 3: Выполняем загрузку последовательно с задержками
            logger.info(
                f"[{task_id}] Начало загрузки картинок (последовательно): "
                f"товаров={len(upload_data_list)}"
            )

            task_info.metadata["images_upload_started_at"] = datetime.now().isoformat()
            task_info.metadata["images_upload_summary"] = images_upload_summary

            successful_uploads = 0
            failed_uploads = 0
            upload_errors = {}

            for idx, upload_data in enumerate(upload_data_list, start=1):
                nm_id = upload_data["nmId"]
                str_nm_id = str(nm_id)

                logger.debug(
                    f"[{task_id}] Загрузка {idx}/{len(upload_data_list)}: "
                    f"nm_id={nm_id}, картинок={len(upload_data['data'])}"
                )

                try:
                    # Загружаем изображения
                    result = await self.api.save_product_images(
                        token=token,
                        nm_id=nm_id,
                        upload_data=upload_data
                    )

                    # Проверяем результат
                    if isinstance(result, dict) and result.get("status") == "success":
                        successful_uploads += 1
                        images_upload_summary[str_nm_id]["status"] = "SUCCESS"
                        logger.info(
                            f"[{task_id}] Загрузка {idx}/{len(upload_data_list)} успешна: "
                            f"nm_id={nm_id}, картинок={len(upload_data['data'])}"
                        )
                    else:
                        # Ошибка API
                        failed_uploads += 1
                        error = result.get("error", "Unknown error") if isinstance(result, dict) else str(result)
                        images_upload_summary[str_nm_id]["status"] = "FAILED"
                        images_upload_summary[str_nm_id]["error"] = error
                        upload_errors[str_nm_id] = error
                        logger.warning(
                            f"[{task_id}] Загрузка {idx}/{len(upload_data_list)} не удалась: "
                            f"nm_id={nm_id}, ошибка={error}"
                        )

                except Exception as e:
                    # Исключение при загрузке
                    failed_uploads += 1
                    error_msg = str(e)
                    images_upload_summary[str_nm_id]["status"] = "EXCEPTION"
                    images_upload_summary[str_nm_id]["error"] = error_msg
                    upload_errors[str_nm_id] = error_msg
                    logger.error(
                        f"[{task_id}] Загрузка {idx}/{len(upload_data_list)} исключение: "
                        f"nm_id={nm_id}, ошибка={error_msg}",
                        exc_info=True
                    )
                await asyncio.sleep(0.7)

                # Сохраняем прогресс каждые 10 товаров
                if idx % 10 == 0:
                    task_info.metadata["images_upload_summary"] = images_upload_summary
                    task_info.metadata["images_upload_progress"] = {
                        "current": idx,
                        "total": len(upload_data_list),
                        "successful": successful_uploads,
                        "failed": failed_uploads
                    }
                    await self.task_manager.save_task(task_info)

            # ШАГ 4: Сохраняем финальные результаты
            task_info.metadata["images_upload_status"] = "COMPLETED"
            task_info.metadata["images_upload_completed_at"] = datetime.now().isoformat()
            task_info.metadata["images_upload_summary"] = images_upload_summary
            task_info.metadata["images_upload_stats"] = {
                "successful": successful_uploads,
                "failed": failed_uploads,
                "total": len(upload_data_list)
            }

            if upload_errors:
                task_info.metadata["images_upload_errors"] = upload_errors

            logger.info(
                f"[{task_id}] Загрузка картинок завершена: "
                f"успешно={successful_uploads}, ошибок={failed_uploads}, "
                f"всего={len(upload_data_list)}"
            )

            await self.task_manager.save_task(task_info)

        except Exception as e:
            logger.error(
                f"[{task_id}] Критическая ошибка при загрузке картинок: {str(e)}",
                exc_info=True
            )
            task_info.metadata["images_upload_status"] = "ERROR"
            task_info.metadata["images_upload_error"] = str(e)
            await self.task_manager.save_task(task_info)

    async def _upload_prices_for_successful_products(
            self,
            token: str,
            task_id: str,
            vendor_codes_to_nm_id: Dict[str, int],
            npr_data: Dict[str, Dict],
            task_info: TaskInfo
    ) -> None:
        """
        Загружает цены для успешно созданных товаров.

        Args:
            token: WB API токен
            task_id: ID задачи
            vendor_codes_to_nm_id: Маппинг {vendor_code: nmID}
            npr_data: Данные NPR {vendor_code: {"price": ..., ...}}
            task_info: Объект задачи
        """
        try:
            logger.info(
                f"[{task_id}] Начало загрузки цен: "
                f"товаров={len(vendor_codes_to_nm_id)}"
            )

            # ШАГ 1: Подготавливаем данные цен по nmID
            prices_by_nm_id = {}

            for vendor_code, nm_id in vendor_codes_to_nm_id.items():
                vendor_npr = npr_data[vendor_code]
                price = vendor_npr.get("price")

                # Сохраняем цену по nmID
                prices_by_nm_id[nm_id] = price

            logger.info(
                f"[{task_id}] Подготовлено цен: {len(prices_by_nm_id)} товаров"
            )

            # ШАГ 2: Формируем батчи для API (макс 1000 товаров за запрос)
            WB_PRICES_BATCH_SIZE = 1000

            prices_list = [
                {"nmId": nm_id, "price": price}
                for nm_id, price in prices_by_nm_id.items()
            ]

            batches = []
            for i in range(0, len(prices_list), WB_PRICES_BATCH_SIZE):
                batch = prices_list[i:i + WB_PRICES_BATCH_SIZE]
                batches.append(batch)

            logger.info(
                f"[{task_id}] Сформировано батчей: {len(batches)} "
                f"(размер батча={WB_PRICES_BATCH_SIZE})"
            )

            # ШАГ 3: Инициализация трекинга
            prices_upload_summary = {}
            for nm_id in prices_by_nm_id.keys():
                prices_upload_summary[str(nm_id)] = {
                    "price": prices_by_nm_id[nm_id],
                    "status": "PENDING"
                }

            task_info.metadata["prices_upload_started_at"] = datetime.now().isoformat()
            task_info.metadata["prices_upload_summary"] = prices_upload_summary

            # ШАГ 4: Выполняем загрузку батчами
            logger.info(
                f"[{task_id}] Начало загрузки цен (батчами): "
                f"батчей={len(batches)}, товаров={len(prices_list)}"
            )

            successful_uploads = 0
            failed_uploads = 0
            upload_errors = {}
            processed_nm_ids = set()

            for batch_idx, batch in enumerate(batches, start=1):
                logger.info(
                    f"[{task_id}] Загрузка батча {batch_idx}/{len(batches)}: "
                    f"товаров={len(batch)}"
                )

                try:
                    # Отправляем батч цен в WB API
                    result = await self.api.upload_prices(
                        token=token,
                        prices_data={"data": batch}
                    )

                    # Проверяем результат
                    if isinstance(result, dict) and result.get("status") == "success":
                        # Успешная загрузка всего батча
                        batch_success_count = len(batch)
                        successful_uploads += batch_success_count

                        for item in batch:
                            nm_id = item["nmId"]
                            str_nm_id = str(nm_id)
                            prices_upload_summary[str_nm_id]["status"] = "SUCCESS"
                            processed_nm_ids.add(nm_id)

                        logger.info(
                            f"[{task_id}] Батч {batch_idx}/{len(batches)} успешно загружен: "
                            f"товаров={batch_success_count}, uploadID={result.get('data', {}).get('uploadId')}"
                        )
                    else:
                        # Ошибка API для всего батча
                        error = result.get("error", "Unknown error") if isinstance(result, dict) else str(result)
                        failed_uploads += len(batch)

                        for item in batch:
                            nm_id = item["nmId"]
                            str_nm_id = str(nm_id)
                            prices_upload_summary[str_nm_id]["status"] = "FAILED"
                            prices_upload_summary[str_nm_id]["error"] = error
                            upload_errors[str_nm_id] = error

                        logger.warning(
                            f"[{task_id}] Батч {batch_idx}/{len(batches)} не удался: "
                            f"товаров={len(batch)}, ошибка={error}"
                        )

                except Exception as e:
                    # Исключение при загрузке батча
                    error_msg = str(e)
                    failed_uploads += len(batch)

                    for item in batch:
                        nm_id = item["nmId"]
                        str_nm_id = str(nm_id)
                        prices_upload_summary[str_nm_id]["status"] = "EXCEPTION"
                        prices_upload_summary[str_nm_id]["error"] = error_msg
                        upload_errors[str_nm_id] = error_msg

                    logger.error(
                        f"[{task_id}] Батч {batch_idx}/{len(batches)} исключение: "
                        f"товаров={len(batch)}, ошибка={error_msg}",
                        exc_info=True
                    )

                # Задержка между батчами
                if batch_idx < len(batches):
                    await asyncio.sleep(1.0)

                # Сохраняем прогресс
                task_info.metadata["prices_upload_summary"] = prices_upload_summary
                task_info.metadata["prices_upload_progress"] = {
                    "current_batch": batch_idx,
                    "total_batches": len(batches),
                    "successful": successful_uploads,
                    "failed": failed_uploads
                }
                await self.task_manager.save_task(task_info)

            # ШАГ 5: Сохраняем финальные результаты
            task_info.metadata["prices_upload_status"] = "COMPLETED"
            task_info.metadata["prices_upload_completed_at"] = datetime.now().isoformat()
            task_info.metadata["prices_upload_summary"] = prices_upload_summary
            task_info.metadata["prices_upload_stats"] = {
                "successful": successful_uploads,
                "failed": failed_uploads,
                "total": len(prices_list)
            }

            if upload_errors:
                task_info.metadata["prices_upload_errors"] = upload_errors

            logger.info(
                f"[{task_id}] Загрузка цен завершена: "
                f"успешно={successful_uploads}, ошибок={failed_uploads}, "
                f"всего={len(prices_list)}"
            )

            await self.task_manager.save_task(task_info)

        except Exception as e:
            logger.error(
                f"[{task_id}] Критическая ошибка при загрузке цен: {str(e)}",
                exc_info=True
            )
            task_info.metadata["prices_upload_status"] = "ERROR"
            task_info.metadata["prices_upload_error"] = str(e)
            await self.task_manager.save_task(task_info)
