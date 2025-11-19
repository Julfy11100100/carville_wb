import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Set

from app.exceptions.wb_api import WildberriesRateLimitError
from app.schemas.task import TaskStatus, TaskInfo
from app.services.elasticsearch_service import ElasticsearchService
from app.services.sql_category_service import SqlCategoryService
from app.services.task_manager import TaskManager
from app.services.wb_api import WildberriesAPI
from app.utils.logging import get_logger
from app.utils.token import hash_token

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
            sql_category_service: SqlCategoryService
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

            while True:
                try:
                    response = await self.api.get_products_page(token, limit=100, cursor=cursor)
                    cards = response.get("cards", [])

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

                all_errors = await self.api.get_all_errors_for_update(token)
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
        error_details_by_nm_id = {}

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

                # Сохраняем детали для nmID
                error_details_by_nm_id[str(nm_id)] = {
                    "vendor_code": vendor_code,
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
            "error_details": error_details_by_nm_id,
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
