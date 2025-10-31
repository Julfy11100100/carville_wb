import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Set

from app.exceptions.wb_api import WildberriesRateLimitError
from app.schemas.task import TaskStatus, TaskInfo
from app.services.elasticsearch_service import ElasticsearchService
from app.services.product_file_service import ProductFileService
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

    async def get_product(self, token: str) -> Dict[str, Any]:
        """Получение одного товара"""
        return await self.api.get_product(token)

    async def collect_products_background(self, token: str, task_info: TaskInfo):
        """Фоновая задача для получения всех товаров"""
        try:
            task_info.status = TaskStatus.RUNNING
            await self.task_manager.save_task(task_info)

            all_products = []
            category_ids: Set[int] = set()
            cursor = {}

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

                    all_products.extend(cards)

                    # Обновляем курсор
                    cursor = response.get("cursor", {})
                    if not cursor or len(cards) < cursor.get("limit", 100):
                        break

                    # Обновляем прогресс
                    task_info.total_items = len(all_products)
                    await self.task_manager.save_task(task_info)

                    logger.debug(
                        f"Прогресс сбора: {task_info.task_id}, "
                        f"собрано={len(all_products)}, батч={len(cards)}"
                    )

                    await asyncio.sleep(0.1)

                except WildberriesRateLimitError:
                    logger.warning(f"Rate limit для {task_info.task_id}, ожидание 60с...")
                    await asyncio.sleep(60)
                    continue

            # Сохраняем результаты
            file_path = await ProductFileService.save_products_to_file(
                task_info.task_id,
                all_products
            )

            # Индексируем
            await self.elasticsearch_service.index_products(
                token=hash_token(token),
                products=all_products
            )

            task_info.status = TaskStatus.COMPLETED
            task_info.completed_at = datetime.now()
            task_info.total_items = len(all_products)
            task_info.category_ids = list(category_ids)
            task_info.file_path = file_path

            logger.info(
                f"Сбор товаров завершён: {task_info.task_id}, "
                f"всего={len(all_products)}, категорий={len(category_ids)}, "
                f"файл={file_path}"
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
            products: List[Dict[str, Any]],
            task_info: TaskInfo
    ):
        """Фоновая задача для обновления карточек товаров с гарантированной проверкой."""
        try:
            task_info.status = TaskStatus.RUNNING
            task_info.total_items = len(products)

            nm_ids = [int(p.get("nmID")) for p in products if p.get("nmID")]

            task_info.metadata = {
                "nm_ids": nm_ids,
                "update_started_at": datetime.now().isoformat(),
                "update_completed_at": None,
                "check_results": None,
                "polling_attempts": 0,
                "final_error_count": None,
                "final_success_count": None
            }

            await self.task_manager.save_task(task_info)

            logger.info(f"Начало обновления товаров: {task_info.task_id}, всего={len(products)}")

            # 1. Отправляем товары в WB
            await self.api.update_products_chunked(token, products, delay_between_chunks=6)

            # 2. Сохраняем дату отправки
            task_info.metadata["update_sent_at"] = datetime.now().isoformat()
            await self.task_manager.save_task(task_info)

            # 3. Проверяем результаты с поллингом
            result = await self._check_update_results_with_polling(
                token=token,
                task_info=task_info,
                max_attempts=5,
                initial_delay=10,
                retry_delay=15
            )

            # 4. Сохраняем финальные результаты
            task_info.metadata["check_results"] = result
            task_info.metadata["update_completed_at"] = datetime.now().isoformat()
            task_info.processed_items = result.get("success_count", 0)

            if result.get("error_count", 0) > 0:
                task_info.status = TaskStatus.COMPLETED_WITH_ERRORS
                logger.warning(
                    f"Обновление {task_info.task_id} завершено с ошибками: "
                    f"успешно={result['success_count']}, ошибок={result['error_count']}, "
                    f"попыток={result.get('polling_attempts', 0)}"
                )
            else:
                task_info.status = TaskStatus.COMPLETED
                logger.info(
                    f"Обновление {task_info.task_id} успешно: "
                    f"успешно={result['success_count']}, попыток={result.get('polling_attempts', 0)}"
                )

        except Exception as e:
            task_info.status = TaskStatus.FAILED
            task_info.completed_at = datetime.now()
            task_info.error = str(e)

            logger.error(f"Ошибка при обновлении товаров {task_info.task_id}: {e}", exc_info=True)

        finally:
            await self.task_manager.save_task(task_info)

    async def _check_update_results_with_polling(
            self,
            token: str,
            task_info: TaskInfo,
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
            max_attempts: Максимальное количество попыток
            initial_delay: Первая задержка (сек)
            retry_delay: Задержка между попытками (сек)
            timeout_minutes: Максимальное время ожидания (мин)

        Returns:
            Результаты с информацией об ошибках
        """
        metadata = task_info.metadata or {}
        nm_ids = set(metadata.get("nm_ids", []))
        total_products = task_info.total_items or 0

        if not nm_ids:
            return {
                "checked": False,
                "reason": "Нет nmID в метаданных задачи",
                "polling_attempts": 0
            }

        previous_error_count = -1
        stable_count = 0
        attempt = 0
        timeout_deadline = datetime.now() + timedelta(minutes=timeout_minutes)

        await asyncio.sleep(initial_delay)

        logger.info(f"Начало поллинга результатов: {task_info.task_id}, макс_попыток={max_attempts}")

        while attempt < max_attempts:
            if datetime.now() > timeout_deadline:
                logger.warning(f"Таймаут поллинга {task_info.task_id} ({timeout_minutes} мин)")
                break

            attempt += 1

            try:
                logger.debug(f"Проверка результатов {task_info.task_id}: попытка {attempt}/{max_attempts}")

                all_errors = await self.api.get_all_errors_for_update(token)
                stats = self._calculate_error_statistics(all_errors, nm_ids, total_products)

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
                            "error_details": {
                                str(nm_id): errors
                                for nm_id, errors in stats["error_details"].items()
                            },
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
                        "error_details": {
                            str(nm_id): errors
                            for nm_id, errors in stats["error_details"].items()
                        },
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
            nm_ids: Set[int]
    ) -> List[Dict[str, Any]]:
        """Фильтрует пакеты ошибок по nmID"""
        relevant = []
        for batch in all_errors:
            batch_nm_ids = set(batch.get("nmIDs", []))
            if batch_nm_ids & nm_ids:
                relevant.append(batch)
        return relevant

    def _calculate_error_statistics(
            self,
            all_errors: List[Dict[str, Any]],
            nm_ids: Set[int],
            total_products: int
    ) -> Dict[str, Any]:
        """
        Подсчитывает статистику ошибок.
        ВАЖНО: Считает КАРТОЧКИ с ошибками, не сами ошибки.
        """
        relevant_errors = self._filter_relevant_errors(all_errors, nm_ids)

        error_nm_ids = set()
        error_details = {}

        for batch in relevant_errors:
            batch_errors = batch.get("errors", {})
            error_details.update(batch_errors)
            error_nm_ids.update(batch_errors.keys())

        error_count = len(error_nm_ids)
        success_count = total_products - error_count
        success_rate = success_count / total_products if total_products > 0 else 0

        return {
            "success_count": success_count,
            "error_count": error_count,
            "success_rate": success_rate,
            "error_nm_ids": error_nm_ids,
            "error_details": error_details,
            "error_batches": relevant_errors
        }

    async def check_update_results(
            self,
            token: str,
            task_info: TaskInfo
    ) -> Dict[str, Any]:
        """
        Проверяет результаты обновления товаров через cards/error/list.
        Сохраняет результаты в метаданные задачи.

        Args:
            token: API токен
            task_info: Информация о задаче обновления

        Returns:
            Словарь с статистикой обновления
        """
        try:
            metadata = task_info.metadata or {}
            nm_ids = set(metadata.get("nm_ids", []))
            total_products = task_info.total_items or 0

            if not nm_ids:
                return {
                    "checked": False,
                    "reason": "Нет nmID в метаданных задачи"
                }

            logger.info(f"Проверка результатов обновления: {task_info.task_id}, nmID={len(nm_ids)}")

            all_errors = await self.api.get_all_errors_for_update(token)

            stats = self._calculate_error_statistics(
                all_errors,
                nm_ids,
                total_products
            )

            success_rate_pct = stats['success_rate'] * 100
            logger.info(
                f"Результаты проверки {task_info.task_id}: "
                f"успешно={stats['success_count']}, ошибок={stats['error_count']}, "
                f"процент={success_rate_pct:.1f}%"
            )

            error_nm_ids_list = list(stats["error_nm_ids"])
            error_details_serializable = {
                str(nm_id): errors for nm_id, errors in stats["error_details"].items()
            }

            check_results = {
                "checked": True,
                "success_count": stats["success_count"],
                "error_count": stats["error_count"],
                "success_rate": stats["success_rate"],
                "error_nm_ids": error_nm_ids_list,
                "error_details": error_details_serializable,
                "error_batches_count": len(stats["error_batches"])
            }

            task_info.metadata["error_statistics"] = check_results

            return check_results

        except Exception as e:
            logger.error(f"Ошибка при проверке результатов {task_info.task_id}: {e}", exc_info=True)

            error_result = {
                "checked": False,
                "reason": f"Ошибка при проверке результатов: {e}"
            }

            task_info.metadata["error_statistics"] = error_result

            return error_result
