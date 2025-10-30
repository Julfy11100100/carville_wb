import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

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
    Высокоуровневый клиент для работы с Wildberries.
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

    async def __aenter__(self):
        """Создание сессии при входе в контекстный менеджер"""
        await self.api.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие сессии при выходе из контекстного менеджера"""
        await self.api.__aexit__(exc_type, exc_val, exc_tb)

    async def close(self):
        """Закрывает все активные соединения"""
        await self.api.close()

    async def get_product(self, token: str) -> Dict[str, Any]:
        """Получение одного товара"""
        return await self.api.get_product(token)

    async def collect_products_background(self, token: str, task_info: TaskInfo):
        """
        Фоновая задача для получения всех товаров
        """
        try:
            task_info.status = TaskStatus.RUNNING
            await self.task_manager.save_task(task_info)

            all_products = []
            category_ids: Set[int] = set()
            cursor = {}

            logger.info(
                "Начало сбора товаров",
                extra={"task_id": task_info.task_id}
            )
            async with WildberriesAPI() as wb_api_client:
                while True:
                    try:
                        response = await wb_api_client.get_products_page(token, limit=100, cursor=cursor)
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
                            "Прогресс сбора товаров",
                            extra={
                                "task_id": task_info.task_id,
                                "collected": len(all_products),
                                "batch_size": len(cards)
                            }
                        )

                        await asyncio.sleep(0.1)

                    except WildberriesRateLimitError:
                        logger.warning(
                            "Достигнут лимит запросов во время сбора, ожидание",
                            extra={"task_id": task_info.task_id}
                        )
                        await asyncio.sleep(60)
                        continue

                # Сохраняем результаты
                file_path = await ProductFileService.save_products_to_file(
                    task_info.task_id,
                    all_products
                )

            # Индексируем
            await self.elasticsearch_service.index_products(
                token=hash_token(token)[:10],
                products=all_products
            )

            task_info.status = TaskStatus.COMPLETED
            task_info.completed_at = datetime.now()
            task_info.total_items = len(all_products)
            task_info.category_ids = list(category_ids)
            task_info.file_path = file_path

            logger.info(
                "Сбор товаров завершён",
                extra={
                    "task_id": task_info.task_id,
                    "total_products": len(all_products),
                    "file_path": file_path,
                    "categories_count": len(category_ids)
                }
            )

        except Exception as e:
            task_info.status = TaskStatus.FAILED
            task_info.completed_at = datetime.now()
            task_info.error = str(e)

            logger.error(
                "Ошибка при сборе товаров",
                extra={
                    "task_id": task_info.task_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )

        finally:
            await self.task_manager.save_task(task_info)

    async def update_products_background(
            self,
            token: str,
            products: List[Dict[str, Any]],
            task_info: TaskInfo
    ):
        """
        Фоновая задача для обновления карточек товаров.
        """
        try:
            task_info.status = TaskStatus.RUNNING
            task_info.total_items = len(products)

            # Сохраняем vendorCodes для последующей проверки
            vendor_codes = {p.get("vendorCode") for p in products if p.get("vendorCode")}
            nm_ids = [p.get("nmID") for p in products if p.get("nmID")]

            task_info.metadata = {
                "vendor_codes": list(vendor_codes),
                "nm_ids": nm_ids,
                "update_started_at": datetime.now().isoformat(),
                "update_completed_at": None
            }

            await self.task_manager.save_task(task_info)

            logger.info(
                "Начало обновления товаров",
                extra={
                    "task_id": task_info.task_id,
                    "total_products": len(products),
                    "unique_vendor_codes": len(vendor_codes)
                }
            )

            # Обновляем с автоматическим разбиением на чанки
            async with WildberriesAPI() as wb_api_client:
                await wb_api_client.update_products_chunked(token, products, delay_between_chunks=6)

            # Задача завершена - данные отправлены в WB
            task_info.status = TaskStatus.COMPLETED
            task_info.completed_at = datetime.now()
            task_info.processed_items = len(products)
            task_info.metadata["update_completed_at"] = datetime.now().isoformat()

            logger.info(
                "Обновление товаров отправлено в WB API",
                extra={
                    "task_id": task_info.task_id,
                    "total_products": len(products)
                }
            )

        except Exception as e:
            task_info.status = TaskStatus.FAILED
            task_info.completed_at = datetime.now()
            task_info.error = str(e)

            logger.error(
                "Ошибка при обновлении товаров",
                extra={
                    "task_id": task_info.task_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )

        finally:
            await self.task_manager.save_task(task_info)

    @staticmethod
    def _filter_relevant_errors(
            all_errors: List[Dict[str, Any]],
            vendor_codes: Set[str]
    ) -> List[Dict[str, Any]]:
        """
        Фильтрует пакеты ошибок по vendorCode, относящиеся к текущему обновлению.
        """
        relevant = []
        for batch in all_errors:
            batch_vendor_codes = set(batch.get("vendorCodes", []))
            if batch_vendor_codes & vendor_codes:
                relevant.append(batch)
        return relevant

    def _calculate_error_statistics(
            self,
            all_errors: List[Dict[str, Any]],
            vendor_codes: Set[str],
            total_products: int
    ) -> Dict[str, Any]:
        """
        Правильно подсчитывает статистику ошибок.
        ВАЖНО: Считает количество КАРТОЧЕК с ошибками, а не количество самих ошибок.
        Одна карточка может иметь несколько ошибок валидации.

        Args:
            all_errors: Все пакеты ошибок от API
            vendor_codes: Множество vendorCode обновляемых товаров
            total_products: Всего товаров для обновления

        Returns:
            Словарь со статистикой
        """
        # Фильтруем релевантные ошибки
        relevant_errors = self._filter_relevant_errors(all_errors, vendor_codes)

        # Собираем УНИКАЛЬНЫЕ vendorCode с ошибками (не считаем количество ошибок!)
        error_vendor_codes = set()
        error_details = {}

        for batch in relevant_errors:
            batch_errors = batch.get("errors", {})
            error_details.update(batch_errors)
            error_vendor_codes.update(batch_errors.keys())

        # Подсчитываем КАРТОЧКИ, а не ошибки
        error_count = len(error_vendor_codes)
        success_count = total_products - error_count
        success_rate = success_count / total_products if total_products > 0 else 0

        return {
            "success_count": success_count,
            "error_count": error_count,
            "success_rate": success_rate,
            "error_vendor_codes": error_vendor_codes,
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

        Args:
            token: API токен
            task_info: Информация о задаче обновления

        Returns:
            Словарь с детальной статистикой обновления
        """
        try:
            metadata = task_info.metadata or {}
            vendor_codes = set(metadata.get("vendor_codes", []))
            total_products = task_info.total_items or 0

            if not vendor_codes:
                return {
                    "checked": False,
                    "reason": "No vendor codes found in task metadata"
                }

            logger.info(
                "Проверка результатов обновления",
                extra={
                    "task_id": task_info.task_id,
                    "vendor_codes_count": len(vendor_codes)
                }
            )

            # Получаем все ошибки
            all_errors = await self.api.get_all_errors_for_update(token)

            # Вычисляем статистику
            stats = self._calculate_error_statistics(
                all_errors,
                vendor_codes,
                total_products
            )

            logger.info(
                "Результаты проверки обновления",
                extra={
                    "task_id": task_info.task_id,
                    "success_count": stats["success_count"],
                    "error_count": stats["error_count"],
                    "success_rate": f"{stats['success_rate'] * 100:.1f}%"
                }
            )

            return {
                "checked": True,
                "success_count": stats["success_count"],
                "error_count": stats["error_count"],
                "success_rate": stats["success_rate"],
                "error_vendor_codes": list(stats["error_vendor_codes"]),
                "error_details": stats["error_details"],
                "error_batches": stats["error_batches"]
            }

        except Exception as e:
            logger.error(
                "Ошибка при проверке результатов обновления",
                extra={
                    "task_id": task_info.task_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )

            return {
                "checked": False,
                "reason": f"Error checking results: {str(e)}"
            }
