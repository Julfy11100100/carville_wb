from datetime import datetime
from typing import Optional
from uuid import uuid4

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import PyMongoError, DuplicateKeyError

from app.services.mongo_repository import MongoRepository
from app.exceptions.task import TaskDatabaseError, TaskAlreadyExistsError
from app.schemas.task import TaskInfo, TaskStatus, TaskType, GetTaskRequest
from app.utils.logging import get_logger
from app.utils.token import hash_token

logger = get_logger()


class TaskManager:
    """
    Менеджер для управления задачами в MongoDB.

    Предоставляет методы для создания, получения и обновления задач
    с полной обработкой ошибок и оптимизированными запросами.
    """

    def __init__(self, mongo_service: MongoRepository):
        """
        Args:
            mongo_service: Сервис для работы с MongoDB
        """
        self.tasks: AsyncIOMotorCollection = mongo_service.collection
        self._indexes_created = False

    async def ensure_indexes(self) -> None:
        """
        Создаёт необходимые индексы для оптимизации запросов.

        Идемпотентная операция - безопасно вызывать несколько раз.

        Raises:
            TaskDatabaseError: При ошибке создания индексов
        """
        if self._indexes_created:
            return

        try:
            # Составной индекс для поиска активных задач по токену
            await self.tasks.create_index(
                [
                    ("wb_token", ASCENDING),
                    ("status", ASCENDING),
                    ("created_at", DESCENDING)
                ],
                name="wb_token_status_created_idx",
                background=True
            )

            # Уникальный индекс на task_id для быстрого поиска
            await self.tasks.create_index(
                [("task_id", ASCENDING)],
                name="task_id_unique_idx",
                unique=True,
                background=True
            )

            # Индекс для фильтрации по типу задачи
            await self.tasks.create_index(
                [
                    ("wb_token", ASCENDING),
                    ("task_type", ASCENDING),
                    ("created_at", DESCENDING)
                ],
                name="wb_token_type_created_idx",
                background=True
            )

            # Индекс для очистки старых задач
            await self.tasks.create_index(
                [("created_at", DESCENDING)],
                name="created_at_idx",
                background=True
            )

            self._indexes_created = True

            logger.info(
                "Индексы TaskManager созданы успешно: "
                "wb_token_status_created_idx, task_id_unique_idx, "
                "wb_token_type_created_idx, created_at_idx"
            )

        except PyMongoError as e:
            logger.error(f"Ошибка при создании индексов TaskManager: {e}", exc_info=True)
            raise TaskDatabaseError(
                "Ошибка при создании индексов базы данных",
                details={"original_error": str(e)}
            )

    async def get_active_task_by_token(
            self,
            wb_token: str,
            task_type: TaskType,
    ) -> Optional[TaskInfo]:
        """
        Получает последнюю активную задачу по WB токену.

        Args:
            wb_token: WildBerries API токен
            task_type: Тип задачи

        Returns:
            TaskInfo если найдена активная задача, иначе None

        Raises:
            TaskDatabaseError: При ошибке запроса к базе данных
        """
        hash_wb_token = hash_token(wb_token)

        try:
            active_task = await self.tasks.find_one(
                {
                    "wb_token": hash_wb_token,
                    "task_type": task_type.value,
                    "status": {
                        "$in": [TaskStatus.PENDING.value, TaskStatus.RUNNING.value]
                    }
                },
                sort=[("created_at", DESCENDING)]
            )

            if active_task:
                task_id = active_task.get("task_id")
                task_type = active_task.get("task_type")
                status = active_task.get("status")
                logger.debug(f"Найдена активная задача: {task_id}, тип={task_type}, статус={status}")
                return TaskInfo(**active_task)

            logger.debug(f"Активных задач не найдено для токена {hash_wb_token[:8]}...")
            return None

        except PyMongoError as e:
            logger.error(f"Ошибка БД при получении активной задачи: {e}", exc_info=True)
            raise TaskDatabaseError(
                "Ошибка при получении активной задачи",
                details={"original_error": str(e)}
            )

    async def create_task(
            self,
            wb_token: str,
            task_type: TaskType,
            task_id: Optional[str] = None
    ) -> TaskInfo:
        """
        Создаёт новую задачу.

        Args:
            wb_token: WildBerries API токен
            task_type: Тип задачи
            task_id: Опциональный ID задачи (генерируется автоматически если не указан)

        Returns:
            Созданная задача

        Raises:
            TaskAlreadyExistsError: Если задача с таким ID уже существует
            TaskDatabaseError: При ошибке записи в базу данных
        """
        task_info = TaskInfo(
            task_id=task_id or str(uuid4()),
            wb_token=hash_token(wb_token),
            task_type=task_type,
            status=TaskStatus.PENDING,
            created_at=datetime.now()
        )

        try:
            await self.tasks.insert_one(task_info.model_dump())

            logger.info(
                f"Задача создана успешно: {task_info.task_id}, "
                f"тип={task_type.value}, статус={task_info.status.value}"
            )

            return task_info

        except DuplicateKeyError:
            logger.warning(f"Попытка создания задачи с дублирующимся ID: {task_info.task_id}")
            raise TaskAlreadyExistsError(
                f"Задача с ID {task_info.task_id} уже существует",
                details={"task_id": task_info.task_id}
            )

        except PyMongoError as e:
            logger.error(f"Ошибка БД при создании задачи {task_info.task_id}: {e}", exc_info=True)
            raise TaskDatabaseError(
                "Ошибка при создании задачи",
                details={
                    "task_id": task_info.task_id,
                    "original_error": str(e)
                }
            )

    async def save_task(self, task_info: TaskInfo) -> None:
        """
        Сохраняет (обновляет) задачу в базе данных.

        Args:
            task_info: Информация о задаче для сохранения

        Raises:
            TaskDatabaseError: При ошибке записи в базу данных
        """
        try:
            result = await self.tasks.update_one(
                {"task_id": task_info.task_id},
                {"$set": task_info.model_dump()},
                upsert=True
            )

            logger.debug(
                f"Задача сохранена: {task_info.task_id}, "
                f"совпадений={result.matched_count}, изменений={result.modified_count}"
            )

        except PyMongoError as e:
            logger.error(f"Ошибка БД при сохранении задачи {task_info.task_id}: {e}", exc_info=True)
            raise TaskDatabaseError(
                "Ошибка при сохранении задачи",
                details={
                    "task_id": task_info.task_id,
                    "original_error": str(e)
                }
            )

    async def get_task_by_id(self, wb_token: str, task_id: str) -> Optional[TaskInfo]:
        """
        Получает задачу по её ID.

        Args:
            wb_token: WildBerries API токен
            task_id: ID задачи

        Returns:
            TaskInfo если задача найдена, иначе None

        Raises:
            TaskDatabaseError: При ошибке запроса к базе данных
        """
        try:
            doc = await self.tasks.find_one(
                {
                    "task_id": task_id,
                    "wb_token": hash_token(wb_token)
                })

            if not doc:
                logger.debug(f"Задача не найдена: {task_id}")
                return None

            status = doc.get("status")
            logger.debug(f"Задача найдена: {task_id}, статус={status}")

            return TaskInfo(**doc)

        except PyMongoError as e:
            logger.error(f"Ошибка БД при получении задачи {task_id}: {e}", exc_info=True)
            raise TaskDatabaseError(
                "Ошибка при получении задачи по ID",
                details={
                    "task_id": task_id,
                    "original_error": str(e)
                }
            )

    async def get_tasks_by_token(
            self,
            wb_token: str,
            filters: GetTaskRequest
    ) -> list[TaskInfo]:
        """
        Получает список задач по токену с фильтрацией.

        Args:
            wb_token: WildBerries API токен
            filters: Фильтры для поиска задач

        Returns:
            Список задач, соответствующих фильтрам

        Raises:
            TaskDatabaseError: При ошибке запроса к базе данных
        """
        hash_wb_token = hash_token(wb_token)

        # Строим query динамически
        query = {"wb_token": hash_wb_token}

        if filters.task_id:
            query["task_id"] = filters.task_id

        if filters.task_type:
            query["task_type"] = filters.task_type.value

        if filters.status:
            query["status"] = filters.status.value

        try:
            tasks = []
            cursor = self.tasks.find(query).sort("created_at", DESCENDING)

            async for doc in cursor:
                tasks.append(TaskInfo(**doc))

            task_type = filters.task_type.value if filters.task_type else None
            status = filters.status.value if filters.status else None
            logger.info(
                f"Задачи получены: всего={len(tasks)}, "
                f"фильтры: task_id={filters.task_id}, тип={task_type}, статус={status}"
            )

            return tasks

        except PyMongoError as e:
            logger.error(f"Ошибка БД при получении задач по токену: {e}", exc_info=True)
            raise TaskDatabaseError(
                "Ошибка при получении задач по токену",
                details={"original_error": str(e)}
            )

    async def delete_task(self, task_id: str) -> bool:
        """
        Удаляет задачу по ID.

        Args:
            task_id: ID задачи для удаления

        Returns:
            True если задача была удалена, False если не найдена

        Raises:
            TaskDatabaseError: При ошибке удаления из базы данных
        """
        try:
            result = await self.tasks.delete_one({"task_id": task_id})

            if result.deleted_count > 0:
                logger.info(f"Задача удалена успешно: {task_id}")
                return True
            else:
                logger.debug(f"Задача не найдена для удаления: {task_id}")
                return False

        except PyMongoError as e:
            logger.error(f"Ошибка БД при удалении задачи {task_id}: {e}", exc_info=True)
            raise TaskDatabaseError(
                "Ошибка при удалении задачи",
                details={
                    "task_id": task_id,
                    "original_error": str(e)
                }
            )

    async def delete_old_tasks(
            self,
            older_than: datetime,
            statuses: Optional[list[TaskStatus]] = None
    ) -> int:
        """
        Удаляет старые задачи для очистки базы данных.

        Args:
            older_than: Удалить задачи созданные до этой даты
            statuses: Опциональный список статусов для фильтрации (удаляются только эти статусы)

        Returns:
            Количество удалённых задач

        Raises:
            TaskDatabaseError: При ошибке удаления из базы данных
        """
        query = {"created_at": {"$lt": older_than}}

        if statuses:
            query["status"] = {"$in": [s.value for s in statuses]}

        try:
            result = await self.tasks.delete_many(query)

            statuses_str = ", ".join([s.value for s in statuses]) if statuses else "все"
            logger.info(
                f"Старые задачи удалены: всего удалено={result.deleted_count}, "
                f"старше={older_than.isoformat()}, статусы={statuses_str}"
            )

            return result.deleted_count

        except PyMongoError as e:
            logger.error(f"Ошибка БД при удалении старых задач: {e}", exc_info=True)
            raise TaskDatabaseError(
                "Ошибка при удалении старых задач",
                details={"original_error": str(e)}
            )

    async def count_tasks(
            self,
            wb_token: Optional[str] = None,
            status: Optional[TaskStatus] = None,
            task_type: Optional[TaskType] = None
    ) -> int:
        """
        Подсчитывает количество задач с заданными фильтрами.

        Args:
            wb_token: Опциональный WB токен для фильтрации
            status: Опциональный статус для фильтрации
            task_type: Опциональный тип задачи для фильтрации

        Returns:
            Количество задач

        Raises:
            TaskDatabaseError: При ошибке запроса к базе данных
        """
        query = {}

        if wb_token:
            query["wb_token"] = hash_token(wb_token)
        if status:
            query["status"] = status.value
        if task_type:
            query["task_type"] = task_type.value

        try:
            count = await self.tasks.count_documents(query)

            logger.debug(f"Задачи подсчитаны: всего={count}, фильтры={query}")

            return count

        except PyMongoError as e:
            logger.error(f"Ошибка БД при подсчёте задач: {e}", exc_info=True)
            raise TaskDatabaseError(
                "Ошибка при подсчёте задач",
                details={"original_error": str(e)}
            )

    async def get_task_statistics(
            self,
            wb_token: Optional[str] = None
    ) -> dict[str, int]:
        """
        Получает статистику по задачам.

        Args:
            wb_token: Опциональный WB токен для фильтрации

        Returns:
            Словарь со статистикой по статусам задач

        Raises:
            TaskDatabaseError: При ошибке запроса к базе данных
        """
        match_stage = {}
        if wb_token:
            match_stage = {"wb_token": hash_token(wb_token)}

        pipeline = []
        if match_stage:
            pipeline.append({"$match": match_stage})

        pipeline.extend([
            {
                "$group": {
                    "_id": "$status",
                    "count": {"$sum": 1}
                }
            }
        ])

        try:
            statistics = {status.value: 0 for status in TaskStatus}

            async for doc in self.tasks.aggregate(pipeline):
                statistics[doc["_id"]] = doc["count"]

            filtered_str = "с фильтром по токену" if wb_token else "без фильтра"
            logger.info(f"Статистика задач получена {filtered_str}: {statistics}")

            return statistics

        except PyMongoError as e:
            logger.error(f"Ошибка БД при получении статистики задач: {e}", exc_info=True)
            raise TaskDatabaseError(
                "Ошибка при получении статистики задач",
                details={"original_error": str(e)}
            )
