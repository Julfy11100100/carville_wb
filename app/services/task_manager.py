from datetime import datetime
from typing import Optional
from uuid import uuid4

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import PyMongoError, DuplicateKeyError

from app.services.mongo_repository import MongoService
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

    def __init__(self, mongo_service: MongoService):
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
                "Task Manager indexes created successfully",
                extra={
                    "indexes": [
                        "wb_token_status_created_idx",
                        "task_id_unique_idx",
                        "wb_token_type_created_idx",
                        "created_at_idx"
                    ]
                }
            )

        except PyMongoError as e:
            logger.error(
                "Failed to create Task Manager indexes",
                extra={"error": str(e), "error_type": type(e).__name__},
                exc_info=True
            )
            raise TaskDatabaseError(
                "Failed to create database indexes",
                details={"original_error": str(e)}
            )

    async def get_active_task_by_token(
            self,
            wb_token: str
    ) -> Optional[TaskInfo]:
        """
        Получает последнюю активную задачу по WB токену.

        Args:
            wb_token: WildBerries API токен

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
                    "status": {
                        "$in": [TaskStatus.PENDING.value, TaskStatus.RUNNING.value]
                    }
                },
                sort=[("created_at", DESCENDING)]
            )

            if active_task:
                logger.debug(
                    "Active task found",
                    extra={
                        "task_id": active_task.get("task_id"),
                        "task_type": active_task.get("task_type"),
                        "status": active_task.get("status")
                    }
                )
                return TaskInfo(**active_task)

            logger.debug(
                "No active task found",
                extra={"wb_token_hash": hash_wb_token[:8] + "..."}
            )
            return None

        except PyMongoError as e:
            logger.error(
                "Database error while fetching active task",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )
            raise TaskDatabaseError(
                "Failed to fetch active task",
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
                "Task created successfully",
                extra={
                    "task_id": task_info.task_id,
                    "task_type": task_type.value,
                    "status": task_info.status.value
                }
            )

            return task_info

        except DuplicateKeyError:
            logger.warning(
                "Attempted to create task with duplicate ID",
                extra={"task_id": task_info.task_id}
            )
            raise TaskAlreadyExistsError(
                f"Task with ID {task_info.task_id} already exists",
                details={"task_id": task_info.task_id}
            )

        except PyMongoError as e:
            logger.error(
                "Database error while creating task",
                extra={
                    "task_id": task_info.task_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )
            raise TaskDatabaseError(
                "Failed to create task",
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
                "Task saved successfully",
                extra={
                    "task_id": task_info.task_id,
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count,
                    "upserted": result.upserted_id is not None
                }
            )

        except PyMongoError as e:
            logger.error(
                "Database error while saving task",
                extra={
                    "task_id": task_info.task_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )
            raise TaskDatabaseError(
                "Failed to save task",
                details={
                    "task_id": task_info.task_id,
                    "original_error": str(e)
                }
            )

    async def get_task_by_id(self, task_id: str) -> Optional[TaskInfo]:
        """
        Получает задачу по её ID.

        Args:
            task_id: ID задачи

        Returns:
            TaskInfo если задача найдена, иначе None

        Raises:
            TaskDatabaseError: При ошибке запроса к базе данных
        """
        try:
            doc = await self.tasks.find_one({"task_id": task_id})

            if not doc:
                logger.debug(
                    "Task not found",
                    extra={"task_id": task_id}
                )
                return None

            logger.debug(
                "Task found",
                extra={
                    "task_id": task_id,
                    "status": doc.get("status")
                }
            )

            return TaskInfo(**doc)

        except PyMongoError as e:
            logger.error(
                "Database error while fetching task by ID",
                extra={
                    "task_id": task_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )
            raise TaskDatabaseError(
                "Failed to fetch task by ID",
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

            logger.info(
                "Tasks fetched successfully",
                extra={
                    "count": len(tasks),
                    "filters": {
                        "task_id": filters.task_id,
                        "task_type": filters.task_type.value if filters.task_type else None,
                        "status": filters.status.value if filters.status else None
                    }
                }
            )

            return tasks

        except PyMongoError as e:
            logger.error(
                "Database error while fetching tasks by token",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "filters": query
                },
                exc_info=True
            )
            raise TaskDatabaseError(
                "Failed to fetch tasks by token",
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
                logger.info(
                    "Task deleted successfully",
                    extra={"task_id": task_id}
                )
                return True
            else:
                logger.debug(
                    "Task not found for deletion",
                    extra={"task_id": task_id}
                )
                return False

        except PyMongoError as e:
            logger.error(
                "Database error while deleting task",
                extra={
                    "task_id": task_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )
            raise TaskDatabaseError(
                "Failed to delete task",
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

            logger.info(
                "Old tasks deleted",
                extra={
                    "deleted_count": result.deleted_count,
                    "older_than": older_than.isoformat(),
                    "statuses": [s.value for s in statuses] if statuses else "all"
                }
            )

            return result.deleted_count

        except PyMongoError as e:
            logger.error(
                "Database error while deleting old tasks",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "query": query
                },
                exc_info=True
            )
            raise TaskDatabaseError(
                "Failed to delete old tasks",
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

            logger.debug(
                "Tasks counted",
                extra={
                    "count": count,
                    "filters": query
                }
            )

            return count

        except PyMongoError as e:
            logger.error(
                "Database error while counting tasks",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "query": query
                },
                exc_info=True
            )
            raise TaskDatabaseError(
                "Failed to count tasks",
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

            logger.info(
                "Task statistics retrieved",
                extra={
                    "statistics": statistics,
                    "filtered_by_token": wb_token is not None
                }
            )

            return statistics

        except PyMongoError as e:
            logger.error(
                "Database error while getting task statistics",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )
            raise TaskDatabaseError(
                "Failed to get task statistics",
                details={"original_error": str(e)}
            )
