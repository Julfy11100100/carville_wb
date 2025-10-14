import uuid
from datetime import datetime
from typing import Optional

from app.core.mongo_repository import MongoService
from app.schemas.task import TaskInfo, TaskStatus, TaskType, GetTaskRequest
from app.utils.logging import get_logger
from app.utils.token import hash_token

logger = get_logger()


class TaskManager:
    def __init__(self, mongo_service: MongoService):
        self.tasks = mongo_service.collection

    async def init_indexes(self):
        # Индекс для поиска задач по wb_token и даты создания
        await self.tasks.create_index([("wb_token", 1), ("created_at", -1)])

    async def get_id_task_by_token(self, wb_token: str) -> Optional[TaskInfo]:
        """Получаем id таски по wb токену"""
        hash_wb_token = hash_token(wb_token)
        active_task = await self.tasks.find_one({
            "wb_token": hash_wb_token,
            "status": {"$in": [TaskStatus.PENDING.value, TaskStatus.RUNNING.value]}
        }, sort=[("created_at", -1)])  # Последняя активная task

        if active_task:
            return TaskInfo(**active_task)
        else:
            return None

    async def create_task(self, wb_token: str, task_type: TaskType) -> TaskInfo:
        """
        Создает новую задачу для wb_token,
        """
        task_info = TaskInfo(
            task_id=str(uuid.uuid4()),
            wb_token=hash_token(wb_token),
            task_type=task_type,
            status=TaskStatus.PENDING,
            created_at=datetime.now()
        )

        await self.tasks.insert_one(task_info.model_dump())
        return task_info

    async def save_task(self, task_info: TaskInfo):
        await self.tasks.update_one(
            {"task_id": task_info.task_id},
            {"$set": task_info.model_dump()},
            upsert=True
        )

    async def get_task_status(self, task_id: str) -> TaskInfo | None:
        doc = await self.tasks.find_one({"task_id": task_id})
        if not doc:
            return None
        return TaskInfo(**doc["data"])

    async def get_tasks_by_token(self, wb_token: str, task_info: GetTaskRequest) -> list[TaskInfo]:
        """
        Вернуть все задачи по wb_token, отсортированные по времени создания (новые первыми)
        """
        hash_wb_token = hash_token(wb_token)
        query = {}
        tasks = []
        if task_info.task_id and task_info.task_type:
            query.update({
                "task_id": task_info.task_id,
                "task_type": task_info.task_type
            })
        if task_info.status:
            query.update({
                "status": task_info.status
            })

        if query is not None:
            query.update({"wb_token": hash_wb_token})
            cursor = self.tasks.find(query).sort("created_at", -1)
            async for doc in cursor:
                tasks.append(TaskInfo(**doc))

        return tasks
