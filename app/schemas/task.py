import re
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional, Dict, List

from pydantic import BaseModel, Field, field_validator


class TaskStatus(str, Enum):
    """Статус выполнения задачи"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"


class TaskType(str, Enum):
    """Тип задачи"""
    PRODUCTS_INFO = "products_info"
    PRODUCT_UPDATE = "product_update"
    PRODUCT_CREATE = "product_create"


class TaskInfo(BaseModel):
    """Модель информации о задаче"""
    task_id: str = Field(..., description="Уникальный идентификатор задачи")
    wb_token: str = Field(..., description="Хешированный WB токен")
    task_type: TaskType = Field(..., description="Тип задачи")
    status: TaskStatus = Field(..., description="Текущий статус задачи")
    created_at: datetime = Field(..., description="Время создания задачи")
    completed_at: Optional[datetime] = Field(None, description="Время завершения задачи")
    error: Optional[str] = Field(None, description="Сообщение об ошибке")
    total_items: int = Field(0, description="Общее количество элементов")

    # Для задачи получения карточек
    category_ids: Optional[Dict[str, List[int]]] = Field(None,
                                                         description="Словарь {category_id: [type_id, ...]} с уникальными type_id для каждой категории (только для products_info)")
    categories_count: Optional[int] = Field(None,
                                            description="Количество уникальных категорий (только для products_info)")

    # Для задачи обновления карточки
    processed_items: int = Field(0, description="Количество обработанных элементов")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Дополнительные метаданные")

    @field_validator('task_id')
    @classmethod
    def validate_task_id(cls, v: str) -> str:
        """Валидация task_id"""
        if not v or not v.strip():
            raise ValueError("task_id не может быть пустым")
        return v.strip()

    class Config:
        """Конфигурация Pydantic модели"""
        json_schema_extra = {
            "example": {
                "task_id": "550e8400-e29b-41d4-a716-446655440000",
                "wb_token": "hashed_token_value",
                "task_type": "products_info",
                "status": "running",
                "created_at": "2025-10-23T10:00:00Z",
                "total_items": 1000,
                "processed_items": 500
            }
        }


class TaskStatusRequest(BaseModel):
    """Модель запроса для получения задач с фильтрами"""
    task_id: Optional[str] = Field(None, description="Фильтр по ID задачи")
    task_type: TaskType = Field(..., description="Фильтр по типу задачи")
    status: Optional[TaskStatus] = Field(None, description="Фильтр по статусу")
    period: Optional[str] = Field(None, description="Период в формате: 1h, 2d, 3w")

    def parse_period(self) -> Optional[datetime]:
        """Парсит строку периода и возвращает datetime"""
        if not self.period:
            return None

        pattern = r'^(\d+)([hdw])$'
        match = re.match(pattern, self.period.lower())

        if not match:
            raise ValueError(
                f"Неверный формат периода: '{self.period}'. "
                f"Ожидаемый формат: <число><единица>, где единица это 'h' (часы), 'd' (дни), или 'w' (недели). "
                f"Примеры: '1h', '2d', '3w'"
            )

        amount = int(match.group(1))
        unit = match.group(2)

        if unit == 'h':
            delta = timedelta(hours=amount)
        elif unit == 'd':
            delta = timedelta(days=amount)
        elif unit == 'w':
            delta = timedelta(weeks=amount)

        return datetime.now() - delta

    class Config:
        """Конфигурация Pydantic модели"""
        json_schema_extra = {
            "example": {
                "task_type": "products_info",
                "status": "completed"
            }
        }


class TaskCreateResponse(BaseModel):
    """Ответ при создании задачи"""
    task_id: str = Field(..., description="Уникальный идентификатор созданной задачи")
    task_type: TaskType = Field(..., description="Тип созданной задачи")
    status: TaskStatus = Field(..., description="Начальный статус задачи")
    message: str = Field(..., description="Информационное сообщение о следующих шагах")


class TaskInfoResponse(BaseModel):
    """Response schema для фронтенда"""
    task_id: str
    task_type: str
    status: str
    created_at: str
    completed_at: Optional[str] = None
    products_count: int
    category_ids: Optional[Dict[str, List[int]]] = None
    categories_count: Optional[int] = None
    error: Optional[str] = None
    update_errors: Optional[Dict[int, Any]] = None
    processed_items: Optional[int] = None

    @classmethod
    def from_task_info(cls, task: TaskInfo) -> "TaskInfoResponse":
        data = {
            "task_id": task.task_id,
            "task_type": task.task_type.value,
            "status": task.status.value,
            "created_at": task.created_at.isoformat(),
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
            "products_count": task.total_items,
            "error": task.error,
            "category_ids": None,
            "categories_count": None,
            "processed_items": None,
            "update_errors": None,
        }

        if task.task_type == TaskType.PRODUCTS_INFO:
            data["category_ids"] = task.category_ids
            data["categories_count"] = task.categories_count

        elif task.task_type == TaskType.PRODUCT_UPDATE:
            data["processed_items"] = task.processed_items
            if task.metadata:
                check_results = task.metadata.get('check_results')
                if check_results:
                    data["update_errors"] = check_results.get('error_details') or None

        return cls.model_construct(**data)
