from datetime import datetime
from enum import Enum
from typing import Any, Optional

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
    COLLECT_PRODUCTS = "collect_products"
    UPDATE_PRODUCTS = "update_products"


class TaskInfo(BaseModel):
    """Модель информации о задаче"""
    task_id: str = Field(..., description="Уникальный идентификатор задачи")
    wb_token: str = Field(..., description="Хешированный WB токен")
    task_type: TaskType = Field(..., description="Тип задачи")
    status: TaskStatus = Field(..., description="Текущий статус задачи")
    created_at: datetime = Field(..., description="Время создания задачи")
    completed_at: Optional[datetime] = Field(None, description="Время завершения задачи")
    total_items: int = Field(0, description="Общее количество элементов")
    processed_items: int = Field(0, description="Количество обработанных элементов")
    category_ids: list[int] = Field(default_factory=list, description="ID категорий")
    file_path: Optional[str] = Field(None, description="Путь к файлу с результатами")
    error: Optional[str] = Field(None, description="Сообщение об ошибке")
    metadata: Optional[dict[str, Any]] = Field(None, description="Дополнительные метаданные")

    @field_validator('task_id')
    @classmethod
    def validate_task_id(cls, v: str) -> str:
        """Валидация task_id"""
        if not v or not v.strip():
            raise ValueError("task_id cannot be empty")
        return v.strip()

    class Config:
        """Конфигурация Pydantic модели"""
        json_schema_extra = {
            "example": {
                "task_id": "550e8400-e29b-41d4-a716-446655440000",
                "wb_token": "hashed_token_value",
                "task_type": "collect_products",
                "status": "running",
                "created_at": "2025-10-23T10:00:00Z",
                "total_items": 1000,
                "processed_items": 500
            }
        }


class GetTaskRequest(BaseModel):
    """Модель запроса для получения задач с фильтрами"""
    task_id: Optional[str] = Field(None, description="Фильтр по ID задачи")
    task_type: Optional[TaskType] = Field(None, description="Фильтр по типу задачи")
    status: Optional[TaskStatus] = Field(None, description="Фильтр по статусу")

    class Config:
        """Конфигурация Pydantic модели"""
        json_schema_extra = {
            "example": {
                "task_type": "collect_products",
                "status": "completed"
            }
        }
