import re
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, computed_field


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
            raise ValueError("task_id не может быть пустым")
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
                "task_type": "collect_products",
                "status": "completed"
            }
        }


class TaskCreateResponse(BaseModel):
    """Ответ при создании задачи"""
    task_id: str = Field(..., description="Уникальный идентификатор созданной задачи")
    task_type: TaskType = Field(..., description="Тип созданной задачи")
    status: TaskStatus = Field(..., description="Начальный статус задачи")
    message: str = Field(..., description="Информационное сообщение о следующих шагах")
