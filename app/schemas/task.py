from datetime import datetime
from enum import Enum
from typing import Optional, Dict, List

from pydantic import BaseModel, Field
from pydantic.v1 import root_validator


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskType(str, Enum):
    GET_PRODUCTS = "products_info"


class TaskInfo(BaseModel):
    task_id: str = Field(..., description="id задачи")
    task_type: TaskType = Field(..., description="Тип задачи")
    wb_token: str = Field(..., description="wb токен")
    status: TaskStatus = Field(..., description="Статус задачи")
    error: Optional[str] = Field(None, description="Описание ошибки при завершении")

    created_at: datetime = Field(..., description="Время создания")
    completed_at: Optional[datetime] = Field(None, description="Время завершения задачи")

    category_ids: Optional[List[int]] = Field(None, description="Список id категорий")
    total_items: Optional[int] = Field(None, description="Количество собранных товаров")
    file_path: Optional[str] = Field(None, description="Путь до файла")

    def to_front(self) -> Dict:
        data = {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "status": self.status,
            "created_at": self.created_at,
        }
        # Добавим completed_at, если он не None
        if self.completed_at is not None:
            data["completed_at"] = self.completed_at
        # Добавим total_items, если он не None
        if self.total_items is not None:
            data["total_items"] = self.total_items

        # Добавляем category_ids, если он ен None
        if self.category_ids is not None:
            data["category_ids"] = self.category_ids

        return data

    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class GetTaskRequest(BaseModel):
    task_id: Optional[str] = Field(None, description="id таски")
    task_type: TaskType = Field(..., description="тип таски")
    period: Optional[str] = Field(None, description="Временной период")
    status: Optional[TaskStatus] = Field(None, description="Статус")

    @root_validator
    def check_fields(cls, values):
        task_id = values.get('task_id')
        task_type = values.get('task_type')
        period = values.get('period')
        status = values.get('status')

        if task_id is not None:
            # если есть task_id, то должен быть и task_type, period и status должны быть None
            if task_type is None:
                raise ValueError("Если указан task_id, то должен быть также task_type.")
            if period is not None or status is not None:
                raise ValueError("Если указан task_id, period и status должны быть None.")
        else:
            # если task_id нет, то task_type, period, status должны быть не None
            if task_type is None or period is None or status is None:
                raise ValueError("Если task_id отсутствует, то task_type, period и status должны быть указаны.")

        return values
