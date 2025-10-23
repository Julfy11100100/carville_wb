from typing import Optional


class TaskManagerError(Exception):
    """Базовое исключение для всех ошибок TaskManager"""

    def __init__(self, message: str, details: Optional[dict] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class TaskNotFoundError(TaskManagerError):
    """Исключение для случаев, когда задача не найдена"""
    pass


class TaskAlreadyExistsError(TaskManagerError):
    """Исключение для случаев, когда задача уже существует"""
    pass


class TaskDatabaseError(TaskManagerError):
    """Исключение для ошибок базы данных при работе с задачами"""
    pass
