from enum import Enum


# Доступные типы сущностей
class EntityType(str, Enum):
    """Типы сущностей"""
    PRODUCTS = "products"  # Товары: "📦"
    CATEGORIES = "categories"  # Категории: "📂"
    SYSTEM = "system"  # Система: "⚙️"
    FILE = "file"  # Файл: "📄"
    TASK = "task"  # Задача: "📋"


# Доступные типы действий
class ActionType(str, Enum):
    """Типы действий"""
    SYNC = "sync"  # Синхронизация: "🔄"
    CREATE = "created"  # Создание: "➕"
    UPDATE = "updated"  # Обновление: "✏️"
    DELETE = "deleted"  # Удаление: "🗑️"
    FETCH = "fetch"  # Получение: "📥"
    STATUS = "status"  # Статус: "🔧"
    UPLOADED = "uploaded"  # Загрузка: "⬆️"
    PROCESSED = "processed"  # Обработка: "🏭"
    FAILED = "failed"  # Сбой: "💥"
    NOTIFIED = "notified"  # Уведомление: "📣"


# доступные статусы операций
class NotificationStatus(str, Enum):
    """Статусы операций"""
    SUCCESS = "success"  # "✅"
    ERROR = "error"  # "❌"
    WARNING = "warning"  # "⚠️"
    INFO = "info"  # "ℹ️"
