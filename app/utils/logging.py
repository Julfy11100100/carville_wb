import contextvars
import json
import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

from config import settings

# ============================================================================
# Context Variables для хранения контекстной информации
# ============================================================================

# Эти переменные будут уникальными для каждого async таска/потока
request_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    'request_id', default=None
)
user_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    'user_id', default=None
)
task_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    'task_id', default=None
)


# ============================================================================
# Вспомогательные функции для работы с контекстом
# ============================================================================

def set_request_id(request_id: str) -> None:
    """Устанавливает request_id в текущий контекст"""
    request_id_var.set(request_id)


def get_request_id() -> Optional[str]:
    """Получает request_id из текущего контекста"""
    return request_id_var.get()


def set_user_id(user_id: str) -> None:
    """Устанавливает user_id в текущий контекст"""
    user_id_var.set(user_id)


def get_user_id() -> Optional[str]:
    """Получает user_id из текущего контекста"""
    return user_id_var.get()


def set_task_id(task_id: str) -> None:
    """Устанавливает task_id в текущий контекст"""
    task_id_var.set(task_id)


def get_task_id() -> Optional[str]:
    """Получает task_id из текущего контекста"""
    return task_id_var.get()


def clear_context() -> None:
    """Очищает весь контекст (полезно после обработки запроса)"""
    request_id_var.set(None)
    user_id_var.set(None)
    task_id_var.set(None)


# ============================================================================
# Custom JSON Formatter
# ============================================================================

class JSONFormatter(logging.Formatter):
    """
    Форматтер для вывода логов в JSON формате.

    Автоматически добавляет контекстные данные из contextvars
    и любые дополнительные поля из параметра extra.
    """

    def __init__(
            self,
            include_timestamp: bool = True,
            include_level: bool = True,
            include_logger_name: bool = True,
            include_module: bool = True,
            include_function: bool = True,
            include_line_number: bool = True,
            timestamp_format: str = "iso",  # iso, timestamp, custom
            custom_timestamp_format: Optional[str] = None,
    ):
        """
        Args:
            include_timestamp: Включать ли временную метку
            include_level: Включать ли уровень логирования
            include_logger_name: Включать ли имя логгера
            include_module: Включать ли имя модуля
            include_function: Включать ли имя функции
            include_line_number: Включать ли номер строки
            timestamp_format: Формат временной метки (iso/timestamp/custom)
            custom_timestamp_format: Кастомный формат для datetime.strftime
        """
        super().__init__()
        self.include_timestamp = include_timestamp
        self.include_level = include_level
        self.include_logger_name = include_logger_name
        self.include_module = include_module
        self.include_function = include_function
        self.include_line_number = include_line_number
        self.timestamp_format = timestamp_format
        self.custom_timestamp_format = custom_timestamp_format

    def format(self, record: logging.LogRecord) -> str:
        """
        Форматирует запись лога в JSON строку.

        Args:
            record: Запись лога

        Returns:
            JSON строка
        """
        log_data: Dict[str, Any] = {}

        # Основные поля
        if self.include_timestamp:
            log_data["timestamp"] = self._format_timestamp(record.created)

        if self.include_level:
            log_data["level"] = record.levelname

        if self.include_logger_name:
            log_data["logger"] = record.name

        log_data["message"] = record.getMessage()

        # Информация о местоположении в коде
        location = {}
        if self.include_module:
            location["module"] = record.module
        if self.include_function:
            location["function"] = record.funcName
        if self.include_line_number:
            location["line"] = record.lineno

        if location:
            log_data["location"] = location

        # Добавляем контекстные данные из contextvars
        context = {}
        request_id = get_request_id()
        if request_id:
            context["request_id"] = request_id

        user_id = get_user_id()
        if user_id:
            context["user_id"] = user_id

        task_id = get_task_id()
        if task_id:
            context["task_id"] = task_id

        if context:
            log_data["context"] = context

        # Добавляем дополнительные поля из record.__dict__ (из параметра extra)
        # Это поля, которые не являются стандартными для LogRecord
        extra_fields = {}
        standard_fields = {
            'name', 'msg', 'args', 'created', 'filename', 'funcName', 'levelname',
            'levelno', 'lineno', 'module', 'msecs', 'message', 'pathname', 'process',
            'processName', 'relativeCreated', 'thread', 'threadName', 'exc_info',
            'exc_text', 'stack_info', 'getMessage', 'taskName'
        }

        for key, value in record.__dict__.items():
            if key not in standard_fields and not key.startswith('_'):
                # Пробуем сериализовать значение
                try:
                    json.dumps(value)  # Проверка на сериализуемость
                    extra_fields[key] = value
                except (TypeError, ValueError):
                    extra_fields[key] = str(value)

        if extra_fields:
            log_data["extra"] = extra_fields

        # Обработка исключений
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info)
            }

        # Добавляем stack_info если есть
        if record.stack_info:
            log_data["stack_info"] = record.stack_info

        return json.dumps(log_data, ensure_ascii=False, default=str)

    def _format_timestamp(self, created: float) -> str:
        """Форматирует timestamp в нужный формат"""
        dt = datetime.fromtimestamp(created)

        if self.timestamp_format == "iso":
            return dt.isoformat()
        elif self.timestamp_format == "timestamp":
            return str(int(created * 1000))  # milliseconds
        elif self.timestamp_format == "custom" and self.custom_timestamp_format:
            return dt.strftime(self.custom_timestamp_format)
        else:
            return dt.isoformat()


# ============================================================================
# Custom Context-Aware Formatter (для человекочитаемых логов)
# ============================================================================

class ContextFormatter(logging.Formatter):
    """
    Форматтер с поддержкой контекстных данных для консольного вывода.

    Автоматически добавляет request_id, user_id, task_id в формат логов.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Добавляет контекстные данные в запись лога"""
        # Получаем контекстные данные
        context_parts = []

        request_id = get_request_id()
        if request_id:
            context_parts.append(f"req={request_id[:8]}")

        user_id = get_user_id()
        if user_id:
            context_parts.append(f"user={user_id}")

        task_id = get_task_id()
        if task_id:
            context_parts.append(f"task={task_id[:8]}")

        # Добавляем контекст в начало сообщения
        if context_parts:
            context_str = f"[{' '.join(context_parts)}] "
            record.msg = f"{context_str}{record.msg}"

        return super().format(record)


# ============================================================================
# Logger Factory
# ============================================================================

class LoggerConfig:
    """Конфигурация для логгера"""

    def __init__(
            self,
            name: Optional[str] = None,
            level: str = "INFO",
            log_folder: str = "logs",
            # Console settings
            console_enabled: bool = True,
            console_format: str = "json",  # json или text
            console_level: Optional[str] = None,
            # File settings
            file_enabled: bool = True,
            file_format: str = "json",  # json или text
            file_level: Optional[str] = None,
            # Rotation settings
            rotation_type: str = "size",  # size или time
            max_bytes: int = 10 * 1024 * 1024,  # 10 MB
            backup_count: int = 5,
            when: str = "midnight",  # для TimedRotatingFileHandler
            interval: int = 1,
            # Error file settings
            error_file_enabled: bool = True,
    ):
        self.name = name
        self.level = level.upper()
        self.log_folder = log_folder

        self.console_enabled = console_enabled
        self.console_format = console_format
        self.console_level = (console_level or level).upper()

        self.file_enabled = file_enabled
        self.file_format = file_format
        self.file_level = (file_level or level).upper()

        self.rotation_type = rotation_type
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.when = when
        self.interval = interval

        self.error_file_enabled = error_file_enabled


def get_logger(
        name: Optional[str] = None,
        config: Optional[LoggerConfig] = None
) -> logging.Logger:
    """
    Создаёт и настраивает логгер с поддержкой структурированного логирования.

    Args:
        name: Имя логгера (если None, берётся из caller frame)
        config: Конфигурация логгера (если None, используются настройки по умолчанию)

    Returns:
        Настроенный logger

    Example:
        # Базовое использование
        logger = get_logger()
        logger.info("Hello world")

        # С контекстом
        set_request_id("req-12345")
        set_user_id("user-67890")
        logger.info("Processing request")

        # С дополнительными полями
        logger.info("User action", extra={
            "action": "login",
            "ip_address": "192.168.1.1",
            "user_agent": "Mozilla/5.0"
        })
    """
    # Определяем имя логгера
    if name is None:
        import inspect
        caller_frame = inspect.stack()[1]
        filename = os.path.basename(caller_frame.filename)
        name = filename.replace('.py', '')

    # Создаём конфигурацию если не передана
    if config is None:
        config = LoggerConfig(
            name=name,
            level=getattr(settings, 'LOG_LEVEL', 'INFO'),
            log_folder=getattr(settings, 'LOG_FOLDER', 'logs'),
            console_format=getattr(settings, 'CONSOLE_LOG_FORMAT', 'text'),
            file_format=getattr(settings, 'FILE_LOG_FORMAT', 'json'),
        )

    # Получаем или создаём логгер
    logger = logging.getLogger(name)
    logger.setLevel(config.level)

    # Предотвращаем дублирование хендлеров
    if logger.hasHandlers():
        return logger

    # Создание директории логов
    log_dir = Path(config.log_folder)
    log_dir.mkdir(parents=True, exist_ok=True)

    # ========================================================================
    # Console Handler
    # ========================================================================
    if config.console_enabled:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(config.console_level)

        if config.console_format == "json":
            console_formatter = JSONFormatter(
                include_module=False,
                include_function=False,
                include_line_number=False
            )
        else:
            console_formatter = ContextFormatter(
                fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    # ========================================================================
    # File Handler (с ротацией)
    # ========================================================================
    if config.file_enabled:
        log_filename = log_dir / f"{name}.log"

        if config.rotation_type == "size":
            file_handler = RotatingFileHandler(
                log_filename,
                maxBytes=config.max_bytes,
                backupCount=config.backup_count,
                encoding='utf-8'
            )
        else:  # time-based rotation
            file_handler = TimedRotatingFileHandler(
                log_filename,
                when=config.when,
                interval=config.interval,
                backupCount=config.backup_count,
                encoding='utf-8'
            )

        file_handler.setLevel(config.file_level)

        if config.file_format == "json":
            file_formatter = JSONFormatter()
        else:
            file_formatter = ContextFormatter(
                fmt='%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s:%(lineno)d - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    # ========================================================================
    # Error File Handler (только ERROR и выше)
    # ========================================================================
    if config.error_file_enabled:
        error_filename = log_dir / f"{name}_error.log"

        error_handler = RotatingFileHandler(
            error_filename,
            maxBytes=config.max_bytes,
            backupCount=config.backup_count,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)

        if config.file_format == "json":
            error_formatter = JSONFormatter()
        else:
            error_formatter = ContextFormatter(
                fmt='%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s:%(lineno)d - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

        error_handler.setFormatter(error_formatter)
        logger.addHandler(error_handler)

    # Предотвращаем propagation к root логгеру
    logger.propagate = False

    return logger


# ============================================================================
# Convenience function для создания логгера с кастомной конфигурацией
# ============================================================================

def create_logger(
        name: str,
        level: str = "INFO",
        console_format: str = "text",
        file_format: str = "json",
        **kwargs
) -> logging.Logger:
    """
    Удобная функция для создания логгера с кастомными настройками.

    Args:
        name: Имя логгера
        level: Уровень логирования
        console_format: Формат для консоли (text/json)
        file_format: Формат для файла (text/json)
        **kwargs: Дополнительные параметры для LoggerConfig

    Returns:
        Настроенный logger
    """
    config = LoggerConfig(
        name=name,
        level=level,
        console_format=console_format,
        file_format=file_format,
        **kwargs
    )
    return get_logger(name, config)
