import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from config import settings


class ColoredFormatter(logging.Formatter):
    """Форматер с цветным выводом в консоль"""

    COLORS = {
        'DEBUG': '\033[36m',  # Cyan
        'INFO': '\033[92m',  # Green
        'WARNING': '\033[93m',  # Yellow
        'ERROR': '\033[91m',  # Red
        'CRITICAL': '\033[95m',  # Magenta
        'RESET': '\033[0m'  # Reset
    }

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        formatted = super().format(record)
        return f"{color}{formatted}{self.COLORS['RESET']}"


_logging_configured = False  # Флаг, что логирование настроено


def setup_logging():
    """Глобальная настройка логирования (вызовите один раз в main)"""

    global _logging_configured

    # Пропускаем, если уже настроено
    if _logging_configured:
        return

    _logging_configured = True

    log_dir = Path(settings.LOG_FOLDER)
    log_dir.mkdir(exist_ok=True, parents=True)

    file_formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    console_formatter = ColoredFormatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    log_file = log_dir / "app.log"

    # Файловый handler (ротация по дням)
    file_handler = TimedRotatingFileHandler(
        filename=str(log_file),
        when='midnight',
        interval=1,
        backupCount=365,
        encoding='utf-8'
    )
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setFormatter(file_formatter)

    # Консольный handler (с цветами)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)

    # Настраиваем логгер приложения (только app.*)
    app_logger = logging.getLogger('app')
    app_logger.setLevel(logging.DEBUG)
    app_logger.propagate = False

    # Удаляем старые handlers перед добавлением новых
    for handler in app_logger.handlers[:]:
        app_logger.removeHandler(handler)

    app_logger.addHandler(file_handler)
    app_logger.addHandler(console_handler)

    # Корневой логгер — только WARNING (чтобы не было шума от сторонних библиотек)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)


def get_logger(name: str = None) -> logging.Logger:
    """Получить логгер с указанным или автоматическим именем"""

    if name is None:
        import inspect
        caller_frame = inspect.stack()[1]
        filename = os.path.basename(caller_frame.filename).replace('.py', '')
        # Возвращаем логгер с префиксом 'app.'
        name = f'app.{filename}'

    return logging.getLogger(name)
