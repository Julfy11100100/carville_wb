import logging
import os
from datetime import datetime
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
from config import settings


class ColoredFormatter(logging.Formatter):
    """Форматер с цветным выводом в консоль"""

    # ANSI коды цветов
    COLORS = {
        'DEBUG': '\033[36m',  # Cyan
        'INFO': '\033[92m',  # Green
        'WARNING': '\033[93m',  # Yellow
        'ERROR': '\033[91m',  # Red
        'CRITICAL': '\033[95m',  # Magenta
        'RESET': '\033[0m'  # Reset
    }

    def format(self, record: logging.LogRecord) -> str:
        # Получаем цвет по уровню логирования
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])

        # Форматируем сообщение
        formatted = super().format(record)

        # Добавляем цвет
        return f"{color}{formatted}{self.COLORS['RESET']}"


def get_logger(name: str = None) -> logging.Logger:
    # Определяем имя логгера
    if name is None:
        import inspect
        caller_frame = inspect.stack()[1]
        name = os.path.basename(caller_frame.filename).replace('.py', '')

    logger = logging.getLogger(name)

    # Пропускаем, если логгер уже настроен
    if logger.handlers:
        return logger

    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    # Создаём директорию логов
    log_dir = Path(settings.LOG_FOLDER)
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"{datetime.now().strftime('%Y-%m-%d')}.log"

    # Форматер для файла (без цветов)
    file_formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    # Форматер для консоли (с цветами)
    console_formatter = ColoredFormatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    # Файловый handler (ротация по дням)
    file_handler = TimedRotatingFileHandler(
        filename=log_file,
        when='midnight',
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Консольный handler (с цветами)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    logger.propagate = False

    return logger
