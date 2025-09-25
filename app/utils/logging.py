import datetime
import inspect
import logging
import os

from config import settings


def get_logger(name: str = None, level=settings.LOG_LEVEL):
    # Создание директории логов
    log_dir = settings.LOG_FOLDER
    os.makedirs(log_dir, exist_ok=True)

    # Определяем название
    if not name:
        caller_frame = inspect.stack()[1]
        filename = os.path.basename(caller_frame.filename)
        name = filename

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Handler для файла
    fh = logging.FileHandler(f"{log_dir}/{datetime.date.today()}.log")
    fh.setLevel(level)

    # Handler для консоли
    ch = logging.StreamHandler()
    ch.setLevel(level)

    # Формат логов
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Добавляем хендлеры
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger
