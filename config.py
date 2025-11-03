import os

from pydantic_settings import BaseSettings

current_path = os.path.abspath(__file__)
project_root = os.path.dirname(current_path)


class Settings(BaseSettings):
    # Настройки логгера
    LOG_FOLDER: str = f"{project_root}/logs"
    LOG_LEVEL: str = "INFO"

    # Настройка FastApi
    APP_NAME: str = "Carville WB API Service"
    DESCRIPTION: str = "Сервис для работы с WB API и карточками товаров"
    DEBUG: bool = False
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    RELOAD: bool = False

    # Mongo настройки
    MONGO_URL: str = "localhost"
    MONGO_PORT: int = 27017
    MONGO_DB_NAME: str = "general"
    MONGO_COLLECTION_NAME: str = "tasks"
    MONGO_INITDB_ROOT_USERNAME: str = "user"
    MONGO_INITDB_ROOT_PASSWORD: str = "passwd"

    # MS SQL SERVER настройки
    DB_SERVER: str = ""
    DB_PORT: int = 0
    DB_NAME: str = ""
    DB_USER: str = ""
    DB_PASSWORD: str = ""
    DB_DRIVER: str = 'ODBC Driver 17 for SQL Server'
    # Порог 'свежих' категорий из бд. В часах
    WB_TYPES_MAX_AGE_HOURS: int = 24

    # WB API настройки
    # WB_CONTENT_API_URL: str = "https://content-api-sandbox.wildberries.ru"
    WB_CONTENT_API_URL: str = "https://content-api.wildberries.ru"

    # HTTP клиент настройки
    REQUEST_TIMEOUT: int = 30
    MAX_RETRIES: int = 3

    # Дефолтный токен для получения например дерева категорий
    DEFAULT_WB_TOKEN: str = ""

    class Config:
        env_file = f"{project_root}/.env"


settings = Settings()
MONGO_URI = f"mongodb://{settings.MONGO_INITDB_ROOT_USERNAME}:{settings.MONGO_INITDB_ROOT_PASSWORD}@{settings.MONGO_URL}:{settings.MONGO_PORT}"