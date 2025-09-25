import os

from pydantic_settings import BaseSettings

current_path = os.path.abspath(__file__)
project_root = os.path.dirname(current_path)


class Settings(BaseSettings):
    # Настройки логгера
    LOG_FOLDER: str = f"{project_root}/logs"
    LOG_LEVEL: str = "INFO"

    # Настройка FastApi
    APP_NAME: str = "API для вопросов и ответов"
    DEBUG: bool = False
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # Redis настройки
    REDIS_URL: str = "redis://localhost:6379/0"

    # WB API настройки
    WB_CONTENT_API_URL: str = "https://content-api-sandbox.wildberries.ru"

    # HTTP клиент настройки
    REQUEST_TIMEOUT: int = 30
    MAX_RETRIES: int = 3

    # Rate limiting настройки
    DEFAULT_RATE_LIMIT: int = 100
    DEFAULT_WINDOW: int = 60

    class Config:
        env_file = f"{project_root}/.env"


settings = Settings()
