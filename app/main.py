import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api import wildberries
from app.api import categories
from app.api.categories import router as categories_router
from app.api.default import router as default_router
from app.api.wildberries import router as wb_router
from app.containers import Container
from app.utils.logging import get_logger
from config import settings

logger = get_logger()

container = Container()


def init_dependency_injector() -> Container:
    """
        Инициализация инъекций
    """

    container.config.from_pydantic(settings=settings, required=True)

    container.wire(
        modules=[wildberries, categories]
    )

    return container


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""

    # Startup
    logger.info("Запуск API-сервиса Wildberries...")

    try:

        init_dependency_injector()
        logger.info("Произведены инъекции зависимостей")

        await container.init_resources()
        logger.info("Ресурсы инициализированы")

        # Можно ещё что-нибудь напихать
        logger.info("Сервис успешно запущен")

        yield

        api_instance = container.wildberries_api()
        await api_instance.close()
        await container.shutdown_resources()

    except Exception as e:
        logger.error(f"Ошибка запуска сервиса: {e}")
        raise

    finally:

        logger.info("Отключение службы...")
        logger.info("Завершение работы сервиса завершено")


app = FastAPI(
    title=settings.APP_NAME,
    description=settings.DESCRIPTION,
    lifespan=lifespan,
    root_path=settings.ROOT_PATH
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(default_router)
app.include_router(wb_router)
app.include_router(categories_router)


@app.middleware("http")
async def log_requests(request, call_next):
    """Middleware для логирования запросов"""
    start_time = time.time()

    # Логируем входящий запрос
    logger.info(f"Incoming request: {request.method} {request.url}")

    response = await call_next(request)

    # Логируем ответ
    process_time = time.time() - start_time
    logger.info(f"Request completed: {response.status_code} in {process_time:.4f}s")

    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Обработчик HTTP исключений"""
    logger.error(f"HTTP Exception: {exc.status_code} - {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "errorText": exc.detail,
            "status_code": exc.status_code
        },
        headers=getattr(exc, 'headers', None)
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Обработчик общих исключений"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": True,
            "errorText": "Internal server error",
            "status_code": 500
        }
    )


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.RELOAD,
        log_level="error",
        access_log=True
    )
