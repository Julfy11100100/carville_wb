import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api import wildberries
from app.api.wildberries import router as wb_router
from app.containers import Container
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


def init_dependency_injector() -> Container:
    """
        Инициализация инъекций
    """
    container = Container()
    container.config.from_pydantic(settings=settings, required=True)
    container.wire(
        modules=[wildberries]
    )
    return container


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""

    # Startup
    logger.info("Starting Wildberries API Service...")

    try:

        init_dependency_injector()
        logger.info("Dependency injections")

        # Можно ещё что-нибудь напихать
        logger.info("Service startup completed")

        yield

    except Exception as e:
        logger.error(f"Failed to initialize service: {e}")
        raise

    finally:
        logger.info("Shutting down service...")
        logger.info("Service shutdown completed")


app = FastAPI(
    title="Wildberries API Proxy",
    description="Production-ready прокси к Wildberries API с rate limiting",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(wb_router)


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
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="error",
        access_log=True
    )
