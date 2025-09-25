"""
Wildberries API Proxy Service
============================

Production-ready сервис как прокси к Wildberries API с rate limiting на Redis.

Автор: Senior Python Developer
Специфика WB API:
- Авторизация: заголовок "Authorization: {token}" без Bearer
- Rate limiting: token bucket, заголовки X-Ratelimit-Remaining/Retry/Limit/Reset
- 409 код = 5 запросов, 429 требует паузы X-Ratelimit-Retry секунд
- Домены: content-api.wildberries.ru (товары), common-api.wildberries.ru (пинг)
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Union

import aioredis
import httpx
from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn


# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

class Config:
    """Конфигурация приложения через переменные окружения"""

    # Redis настройки
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # WB API настройки
    WB_CONTENT_API_URL: str = os.getenv("WB_CONTENT_API_URL", "https://content-api.wildberries.ru")
    WB_COMMON_API_URL: str = os.getenv("WB_COMMON_API_URL", "https://common-api.wildberries.ru")

    # HTTP клиент настройки
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))

    # Rate limiting настройки
    DEFAULT_RATE_LIMIT: int = int(os.getenv("DEFAULT_RATE_LIMIT", "100"))
    DEFAULT_WINDOW: int = int(os.getenv("DEFAULT_WINDOW", "60"))

    # Логирование
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


# ============================================================================
# ЛОГИРОВАНИЕ
# ============================================================================

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================================
# PYDANTIC МОДЕЛИ
# ============================================================================

class ProductFilters(BaseModel):
    """Фильтры для получения списка товаров"""
    search: Optional[str] = Field(None, description="Поиск по названию товара")
    limit: int = Field(100, ge=1, le=1000, description="Количество товаров")
    offset: int = Field(0, ge=0, description="Смещение для пагинации")


class ProductCreateItem(BaseModel):
    """Модель для создания товара"""
    subject_id: int = Field(..., description="ID предмета")
    vendor_code: str = Field(..., max_length=75, description="Артикул продавца")
    title: str = Field(..., max_length=60, description="Наименование товара")
    description: str = Field(..., description="Описание товара")
    brand: str = Field(..., max_length=50, description="Бренд")
    dimensions: Dict[str, Union[int, float]] = Field(..., description="Габариты и вес")
    characteristics: List[Dict[str, Any]] = Field(..., description="Характеристики товара")
    sizes: List[Dict[str, Any]] = Field(..., description="Размеры товара")


class ProductUpdate(BaseModel):
    """Модель для обновления товара"""
    vendor_code: str = Field(..., description="Артикул продавца")
    title: Optional[str] = Field(None, max_length=60, description="Наименование товара")
    description: Optional[str] = Field(None, description="Описание товара")
    brand: Optional[str] = Field(None, max_length=50, description="Бренд")
    dimensions: Optional[Dict[str, Union[int, float]]] = Field(None, description="Габариты и вес")
    characteristics: Optional[List[Dict[str, Any]]] = Field(None, description="Характеристики")
    sizes: Optional[List[Dict[str, Any]]] = Field(None, description="Размеры")


class RateLimitInfo(BaseModel):
    """Информация о лимитах API"""
    remaining: Optional[int] = Field(None, description="Оставшееся количество запросов")
    limit: Optional[int] = Field(None, description="Общий лимит запросов")
    reset: Optional[int] = Field(None, description="Время сброса лимита в секундах")
    retry_after: Optional[int] = Field(None, description="Время ожидания при 429")


# ============================================================================
# REDIS RATE LIMITER (TOKEN BUCKET)
# ============================================================================

class RedisLimiter:
    """
    Redis-based rate limiter с token bucket алгоритмом.
    Особенности для WB API:
    - Учет 409 как 5 запросов
    - Синхронизация с заголовками X-Ratelimit-*
    - Предотвращение спама при 429
    """

    def __init__(self, redis: aioredis.Redis):
        self.redis = redis

        # Lua скрипт для атомарного token bucket
        self.lua_script = self.redis.register_script("""
            local key = KEYS[1]
            local capacity = tonumber(ARGV[1])
            local tokens = tonumber(ARGV[2])
            local interval = tonumber(ARGV[3])
            local requested = tonumber(ARGV[4])
            local now = tonumber(ARGV[5])

            local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
            local current_tokens = tonumber(bucket[1]) or capacity
            local last_refill = tonumber(bucket[2]) or now

            -- Пополняем токены на основе времени
            local time_passed = now - last_refill
            local tokens_to_add = math.floor(time_passed / interval * tokens)
            current_tokens = math.min(capacity, current_tokens + tokens_to_add)

            -- Проверяем, достаточно ли токенов
            if current_tokens >= requested then
                current_tokens = current_tokens - requested
                redis.call('HMSET', key, 'tokens', current_tokens, 'last_refill', now)
                redis.call('EXPIRE', key, 3600)
                return {1, current_tokens}
            else
                redis.call('HMSET', key, 'tokens', current_tokens, 'last_refill', now)
                redis.call('EXPIRE', key, 3600)
                return {0, current_tokens}
            end
        """)

    async def check_rate_limit(
        self,
        token: str,
        method: str,
        capacity: int = Config.DEFAULT_RATE_LIMIT,
        refill_tokens: int = 1,
        refill_interval: int = 1,
        requested_tokens: int = 1
    ) -> tuple[bool, int]:
        """
        Проверяет rate limit для token bucket.

        Args:
            token: WB API токен
            method: HTTP метод
            capacity: Максимальное количество токенов
            refill_tokens: Количество токенов для пополнения
            refill_interval: Интервал пополнения в секундах
            requested_tokens: Запрашиваемые токены (для 409 = 5)

        Returns:
            tuple: (allowed: bool, remaining_tokens: int)
        """
        try:
            key = f"wb:{hash(token)}:{method}"
            now = int(time.time())

            result = await self.lua_script(
                keys=[key],
                args=[capacity, refill_tokens, refill_interval, requested_tokens, now]
            )

            allowed = bool(result[0])
            remaining = int(result[1])

            logger.debug(f"Rate limit check: key={key}, allowed={allowed}, remaining={remaining}")
            return allowed, remaining

        except Exception as e:
            logger.error(f"Redis rate limit error: {e}")
            # В случае ошибки Redis разрешаем запрос
            return True, capacity

    async def update_from_headers(self, token: str, method: str, headers: Dict[str, str]):
        """
        Обновляет состояние rate limiter на основе заголовков WB API.

        Args:
            token: WB API токен
            method: HTTP метод
            headers: Заголовки ответа от WB API
        """
        try:
            remaining = headers.get('X-Ratelimit-Remaining')
            limit = headers.get('X-Ratelimit-Limit')

            if remaining and limit:
                key = f"wb:{hash(token)}:{method}"
                now = int(time.time())

                await self.redis.hmset(key, {
                    'tokens': int(remaining),
                    'capacity': int(limit),
                    'last_refill': now
                })
                await self.redis.expire(key, 3600)

                logger.debug(f"Updated rate limit from headers: remaining={remaining}, limit={limit}")

        except Exception as e:
            logger.error(f"Error updating rate limit from headers: {e}")


# ============================================================================
# WILDBERRIES HTTP CLIENT
# ============================================================================

class WildberriesClient:
    """
    HTTP клиент для работы с Wildberries API.
    Обрабатывает специфичные особенности WB API:
    - Авторизация без Bearer
    - Автоматический retry при 429
    - Обработка 409 как 5 запросов
    """

    def __init__(self, rate_limiter: RedisLimiter):
        self.rate_limiter = rate_limiter
        self.client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(Config.REQUEST_TIMEOUT),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            headers={"User-Agent": "WB-Proxy/1.0"}
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    def _get_headers(self, token: str) -> Dict[str, str]:
        """Создает заголовки для WB API (без Bearer!)"""
        return {
            "Authorization": token,  # БЕЗ Bearer!
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    async def _make_request(
        self,
        method: str,
        url: str,
        token: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Выполняет HTTP запрос с обработкой специфики WB API.

        Args:
            method: HTTP метод
            url: URL запроса
            token: WB API токен
            **kwargs: Дополнительные параметры запроса

        Returns:
            Dict с ответом API
        """
        if not self.client:
            raise RuntimeError("HTTP client not initialized")

        headers = self._get_headers(token)
        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))

        # Проверяем rate limit перед запросом
        tokens_needed = 1
        allowed, remaining = await self.rate_limiter.check_rate_limit(
            token, method, requested_tokens=tokens_needed
        )

        if not allowed:
            logger.warning(f"Rate limit exceeded for token {hash(token)}")
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded",
                headers={"Retry-After": "60"}
            )

        retries = 0
        while retries <= Config.MAX_RETRIES:
            try:
                logger.info(f"Making {method} request to {url}")

                response = await self.client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    **kwargs
                )

                # Обновляем rate limiter на основе заголовков ответа
                await self.rate_limiter.update_from_headers(
                    token, method, dict(response.headers)
                )

                # Обрабатываем специфичные статус-коды WB API
                if response.status_code == 409:
                    # 409 считается как 5 запросов в WB API
                    logger.warning(f"409 Conflict received, counting as 5 requests")
                    await self.rate_limiter.check_rate_limit(
                        token, method, requested_tokens=5
                    )

                elif response.status_code == 429:
                    # Получаем время ожидания из заголовка
                    retry_after = int(response.headers.get('X-Ratelimit-Retry', 60))
                    logger.warning(f"429 Too Many Requests, waiting {retry_after}s")

                    if retries < Config.MAX_RETRIES:
                        await asyncio.sleep(retry_after)
                        retries += 1
                        continue
                    else:
                        raise HTTPException(
                            status_code=429,
                            detail="Rate limit exceeded, max retries reached",
                            headers={"Retry-After": str(retry_after)}
                        )

                # Логируем результат запроса
                logger.info(f"Request completed: {response.status_code}")

                if response.status_code >= 400:
                    error_detail = f"WB API error: {response.status_code}"
                    try:
                        error_data = response.json()
                        error_detail = error_data.get('errorText', error_detail)
                    except:
                        pass

                    raise HTTPException(
                        status_code=response.status_code,
                        detail=error_detail
                    )

                return response.json() if response.content else {}

            except httpx.RequestError as e:
                logger.error(f"Request error: {e}")
                if retries < Config.MAX_RETRIES:
                    retries += 1
                    await asyncio.sleep(2 ** retries)  # Exponential backoff
                    continue
                raise HTTPException(status_code=503, detail=f"Service unavailable: {e}")

            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                raise HTTPException(status_code=500, detail="Internal server error")

    # ========================================================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С ТОВАРАМИ
    # ========================================================================

    async def get_products(
        self,
        token: str,
        search: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Получение списка товаров с фильтрами.
        Использует POST /content/v2/get/cards/list
        """
        url = f"{Config.WB_CONTENT_API_URL}/content/v2/get/cards/list"

        # Формируем тело запроса согласно документации WB API
        body = {
            "settings": {
                "sort": {"ascending": False},
                "filter": {
                    "withPhoto": -1,  # Все товары
                    "allowedCategoriesOnly": True
                },
                "cursor": {
                    "limit": limit,
                    "offset": offset
                }
            }
        }

        # Добавляем поиск если указан
        if search:
            body["settings"]["filter"]["textSearch"] = search

        return await self._make_request("POST", url, token, json=body)

    async def create_products(
        self,
        token: str,
        products: List[ProductCreateItem]
    ) -> Dict[str, Any]:
        """
        Создание товаров (batch операция).
        Использует POST /content/v2/cards/upload
        """
        url = f"{Config.WB_CONTENT_API_URL}/content/v2/cards/upload"

        # Преобразуем данные в формат WB API
        wb_products = []
        for product in products:
            wb_product = {
                "subjectID": product.subject_id,
                "variants": [{
                    "vendorCode": product.vendor_code,
                    "title": product.title,
                    "description": product.description,
                    "brand": product.brand,
                    "dimensions": product.dimensions,
                    "characteristics": product.characteristics,
                    "sizes": product.sizes
                }]
            }
            wb_products.append(wb_product)

        return await self._make_request("POST", url, token, json=wb_products)

    async def update_product(
        self,
        token: str,
        product_id: int,
        product_data: ProductUpdate
    ) -> Dict[str, Any]:
        """
        Обновление товара по ID.
        Использует PUT /content/v2/cards/update
        """
        url = f"{Config.WB_CONTENT_API_URL}/content/v2/cards/update"

        # Формируем данные для обновления
        update_data = {
            "nmID": product_id,
            "vendorCode": product_data.vendor_code
        }

        # Добавляем только заполненные поля
        if product_data.title:
            update_data["title"] = product_data.title
        if product_data.description:
            update_data["description"] = product_data.description
        if product_data.brand:
            update_data["brand"] = product_data.brand
        if product_data.dimensions:
            update_data["dimensions"] = product_data.dimensions
        if product_data.characteristics:
            update_data["characteristics"] = product_data.characteristics
        if product_data.sizes:
            update_data["sizes"] = product_data.sizes

        return await self._make_request("PUT", url, token, json=[update_data])

    async def get_api_limits(self, token: str) -> Dict[str, Any]:
        """
        Получение текущих лимитов API через ping.
        Использует GET /ping для common-api
        """
        url = f"{Config.WB_COMMON_API_URL}/ping"

        try:
            response = await self._make_request("GET", url, token)

            # Получаем информацию о лимитах из Redis
            key = f"wb:{hash(token)}:GET"
            rate_info = await self.rate_limiter.redis.hmget(
                key, 'tokens', 'capacity'
            )

            remaining = int(rate_info[0]) if rate_info[0] else None
            limit = int(rate_info[1]) if rate_info[1] else None

            return {
                "status": response.get("Status", "Unknown"),
                "timestamp": response.get("TS"),
                "rate_limit": {
                    "remaining": remaining,
                    "limit": limit,
                    "reset": None,
                    "retry_after": None
                }
            }

        except Exception as e:
            logger.error(f"Error getting API limits: {e}")
            raise


# ============================================================================
# DEPENDENCY INJECTION
# ============================================================================

# Глобальные переменные для хранения подключений
redis_connection: Optional[aioredis.Redis] = None
rate_limiter: Optional[RedisLimiter] = None


async def get_redis() -> aioredis.Redis:
    """Dependency для получения Redis подключения"""
    if not redis_connection:
        raise RuntimeError("Redis connection not initialized")
    return redis_connection


async def get_rate_limiter() -> RedisLimiter:
    """Dependency для получения rate limiter"""
    if not rate_limiter:
        raise RuntimeError("Rate limiter not initialized")
    return rate_limiter


def get_wb_token(x_wb_token: str = Header(..., description="WB API токен")) -> str:
    """Dependency для получения WB API токена из заголовка"""
    if not x_wb_token:
        raise HTTPException(status_code=401, detail="WB API token required")
    return x_wb_token


# ============================================================================
# LIFECYCLE MANAGEMENT
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    global redis_connection, rate_limiter

    # Startup
    logger.info("Starting Wildberries API Proxy Service...")

    try:
        # Инициализация Redis
        redis_connection = aioredis.from_url(
            Config.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            max_connections=20
        )

        # Проверка подключения к Redis
        await redis_connection.ping()
        logger.info("Redis connection established")

        # Инициализация Rate Limiter
        rate_limiter = RedisLimiter(redis_connection)
        logger.info("Rate limiter initialized")

        logger.info("Service startup completed")

        yield

    except Exception as e:
        logger.error(f"Failed to initialize service: {e}")
        raise

    finally:
        # Shutdown
        logger.info("Shutting down service...")

        if redis_connection:
            await redis_connection.aclose()
            logger.info("Redis connection closed")

        logger.info("Service shutdown completed")


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

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


# ============================================================================
# MIDDLEWARE ДЛЯ ЛОГИРОВАНИЯ
# ============================================================================

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


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/health", tags=["Health"])
async def health_check():
    """Health check эндпоинт"""
    return {"status": "healthy", "service": "wb-api-proxy"}


@app.get("/products", tags=["Products"])
async def get_products(
    filters: ProductFilters = Depends(),
    token: str = Depends(get_wb_token),
    limiter: RedisLimiter = Depends(get_rate_limiter)
):
    """
    Получение списка товаров с фильтрами.

    Args:
        search: Поиск по названию товара
        limit: Количество товаров (1-1000)
        offset: Смещение для пагинации
        X-WB-Token: WB API токен в заголовке

    Returns:
        Список товаров в формате WB API
    """
    async with WildberriesClient(limiter) as client:
        return await client.get_products(
            token=token,
            search=filters.search,
            limit=filters.limit,
            offset=filters.offset
        )


@app.post("/products", tags=["Products"])
async def create_products(
    products: List[ProductCreateItem],
    token: str = Depends(get_wb_token),
    limiter: RedisLimiter = Depends(get_rate_limiter)
):
    """
    Создание товаров (batch операция).

    Args:
        products: Список товаров для создания
        X-WB-Token: WB API токен в заголовке

    Returns:
        Результат создания товаров
    """
    if len(products) > 100:
        raise HTTPException(
            status_code=400,
            detail="Максимум 100 товаров за один запрос"
        )

    async with WildberriesClient(limiter) as client:
        return await client.create_products(token=token, products=products)


@app.put("/products/{product_id}", tags=["Products"])
async def update_product(
    product_id: int,
    product_data: ProductUpdate,
    token: str = Depends(get_wb_token),
    limiter: RedisLimiter = Depends(get_rate_limiter)
):
    """
    Обновление товара по ID.

    Args:
        product_id: Артикул WB (nmID)
        product_data: Данные для обновления товара
        X-WB-Token: WB API токен в заголовке

    Returns:
        Результат обновления товара
    """
    async with WildberriesClient(limiter) as client:
        return await client.update_product(
            token=token,
            product_id=product_id,
            product_data=product_data
        )


@app.get("/limits", response_model=Dict[str, Any], tags=["API Info"])
async def get_api_limits(
    token: str = Depends(get_wb_token),
    limiter: RedisLimiter = Depends(get_rate_limiter)
):
    """
    Получение текущих лимитов API.

    Args:
        X-WB-Token: WB API токен в заголовке

    Returns:
        Информация о текущих лимитах и статусе API
    """
    async with WildberriesClient(limiter) as client:
        return await client.get_api_limits(token=token)


# ============================================================================
# ERROR HANDLERS
# ============================================================================

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


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level=Config.LOG_LEVEL.lower(),
        access_log=True
    )
