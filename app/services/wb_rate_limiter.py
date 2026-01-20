"""Token Bucket Rate Limiter для Wildberries API.

Реализует ограничение скорости запросов на основе Token Bucket алгоритма
с поддержкой различных лимитов для разных типов операций.
"""

import asyncio
import time
from dataclasses import dataclass

from app.utils.logging import get_logger
from config import settings

logger = get_logger()


@dataclass
class TokenBucket:
    """Корзина токенов для rate limiting.

    Реализует алгоритм Token Bucket для контроля скорости запросов.
    Токены пополняются с заданной скоростью до максимальной вместимости.
    """

    capacity: float  # Максимальное количество токенов
    rate: float  # Скорость пополнения токенов в секунду
    tokens: float  # Текущее количество токенов
    last_update: float  # Время последнего обновления
    lock: asyncio.Lock  # Блокировка для потокобезопасности

    def __init__(self, rate: float, capacity: float):
        """Инициализирует корзину токенов.

        Args:
            rate: Скорость пополнения токенов в секунду.
            capacity: Максимальная вместимость корзины.
        """
        self.capacity = capacity
        self.rate = rate
        self.tokens = capacity  # Начинаем с полной корзины
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> float:
        """Получить токен из корзины с ожиданием при необходимости.

        Если токен доступен сразу - забирает его и возвращает 0.
        Если токенов нет - вычисляет время ожидания, ждет и затем забирает токен.

        Returns:
            Время ожидания в секундах (0 если токен был доступен сразу).
        """
        async with self.lock:
            now = time.monotonic()

            # Пополняем токены согласно прошедшему времени
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now

            # Если есть токен - берем и возвращаем
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return 0.0

            # Если нет токена - вычисляем сколько ждать
            wait_time = (1.0 - self.tokens) / self.rate

            # Ждем нужное время
            await asyncio.sleep(wait_time)

            # После ожидания обновляем состояние
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now

            # Забираем токен
            self.tokens -= 1.0

            return wait_time


class WBRateLimiter:
    """Rate Limiter для Wildberries API.

    Использует Token Bucket для контроля скорости запросов.
    WB имеет следующие лимиты:
    - GET /content/v2/get/cards/list: ~100 req/min
    - POST /content/v2/cards/update: ~100 req/min
    - POST /content/v2/cards/upload: ~50 req/min (более тяжелая операция)

    Attributes:
        default_rate: Скорость по умолчанию (req/sec)
        burst_capacity: Максимальный burst
        bucket: Корзина токенов
    """

    def __init__(self, rate_limit_per_minute: float = 100, burst_capacity: float = 5):
        """Инициализирует rate limiter.

        Args:
            rate_limit_per_minute: Лимит запросов в минуту
            burst_capacity: Максимум запросов в burst
        """
        # Конвертируем лимит в минуту в лимит в секунду
        # С safety margin 0.9 для надёжности
        effective_rate = (rate_limit_per_minute / 60) * settings.WB_API_RATE_SAFETY_MARGIN

        self.default_rate = effective_rate
        self.burst_capacity = burst_capacity
        self.bucket = TokenBucket(rate=effective_rate, capacity=burst_capacity)

    async def acquire(self) -> float:
        """Получить разрешение на запрос к WB API.

        Блокирует выполнение до получения токена из корзины.

        Returns:
            Время ожидания в секундах.
        """
        wait_time = await self.bucket.acquire()

        # Логируем только когда пришлось ждать
        if wait_time > 0.01:  # Логируем только значимые задержки
            logger.warning(
                f"⏳ WB API rate limiting: waited {wait_time:.3f}s for token "
                f"(rate={self.default_rate:.3f} req/s)"
            )

        return wait_time


# Глобальный экземпляр rate limiter
wb_rate_limiter = WBRateLimiter(
    rate_limit_per_minute=settings.WB_API_RATE_LIMIT,
    burst_capacity=settings.WB_API_BURST_CAPACITY
)
