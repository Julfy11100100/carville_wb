import time
from typing import Dict

from redis.asyncio import Redis

from app.utils.logging import get_logger
from config import settings

logger = get_logger()


class RedisLimiter:
    """
    Redis-based rate limiter с token bucket алгоритмом.
    Особенности для WB API:
    - Учет 409 как 5 запросов
    - Синхронизация с заголовками X-Ratelimit-*
    """

    def __init__(self, redis: Redis):
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
            capacity: int = settings.DEFAULT_RATE_LIMIT,
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
