from typing import Any, Optional, AsyncIterator

from redis.asyncio import Redis


# Функция инициализации Redis подключения
async def init_redis_pool(redis_url: str) -> AsyncIterator[Redis]:
    redis = Redis.from_url(redis_url, decode_responses=True)
    yield redis
    await redis.close()


class RedisStorage:
    def __init__(self, redis_url: str):
        self.redis = Redis.from_url(redis_url, decode_responses=True)

    async def set(self, key: str, value: Any, expire: Optional[int] = None) -> None:
        """
        Сохраняет значение по ключу. Опционально принимает время жизни (expire, в секундах).
        """
        if expire:
            await self.redis.set(key, value, ex=expire)
        else:
            await self.redis.set(key, value)

    async def get(self, key: str) -> Optional[str]:
        """
        Получает значение по ключу. Возвращает строку или None, если ключ не найден.
        """
        value = await self.redis.get(key)
        return value

    async def close(self):
        await self.redis.close()
