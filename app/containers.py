from dependency_injector import containers, providers
from redis.asyncio import Redis

from app.core.rate_limiter import RedisLimiter
from app.core.wb_client import WildberriesClient
from config import settings


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()

    redis_connection = providers.Resource(
        lambda: Redis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            max_connections=20
        )
    )

    redis_limiter = providers.Singleton(
        RedisLimiter,
        redis=redis_connection
    )

    wildberries_client = providers.Factory(
        WildberriesClient,
        rate_limiter=redis_limiter
    )
