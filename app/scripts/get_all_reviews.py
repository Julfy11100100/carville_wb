import asyncio
import time
from datetime import datetime

from app.schemas.tg import EntityType, ActionType, NotificationStatus
from app.services.elasticsearch_service import ElasticsearchService
from app.services.review_service import ReviewService
from app.services.rabbitmq import RabbitMQService
from app.services.wb_api import WildberriesAPI
from app.utils.logging import get_logger, setup_logging
from config import settings

setup_logging()
logger = get_logger()


async def get_all_reviews():
    # Создаем экземпляры сервисов
    rabbitmq_service = RabbitMQService()
    wb_api = WildberriesAPI(
        base_url=settings.REVIEWS_BASE_URL
    )
    elasticsearch_service = ElasticsearchService()
    review_service = ReviewService(
        api_client=wb_api,
        elasticsearch_service=elasticsearch_service
    )

    try:
        started_at = time.perf_counter()
        started_dt = datetime.now()

        await rabbitmq_service.send_notification(
            entity=EntityType.REVIEWS,
            action=ActionType.SYNC,
            status=NotificationStatus.INFO,
            message="🔄 Начата ежедневная подгрузка отзывов",
            details={
                "started_at": started_dt.isoformat()
            }
        )

        review_result = await review_service.fetch_and_index_all_reviews(
            token=settings.REVIEWS_WB_TOKEN
        )

        logger.info(f"Результат подгрузки отзывов: {review_result}")

        await rabbitmq_service.send_notification(
            entity=EntityType.REVIEWS,
            action=ActionType.SYNC,
            status=NotificationStatus.SUCCESS,
            message=(
                f"✅ Ежедневная подгрузка отзывов завершена\n"
                f"{review_result.get('message')}"
            ),
            details={
                "started_at": started_dt.isoformat(),
                "completed_at": datetime.now().isoformat(),
                "duration_seconds": round(time.perf_counter() - started_at, 3)
            }
        )


    except Exception as e:
        logger.error(
            f"Ошибка при получении всех отзывов: {str(e)}",
            exc_info=True
        )
        await rabbitmq_service.send_notification(
            entity=EntityType.REVIEWS,
            action=ActionType.SYNC,
            status=NotificationStatus.ERROR,
            message=f"Критическая ошибка выгрузки отзывов: {str(e)}",
            details={
                "failed_at": datetime.now().isoformat()
            }

        )

    finally:
        await wb_api.close()
        await elasticsearch_service.disconnect()
        await rabbitmq_service.disconnect()


if __name__ == '__main__':
    asyncio.run(get_all_reviews())
