import asyncio
import time
from datetime import datetime

from app.schemas.tg import EntityType, ActionType, NotificationStatus
from app.services.elasticsearch_service import ElasticsearchService
from app.services.feedback_service import FeedbackService
from app.services.rabbitmq import RabbitMQService
from app.services.wb_api import WildberriesAPI
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


async def get_all_feedbacks():
    # Создаем экземпляры сервисов
    rabbitmq_service = RabbitMQService()
    wb_api = WildberriesAPI(
        base_url=settings.FEEDBACKS_BASE_URL
    )
    elasticsearch_service = ElasticsearchService()
    feedback_service = FeedbackService(
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

        feedback_result = await feedback_service.fetch_and_index_all_feedbacks(
            token=settings.FEEDBACKS_WB_TOKEN
        )

        logger.info(f"Результат подгрузки отзывов: {feedback_result}")

        await rabbitmq_service.send_notification(
            entity=EntityType.REVIEWS,
            action=ActionType.SYNC,
            status=NotificationStatus.SUCCESS,
            message=(
                f"✅ Ежедневная подгрузка отзывов завершена\n"
                f"{feedback_result.get('message')}"
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
    asyncio.run(get_all_feedbacks())
