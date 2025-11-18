import asyncio
import json
from typing import Dict, Any, Optional

import aio_pika
from aio_pika.abc import AbstractRobustConnection

from app.utils.logging import get_logger
from config import settings

logger = get_logger()


class RabbitMQService:
    """Сервис для отправки уведомлений в телеграм через RabbitMQ"""

    def __init__(self):
        self._connection: Optional[AbstractRobustConnection] = None
        self._channel = None
        self._is_connected = False

    async def connect(self):
        """Подключение к RabbitMQ"""
        if self._connection and not self._connection.is_closed:
            await self.disconnect()

        connection_url = f"amqp://{settings.RABBITMQ_USER}:{settings.RABBITMQ_PASSWORD}@{settings.RABBITMQ_HOST}:{settings.RABBITMQ_PORT}{settings.RABBITMQ_VHOST}"

        self._connection = await aio_pika.connect_robust(
            connection_url,
            loop=asyncio.get_event_loop()
        )

        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=1)

        # Объявляем очередь для уведомлений
        await self._channel.declare_queue(settings.TELEGRAM_NOTIFICATIONS_QUEUE, durable=True)

        self._is_connected = True
        logger.info("Успешно подключились к RabbitMQ")

    async def disconnect(self):
        """Отключение от RabbitMQ"""
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
        self._connection = None
        self._channel = None
        self._is_connected = False

    async def send_notification(self, entity: str, action: str, status: str, message: str,
                                details: Dict[str, Any] = None, error_code: str = None):
        """Отправка уведомления в telegram_notifications"""
        try:
            # Всегда переподключаемся для отправки уведомления
            await self.connect()

            notification_body = {
                "entity": entity,
                "action": action,
                "status": status,
                "message": message,
                "details": details or {},
                "error_code": error_code
            }

            msg = aio_pika.Message(
                json.dumps(notification_body, ensure_ascii=False).encode('utf-8'),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type='application/json'
            )

            await self._channel.default_exchange.publish(
                msg, routing_key=settings.TELEGRAM_NOTIFICATIONS_QUEUE
            )

            logger.info(f"Отправили уведомление: entity={entity}, action={action}, status={status}")

            # Отключаемся после отправки
            await self.disconnect()

        except Exception as e:
            logger.error(f"Ошибка отправки уведомления: {str(e)}")
            # Не поднимаем исключение - уведомления не критичны
            await self.disconnect()
