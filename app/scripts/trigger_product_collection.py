import asyncio
import sys
from datetime import datetime
from typing import Optional, Dict, Any

import httpx

from app.services.rabbitmq import RabbitMQService
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


async def get_task_status(
        client: httpx.AsyncClient,
        task_id: str,
        headers: Dict[str, str],
        gateway_base_url: str
) -> Optional[Dict[str, Any]]:
    """Получает статус задачи через эндпоинт /api/v1/product/task/status"""
    status_url = gateway_base_url.rstrip("/") + "/api/v1/product/task/status"
    payload = {
        "task_id": task_id,
        "task_type": "products_info"
    }

    try:
        resp = await client.post(status_url, headers=headers, json=payload)

        if 200 <= resp.status_code < 300:
            return resp.json()
        else:
            logger.error(f"Ошибка при получении статуса задачи. Статус: {resp.status_code}, тело: {resp.text}")
            return None
    except Exception as e:
        logger.error(f"Исключение при получении статуса задачи: {e}")
        return None


async def wait_for_task_completion(
        client: httpx.AsyncClient,
        task_id: str,
        headers: Dict[str, str],
        gateway_base_url: str
) -> Optional[Dict[str, Any]]:
    """Ожидает завершения задачи, опрашивая статус каждые 30 секунд"""
    final_statuses = {"completed", "partially_completed", "failed"}

    while True:
        status_data = await get_task_status(client, task_id, headers, gateway_base_url)

        if status_data is None:
            logger.warning("Не удалось получить статус задачи, повторная попытка через 30 секунд...")
            await asyncio.sleep(30)
            continue

        status = status_data.get("status", "").lower()

        if status in final_statuses:
            return status_data

        await asyncio.sleep(30)


async def trigger_product_collection() -> int:
    """Вызывает эндпоинт /api/v1/product/info через шлюз для запуска сбора товаров и ожидает завершения"""
    started_dt = datetime.now()
    rabbitmq_service = RabbitMQService()

    logger.info("🚀 Запуск сбора товаров через эндпоинт /api/v1/product/info")

    # Уведомление о старте
    try:
        await rabbitmq_service.send_notification(
            entity="products",
            action="sync",
            status="info",
            message="🔄 Начата ежедневная выгрузка наших товаров с wb",
            details={
                "started_at": started_dt.isoformat()
            }
        )
    except Exception as e:
        logger.error(f"Не удалось отправить стартовое уведомление: {e}")

    # Формируем URL шлюза
    gateway_url = settings.GATEWAY_BASE_URL.rstrip("/") + "/api/v1/product/info"

    # Заголовки с админскими credentials
    headers = {
        "Content-Type": "application/json",
        "x-wb-token": settings.ADMIN_WB_TOKEN
    }

    try:
        timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            # Создаем задачу
            resp = await client.post(gateway_url, headers=headers)

            if 200 <= resp.status_code < 300:
                try:
                    response_data = resp.json()
                    task_id = response_data.get("task_id")

                    if not task_id:
                        error_msg = "❌ В ответе отсутствует task_id"
                        logger.error(error_msg)
                        # Уведомление об ошибке
                        try:
                            await rabbitmq_service.send_notification(
                                entity="products",
                                action="sync",
                                status="error",
                                message=f"Критическая ошибка: {error_msg}",
                                details={
                                    "failed_at": datetime.now().isoformat()
                                }
                            )
                        except Exception as e:
                            logger.error(f"Не удалось отправить уведомление об ошибке: {e}")
                        return 1

                    logger.info(f"✅ Задача создана. Task ID: {task_id}")
                    logger.info(f"Ответ сервера: {response_data}")

                    # Ожидаем завершения задачи
                    logger.info("⏳ Ожидание завершения задачи...")
                    final_status_data = await wait_for_task_completion(
                        client, task_id, headers, settings.GATEWAY_BASE_URL
                    )

                    if final_status_data is None:
                        error_msg = "❌ Не удалось получить финальный статус задачи"
                        logger.error(error_msg)
                        # Уведомление об ошибке
                        try:
                            await rabbitmq_service.send_notification(
                                entity="products",
                                action="sync",
                                status="error",
                                message=f"Критическая ошибка: {error_msg}",
                                details={
                                    "failed_at": datetime.now().isoformat(),
                                    "task_id": task_id
                                }
                            )
                        except Exception as e:
                            logger.error(f"Не удалось отправить уведомление об ошибке: {e}")
                        return 1

                    # Логируем результаты
                    status = final_status_data.get("status", "").lower()
                    logger.info(f"Статус задачи: {status}")
                    logger.info(f"✅ Задача на сбор товаров завершилась со статусом: {status}")

                    # Собираем статистику для уведомления
                    error_message = final_status_data.get("error_message")
                    products_count = final_status_data.get("products_count")
                    products_count_filtered_out = final_status_data.get("products_count_filtered_out")
                    categories_count = final_status_data.get("categories_count")

                    # Логируем error_message если есть
                    if error_message:
                        logger.warning(f"⚠️ Сообщение об ошибке: {error_message}")

                    # Логируем статистику
                    if products_count is not None:
                        logger.info(f"📦 Товары наших брендов: {products_count}")

                    if products_count_filtered_out is not None:
                        logger.info(f"📦 Товары без брендов или не наших брендов: {products_count_filtered_out}")

                    if categories_count is not None:
                        logger.info(f"📁 Количество категорий в которых лежат наши товары на wb: {categories_count}")

                    # Формируем сообщение для телеграм
                    message_parts = [f"✅ Ежедневная выгрузка товаров завершена\nСтатус: {status}"]

                    if error_message:
                        message_parts.append(f"\n⚠️ Сообщение об ошибке: {error_message}")

                    if products_count is not None:
                        message_parts.append(f"\n📦 Товары наших брендов: {products_count}")

                    if products_count_filtered_out is not None:
                        message_parts.append(
                            f"\n📦 Товары без брендов или не наших брендов: {products_count_filtered_out}")

                    if categories_count is not None:
                        message_parts.append(
                            f"\n📁 Количество категорий в которых лежат наши товары на wb: {categories_count}")

                    telegram_message = "".join(message_parts)

                    # Уведомление о завершении
                    notification_status = "success" if status == "completed" else "error" if status == "failed" else "warning"
                    try:
                        await rabbitmq_service.send_notification(
                            entity="products",
                            action="sync",
                            status=notification_status,
                            message=telegram_message,
                            details={
                                "started_at": started_dt.isoformat(),
                                "completed_at": datetime.now().isoformat(),
                                "task_id": task_id,
                                "status": status
                            }
                        )
                    except Exception as e:
                        logger.error(f"Не удалось отправить финальное уведомление: {e}")

                    return 0
                except Exception as e:
                    error_msg = f"❌ Ошибка при обработке ответа: {e}"
                    logger.error(error_msg)
                    logger.info(f"Ответ сервера (текст): {resp.text}")
                    # Уведомление об ошибке
                    try:
                        await rabbitmq_service.send_notification(
                            entity="products",
                            action="sync",
                            status="error",
                            message=f"Критическая ошибка: {error_msg}",
                            details={
                                "failed_at": datetime.now().isoformat(),
                                "error": str(e)
                            }
                        )
                    except Exception as se:
                        logger.error(f"Не удалось отправить уведомление об ошибке: {se}")
                    return 1
            else:
                error_msg = f"❌ Ошибка при вызове эндпоинта. Статус: {resp.status_code}"
                logger.error(error_msg)
                logger.error(f"Тело ответа: {resp.text}")
                # Уведомление об ошибке
                try:
                    await rabbitmq_service.send_notification(
                        entity="products",
                        action="sync",
                        status="error",
                        message=f"Критическая ошибка: {error_msg}",
                        details={
                            "failed_at": datetime.now().isoformat(),
                            "status_code": resp.status_code
                        }
                    )
                except Exception as e:
                    logger.error(f"Не удалось отправить уведомление об ошибке: {e}")
                return 1

    except Exception as e:
        error_msg = f"❌ Исключение при вызове эндпоинта: {e}"
        logger.error(error_msg)
        # Уведомление об ошибке
        try:
            await rabbitmq_service.send_notification(
                entity="products",
                action="sync",
                status="error",
                message=f"Критическая ошибка: {error_msg}",
                details={
                    "failed_at": datetime.now().isoformat(),
                    "error": str(e)
                }
            )
        except Exception as se:
            logger.error(f"Не удалось отправить уведомление об ошибке: {se}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(trigger_product_collection())
    sys.exit(exit_code)
