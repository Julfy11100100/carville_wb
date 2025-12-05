import asyncio
from datetime import datetime

from app.schemas.tg import EntityType, NotificationStatus, ActionType
from app.services.api_category_service import ApiCategoryService
from app.services.rabbitmq import RabbitMQService
from app.services.sql_category_service import SqlCategoryService
from app.services.sql_repository import SQLDatabaseRepository
from app.services.wb_api import WildberriesAPI
from app.utils.logging import get_logger, setup_logging
from config import settings

setup_logging()
logger = get_logger()


async def insert_categories():
    """
    Сначала получаем категории, затем загружаем их в бд
    """
    # Создаем экземпляры сервисов
    wb_api = WildberriesAPI()
    sql_repository = SQLDatabaseRepository()
    api_category_service = ApiCategoryService(wb_api)
    sql_category_service = SqlCategoryService(sql_repository)
    rabbitmq_service = RabbitMQService()

    try:
        script_start_time = datetime.now()
        logger.info("Начинаем получение дерева категорий")
        await rabbitmq_service.send_notification(
            entity=EntityType.CATEGORIES,
            action=ActionType.SYNC,
            status=NotificationStatus.INFO,
            message="🔄 Начата синхронизация категорий",
            details={"sync_type": "cron_scheduled", "started_at": script_start_time.isoformat()}

        )

        categories_tree = await api_category_service.create_categories_tree(
            token=settings.DEFAULT_WB_TOKEN
        )

        logger.info(
            f"Получено категорий: {len(categories_tree.get('categories', {}))}. "
            f"Начинаем синхронизацию с БД"
        )

        sync_result = await sql_category_service.sync_categories_tree_to_db(
            tree_data=categories_tree
        )
        logger.info(f"Результат синхронизации категорий: {sync_result}")

        outdated_result = await sql_category_service.get_outdated_records()

        # Формируем итоговое сообщение
        stats = sync_result.get("statistics", {})
        outdated_stats = outdated_result.get("statistics", {})

        message = format_success_message(stats, outdated_stats)

        # Успешное завершение
        total_duration = (datetime.now() - script_start_time).total_seconds()
        logger.info(f"Успешное завершение синхронизации категорий. Затраченное время: {total_duration:.1f}s")

        await rabbitmq_service.send_notification(
            entity=EntityType.CATEGORIES,
            action=ActionType.SYNC,
            status=NotificationStatus.SUCCESS,
            message=message,
            details={"total_duration_seconds": total_duration}
        )

        return sync_result

    except Exception as e:
        logger.error(
            f"Ошибка при синхронизации категорий: {str(e)}",
            exc_info=True
        )
        await rabbitmq_service.send_notification(
            entity=EntityType.CATEGORIES,
            action=ActionType.SYNC,
            status=NotificationStatus.ERROR,
            message=f"Ошибка синхронизации категорий: {str(e)}",
            details={
                "sync_type": "cron_scheduled",
                "failed_at": datetime.now().isoformat()
            }
        )
        raise
    finally:
        await wb_api.close()
        await sql_repository.close_pool()
        await rabbitmq_service.disconnect()


def format_success_message(stats: dict, outdated_stats: dict) -> str:
    """Форматирует сообщение об успешной синхронизации на русском языке с адаптивным сокращением"""
    max_length = 4000

    stats['renamed_types'] = [
        renamed_type
        for renamed_type in stats.get('renamed_types', [])
    ]

    # Функция для безопасного добавления текста с проверкой лимита
    def try_add_section(current_msg: str, section: str) -> tuple[str, bool]:
        """Возвращает (новое_сообщение, успешно_добавлено)"""
        if len(current_msg) + len(section) <= max_length:
            return current_msg + section, True
        return current_msg, False

    # === БЛОК 1: Заголовок и сводка (всегда включаем) ===

    title = "✅ Синхронизация категорий завершена\n\n📊 СВОДКА:\n"
    message = title

    # Новые записи
    total_new = stats.get('created_roots', 0) + stats.get('created_categories', 0)
    if total_new > 0:
        message += f"  ➕ Добавлено: {total_new} ("
        parts = []
        if stats.get('created_roots', 0) > 0:
            parts.append(f"корневых: {stats['created_roots']}")
        if stats.get('created_categories', 0) > 0:
            parts.append(f"категорий: {stats['created_categories']}")
        message += ", ".join(parts) + ")\n"
    else:
        message += "0 новых категорий\n"

    # Обновленные записи
    total_updated = stats.get('updated_roots', 0) + stats.get('updated_categories', 0)
    if total_updated > 0:
        message += f"  🔄 Обновлено: {total_updated}\n"
    else:
        message += "0 обновленных категорий\n"

    # Переименованные записи
    total_renamed = stats.get('name_changes_roots', 0) + stats.get('name_changes_categories', 0)
    if total_renamed > 0:
        message += f"  ✏️ Переименовано: {total_renamed} ("
        parts = []
        if stats.get('name_changes_roots', 0) > 0:
            parts.append(f"корневых: {stats['name_changes_roots']}")
        if stats.get('name_changes_categories', 0) > 0:
            parts.append(f"категорий: {stats['name_changes_categories']}")
        message += ", ".join(parts) + ")\n"
    else:
        message += "0 переименованных категорий\n"

    # Устаревшие записи
    total_outdated = outdated_stats.get('outdated_roots', 0) + outdated_stats.get('outdated_categories', 0)
    if total_outdated > 0:
        message += f"  ⚠️ Устаревших (не обновлены): {total_outdated} ("
        parts = []
        if outdated_stats.get('outdated_roots', 0) > 0:
            parts.append(f"корневых: {outdated_stats['outdated_roots']}")
        if outdated_stats.get('outdated_categories', 0) > 0:
            parts.append(f"категорий: {outdated_stats['outdated_categories']}")
        message += ", ".join(parts) + ")\n"
    else:
        message += "0 устаревших категорий\n"

    message += "\n"

    # === БЛОК 2: Новые записи (детали) ===
    new_section = ""
    if stats.get('new_roots') or stats.get('new_categories'):
        new_section = "🆕 НОВЫЕ ЗАПИСИ:\n"

        # Новые корневые категории
        if stats.get('new_roots'):
            new_section += "  Корневые категории:\n"
            for item in stats['new_roots'][:8]:  # Максимум 8
                new_section += f"    • {item.get('name', 'N/A')} (ID: {item.get('id', 'N/A')})\n"
            if len(stats['new_roots']) > 8:
                new_section += f"    ... и еще {len(stats['new_roots']) - 8}\n"

        # Новые категории
        if stats.get('new_categories'):
            new_section += "  Категории:\n"
            for item in stats['new_categories'][:8]:
                new_section += f"    • {item.get('name', 'N/A')} → {item.get('parent_root_name', 'N/A')}\n"
            if len(stats['new_categories']) > 8:
                new_section += f"    ... и еще {len(stats['new_categories']) - 8}\n"

        new_section += "\n"

    message, added = try_add_section(message, new_section)

    # === БЛОК 3: Переименованные записи (детали) ===
    renamed_section = ""
    if stats.get('renamed_roots') or stats.get('renamed_categories'):
        renamed_section = "✏️ ПЕРЕИМЕНОВАНО:\n"

        # Переименованные корневые категории
        if stats.get('renamed_roots'):
            renamed_section += "  Корневые категории:\n"
            for item in stats['renamed_roots'][:8]:
                renamed_section += f"    • {item.get('old_name', 'N/A')} → {item.get('new_name', 'N/A')}\n"
            if len(stats['renamed_roots']) > 8:
                renamed_section += f"    ... и еще {len(stats['renamed_roots']) - 8}\n"

        # Переименованные категории
        if stats.get('renamed_categories'):
            renamed_section += "  Категории:\n"
            for item in stats['renamed_categories'][:8]:
                renamed_section += f"    • {item.get('old_name', 'N/A')} → {item.get('new_name', 'N/A')} ({item.get('parent_root_name', 'N/A')})\n"
            if len(stats['renamed_categories']) > 8:
                renamed_section += f"    ... и еще {len(stats['renamed_categories']) - 8}\n"

        renamed_section += "\n"

    message, added = try_add_section(message, renamed_section)

    # === БЛОК 4: Устаревшие записи (детали) ===
    outdated_section = ""
    if total_outdated > 0:
        outdated_section = "⚠️ УСТАРЕВШИЕ ЗАПИСИ:\n"

        # Устаревшие корневые категории
        if outdated_stats.get('outdated_roots_list'):
            outdated_section += "  🚨 Корневые категории (все дочерние записи также устарели):\n"
            for item in outdated_stats['outdated_roots_list'][:3]:
                outdated_section += f"    • {item.get('name', 'N/A')} (ID: {item.get('id', 'N/A')})\n"
            if len(outdated_stats['outdated_roots_list']) > 3:
                outdated_section += f"    ... и еще {len(outdated_stats['outdated_roots_list']) - 3}\n"

        # Устаревшие категории
        if outdated_stats.get('outdated_categories_list'):
            outdated_section += "  ⚠️ Категории:\n"
            for item in outdated_stats['outdated_categories_list'][:6]:
                outdated_section += f"    • {item.get('name', 'N/A')} ({item.get('parent_root_name', 'N/A')})\n"
            if len(outdated_stats['outdated_categories_list']) > 6:
                outdated_section += f"    ... и еще {len(outdated_stats['outdated_categories_list']) - 6}\n"

        outdated_section += "\n"

    message, added = try_add_section(message, outdated_section)
    if not added and total_outdated > 0:
        # Если не влезло - добавляем хотя бы краткую информацию
        short_outdated = f"⚠️ Детали устаревших записей не помещаются (всего {total_outdated}). См. логи.\n\n"
        message, _ = try_add_section(message, short_outdated)

    # Финальная проверка длины
    if len(message) > max_length:
        message = message[:max_length - 50] + "\n...\n⚠️ Сообщение обрезано. Полные детали в логах."

    logger.info(f"Formatted message length: {len(message)} chars")

    return message


if __name__ == "__main__":
    asyncio.run(insert_categories())
