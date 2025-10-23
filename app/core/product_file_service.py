import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from app.utils.logging import get_logger

logger = get_logger()


class ProductFileService:
    """
    Сервис для работы с файлами товаров

    Выделен в отдельный класс по принципу Single Responsibility
    """

    @staticmethod
    async def save_products_to_file(task_id: str, products: List[Dict]) -> str:
        """
        Сохраняет товары в JSON файл

        Args:
            task_id: ID задачи
            products: Список товаров

        Returns:
            Путь к сохранённому файлу
        """
        output_dir = Path("data/products")
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / f"products_{task_id}.json"

        def default_datetime_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat() + 'Z'
            raise TypeError(f"Type {type(obj)} not serializable")

        # Выполняем блокирующую операцию в executor
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: file_path.write_text(
                json.dumps({
                    "task_id": task_id,
                    "collected_at": datetime.now(),
                    "total_count": len(products),
                    "products": products
                }, ensure_ascii=False, indent=2, default=default_datetime_serializer),
                encoding="utf-8"
            )
        )

        logger.info(
            "Products saved to file",
            extra={
                "file_path": str(file_path),
                "products_count": len(products)
            }
        )

        return str(file_path)

    @staticmethod
    async def load_products_from_file(file_path: str) -> Dict[str, Any]:
        """
        Загружает товары из JSON файла

        Args:
            file_path: Путь к файлу

        Returns:
            Данные из файла
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Выполняем блокирующую операцию в executor
        loop = asyncio.get_event_loop()
        content = await loop.run_in_executor(
            None,
            lambda: path.read_text(encoding="utf-8")
        )

        data = json.loads(content)

        logger.info(
            "Products loaded from file",
            extra={
                "file_path": file_path,
                "products_count": data.get("total_count", 0)
            }
        )

        return data
