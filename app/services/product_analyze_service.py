from app.services.elasticsearch_service import ElasticsearchService
from app.services.product_match_service import ProductMatchService
from app.utils.logging import get_logger
from app.utils.normalize import get_normalize_identifier
from app.utils.token import hash_token
from config import settings

logger = get_logger()


class ProductAnalyzeService:
    """
    Сервис анализа карточек товаров
    """

    def __init__(
            self,
            product_match_service: ProductMatchService,
            elasticsearch_service: ElasticsearchService
    ):
        self.elasticsearch_service = elasticsearch_service
        self.product_match_service = product_match_service

    async def prepare_data(self, brand: str, token: str, carville_match_field: str, client_match_field: str):
        """
        Подготовка данных для анализа

        Args:
            client_match_field: Наше поле на wb для сравнения
            carville_match_field: Поле клиента на wb для сравнения
            brand: нужный бренд
            token: токен
        """

        hashed_token = hash_token(token)
        hashed_admin_token = hash_token(settings.ADMIN_WB_TOKEN)

        # Шаг 1: получаем все товары клиента и фильтруем по бренду
        try:
            filtered_client_products = await self._get_products(hashed_token, brand)
        except RuntimeError as e:
            error_message = str(e)
            if "Пожалуйста, сначала запустите синхронизацию данных." in error_message:
                logger.warning(
                    f"В Elasticsearch не найдено ни одного товара клиента для токена {hashed_token}"
                )
                filtered_client_products = []
            else:
                raise
        # Шаг 2: получаем все наши товары и фильтруем по бренду и статусу
        filtered_npr_products = await self._get_products(
            hashed_admin_token, brand
        )
        # Шаг 3: собираем товары которых нет у клиента
        missing_products = await self._collect_missing_products(
            filtered_client_products, filtered_npr_products,
            carville_match_field, client_match_field
        )
        products = await self._clean_products(missing_products)

        return {
            "products": products,
            "total_products": len(products)
        }

    async def _get_products(self, token: str, brand: str) -> list[dict]:
        """Получить товары из Elasticsearch с фильтрацией по brand
        Args:
            brand: Бренд для фильтрации
            token: token
        Returns:
            Список товаров с полями: offer_id, statuses, attributes
        """
        try:
            # Строим фильтры для Elasticsearch
            filters = {
                "brand": brand
            }

            result = await self.elasticsearch_service.search_products(
                token=token,
                filters=filters,
                limit=100000,
                offset=0,
                fields=["vendorCode", "characteristics", "subjectID", "barcode"]
            )

            if result["status"] == "error":
                error_msg = result.get("error", "Неизвестная ошибка")
                msg = f"Ошибка получения карточек для токена: {token} Ошибка: {error_msg}"
                logger.error(msg)
                raise RuntimeError(msg)

            products = result.get("products", [])
            logger.info(f"Получено {len(products)} товаров из Elasticsearch для {token}")
            return products

        except Exception as e:
            msg = f"Ошибка при получении товаров {token} из Elasticsearch: {str(e)}"
            logger.error(msg)
            raise RuntimeError(msg)

    async def _collect_missing_products(
            self, filtered_client_products: list[dict], filtered_npr_products: list[dict],
            carville_match_field: str, client_match_field: str
    ) -> list[dict]:
        """Собираем товары, которых нет у клиента.

        Args:
            filtered_client_products: Список товаров клиента, отфильтрованных по бренду
            filtered_npr_products: Список наших товаров, отфильтрованных по бренду и статусу
            carville_match_field: Поле наших товаров на WB для сравнения (например, 'offer_id', 'barcode', 'attributes_7236')
            client_match_field: Поле товаров клиента на WB для сравнения (например, 'offer_id', 'barcode', 'attributes_7236')

        Returns:
            Список наших товаров, которых нет у клиента
        """
        logger.info("Начинаем сбор недостающих товаров")

        # Логируем количество товаров до матчинга
        client_products_count = len(filtered_client_products)
        npr_products_count = len(filtered_npr_products)
        logger.info(f"Товары перед матчингом: клиент={client_products_count}, мы={npr_products_count}")

        # Шаг 1: Извлекаем и нормализуем значения из товаров клиента
        logger.info(f"Извлекаем значения из товаров клиента по полю '{client_match_field}'...")
        client_values_set = set()

        for product in filtered_client_products:
            # get_match_values теперь возвращает список или None
            values = self.product_match_service.get_match_values(product, client_match_field)

            if not values:
                continue

            # Проходим по всем значениям из списка (например, если там несколько баркодов)
            for value in values:
                normalized_value = get_normalize_identifier(value)
                if normalized_value:
                    client_values_set.add(normalized_value)

        client_unique_values_count = len(client_values_set)
        logger.info(f"Извлекли {client_unique_values_count} уникальных нормализованных значений из товаров клиента")

        # Шаг 2: Извлекаем значения из наших товаров и выполняем матчинг
        logger.info(f"Извлекаем значения из наших товаров по полю '{carville_match_field}' и выполняем матчинг...")
        missing_products = []
        matched_count = 0

        for product in filtered_npr_products:
            values = self.product_match_service.get_match_values(product, carville_match_field)

            # Если у нашего товара вообще нет значений для матчинга, считаем его недостающим (не с чем сравнить)
            if not values:
                missing_products.append(product)
                continue

            # Проверяем, есть ли хотя бы одно значение нашего товара среди значений клиента
            product_has_match = False
            for value in values:
                normalized_value = get_normalize_identifier(value)

                # Если нормализация вернула пустоту, пропускаем конкретно это значение
                if not normalized_value:
                    continue

                if normalized_value in client_values_set:
                    matched_count += 1
                    product_has_match = True
                    break  # Нашли совпадение, товар у клиента есть, дальше этот товар не проверяем

            if not product_has_match:
                # Ни одно из значений товара не нашлось у клиента -> добавляем в недостающие
                missing_products.append(product)

        missing_products_count = len(missing_products)

        # Шаг 3: Логирование статистики
        logger.info(
            f"Статистика матчинга: "
            f"товары_клиента={client_products_count}, "
            f"наши_товары={npr_products_count}, "
            f"совпадений={matched_count}, "
            f"недостающих={missing_products_count}"
        )
        logger.info(
            f"Найдено {matched_count} товаров клиента среди наших. "
            f"Возвращаем {missing_products_count} недостающих товаров."
        )

        return missing_products

    async def _clean_products(self, missing_products: list[dict]) -> list[dict]:
        """
        Приводим товары к нужному виду

        Args:
            missing_products: продукты которых нет в магазине пользователя
        """

        products = []
        for product in missing_products:
            products.append({
                "vendor_code": product.get("vendorCode"),
                "type_id": product.get("subjectID")
            })

        return products
