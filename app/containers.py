from dependency_injector import containers, providers

from config import MONGO_URI
from app.services.elasticsearch_service import ElasticsearchService
from app.services.mongo_repository import init_mongo_client, init_mongo_collection, MongoService
from app.services.task_manager import TaskManager
from app.services.wb_api import WildberriesAPI
from app.services.wb_client import WildberriesClient


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()

    # Асинхронный ресурс — клиент MongoDB
    mongo_client = providers.Resource(
        init_mongo_client,
        mongo_uri=MONGO_URI
    )

    # Асинхронный ресурс — коллекция MongoDB
    mongo_collection = providers.Resource(
        init_mongo_collection,
        client=mongo_client,
        db_name=config.MONGO_DB_NAME,
        collection_name=config.MONGO_COLLECTION_NAME
    )

    mongo_service = providers.Singleton(
        MongoService,
        collection=mongo_collection,
    )

    task_manager = providers.Singleton(
        TaskManager,
        mongo_service=mongo_service
    )

    elasticsearch_service = providers.Singleton(
        ElasticsearchService
    )

    wildberries_api = providers.Singleton(
        WildberriesAPI,
        base_url=config.WB_CONTENT_API_URL,
        max_retries=config.MAX_RETRIES,
        timeout=config.REQUEST_TIMEOUT

    )

    wildberries_client = providers.Factory(
        WildberriesClient,
        task_manager=task_manager,
        elasticsearch_service=elasticsearch_service,
        api_client=wildberries_api
    )
