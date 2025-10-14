from dependency_injector import containers, providers

from app.core.mongo_repository import init_mongo_client, init_mongo_collection, MongoService
from app.core.task_manager import TaskManager
from app.core.wb_client import WildberriesClient


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()

    # Асинхронный ресурс — клиент MongoDB
    mongo_client = providers.Resource(
        init_mongo_client,
        mongo_uri=config.MONGO_URL
    )

    # Асинхронный ресурс — коллекция MongoDB
    mongo_collection = providers.Resource(
        init_mongo_collection,
        client=mongo_client,
        db_name=config.MONGO_DB_NAME,
        collection_name=config.MONGO_COLLECTION_NAME
    )

    # Сервис для работы с Mongo, фабрика создаёт экземпляры с коллекцией
    mongo_service = providers.Factory(
        MongoService,
        collection=mongo_collection,
    )

    task_manager = providers.Factory(
        TaskManager,
        mongo_service=mongo_service
    )

    wildberries_client = providers.Factory(
        WildberriesClient,
        task_manager=task_manager
    )
