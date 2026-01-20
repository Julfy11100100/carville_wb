from dependency_injector import containers, providers

from app.services.elasticsearch_service import ElasticsearchService
from app.services.mongo_repository import init_mongo_client, init_mongo_collection, MongoRepository
from app.services.npr_product_service import NprProductService
from app.services.product_analyze_service import ProductAnalyzeService
from app.services.product_match_service import ProductMatchService
from app.services.recommendation_service import RecommendationService
from app.services.review_service import ReviewService
from app.services.sql_category_service import SqlCategoryService
from app.services.sql_repository import SQLDatabaseRepository
from app.services.sql_review_service import SqlReviewService
from app.services.task_manager import TaskManager
from app.services.wb_api_service import WildberriesAPI
from app.services.wb_client_service import WildberriesClient
from config import MONGO_URI


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
        MongoRepository,
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

    sql_database_repository = providers.Singleton(
        SQLDatabaseRepository
    )

    product_match_service = providers.Factory(
        ProductMatchService,
        elasticsearch_service=elasticsearch_service,
        database_service=sql_database_repository
    )

    sql_category_service = providers.Singleton(
        SqlCategoryService,
        sql_repository=sql_database_repository
    )

    review_api = providers.Singleton(
        WildberriesAPI,
        base_url=config.REVIEWS_BASE_URL,
        max_retries=config.MAX_RETRIES,
        timeout=config.REQUEST_TIMEOUT

    )

    sql_review_service = providers.Singleton(
        SqlReviewService,
        sql_repository=sql_database_repository
    )

    review_service = providers.Factory(
        ReviewService,
        api_client=review_api,
        elasticsearch_service=elasticsearch_service,
        sql_service=sql_review_service
    )

    recommendation_service = providers.Singleton(
        RecommendationService,
        sql_repository=sql_database_repository
    )

    npr_product_service = providers.Singleton(
        NprProductService,
        elasticsearch_service=elasticsearch_service
    )

    product_analyze_service = providers.Factory(
        ProductAnalyzeService,
        product_match_service=product_match_service,
        elasticsearch_service=elasticsearch_service,
        recommendation_service=recommendation_service,
        npr_product_service=npr_product_service,
        sql_category_service=sql_category_service
    )

    wildberries_client = providers.Factory(
        WildberriesClient,
        task_manager=task_manager,
        elasticsearch_service=elasticsearch_service,
        api_client=wildberries_api,
        sql_category_service=sql_category_service,
        recommendation_service=recommendation_service,
        npr_product_service=npr_product_service
    )
