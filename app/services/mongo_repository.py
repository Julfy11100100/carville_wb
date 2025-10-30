from typing import Optional, List

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorClient
from pymongo import ReturnDocument

from app.utils.logging import get_logger

logger = get_logger()


async def init_mongo_client(mongo_uri: str) -> AsyncIOMotorClient:
    client = AsyncIOMotorClient(mongo_uri)
    return client


async def init_mongo_collection(client: AsyncIOMotorClient, db_name: str,
                                collection_name: str) -> AsyncIOMotorCollection:
    db = client[db_name]
    collection = db[collection_name]
    return collection


class MongoService:
    def __init__(self, collection: AsyncIOMotorCollection):
        self.collection = collection

    async def insert_one(self, data: dict) -> str:
        result = await self.collection.insert_one(data)
        logger.debug(f"Вставка документа id={result.inserted_id}")
        return str(result.inserted_id)

    async def find_one(self, query: dict) -> Optional[dict]:
        doc = await self.collection.find_one(query)
        logger.debug(f"По запросу {query}: found={bool(doc)}")
        return doc

    async def find_by_id(self, task_id: str) -> Optional[dict]:
        doc = await self.collection.find_one({"task_id": task_id})
        logger.debug(f"Запрос по task_id {task_id}: found={bool(doc)}")
        return doc

    async def update_one(self, query: dict, update: dict, upsert: bool = False) -> Optional[dict]:
        result = await self.collection.find_one_and_update(
            query,
            {"$set": update},
            upsert=upsert,
            return_document=ReturnDocument.AFTER
        )
        logger.debug(f"Обновили один {query}: upsert={upsert}, found={bool(result)}")
        return result

    async def delete_one(self, query: dict) -> int:
        result = await self.collection.delete_one(query)
        logger.debug(f"Удалили один {query}: deleted={result.deleted_count}")
        return result.deleted_count

    async def find_many(self, query: dict = {}, limit: int = 100) -> List[dict]:
        cursor = self.collection.find(query).limit(limit)
        docs = await cursor.to_list(length=limit)
        logger.debug(f"Нашли {query}: found={len(docs)}")
        return docs

    async def create_index(self, keys, **kwargs):
        idx = await self.collection.create_index(keys, **kwargs)
        logger.debug(f"Создали индекс: {idx}")
        return idx
