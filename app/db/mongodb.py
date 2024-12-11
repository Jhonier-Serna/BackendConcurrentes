from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from app.config import settings


# Cliente asíncrono para operaciones no bloqueantes
class AsyncMongoDB:
    client: AsyncIOMotorClient = None
    db = None


# Cliente síncrono para operaciones que lo requieran
class SyncMongoDB:
    client: MongoClient = None
    db = None


async def connect_to_mongo():
    # Configuración para conexión asíncrona
    AsyncMongoDB.client = AsyncIOMotorClient(settings.MONGODB_URL)
    AsyncMongoDB.db = AsyncMongoDB.client[settings.MONGODB_DATABASE]

    # Configuración para conexión síncrona
    SyncMongoDB.client = MongoClient(settings.MONGODB_URL)
    SyncMongoDB.db = SyncMongoDB.client[settings.MONGODB_DATABASE]


async def close_mongo_connection():
    AsyncMongoDB.client.close()
    SyncMongoDB.client.close()


def get_async_database():
    return AsyncMongoDB.db


def get_sync_database():
    return SyncMongoDB.db
