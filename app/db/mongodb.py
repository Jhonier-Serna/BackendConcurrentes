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
    AsyncMongoDB.db.max_pool_size = 15000
    AsyncMongoDB.db.max_idle_time_ms = 3000
    AsyncMongoDB.db.server_selection_timeout_ms = 3000
    AsyncMongoDB.db.retry_writes = True

async def close_mongo_connection():
    AsyncMongoDB.client.close()
    SyncMongoDB.client.close()


def get_async_database():
    return AsyncMongoDB.db

