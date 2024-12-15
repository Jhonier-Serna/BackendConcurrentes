import uvicorn
from app.db.mongodb import connect_to_mongo, close_mongo_connection
import asyncio
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    MONGODB_URL: str
    MONGODB_DATABASE: str
    SECRET_KEY: str
    RABBITMQ_HOST: str
    RABBITMQ_PORT: int
    UPLOAD_FOLDER: str
    CUDA_LIBRARY_PATH: str
    REDIS_URL: str
    collection_name: str
    num_processes: int
    chunk_size: int
    batch_size: int

    class Config:
        env_file = ".env"

async def main():
    try:
        await connect_to_mongo()
        uvicorn.run(
            "app.main:app",
            reload=True
        )
    finally:
        await close_mongo_connection()

if __name__ == "__main__":
    asyncio.run(main())
