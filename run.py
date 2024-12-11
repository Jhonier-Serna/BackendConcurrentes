import uvicorn
from app.db.mongodb import connect_to_mongo, close_mongo_connection
import asyncio

async def main():
    try:
        uvicorn.run(
            "app.main:app",
            reload=True
        )
    finally:
        await close_mongo_connection()

if __name__ == "__main__":
    asyncio.run(main())
