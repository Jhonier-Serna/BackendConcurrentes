from app.services.security_key_consumer import start_consumer
import uvicorn
import asyncio


async def main():
    uvicorn.run("app.main:app", reload=True)
    start_consumer()


if __name__ == "__main__":
    asyncio.run(main())
