import uvicorn
import asyncio

async def main():
        uvicorn.run(
            "app.main:app",
            reload=True
        )

if __name__ == "__main__":
    asyncio.run(main())
