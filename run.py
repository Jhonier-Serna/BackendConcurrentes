import threading
import uvicorn
import asyncio
from app.services.security_key_consumer import start_consumer


def run_consumer():
    asyncio.run(start_consumer())


async def main():
    # Iniciar el consumidor en un hilo demonio
    consumer_thread = threading.Thread(target=run_consumer)
    consumer_thread.daemon = True  # Establecer el hilo como demonio
    consumer_thread.start()

    # Iniciar el servidor Uvicorn
    uvicorn.run("app.main:app", reload=True)


if __name__ == "__main__":
    asyncio.run(main())