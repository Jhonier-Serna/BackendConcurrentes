from typing import ClassVar
from pydantic_settings import BaseSettings
from pydantic import BaseModel

class Settings(BaseModel):
    # Configuración general
    PROJECT_NAME: str = "Gene Search Backend for Vineyard Research"
    PROJECT_VERSION: str = "0.1.0"

    # Configuración de base de datos MongoDB
    MONGODB_URL: str = "mongodb://localhost:27017"
    MONGODB_DATABASE: str = "gene_search_db"

    # Configuración de seguridad
    SECRET_KEY: str = "your_very_secret_key_here"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # Configuración de RabbitMQ
    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"

    # Configuración de almacenamiento de archivos
    UPLOAD_FOLDER: str = "/path/to/research/files"
    MAX_FILE_SIZE: int = 5368709120

    # Configuración de CUDA
    CUDA_LIBRARY_PATH: str = "./cuda_lib.so"

    # Configuración de caché (Redis)
    REDIS_URL: str = "redis://localhost:6379/0"
    CACHE_TIMEOUT: int = 300  # 5 minutos por defecto

    # Define los campos que necesitas
    collection_name: str = "genes"
    num_processes: int = 4
    chunk_size: int = 10000
    batch_size: int = 1000

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"  # Permite entradas adicionales

# Instancia de configuración para usar en toda la aplicación
settings = Settings()
