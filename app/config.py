from typing import ClassVar
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Configuración general
    PROJECT_NAME: str = "Gene Search Backend for Vineyard Research"
    PROJECT_VERSION: str = "0.1.0"

    # Configuración de base de datos MongoDB
    MONGODB_URL: str
    MONGODB_DATABASE: str

    # Configuración de seguridad
    SECRET_KEY: str
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # Configuración de RabbitMQ
    RABBITMQ_HOST: str
    RABBITMQ_PORT: int
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"

    # Configuración de almacenamiento de archivos
    UPLOAD_FOLDER: str
    MAX_FILE_SIZE: int = 5368709120

    # Configuración de SendGrid
    SENDGRID: str
    SENDGRID_EMAIL: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# Instancia de configuración para usar en toda la aplicación
settings = Settings()
