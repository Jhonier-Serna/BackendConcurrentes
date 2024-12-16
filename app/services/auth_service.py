import secrets
from datetime import datetime, timedelta, timezone
import threading
from typing import Optional
import json

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext
from pydantic import EmailStr
from bson import ObjectId
import pika  # Importar pika para RabbitMQ

from app.models.user import UserCreate, UserInDB, UserResponse
from app.db.mongodb import get_async_database, connect_to_mongo

import logging

from app.services.security_key_consumer import start_consumer

# Configurar nivel de logging para silenciar mensajes de RabbitMQ (pika)
logging.getLogger("pika").setLevel(logging.WARNING)

# Configuración de seguridad
SECRET_KEY = (
    "tu_clave_secreta_muy_segura"  # En producción, usar una variable de entorno
)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="users/login")


class AuthService:
    def __init__(self):
        self.db = None  # Inicialización diferida
        self.users_collection = None

    async def get_database(self):
        if self.db is None:
            await connect_to_mongo()
            self.db = get_async_database()
            self.users_collection = self.db["users"]
        return self.db

    async def get_user_by_email(self, email: str) -> Optional[UserInDB]:
        db = await self.get_database()
        user_dict = await db.users.find_one({"email": email})
        if user_dict:
            user_dict["id"] = str(user_dict.pop("_id"))
            return UserInDB(**user_dict)
        return None

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verificar contraseña"""
        return pwd_context.verify(plain_password, hashed_password)

    def get_password_hash(self, password: str) -> str:
        """Generar hash de contraseña"""
        return pwd_context.hash(password)

    async def create_user(self, user: UserCreate) -> UserResponse:
        """Crear nuevo usuario"""
        # Verificar si el usuario ya existe
        existing_user = await self.get_user_by_email(user.email)
        if existing_user:
            raise ValueError("El usuario ya existe")

        # Crear usuario con contraseña hasheada
        hashed_password = self.get_password_hash(user.password)
        user_dict = user.model_dump(exclude={"password"})
        user_dict["hashed_password"] = hashed_password

        # Generar clave de seguridad
        security_key = self.generate_security_key()
        user_dict["security_key"] = security_key
        user_dict["security_key_expires"] = datetime.now(tz=timezone.utc) + timedelta(
            hours=24
        )

        # Insertar usuario en base de datos
        result = await self.users_collection.insert_one(user_dict)
        user_dict["id"] = str(result.inserted_id)
        consumer_thread = threading.Thread(target=start_consumer, daemon=True)
        consumer_thread.start()

        # Publicar un mensaje en RabbitMQ para enviar la clave de seguridad
        #self.publish_security_key_email(user.email, security_key)

        return UserResponse(**user_dict)

    def publish_security_key_email(self, email: str, security_key: str):
        """Publicar un mensaje en RabbitMQ para enviar la clave de seguridad"""
        connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        channel = connection.channel()
        channel.queue_declare(queue="security_key_queue")

        message = {"email": email, "security_key": security_key}
        channel.basic_publish(
            exchange="", routing_key="security_key_queue", body=json.dumps(message)
        )
        connection.close()

    def generate_security_key(self) -> str:
        """Generar clave de seguridad aleatoria"""
        return secrets.token_urlsafe(16)

    async def authenticate_user(self, email: str, password: str) -> Optional[UserInDB]:
        """Autenticar usuario"""
        user = await self.get_user_by_email(email)
        if not user:
            return None
        if not self.verify_password(password, user.hashed_password):
            return None
        return user

    def create_access_token(
        self, data: dict, expires_delta: Optional[timedelta] = None
    ) -> str:
        """Crear token de acceso JWT"""
        to_encode = data.copy()
        expire = datetime.now(tz=timezone.utc) + (
            expires_delta or timedelta(minutes=15)
        )
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt

    async def get_current_user(self, token: str) -> UserResponse:
        """Obtener usuario actual desde token"""
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            email: str = payload.get("sub")
        except jwt.PyJWTError:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

        user = await self.get_user_by_email(email)
        if user is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

        return UserResponse(**user.dict())

    async def request_security_key(self, email: EmailStr) -> None:
        """Generar y almacenar una nueva clave de seguridad para un usuario"""
        user = await self.get_user_by_email(email)
        if not user:
            raise ValueError("Usuario no encontrado")

        # Generar nueva clave de seguridad
        new_security_key = self.generate_security_key()
        expires_at = datetime.now(tz=timezone.utc) + timedelta(hours=24)

        # Actualizar usuario con la nueva clave
        await self.users_collection.update_one(
            {"email": email},
            {
                "$set": {
                    "security_key": new_security_key,
                    "security_key_expires": expires_at,
                }
            },
        )

    async def verify_security_key(self, email: EmailStr, security_key: str) -> bool:
        """Verificar si la clave de seguridad es válida"""
        user = await self.get_user_by_email(email)
        if not user:
            raise ValueError("Usuario no encontrado")

        if user.security_key != security_key:
            raise ValueError("Clave de seguridad incorrecta")

        if user.security_key_expires < datetime.now(tz=timezone.utc):
            raise ValueError("Clave de seguridad expirada")

        # Limpiar la clave de seguridad después de su uso
        await self.users_collection.update_one(
            {"email": email},
            {"$set": {"security_key": None, "security_key_expires": None}},
        )

        return True


# Funciones de conveniencia para inyección de dependencias
auth_service = AuthService()


async def get_current_user(token: str = Depends(oauth2_scheme)) -> UserResponse:
    return await auth_service.get_current_user(token)


def create_user(user: UserCreate):
    return auth_service.create_user(user)


def authenticate_user(email: str, password: str):
    return auth_service.authenticate_user(email, password)


def create_access_token(data: dict):
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    return auth_service.create_access_token(data, expires_delta=access_token_expires)
