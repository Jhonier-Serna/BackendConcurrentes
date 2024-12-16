from pydantic import BaseModel, EmailStr, field_validator
from typing import Optional
from datetime import datetime
import re


class UserBase(BaseModel):
    email: EmailStr
    is_active: bool = True
    created_at: datetime = datetime.utcnow()
    last_login: Optional[datetime] = None


class UserCreate(UserBase):
    password: str

    @field_validator('password')
    def validate_password(cls, password):
        # Validaciones de contraseña
        if len(password) < 8:
            raise ValueError("La contraseña debe tener al menos 8 caracteres")

        # Verificar complejidad
        if not re.search(r'[A-Z]', password):
            raise ValueError("La contraseña debe contener al menos una mayúscula")

        if not re.search(r'[a-z]', password):
            raise ValueError("La contraseña debe contener al menos una minúscula")

        if not re.search(r'\d', password):
            raise ValueError("La contraseña debe contener al menos un número")

        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            raise ValueError("La contraseña debe contener al menos un carácter especial")

        return password


class UserInDB(UserBase):
    id: str
    hashed_password: str
    security_key: Optional[str] = None
    security_key_expires: Optional[datetime] = None


class UserResponse(UserBase):
    id: str

    class Config:
        from_attributes = True


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class SecurityKeyRequest(BaseModel):
    email: EmailStr


class SecurityKeyVerify(BaseModel):
    email: EmailStr
    security_key: str