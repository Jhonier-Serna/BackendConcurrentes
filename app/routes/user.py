from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel

from app.models.user import (
    UserCreate,
    UserResponse,
    SecurityKeyRequest,
    SecurityKeyVerify,
)
from app.services.auth_service import (
    auth_service,  # Instancia de AuthService
    create_user,
    authenticate_user,
    create_access_token,
    get_current_user,
)

router = APIRouter()

@router.post("/register", response_model=UserResponse)
async def register_user(user: UserCreate):
    """
    Registrar un nuevo usuario
    - Valida la información del usuario
    - Genera hash de contraseña
    - Envía correo con clave de seguridad
    """
    try:
        new_user = await create_user(user)
        return new_user
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Inicio de sesión de usuario
    - Autentica credenciales
    - Genera token de acceso
    """
    user = await authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales incorrectas",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(data={"sub": user.email})
    return {
        "access_token": access_token,
        "token_type": "bearer",
    }


@router.post("/request-security-key")
async def request_security_key_route(request: SecurityKeyRequest):
    """
    Solicitar clave de seguridad para verificación
    """
    try:
        await auth_service.request_security_key(request.email)
        return {"message": "Clave de seguridad enviada"}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/verify-security-key")
async def verify_security_key_route(verification: SecurityKeyVerify):
    """
    Verificar clave de seguridad
    """
    try:
        result = await auth_service.verify_security_key(
            verification.email,
            verification.security_key,
        )
        return {"message": "Clave verificada correctamente", "valid": result}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get("/me", response_model=UserResponse)
async def read_users_me(current_user: UserResponse = Depends(get_current_user)):
    """
    Obtener información del usuario actual
    """
    return current_user

class LoginRequest(BaseModel):
    username: str
    password: str

@router.post("/simple-login")
async def simple_login(request: LoginRequest):
    username = request.username
    password = request.password
    """
    Inicio de sesión simple con solo nombre de usuario y contraseña
    """
    user = await authenticate_user(username, password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales incorrectas",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(data={"sub": user.email})
    return {
        "access_token": access_token,
        "token_type": "bearer",
    }

class RegisterRequest(BaseModel):
    username: str
    password: str


@router.post("/simple-register")
async def simple_register(request: RegisterRequest):
    username = request.username
    password = request.password
    """
    Registro simple de usuario con solo nombre de usuario y contraseña
    """
    user_data = UserCreate(email=username, password=password)  # Suponiendo que UserCreate tiene un campo para la contraseña
    try:
        new_user = await create_user(user_data)
        return new_user
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
