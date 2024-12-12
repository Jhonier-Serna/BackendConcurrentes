from fastapi import (
    APIRouter,
    File,
    UploadFile,
    Depends,
    HTTPException,
    Form
)
from typing import Optional

from app.models.user import UserResponse
from app.models.file import ResearchFileCreate, FileStatus
from app.models.gene import WineType
from app.services.auth_service import get_current_user
from app.services.file_processor import FileProcessorService

router = APIRouter()


@router.post("/research-file")
async def upload_research_file(
        file: UploadFile = File(...),
        wine_type: str = File(...),
        current_user: UserResponse = Depends(get_current_user)
):
    """
    Cargar archivo de investigación genética
    - Valida tipo de archivo
    - Procesa archivo de investigación
    - Requiere autenticación
    """
    if not file.filename.endswith(('.vcf', '.txt', '.csv')):
        raise HTTPException(
            status_code=400,
            detail="Formato de archivo no permitido"
        )

    file_processor = FileProcessorService()

    file_data = ResearchFileCreate(
        filename=file.filename,
        original_filename=file.filename,
        file_size=file.file.seek(0, 2),  # Obtener tamaño
        wine_type=wine_type,
        uploaded_by_user_id=current_user.id
    )

    try:
        result = await file_processor.process_file(
            file=file,
            file_metadata=file_data
        )
        return {
            "message": "Archivo procesado exitosamente",
            "file_id": result.id,
            "status": result.status
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error procesando archivo: {str(e)}"
        )


@router.get("/upload-status/{file_id}")
async def check_file_upload_status(
        file_id: str,
        current_user: UserResponse = Depends(get_current_user)
):
    """
    Verificar estado de procesamiento de archivo
    """
    file_processor = FileProcessorService()

    try:
        status = await file_processor.get_file_status(file_id)
        return {"file_id": file_id, "status": status}
    except Exception as e:
        raise HTTPException(
            status_code=404,
            detail=f"Archivo no encontrado: {str(e)}"
        )


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    wine_type: WineType = Form(...),
    description: Optional[str] = Form(None)
):
    """
    Endpoint para subir archivos vía CURL
    """
    # Obtener el tamaño del archivo
    file_content = await file.read()
    file_size = len(file_content)
    await file.seek(0)  # Regresar al inicio del archivo
    
    file_metadata = ResearchFileCreate(
        filename=file.filename,
        original_filename=file.filename,
        file_size=file_size,
        wine_type=wine_type,
        description=description or "",
        uploaded_by_user_id="system"  # En un sistema real, esto vendría del usuario autenticado
    )
    
    processor = FileProcessorService()
    result = await processor.process_file(file, file_metadata)
    
    return {
        "message": "Archivo subido exitosamente",
        "file_id": str(result.id),
        "status": result.status,
        "file_size": file_size,
        "filename": file.filename
    }