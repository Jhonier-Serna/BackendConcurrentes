import tempfile
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
):
    """
    Endpoint para subir archivos vía CURL
    """
    try:
        fileTemp = None
        data = await file.read()

        with tempfile.NamedTemporaryFile(delete=False) as fileTemp:
            fileTemp.write(data)
            fileTemp.flush()
            fileTempPath = fileTemp.name
        
        time = time.time()

        try:
            status = await FileProcessorService.process_file(fileTempPath,1)





        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error al procesar el archivo: {str(e)}"
            )

        return {"message": "Archivo subido exitosamente"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al procesar el archivo: {str(e)}"
        )