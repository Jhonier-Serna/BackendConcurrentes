from fastapi import (
    APIRouter,
    File,
    UploadFile,
    HTTPException,
)
from app.services.file_processor import FileProcessorService

router = APIRouter()


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
):
    """
    Endpoint para subir archivos vía CURL
    """
    # Obtener el tamaño del archivo
    file_content = await file.read()
    file_size = len(file_content)
    await file.seek(0)  # Regresar al inicio del archivo
    
    
    processor = FileProcessorService()
    result = await processor.process_file(file)
    
    if result['status'] == "error":
        raise HTTPException(
            status_code=500,
            detail=f"Error al procesar el archivo: {result['message']}"
        )

    return {
        "message": "Archivo subido exitosamente",
        "file_id": str(result['data']['file_path']),
        "total_genes": result['data']['total_genes'],
        "file_size": file_size,
        "filename": file.filename
    }