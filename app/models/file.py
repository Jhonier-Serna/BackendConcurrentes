from pydantic import BaseModel, Field
from typing import Optional, Dict, Union
from enum import Enum
from datetime import datetime, timezone
from app.models.gene import WineType


class FileStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ResearchFileBase(BaseModel):
    filename: str = Field(..., description="Nombre del archivo de investigación")
    original_filename: str = Field(..., description="Nombre original del archivo")
    file_size: int = Field(..., description="Tamaño del archivo en bytes")
    wine_type: WineType = Field(..., description="Tipo de vino (chardonnay/cabernet)")

    # Metadatos adicionales
    research_team: Optional[str] = None
    research_date: Optional[datetime] = datetime.now(timezone.utc)


class ResearchFileCreate(BaseModel):
    filename: str
    original_filename: str
    file_size: int
    wine_type: WineType
    description: Optional[str] = ""
    uploaded_by_user_id: Optional[str] = "system"  # Valor por defecto para pruebas


class ResearchFileInDB(ResearchFileCreate):
    status: FileStatus = FileStatus.PENDING
    total_genes_processed: Optional[int] = None
    processed_timestamp: Optional[float] = None


class FileProcessingLog(BaseModel):
    file_id: str
    status: FileStatus
    message: str
    timestamp: datetime = datetime.now(timezone.utc)
    additional_details: Optional[Dict[str, Union[str, int, float]]] = None