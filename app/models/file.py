from pydantic import BaseModel, Field
from typing import Optional, Dict, Union
from enum import Enum
from datetime import datetime, timezone


class FileStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class ResearchFileBase(BaseModel):
    filename: str = Field(..., description="Nombre del archivo de investigación")
    original_filename: str = Field(..., description="Nombre original del archivo")
    file_size: int = Field(..., description="Tamaño del archivo en bytes")
    wine_type: str = Field(..., description="Tipo de vino (chardonnay/cabernet)")

    # Metadatos adicionales
    research_team: Optional[str] = None
    research_date: Optional[datetime] = datetime.now(timezone.utc)


class ResearchFileCreate(ResearchFileBase):
    uploaded_by_user_id: str


class ResearchFileInDB(ResearchFileBase):
    id: str
    status: FileStatus = FileStatus.PENDING
    upload_timestamp: datetime = datetime.now(timezone.utc)
    processed_timestamp: Optional[datetime] = None
    total_genes_processed: Optional[int] = None

    # Información de procesamiento
    processing_details: Optional[Dict[str, Union[str, int, float]]] = None


class FileProcessingLog(BaseModel):
    file_id: str
    status: FileStatus
    message: str
    timestamp: datetime = datetime.now(timezone.utc)
    additional_details: Optional[Dict[str, Union[str, int, float]]] = None