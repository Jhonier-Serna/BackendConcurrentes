from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Union
from enum import Enum


class WineType(str, Enum):
    CHARDONNAY = "chardonnay"
    CABERNET = "cabernet"


class GeneSearchCriteria(BaseModel):
    chromosome: Optional[str] = None
    filter_criteria: Optional[str] = None
    wine_type: Optional[WineType] = None


class GeneBase(BaseModel):
    chromosome: str = Field(..., description="Cromosoma donde se encuentra el gen")
    position: int = Field(..., description="Posición del gen en el cromosoma")
    id: Optional[str] = Field(None, description="Identificador único del gen")
    reference: str = Field(..., description="Secuencia de referencia")
    alternate: str = Field(..., description="Secuencia alternativa")
    quality: float = Field(..., description="Calidad del gen")
    filter_status: str = Field(..., description="Estado del filtro")

    # Campos flexibles para info y formato
    additional_info: Optional[Dict[str, Union[str, float, int]]] = None
    format_info: Optional[Dict[str, Union[str, float, int]]] = None


class GeneCreate(GeneBase):
    wine_type: WineType
    research_file_id: str


class GeneInDB(GeneBase):
    id: str
    wine_type: WineType
    research_file_id: str
    created_at: str
    last_updated: str


class GeneBatchUpload(BaseModel):
    genes: List[GeneCreate]
    research_file_metadata: Dict[str, Union[str, int]]


class GeneSearchResult(BaseModel):
    total_results: int
    page: int
    per_page: int
    results: List[GeneInDB]