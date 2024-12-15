from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional

from app.models.gene import (
    GeneSearchCriteria,
    GeneSearchResult,
    WineType
)
from app.models.user import UserResponse
from app.services.gene_search_service import GeneSearchService
from app.services.auth_service import get_current_user

router = APIRouter()


@router.get("/", response_model=GeneSearchResult)
async def search_genes(
        search: Optional[str] = Query(None, description="Filtro"),
        page: int = Query(1, ge=1, description="Número de página"),
        per_page: int = Query(50, ge=1, le=200, description="Resultados por página"),
):
    """
    Búsqueda avanzada de genes con múltiples criterios
    - Soporta filtrado por cromosoma, tipo de vino, estado
    - Paginación de resultados
    - Requiere autenticación
    """
    search_service = GeneSearchService()

    search_criteria = GeneSearchCriteria(
        search=search,
    )

    results = await search_service.search(
        criteria=search_criteria,
        page=page,
        per_page=per_page
    )
    return results
  