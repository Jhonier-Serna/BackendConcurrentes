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
        chromosome: Optional[str] = Query(None, description="Filtrar por cromosoma"),
        wine_type: Optional[WineType] = Query(None, description="Tipo de vino"),
        filter_status: Optional[str] = Query(None, description="Estado del filtro"),
        page: int = Query(1, ge=1, description="Número de página"),
        per_page: int = Query(50, ge=1, le=200, description="Resultados por página"),
        current_user: UserResponse = Depends(get_current_user)
):
    """
    Búsqueda avanzada de genes con múltiples criterios
    - Soporta filtrado por cromosoma, tipo de vino, estado
    - Paginación de resultados
    - Requiere autenticación
    """
    search_service = GeneSearchService()

    search_criteria = GeneSearchCriteria(
        chromosome=chromosome,
        wine_type=wine_type
    )

    try:
        results = await search_service.search(
            criteria=search_criteria,
            page=page,
            per_page=per_page
        )
        return results
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error en la búsqueda: {str(e)}"
        )


@router.get("/statistics")
async def get_gene_statistics(
        current_user: UserResponse = Depends(get_current_user)
):
    """
    Obtener estadísticas generales de genes
    """
    search_service = GeneSearchService()

    try:
        stats = await search_service.get_statistics()
        return stats
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener estadísticas: {str(e)}"
        )