from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional

from app.models.gene import (
    GeneSearchCriteria,
    GeneSearchResult,
)
from app.services.auth_service import (
    get_current_user,
)
from app.models.user import UserResponse
from app.services.gene_search_service import GeneSearchService
from app.services.auth_service import get_current_user

router = APIRouter()


@router.get("/", response_model=GeneSearchResult)
async def search_genes(
    current_user: UserResponse = Depends(
        get_current_user
    ),  # Añadir dependencia de autenticación
    search: Optional[str] = Query(None, description="Filtro"),
    page: int = Query(1, ge=1, description="Número de página"),
    per_page: int = Query(25, ge=1, le=200, description="Resultados por página"),
    collection_name: str = Query(
        ..., description="Nombre de la colección donde buscar"
    ),
):
    """
    Búsqueda avanzada de genes con múltiples criterios
    - Soporta filtrado por cromosoma, tipo de vino, estado
    - Paginación de resultados
    - Requiere autenticación
    """
    # Validar que el término de búsqueda no esté vacío si se proporciona
    if search is not None and search.strip() == "":
        raise HTTPException(
            status_code=400, detail="El término de búsqueda no puede estar vacío"
        )

    search_service = GeneSearchService()

    search_criteria = GeneSearchCriteria(
        search=search,
    )

    try:
        results = await search_service.search(
            criteria=search_criteria,
            page=page,
            per_page=per_page,
            collection_name=collection_name,
        )
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"No se encontraron coincidencias: {str(e)}")
