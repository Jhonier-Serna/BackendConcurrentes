from app.models.gene import GeneSearchCriteria, GeneSearchResult, GeneInDB
from app.db.mongodb import get_async_database
from motor.motor_asyncio import AsyncIOMotorCursor
from typing import List, Dict, Any
import asyncio


class GeneSearchService:
    def __init__(self):
        self.db = get_async_database()
        self.genes_collection = self.db.genes

    async def search(
            self,
            criteria: GeneSearchCriteria,
            page: int = 1,
            per_page: int = 50
    ) -> GeneSearchResult:
        """
        Búsqueda avanzada de genes con múltiples criterios
        """
        # Construir filtro de búsqueda
        query_filter = {}
        
        if criteria.chromosome:
            query_filter['chromosome'] = criteria.chromosome
        if criteria.filter_status:
            query_filter['filter_status'] = criteria.filter_status
        if criteria.format:
            query_filter['format'] = criteria.format
        if criteria.info_query:
            for key, value in criteria.info_query.items():
                query_filter[f'info.{key}'] = value

        # Configurar ordenamiento
        sort_options = {}
        if criteria.sort_by:
            sort_direction = 1 if criteria.sort_direction == 'asc' else -1
            sort_options[criteria.sort_by] = sort_direction

        # Ejecutar búsqueda y conteo en paralelo
        count_task = asyncio.create_task(
            self.genes_collection.count_documents(query_filter)
        )
        
        skip = (page - 1) * per_page
        cursor = self.genes_collection.find(query_filter)
        
        if sort_options:
            cursor = cursor.sort(list(sort_options.items()))
        
        cursor = cursor.skip(skip).limit(per_page)
        
        # Ejecutar búsqueda paginada
        results_task = asyncio.create_task(cursor.to_list(length=per_page))
        
        # Esperar resultados
        total_results, results = await asyncio.gather(count_task, results_task)
        
        # Convertir resultados
        parsed_results = [GeneInDB(**result) for result in results]

        return GeneSearchResult(
            total_results=total_results,
            page=page,
            per_page=per_page,
            results=parsed_results
        )

    async def get_statistics(self):
        """
        Obtener estadísticas generales de genes
        """
        pipeline = [
            {
                '$group': {
                    '_id': '$wine_type',
                    'total_genes': {'$sum': 1},
                    'avg_quality': {'$avg': '$quality'},
                    'chromosomes': {'$addToSet': '$chromosome'}
                }
            }
        ]

        stats = await self.genes_collection.aggregate(pipeline).to_list(None)

        return {
            "wine_types": {
                stat['_id']: {
                    "total_genes": stat['total_genes'],
                    "avg_quality": stat['avg_quality'],
                    "unique_chromosomes": stat['chromosomes']
                } for stat in stats
            }
        }