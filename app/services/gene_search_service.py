from app.models.gene import GeneSearchCriteria, GeneSearchResult, GeneInDB
from app.db.mongodb import get_async_database


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

        if criteria.wine_type:
            query_filter['wine_type'] = criteria.wine_type

        # Contar total de resultados
        total_results = await self.genes_collection.count_documents(query_filter)

        # Realizar búsqueda con paginación
        skip = (page - 1) * per_page
        cursor = self.genes_collection.find(query_filter) \
            .skip(skip) \
            .limit(per_page)

        results = await cursor.to_list(length=per_page)

        # Convertir resultados a modelo GeneInDB
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