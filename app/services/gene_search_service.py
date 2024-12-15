from app.models.gene import GeneSearchCriteria, GeneSearchResult, GeneInDB
from app.db.mongodb import get_async_database
import asyncio
from concurrent.futures import ThreadPoolExecutor


class GeneSearchService:
    def __init__(self):
        self.db = get_async_database()
        self.genes_collection = self.db.genes

    async def fetch_results(self, query_filter, page, per_page):
        cursor = self.genes_collection.find(query_filter).skip((page - 1) * per_page).limit(per_page)
        return await cursor.to_list(length=per_page)

    async def search(
        self,
        criteria: GeneSearchCriteria,
        page: int = 1,
        per_page: int = 50
    ) -> GeneSearchResult:
        """
        Búsqueda avanzada de genes con múltiples criterios.
        """

        # Construir el filtro de búsqueda para MongoDB
        search_term = criteria.search 
        query_filter = {
            '$or': [
                {'chromosome': {'$regex': search_term, '$options': 'i'}},
                {'filter_status': {'$regex': search_term, '$options': 'i'}},
                {'info': {'$regex': search_term, '$options': 'i'}},
                {'format': {'$regex': search_term, '$options': 'i'}}
            ]
        }

        # Contar el total de resultados antes de crear las tareas
        total_results = await self.genes_collection.count_documents(query_filter)

        # Crear un grupo de hilos para paralelizar la consulta
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            tasks = [loop.run_in_executor(executor, self.fetch_results, query_filter, p, per_page) for p in range(1, (total_results // per_page) + 1)]
            results = await asyncio.gather(*tasks)

        # Convertir los resultados a modelos de datos
        parsed_results = [
            GeneInDB(**result) for sublist in results for result in sublist
        ]

        return GeneSearchResult(
            total_results=total_results,
            page=page,
            per_page=per_page,
            results=parsed_results
        )
