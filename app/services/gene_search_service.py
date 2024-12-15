import re
import asyncio
from typing import List
from fastapi import HTTPException
from app.models.gene import GeneSearchCriteria, GeneSearchResult, GeneInDB, GeneCreate
from app.db.mongodb import get_async_database


class GeneSearchService:
    def __init__(self):
        self.db = get_async_database()
        self.genes_collection = self.db.genes

    async def search(
        self,
        criteria: GeneSearchCriteria,
        page: int = 1,
        per_page: int = 10,
        timeout: int = 30,
    ) -> GeneSearchResult:
        """
        Realiza una búsqueda parcial optimizada utilizando índices y expresiones regulares.
        """
        # Preparar el término de búsqueda
        search_term = re.escape(
            criteria.search.strip()
        )  # Escapar caracteres especiales
        query = {
            "$or": [
                {"chromosome": {"$regex": search_term, "$options": "i"}},
                {"filter_status": {"$regex": search_term, "$options": "i"}},
                {"info": {"$regex": search_term, "$options": "i"}},
                {"format": {"$regex": search_term, "$options": "i"}},
            ]
        }

        # Paginación
        skip = (page - 1) * per_page

        try:
            # Contar resultados totales
            total_results = await self.genes_collection.count_documents(query)

            if total_results == 0:
                return GeneSearchResult(
                    total_results=0, page=page, per_page=per_page, results=[]
                )

            # Ejecutar búsqueda con un pipeline optimizado
            async with asyncio.timeout(timeout):
                pipeline = [
                    {"$match": query},
                    {"$sort": {"_id": 1}},
                    {"$skip": skip},
                    {"$limit": per_page},
                    {
                        "$project": {
                            "_id": 0,
                            "chromosome": 1,
                            "position": 1,
                            "id": 1,
                            "reference": 1,
                            "alternate": 1,
                            "quality": 1,
                            "filter_status": 1,
                            "info": 1,
                            "format": 1,
                            "outputs": 1,
                        }
                    },
                ]

                # Ejecutar la agregación
                cursor = self.genes_collection.aggregate(
                    pipeline,
                    allowDiskUse=True,
                    batchSize=2000,
                    maxTimeMS=timeout * 1000,
                )

                documents = await cursor.to_list(length=per_page)

                results = [
                    GeneCreate(
                        chromosome=doc["chromosome"],
                        position=doc.get("position", 0),
                        id=doc.get("id", ""),
                        reference=doc.get("reference", ""),
                        alternate=doc.get("alternate", ""),
                        quality=doc.get("quality", 0.0),
                        filter_status=doc["filter_status"],
                        info=doc.get("info", ""),
                        format=doc.get("format", ""),
                        outputs=doc.get("outputs", {}),
                    )
                    for doc in documents
                ]

                return GeneSearchResult(
                    total_results=total_results,
                    page=page,
                    per_page=per_page,
                    results=results,
                )

        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=408,
                detail="La búsqueda tomó demasiado tiempo. Por favor, refine su término de búsqueda.",
            )
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error en la búsqueda: {str(e)}"
            )
