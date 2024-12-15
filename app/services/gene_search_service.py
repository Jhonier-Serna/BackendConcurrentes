import re
import asyncio
from fastapi import HTTPException
from app.models.gene import GeneSearchResult, GeneCreate
from app.db.mongodb import get_async_database


class GeneSearchService:
    def __init__(self):
        self.db = get_async_database()
        self.genes_collection = self.db.genes

    async def parallel_search(self, query, skip, limit):
        pipeline = [
            {"$match": query},
            {"$sort": {"_id": 1}},
            {"$skip": skip},
            {"$limit": limit},
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
        cursor = self.genes_collection.aggregate(pipeline)
        return await cursor.to_list(length=limit)

    async def search(self, criteria, page=1, per_page=10, timeout=30):
        search_term = re.escape(criteria.search.strip())
        query = {
            "$or": [
                {"chromosome": {"$regex": search_term, "$options": "i"}},
                {"filter_status": {"$regex": search_term, "$options": "i"}},
                {"info": {"$regex": search_term, "$options": "i"}},
                {"format": {"$regex": search_term, "$options": "i"}},
            ]
        }
        skip = (page - 1) * per_page
        tasks = []
        partition_size = per_page // 4  # Divide en 4 subconsultas paralelas
        for i in range(4):
            tasks.append(
                self.parallel_search(query, skip + i * partition_size, partition_size)
            )
        try:
            async with asyncio.timeout(timeout):
                results = await asyncio.gather(*tasks)
                flattened_results = [item for sublist in results for item in sublist]
                # total_results = await self.genes_collection.count_documents(query)
                total_results = len(flattened_results)

                return GeneSearchResult(
                    total_results=total_results,
                    page=page,
                    per_page=per_page,
                    results=[
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
                        for doc in flattened_results
                    ],
                )

        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=408,
                detail="La búsqueda tomó demasiado tiempo.",
            )
