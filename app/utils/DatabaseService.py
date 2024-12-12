import time
import logging
from typing import List
from app.models.gene import GeneCreate
from app.db.mongodb import get_async_database
from app.models.file import (
    ResearchFileCreate,
    ResearchFileInDB,
    FileStatus,
    FileProcessingLog
)
# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseService:
    """Handles database operations for files, genes, and logs."""
    def __init__(self):
        self.db = get_async_database()
        self.files_collection = self.db.research_files
        self.genes_collection = self.db.genes
        self.processing_logs_collection = self.db.processing_logs

    async def create_file_record(self, file_metadata: ResearchFileCreate) -> ResearchFileInDB:
        """
        Create a new file record in the database.
        
        :param file_metadata: Metadata for the file
        :return: File record with database ID
        """
        file_record = ResearchFileInDB(
            **file_metadata.model_dump(),
            status=FileStatus.PROCESSING
        )

        result = await self.files_collection.insert_one(
            file_record.model_dump(exclude_unset=True)
        )
        file_record.id = str(result.inserted_id)
        return file_record

    async def update_file_status(
            self, 
            file_id: str, 
            status: FileStatus, 
            total_genes: int = None
    ):
        """
        Update the status of a file record.
        
        :param file_id: ID of the file record
        :param status: New status
        :param total_genes: Total number of genes processed (optional)
        """
        update_data = {"status": status}
        if total_genes is not None:
            update_data.update({
                "total_genes_processed": total_genes,
                "processed_timestamp": time.time()
            })

        await self.files_collection.update_one(
            {"_id": file_id},
            {"$set": update_data}
        )

    async def log_processing(
            self,
            file_id: str,
            status: FileStatus,
            message: str
    ):
        """
        Log file processing events.
        
        :param file_id: ID of the file
        :param status: Current processing status
        :param message: Log message
        """
        log = FileProcessingLog(
            file_id=file_id,
            status=status,
            message=message
        )
        await self.processing_logs_collection.insert_one(log.dict())

    async def bulk_insert_genes(self, genes: List[GeneCreate], file_id: str, chunk_index: int, total_chunks: int):
        """
        Bulk insert genes into the database.
        
        :param genes: List of genes to insert
        :param file_id: ID of the research file
        :param chunk_index: Current chunk index
        :param total_chunks: Total number of chunks
        """
        try:
            await self.genes_collection.insert_many(
                [gene.model_dump() for gene in genes],
                ordered=False
            )

            await self.log_processing(
                file_id,
                FileStatus.PROCESSING,
                f"Processed chunk {chunk_index + 1}/{total_chunks} ({len(genes)} genes)"
            )
        except Exception as e:
            logger.error(f"Error in chunk {chunk_index + 1}: {str(e)}")
            raise