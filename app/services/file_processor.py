import os
import logging
import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from fastapi import UploadFile
from datetime import datetime

from app.models.file import (
    ResearchFileCreate,
    ResearchFileInDB,
    FileStatus,
)
from app.utils.FileStorageService import FileStorageService
from app.utils.VCFParserService import VCFParserService
from app.utils.DatabaseService import DatabaseService

# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileProcessorService:
    """Orchestrates the entire file processing workflow."""
    def __init__(self):
        self.file_storage = FileStorageService()
        self.vcf_parser = VCFParserService()
        self.database = DatabaseService()
        self.n_cores = multiprocessing.cpu_count()

    async def process_file(
            self,
            file: UploadFile,
            file_metadata: ResearchFileCreate
    ) -> ResearchFileInDB:
        """
        Main method to process an uploaded file.
        
        :param file: Uploaded file
        :param file_metadata: File metadata
        :return: Processed file record
        """
        start_time = datetime.now()
        logger.info(f"Starting file processing: {file_metadata.filename}")

        # Save the file
        file_path = await self.file_storage.save_uploaded_file(file)

        # Create file record in database
        file_record = await self.database.create_file_record(file_metadata)

        try:
            logger.info("Starting gene parsing...")
            total_genes = 0
            chunks = []

            # Parse genes
            async for genes_chunk in self.vcf_parser.parse_vcf(file_path, file_record):
                total_genes += len(genes_chunk)
                chunks.append(genes_chunk)

            logger.info(f"Total genes to process: {total_genes}")

            # Process chunks in parallel
            executor = ProcessPoolExecutor(max_workers=self.n_cores)
            try:
                tasks = []
                for i, chunk in enumerate(chunks):
                    task = asyncio.create_task(
                        self._process_chunk_parallel(chunk, i, len(chunks), file_record.id)
                    )
                    tasks.append(task)

                await asyncio.gather(*tasks)
            finally:
                executor.shutdown(wait=True)

            # Calculate processing time and speed
            total_time = (datetime.now() - start_time).total_seconds()
            genes_per_second = total_genes / total_time

            # Update file status
            await self.database.update_file_status(
                file_record.id, 
                FileStatus.COMPLETED, 
                total_genes
            )

            # Log processing completion
            await self.database.log_processing(
                file_record.id,
                FileStatus.COMPLETED,
                f"Processing completed. Total genes: {total_genes}. "
                f"Total time: {total_time:.2f} seconds. "
                f"Speed: {genes_per_second:.2f} genes/second"
            )

            logger.info(f"Processing completed successfully in {total_time:.2f} seconds")
            os.remove(file_path)  # Remover el archivo temporal
            return file_record

        except Exception as e:
            logger.error(f"Processing error: {str(e)}")
            await self.database.update_file_status(file_record.id, FileStatus.FAILED)
            await self.database.log_processing(
                file_record.id,
                FileStatus.FAILED,
                f"Error: {str(e)}"
            )
            raise

    async def _process_chunk_parallel(self, chunk, chunk_index, total_chunks, file_id):
        """
        Process a single chunk of genes in parallel.
        
        :param chunk: Chunk of genes to process
        :param chunk_index: Current chunk index
        :param total_chunks: Total number of chunks
        :param file_id: ID of the research file
        """
        await self.database.bulk_insert_genes(chunk, file_id, chunk_index, total_chunks)