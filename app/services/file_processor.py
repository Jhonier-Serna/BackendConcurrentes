import os
import logging
import multiprocessing
from fastapi import UploadFile
from datetime import datetime

from app.utils.FileStorageService import FileStorageService
from app.utils.VCFParserService import VCFParserService
from app.db.mongodb import get_async_database

# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileProcessorService:
    """Orchestrates the entire file processing workflow."""
    def __init__(self):
        self.file_storage = FileStorageService()
        self.vcf_parser = VCFParserService()
        self.n_cores = multiprocessing.cpu_count()
        self.database =  get_async_database()
        self.genes_collection = self.database.genes

    async def process_file(
            self,
            file: UploadFile,
    ):
        """
        Main method to process an uploaded file.
        
        :param file: Uploaded file
        :param file_metadata: File metadata
        :return: Processed file record
        """
        start_time = datetime.now()

        # Save the file
        file_path = await self.file_storage.save_uploaded_file(file)


        try:
            logger.info("Starting gene parsing...")
            total_genes = 0

            # Parse genes y guarda en la base de datos simultáneamente
            async for genes_chunk in self.vcf_parser.parse_vcf(file_path):
                total_genes += len(genes_chunk)
                await self._process_chunk_parallel(genes_chunk, 0, 1, file_path)  # Procesar cada chunk inmediatamente

            logger.info(f"Total genes processed: {total_genes}")

            # Calculate processing time and speed
            total_time = (datetime.now() - start_time).total_seconds()/60

            logger.info(f"Processing completed successfully in {total_time:.2f} min")
            file_record = {"file_path": file_path, "total_genes": total_genes}  
            os.remove(file_path)  # Remover el archivo temporal
            return {"status": "success", "data": file_record}

        except Exception as e:
            logger.error(f"Processing error: {str(e)}")
            return {"status": "error", "message": str(e)}

    async def _process_chunk_parallel(self, chunk, chunk_index):
        """
        Process a single chunk of genes in parallel.
        
        :param chunk: Chunk of genes to process
        :param chunk_index: Current chunk index
        """
        try:
            await self.genes_collection.insert_many([gene.model_dump() for gene in chunk], ordered=False)
        except Exception as e:
            logger.error(f"Error inserting chunk {chunk_index + 1} into database: {str(e)}")
            raise