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
        self.database = get_async_database()
        self.genes_collection = self.database.genes

    async def _create_indexes(self):
        """
        Crea índices optimizados para búsquedas parciales con expresiones regulares.
        """
        try:
            await self.genes_collection.create_index(
                [("chromosome", 1)], name="chromosome_index", background=True
            )
            await self.genes_collection.create_index(
                [("filter_status", 1)], name="filter_status_index", background=True
            )
            await self.genes_collection.create_index(
                [("info", 1)], name="info_index", background=True
            )
            await self.genes_collection.create_index(
                [("format", 1)], name="format_index", background=True
            )
            logger.info("Índices creados para búsquedas parciales.")
        except Exception as e:
            logger.error(f"Error creando índices: {e}")

    async def process_file(
        self,
        file: UploadFile,
    ):
        """
        Main method to process an uploaded file.

        :param file: Uploaded file
        :return: Processed file record
        """
        start_time = datetime.now()

        # Save the file
        file_path = await self.file_storage.save_uploaded_file(file)

        # Crear una colección para el archivo subido
        collection_name = f"genes_{int(datetime.now().timestamp())}"
        genes_collection = self.database[collection_name]

        # Verificar y crear la colección de archivos subidos si no existe
        if "uploaded_files" not in await self.database.list_collection_names():
            await self.database.create_collection("uploaded_files")

        try:
            logger.info("Starting gene parsing...")
            total_genes = 0

            # Parse genes y guarda en la nueva colección
            async for genes_chunk in self.vcf_parser.parse_vcf(file_path):
                total_genes += len(genes_chunk)
                await self._process_chunk_parallel(
                    genes_chunk, genes_collection
                )  # Procesar cada chunk en la nueva colección

            # Guardar información del archivo en la colección de archivos subidos
            await self.database.uploaded_files.insert_one(
                {
                    "file_path": file_path,
                    "collection_name": collection_name,
                    "total_genes": total_genes,
                    "upload_time": datetime.now(),
                }
            )

            # Calculate processing time and speed
            total_time = (datetime.now() - start_time).total_seconds() / 60

            logger.info(f"Processing completed successfully in {total_time:.2f} min")
            file_record = {"file_path": file_path, "total_genes": total_genes}
            os.remove(file_path)  # Remover el archivo temporal

            await self._create_indexes()  # Indices
            return {"status": "success", "data": file_record}

        except Exception as e:
            logger.error(f"Processing error: {str(e)}")
            return {"status": "error", "message": str(e)}

    async def _process_chunk_parallel(self, chunk, genes_collection):
        """
        Process a single chunk of genes in parallel.

        :param chunk: Chunk of genes to process
        :param genes_collection: Collection to insert genes into
        """
        try:
            await genes_collection.insert_many(
                [gene.model_dump() for gene in chunk], ordered=False
            )
        except Exception as e:
            logger.error(f"Error inserting chunk into database: {str(e)}")
            raise
