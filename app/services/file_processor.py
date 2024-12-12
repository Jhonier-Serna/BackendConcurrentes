import os
import time
import logging
import asyncio
import aiofiles
import mmap
import multiprocessing
from typing import List, Dict, Any, AsyncGenerator
from concurrent.futures import ProcessPoolExecutor
from fastapi import UploadFile
from datetime import datetime

from app.models.file import (
    ResearchFileCreate,
    ResearchFileInDB,
    FileStatus,
    FileProcessingLog
)
from app.models.gene import GeneCreate, WineType
from app.db.mongodb import get_async_database

# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileStorageService:
    """Handles file storage and management operations."""
    def __init__(self, upload_folder='/tmp/research_files'):
        self.upload_folder = upload_folder
        os.makedirs(upload_folder, exist_ok=True)

    async def save_uploaded_file(self, file: UploadFile) -> str:
        """
        Save the uploaded file to disk with a unique filename.
        
        :param file: Uploaded file object
        :return: Path to the saved file
        """
        unique_filename = f"{time.time()}_{file.filename}"
        file_path = os.path.join(self.upload_folder, unique_filename)

        async with aiofiles.open(file_path, "wb") as buffer:
            while content := await file.read(1024 * 1024):
                await buffer.write(content)

        logger.info(f"File saved to: {file_path}")
        return file_path

class VCFParserService:
    """Handles parsing of VCF files."""
    def __init__(self, chunk_size=10000):
        self.chunk_size = chunk_size

    def _parse_info_field(self, info_str: str) -> Dict[str, Any]:
        """Parse the INFO field of a VCF file."""
        if info_str == '.' or not info_str:
            return {}

        info_dict = {}
        for item in info_str.split(';'):
            if '=' in item:
                key, value = item.split('=', 1)
                # Convert numeric values when possible
                try:
                    if ',' in value:
                        value = [float(v) if '.' in v else int(v)
                                 for v in value.split(',')]
                    elif '.' in value:
                        value = float(value)
                    else:
                        value = int(value)
                except ValueError:
                    pass
                info_dict[key] = value
            else:
                info_dict[item] = True
        return info_dict

    async def parse_vcf(
            self,
            filepath: str,
            file_record: ResearchFileInDB
    ) -> AsyncGenerator[List[GeneCreate], None]:
        """
        Asynchronous generator to parse VCF file and yield gene chunks.
        
        :param filepath: Path to the VCF file
        :param file_record: Metadata about the research file
        :yields: Chunks of parsed genes
        """
        genes = []

        try:
            with open(filepath, 'rb') as f:
                mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

                # Skip metadata lines
                line = mm.readline().decode('utf-8')
                sample_names = []
                while line.startswith('#'):
                    if line.startswith('#CHROM'):
                        # Extract sample names from header line
                        sample_names = line.strip().split('\t')[9:]
                    line = mm.readline().decode('utf-8')

                while line:
                    if not line.strip():
                        line = mm.readline().decode('utf-8')
                        continue

                    fields = line.strip().split('\t')
                    if len(fields) < 8:
                        logger.warning(f"Incorrect line format: {line.strip()}")
                        line = mm.readline().decode('utf-8')
                        continue

                    try:
                        chrom, pos, id_, ref, alt, qual, filter_status, info = fields[:8]
                        format_str = fields[8] if len(fields) > 8 else ''
                        sample_data = fields[9:] if len(fields) > 9 else []

                        # Parse INFO field
                        parsed_info = self._parse_info_field(info)

                        # Process sample outputs
                        outputs = {}
                        if sample_names and sample_data:
                            for name, data in zip(sample_names, sample_data):
                                outputs[name] = data

                        # Create GeneCreate instance
                        gene = GeneCreate(
                            chromosome=chrom,
                            position=int(pos),
                            id=id_ if id_ != '.' else '',
                            reference=ref,
                            alternate=alt,
                            quality=float(qual) if qual != '.' else 0.0,
                            filter_status=filter_status if filter_status != '.' else 'PASS',
                            info=parsed_info,
                            format=format_str,
                            outputs=outputs,
                            wine_type=file_record.wine_type,
                            research_file_id=file_record.id
                        )
                        genes.append(gene)

                        if len(genes) >= self.chunk_size:
                            yield genes
                            genes = []

                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error processing line: {line.strip()} - {str(e)}")

                    line = mm.readline().decode('utf-8')

                mm.close()

            if genes:
                yield genes

        except Exception as e:
            logger.error(f"Error reading VCF file: {str(e)}")

        finally:
            if 'mm' in locals() and not mm.closed:
                mm.close()

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

class FileProcessorService:
    """Orchestrates the entire file processing workflow."""
    def __init__(self):
        self.file_storage = FileStorageService()
        self.vcf_parser = VCFParserService()
        self.database = DatabaseService()
        self.max_workers = int(os.getenv('MAX_WORKERS', 4))
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