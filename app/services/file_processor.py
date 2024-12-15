import traceback

from pymongo import InsertOne
import pymongo
from app.models.stats import PStats
import os
import logging
import asyncio
import time
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from dotenv import load_dotenv
from fastapi import UploadFile
from datetime import datetime
from typing import List, Dict, Tuple

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

load_dotenv()
nProcess = int(os.getenv("NUM_PROCESSES", os.cpu_count()))
chunk = int(os.getenv("CHUNK_SIZE", 10000))
MONGO_URI = os.getenv("MONGO_URL")
client = pymongo.MongoClient(
    MONGO_URI,
    maxPoolSize=None,
    connectTimeoutMS=30000,
    socketTimeoutMS=None,
    connect=False,
    retryWrites=True,
    w=1  # Asegura escrituras confirmadas
)
db = client["gene_search_db"]
collection = db["genes"]

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
            file_path: str,
            num_processes: int,
    ) -> PStats:
        """
        Main method to process an uploaded file.
        
        :param file: Uploaded file
        :param file_metadata: File metadata
        :return: Processed file record
        """
        stats = PStats()
        stats.begin()
        numProcess = num_processes or nProcess
        logger.info(f"Starting file processing: {file_path}")
        logger.info(f"procesadores {numProcess}")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        fileSize = os.path.getsize(file_path)
        logger.info(f"File size: {fileSize / (1024*1024):.2f} MB")
        tLines = 0
        with open(file_path, 'rb') as f:
            for line in f:
                if not line.startswith(b'#'):
                    tLines += 1
        stats.totalLines = tLines
        logger.info(f"Total non-header lines: {tLines}")
        chunkPos = []
        lChunk = tLines // (4 * numProcess)
        current_position = 0
        cLines = 0
        with open(file_path, 'rb') as f:
            # Saltar las líneas de encabezado
            current_position = f.tell()
            for line in f:
                if line.startswith(b'#'):
                    current_position = f.tell()
                    continue
                else:
                    break      
            chunk_start = current_position
            for line in f:
                cLines += 1
                if cLines >= lChunk:
                    chunkPos.append((chunk_start, current_position - chunk_start))
                    chunk_start = current_position
                    cLines = 0
                current_position = f.tell()
            if current_position > chunk_start:
                chunkPos.append((chunk_start, current_position - chunk_start))
            logger.info(f"Created {len(chunkPos)} chunks")

            try:
                sample_columns, column_positions = self.getInfo(file_path)
                logger.info(f"Found {len(sample_columns)} sample columns")
            except Exception as e:
                logger.error(f"Error getting header info: {str(e)}")
                raise

            # Procesar chunks en paralelo
            with ProcessPoolExecutor(max_workers=numProcess) as executor:
                futures = []
                for start_pos, length in chunkPos:
                    futures.append(
                        executor.submit(
                            self.process_chunk_parallel,
                            file_path,
                            start_pos,
                            length,
                            column_positions
                        )
                    )

                # Monitorear progreso
                completed = 0
                for future in futures:
                    try:
                        result = future.result(timeout=300)
                        stats.processed_lines += result['processed']
                        stats.inserted_documents += result['inserted']
                        completed += 1
                        
                        progress = (completed / len(chunkPos)) * 100
                        throughput = stats.processed_lines / (time.time() - stats.start_time)
                        logger.info(f"Progress: {progress:.1f}% - Documents inserted: {stats.inserted_documents}")
                        
                    except Exception as e:
                        logger.error(f"Error processing chunk: {str(e)}")
                        continue

            stats.end()
            processing_time = stats.get_processing_time()/60
            logger.info(f"Processing completed in {processing_time:.2f} minutes")
            logger.info(f"Documents processed: {stats.processed_lines}")
            logger.info(f"Documents inserted: {stats.inserted_documents}")
            
            return stats



        # # Save the file
        # file_path = await self.file_storage.save_uploaded_file(file)

        # # Create file record in database
        # file_record = await self.database.create_file_record(file_metadata)

        # try:
        #     logger.info("Starting gene parsing...")
        #     total_genes = 0
        #     chunks = []

        #     # Parse genes
        #     async for genes_chunk in self.vcf_parser.parse_vcf(file_path, file_record):
        #         total_genes += len(genes_chunk)
        #         chunks.append(genes_chunk)

        #     logger.info(f"Total genes to process: {total_genes}")

        #     # Process chunks in parallel
        #     executor = ProcessPoolExecutor(max_workers=self.n_cores)
        #     try:
        #         tasks = []
        #         for i, chunk in enumerate(chunks):
        #             task = asyncio.create_task(
        #                 self._process_chunk_parallel(chunk, i, len(chunks), file_record.id)
        #             )
        #             tasks.append(task)

        #         await asyncio.gather(*tasks)
        #     finally:
        #         executor.shutdown(wait=True)

        #     # Calculate processing time and speed
        #     total_time = (datetime.now() - start_time).total_seconds()
        #     genes_per_second = total_genes / total_time

        #     # Update file status
        #     await self.database.update_file_status(
        #         file_record.id, 
        #         FileStatus.COMPLETED, 
        #         total_genes
        #     )

        #     # Log processing completion
        #     await self.database.log_processing(
        #         file_record.id,
        #         FileStatus.COMPLETED,
        #         f"Processing completed. Total genes: {total_genes}. "
        #         f"Total time: {total_time:.2f} seconds. "
        #         f"Speed: {genes_per_second:.2f} genes/second"
        #     )

        #     logger.info(f"Processing completed successfully in {total_time:.2f} seconds")
            return file_record



    async def process_chunk_parallel(self,file_path: str, start_pos: int, chunk_size: int, column_positions: Dict[str, int]) -> Dict:
        """
        Process a single chunk of genes in parallel.
        """
        processed = 0
        inserted = 0
        batch = []
        batch_size = 1000
        try:
            with open(file_path, 'rb') as f:
                f.seek(start_pos)
                chunk = f.read(chunk_size)
                
                # Encontrar el inicio de la siguiente línea si no estamos al principio
                if start_pos > 0:
                    first_newline = chunk.find(b'\n')
                    if first_newline != -1:
                        chunk = chunk[first_newline + 1:]
                
                # Asegurar que terminamos en una línea completa
                last_newline = chunk.rfind(b'\n')
                if last_newline != -1:
                    chunk = chunk[:last_newline]
                
                lines = chunk.split(b'\n')
                
                for line in lines:
                    if not line:
                        continue
                        
                    try:
                        line_str = line.decode('utf-8')
                        if line_str.startswith('#'):
                            continue
                            
                        processed += 1
                        document = self.processL(line_str, column_positions)
                        
                        if document:
                            batch.append(document)
                            
                            if len(batch) >= batch_size:
                                inserted_count = self.insertMongo(batch)
                                if inserted_count > 0:
                                    inserted += inserted_count
                                batch = []
                            
                    except UnicodeDecodeError as e:
                        logger.error(f"Unicode decode error: {str(e)}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing line: {str(e)}")
                        continue
                
                # Insertar el último batch
                if batch:
                    inserted_count = self.insertMongo(batch)
                    if inserted_count > 0:
                        inserted += inserted_count
                
                logger.info(f"Chunk processed: {processed} lines, {inserted} documents inserted")
                
                return {
                    "processed": processed,
                    "inserted": inserted
                }
                
        except Exception as e:
            logger.error(f"Error processing chunk: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise   
        


    def getInfo(self, file_path: str) -> Tuple[List[str], Dict[str, int]]:
        """
        Extrae la información del encabezado del archivo VCF.
        Retorna los nombres de las columnas y sus posiciones.
        """
        with open(file_path, 'r') as f:
            for line in f:
                if line.startswith('#CHROM'):
                    headers = line.strip().split('\t')
                    sample_columns = headers[9:]
                    column_positions = {}
                    for idx, name in enumerate(sample_columns):
                        column_positions[name] = idx + 9
                    return sample_columns, column_positions
        raise ValueError("No se encontró la línea de encabezado en el archivo VCF")
    
        
    def processL(line: str, column_positions: Dict[str, int]) -> Dict:
        """
        Procesa una línea del archivo VCF y retorna un documento.
        Incluye validación extensiva y logging para prevenir pérdida de datos.
        """
        if not line or line.startswith('#'):
            return None
            
        fields = line.strip().split('\t')
        try:
            # Verificación de campos mínimos requeridos
            if len(fields) < 8:
                logger.warning(f"Line has insufficient fields ({len(fields)} < 8)")
                return None
            
            # Documento base con campos comunes
            document = {
                "CHROM": fields[0],
                "POS": fields[1],
                "ID": fields[2] if fields[2] != '.' else None,
                "REF": fields[3],
                "ALT": fields[4],
                "QUAL": fields[5],
                "FILTER": fields[6],
                "INFO": fields[7]
            }
            
            # Validar campos requeridos
            if not document["CHROM"] or not document["POS"]:
                logger.warning("Missing required CHROM or POS fields")
                return None
                
            # Agregar FORMAT si existe
            if len(fields) > 8:
                document["FORMAT"] = fields[8]
                
                # Procesar campos de muestra si existen
                for sample_name, position in column_positions.items():
                    if position < len(fields):
                        sample_value = fields[position].strip()
                        if sample_value:  # Solo agregar si tiene valor
                            document[sample_name] = sample_value
            
            # Validación final del documento
            if not all(key in document for key in ["CHROM", "POS", "REF", "ALT"]):
                logger.warning("Missing required fields in document")
                return None
                
            return document
            
        except Exception as e:
            logger.error(f"Error processing line: {str(e)}")
            logger.error(f"Line content: {line[:200]}...")  # Log primeros 200 caracteres
            return None    

    def insertMongo(data: List[Dict]) -> int:
        """
        Inserta múltiples documentos en MongoDB y retorna la cantidad de documentos insertados.
        """
        if not data:
            return 0
            
        inserted_count = 0
        operations = [
            InsertOne(doc) for doc in data
            if doc is not None
        ]
        
        if operations:
            try:
                result = collection.bulk_write(operations, ordered=False)
                inserted_count = result.inserted_count
                logger.info(f"Inserted {inserted_count} documents")
                return inserted_count
            except pymongo.errors.BulkWriteError as bwe:
                logger.error(f"Bulk write error: {str(bwe)}")
                # Contar documentos insertados exitosamente
                inserted_count = bwe.details.get('nInserted', 0)
                return inserted_count
            except Exception as e:
                logger.error(f"Error during bulk insert: {str(e)}")
                raise
                
        return inserted_count        