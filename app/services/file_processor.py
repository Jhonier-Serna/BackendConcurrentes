import os
from typing import List, Dict, Any, AsyncGenerator
from fastapi import UploadFile
import pandas as pd
import time
import logging
from datetime import datetime
import asyncio
import aiofiles
import mmap
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

from app.models.file import (
    ResearchFileCreate,
    ResearchFileInDB,
    FileStatus,
    FileProcessingLog
)
from app.models.gene import GeneCreate, WineType
from app.db.mongodb import get_async_database

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FileProcessorService:
    def __init__(self):
        self.db = get_async_database()
        self.files_collection = self.db.research_files
        self.genes_collection = self.db.genes
        self.processing_logs_collection = self.db.processing_logs
        self.upload_folder = os.getenv('UPLOAD_FOLDER', '/tmp/research_files')
        self.max_workers = int(os.getenv('MAX_WORKERS', 4))
        self.chunk_size = 10000
        self.n_cores = multiprocessing.cpu_count()
        self.buffer_size = 64 * 1024  # 64KB buffer

    async def process_file(
            self,
            file: UploadFile,
            file_metadata: ResearchFileCreate
    ) -> ResearchFileInDB:
        inicio_proceso = datetime.now()
        logger.info(f"Iniciando procesamiento del archivo: {file_metadata.filename}")

        unique_filename = f"{time.time()}_{file_metadata.filename}"
        file_path = os.path.join(self.upload_folder, unique_filename)

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        async with aiofiles.open(file_path, "wb") as buffer:
            while content := await file.read(1024 * 1024):
                await buffer.write(content)

        logger.info(f"Archivo guardado en: {file_path}")

        file_record = ResearchFileInDB(
            **file_metadata.model_dump(),
            status=FileStatus.PROCESSING
        )

        result = await self.files_collection.insert_one(
            file_record.model_dump(exclude_unset=True)
        )
        file_record.id = str(result.inserted_id)

        try:
            logger.info("Iniciando parseo de genes...")
            total_genes = 0
            chunks = []

            # Procesar el generador de genes
            async for genes_chunk in self._parse_gene_file(file_path, file_record):
                total_genes += len(genes_chunk)
                chunks.append(genes_chunk)

            logger.info(f"Total de genes a procesar: {total_genes}")

            # Crear el executor fuera del context manager
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

            tiempo_total = (datetime.now() - inicio_proceso).total_seconds()
            genes_por_segundo = total_genes / tiempo_total

            await self.files_collection.update_one(
                {"_id": result.inserted_id},
                {"$set": {
                    "status": FileStatus.COMPLETED,
                    "total_genes_processed": total_genes,
                    "processed_timestamp": time.time()
                }}
            )

            await self._log_processing(
                file_record.id,
                FileStatus.COMPLETED,
                f"Procesamiento completado. Total genes: {total_genes}. "
                f"Tiempo total: {tiempo_total:.2f} segundos. "
                f"Velocidad: {genes_por_segundo:.2f} genes/segundo"
            )

            logger.info(f"Procesamiento completado exitosamente en {tiempo_total:.2f} segundos")
            return file_record

        except Exception as e:
            logger.error(f"Error durante el procesamiento: {str(e)}")
            await self.files_collection.update_one(
                {"_id": result.inserted_id},
                {"$set": {"status": FileStatus.FAILED}}
            )
            await self._log_processing(
                file_record.id,
                FileStatus.FAILED,
                f"Error: {str(e)}"
            )
            raise

    async def _parse_gene_file(
            self,
            filepath: str,
            file_record: ResearchFileInDB
    ) -> AsyncGenerator[List[GeneCreate], None]:
        if filepath.endswith('.vcf'):
            async for genes in self._parse_vcf(filepath, file_record):
                yield genes
        else:
            raise ValueError("Formato de archivo no soportado")


    async def _parse_vcf(
            self,
            filepath: str,
            file_record: ResearchFileInDB
    ) -> AsyncGenerator[List[GeneCreate], None]:
        """
        Método para parsear un archivo VCF y generar genes en lotes.

        :param filepath: Ruta del archivo VCF.
        :param file_record: Información del archivo de investigación.
        :return: Generador asíncrono de lotes de genes.
        """
        genes = []

        try:
            with open(filepath, 'rb') as f:
                mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

                # Saltar líneas de metadatos
                line = mm.readline().decode('utf-8')
                while line.startswith('#'):
                    if line.startswith('#CHROM'):
                        # Extraer nombres de muestras desde la línea de encabezado
                        sample_names = line.strip().split('\t')[9:]
                    line = mm.readline().decode('utf-8')

                while line:
                    if not line.strip():
                        line = mm.readline().decode('utf-8')
                        continue

                    fields = line.strip().split('\t')
                    if len(fields) < 8:  # Verificar campos mínimos requeridos
                        logger.warning(f"Línea con formato incorrecto: {line.strip()}")
                        line = mm.readline().decode('utf-8')
                        continue

                    try:
                        chrom, pos, id_, ref, alt, qual, filter_status, info = fields[:8]
                        format_str = fields[8] if len(fields) > 8 else ''
                        sample_data = fields[9:] if len(fields) > 9 else []

                        # Parsear el campo INFO
                        parsed_info = self._parse_info_field(info)

                        # Procesar los outputs a partir de los datos de muestras
                        outputs = {}
                        if sample_names and sample_data:
                            for name, data in zip(sample_names, sample_data):
                                outputs[name] = data

                        # Crear la instancia de GeneCreate
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
                        logger.warning(f"Error procesando la línea: {line.strip()} - {str(e)}")

                    line = mm.readline().decode('utf-8')

                mm.close()

            if genes:
                yield genes

        except Exception as e:
            logger.error(f"Error al leer el archivo VCF: {str(e)}")

        finally:
            if 'mm' in locals() and not mm.closed:
                mm.close()


    def _parse_info_field(self, info_str: str) -> Dict[str, Any]:
        if info_str == '.' or not info_str:
            return {}

        info_dict = {}
        for item in info_str.split(';'):
            if '=' in item:
                key, value = item.split('=', 1)
                # Convertir valores numéricos cuando sea posible
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

    async def _log_processing(
            self,
            file_id: str,
            status: FileStatus,
            message: str
    ):
        log = FileProcessingLog(
            file_id=file_id,
            status=status,
            message=message
        )
        await self.processing_logs_collection.insert_one(log.dict())

    async def get_file_status(self, file_id: str) -> FileStatus:
        file = await self.files_collection.find_one({"_id": file_id})
        if not file:
            raise ValueError("Archivo no encontrado")
        return file.get('status', FileStatus.FAILED)

    async def _process_chunk_parallel(self, chunk, chunk_index, total_chunks, file_id):
        try:
            # Inserción en lotes optimizada
            await self.genes_collection.insert_many(
                [gene.model_dump() for gene in chunk],
                ordered=False
            )

            await self._log_processing(
                file_id,
                FileStatus.PROCESSING,
                f"Procesado chunk {chunk_index + 1}/{total_chunks} ({len(chunk)} genes)"
            )
        except Exception as e:
            logger.error(f"Error en chunk {chunk_index + 1}: {str(e)}")
            raise
