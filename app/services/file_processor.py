import os
from typing import List, Dict, Any
from fastapi import UploadFile
import pandas as pd
import time

from app.models.file import (
    ResearchFileCreate,
    ResearchFileInDB,
    FileStatus,
    FileProcessingLog
)
from app.models.gene import GeneCreate, WineType
from app.db.mongodb import get_async_database


class FileProcessorService:
    def __init__(self):
        self.db = get_async_database()
        self.files_collection = self.db.research_files
        self.genes_collection = self.db.genes
        self.processing_logs_collection = self.db.processing_logs
        self.upload_folder = os.getenv('UPLOAD_FOLDER', '/tmp/research_files')

    async def process_file(
            self,
            file: UploadFile,
            file_metadata: ResearchFileCreate
    ) -> ResearchFileInDB:
        """
        Procesar archivo de investigación genética
        """
        # Generar nombre único para el archivo
        unique_filename = f"{time.time()}_{file_metadata.filename}"
        file_path = os.path.join(
            self.upload_folder,
            unique_filename
        )

        # Crear estructura de directorios si no existe
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Guardar archivo
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
            await file.seek(0)  # Regresar al inicio del archivo para futuros usos

        # Crear registro de archivo
        file_record = ResearchFileInDB(
            **file_metadata.model_dump(),
            status=FileStatus.PROCESSING
        )

        # Insertar registro
        result = await self.files_collection.insert_one(
            file_record.model_dump(exclude_unset=True)
        )

        # Asignar el id del documento insertado
        file_record.id = str(result.inserted_id)

        try:
            # Procesar archivo
            genes = await self._parse_gene_file(file_path, file_record)

            # Insertar genes en chunks para mejor rendimiento
            chunk_size = 1000
            for i in range(0, len(genes), chunk_size):
                chunk = genes[i:i + chunk_size]
                await self.genes_collection.insert_many(
                    [gene.model_dump() for gene in chunk]
                )

            # Actualizar estado
            await self.files_collection.update_one(
                {"_id": result.inserted_id},
                {"$set": {
                    "status": FileStatus.COMPLETED,
                    "total_genes_processed": len(genes),
                    "processed_timestamp": time.time()
                }}
            )

            # Registrar log
            await self._log_processing(
                file_record.id,
                FileStatus.COMPLETED,
                f"Procesados {len(genes)} genes"
            )

            return file_record

        except Exception as e:
            # Manejar errores de procesamiento
            await self.files_collection.update_one(
                {"_id": result.inserted_id},
                {"$set": {"status": FileStatus.FAILED}}
            )

            await self._log_processing(
                file_record.id,
                FileStatus.FAILED,
                str(e)
            )

            raise

    async def _parse_gene_file(
            self,
            filepath: str,
            file_record: ResearchFileInDB
    ) -> List[GeneCreate]:
        """
        Parsear archivo de genes con pandas
        Soporta múltiples formatos (VCF, CSV, TSV)
        """
        # Detectar formato de archivo
        if filepath.endswith('.vcf'):
            genes = self._parse_vcf(filepath, file_record)
        elif filepath.endswith(('.csv', '.txt')):
            genes = self._parse_csv(filepath, file_record)
        else:
            raise ValueError("Formato de archivo no soportado")

        return genes

    def _parse_vcf(
            self,
            filepath: str,
            file_record: ResearchFileInDB
    ) -> List[GeneCreate]:
        """Parsear archivos VCF sin PyVCF"""
        genes = []
        with open(filepath, 'r') as file:
            header_info = {}
            for line in file:
                if line.startswith('#'):
                    if line.startswith('#CHROM'):
                        header_info['output_columns'] = line.strip().split('\t')[9:]
                    continue
                
                parts = line.strip().split('\t')
                info_dict = self._parse_info_field(parts[7])
                format_fields = parts[8].split(':')
                outputs = {}
                
                # Procesar columnas de output variables
                for idx, sample in enumerate(parts[9:]):
                    sample_name = header_info['output_columns'][idx]
                    outputs[sample_name] = dict(zip(format_fields, sample.split(':')))

                gene = GeneCreate(
                    chromosome=parts[0],
                    position=int(parts[1]),
                    id=parts[2] if parts[2] != '.' else '',
                    reference=parts[3],
                    alternate=parts[4],
                    quality=float(parts[5]) if parts[5] != '.' else 0.0,
                    filter_status=parts[6],
                    info=info_dict,
                    format=parts[8],
                    outputs=outputs,
                    wine_type=file_record.wine_type,
                    research_file_id=file_record.id
                )
                genes.append(gene)
        return genes

    def _parse_info_field(self, info_str: str) -> Dict[str, Any]:
        """Parsear el campo INFO del VCF"""
        if info_str == '.':
            return {}
        
        info_dict = {}
        for item in info_str.split(';'):
            if '=' in item:
                key, value = item.split('=', 1)
                info_dict[key] = value
            else:
                info_dict[item] = True
        return info_dict

    def _parse_csv(
            self,
            filepath: str,
            file_record: ResearchFileInDB
    ) -> List[GeneCreate]:
        """Parsear archivos CSV/TSV"""
        df = pd.read_csv(filepath, sep='\t')

        genes = []
        for _, row in df.iterrows():
            gene = GeneCreate(
                chromosome=row['Chrom'],
                position=row['Pos'],
                id=row.get('Id', ''),
                reference=row['Ref'],
                alternate=row['Alt'],
                quality=row.get('Qual', 0.0),
                filter_status=row.get('Filter', ''),
                wine_type=file_record.wine_type,
                research_file_id=file_record.id
            )
            genes.append(gene)

        return genes

    async def _log_processing(
            self,
            file_id: str,
            status: FileStatus,
            message: str
    ):
        """Registrar log de procesamiento"""
        log = FileProcessingLog(
            file_id=file_id,
            status=status,
            message=message
        )
        await self.processing_logs_collection.insert_one(log.dict())

    async def get_file_status(self, file_id: str) -> FileStatus:
        """Consultar estado de procesamiento de archivo"""
        file = await self.files_collection.find_one({"_id": file_id})
        if not file:
            raise ValueError("Archivo no encontrado")

        return file.get('status', FileStatus.FAILED)