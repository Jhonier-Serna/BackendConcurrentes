import logging
import mmap
from typing import List, Dict, Any, AsyncGenerator
from app.models.gene import GeneCreate
from app.models.file import (
    ResearchFileInDB,
)
# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
