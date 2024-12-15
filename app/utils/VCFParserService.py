import logging
import mmap
from typing import List, AsyncGenerator
from app.models.gene import GeneCreate

# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VCFParserService:
    """Handles parsing of VCF files."""

    def __init__(self, chunk_size=1000):
        self.chunk_size = chunk_size

    async def parse_vcf(
        self,
        filepath: str,
    ) -> AsyncGenerator[List[GeneCreate], None]:
        """
        Asynchronous generator to parse VCF file and yield gene chunks.

        :param filepath: Path to the VCF file
        :param file_record: Metadata about the research file
        :yields: Chunks of parsed genes
        """
        genes = []

        try:
            with open(filepath, "rb") as f:
                mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

                # Skip metadata lines
                line = mm.readline().decode("utf-8")
                sample_names = []
                while line.startswith("#"):
                    if line.startswith("#CHROM"):
                        # Extract sample names from header line
                        sample_names = line.strip().split("\t")[9:]
                    line = mm.readline().decode("utf-8")

                while line:
                    if not line.strip():
                        line = mm.readline().decode("utf-8")
                        continue

                    fields = line.strip().split("\t")
                    if len(fields) < 8:
                        logger.warning(f"Incorrect line format: {line.strip()}")
                        line = mm.readline().decode("utf-8")
                        continue

                    try:
                        chrom, pos, id_, ref, alt, qual, filter_status, info = fields[
                            :8
                        ]
                        format_str = fields[8] if len(fields) > 8 else ""
                        sample_data = fields[9:] if len(fields) > 9 else []

                        # Create GeneCreate instance
                        gene = GeneCreate(
                            chromosome=chrom,
                            position=int(pos),
                            id=id_ if id_ != "." else "",
                            reference=ref,
                            alternate=alt,
                            quality=float(qual) if qual != "." else 0.0,
                            filter_status=(
                                filter_status if filter_status != "." else "PASS"
                            ),
                            info=info if info != "." else "",
                            format=format_str,
                            outputs=dict(zip(sample_names, sample_data)),
                        )
                        genes.append(gene)

                        if len(genes) >= self.chunk_size:
                            yield genes
                            genes = []

                    except (ValueError, IndexError) as e:
                        logger.warning(
                            f"Error processing line: {line.strip()} - {str(e)}"
                        )

                    line = mm.readline().decode("utf-8")

                if genes:
                    yield genes

        except Exception as e:
            logger.error(f"Error reading VCF file: {str(e)}")

        finally:
            if "mm" in locals() and not mm.closed:
                mm.close()
