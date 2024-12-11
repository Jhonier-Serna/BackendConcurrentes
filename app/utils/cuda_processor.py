import ctypes
from pathlib import Path

class CUDAProcessor:
    def __init__(self, library_path: str):
        # Cargar la biblioteca compilada con CUDA
        self.lib = ctypes.CDLL(library_path)

        # Definir los prototipos de funciones
        self.lib.process_file.argtypes = [ctypes.c_char_p]
        self.lib.process_file.restype = ctypes.POINTER(ctypes.c_char_p)

        self.lib.query_genes.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        self.lib.query_genes.restype = ctypes.POINTER(ctypes.c_char_p)

    def process_file(self, file_path: str):
        """
        Procesar un archivo usando CUDA.
        """
        file_path_bytes = file_path.encode('utf-8')
        result = self.lib.process_file(file_path_bytes)
        if not result:
            raise RuntimeError("CUDA file processing failed.")
        return result

    def query_genes(self, search_params: dict):
        """
        Buscar genes usando CUDA.
        """
        params_str = ",".join(f"{k}={v}" for k, v in search_params.items())
        params_bytes = params_str.encode('utf-8')
        result = self.lib.query_genes(params_bytes)
        if not result:
            raise RuntimeError("CUDA gene search failed.")
        return result

# Ruta por defecto para la biblioteca CUDA
DEFAULT_CUDA_LIBRARY = Path(__file__).parent.parent / "cuda_lib.so"

# Instancia global del procesador CUDA
cuda_processor = CUDAProcessor(str(DEFAULT_CUDA_LIBRARY))
