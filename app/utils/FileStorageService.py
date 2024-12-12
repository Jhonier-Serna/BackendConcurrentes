import os
import time
import aiofiles
import logging
from fastapi import UploadFile
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
