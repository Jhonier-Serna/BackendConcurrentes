import time
from typing import Dict, List, Tuple

class PStats:
    def __init__(self):
        self.start_time = None
        self.endTime = None
        self.totalLines = 0
        self.processedLines = 0
        self.insertedDocuments = 0

    def begin(self):
        self.start_time = time.time()

    def end(self):
        self.endTime = time.time()

    def get_processing_time(self) -> float:
        return self.endTime - self.start_time

    def get_throughput(self) -> float:
        return self.processedLines / self.get_processing_time()

    def validate_processing(self) -> Tuple[bool, str]:
        difference = self.totalLines - self.insertedDocuments
        if difference > 1340:
            return False, f"Lost too many records: {difference} records lost"
        return True, f"Processing successful. Lost records: {difference}"