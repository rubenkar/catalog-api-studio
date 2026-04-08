"""CSV file importer."""

import logging
from pathlib import Path

import pandas as pd

from app.importers.base import BaseImporter

logger = logging.getLogger(__name__)


class CSVImporter(BaseImporter):
    """Import raw rows from CSV files."""

    def __init__(self, file_path: Path, encoding: str = "utf-8") -> None:
        super().__init__(file_path)
        self.encoding = encoding

    def extract(self) -> list[dict]:
        logger.info("Importing CSV: %s", self.file_path)
        try:
            df = pd.read_csv(self.file_path, encoding=self.encoding)
        except UnicodeDecodeError:
            df = pd.read_csv(self.file_path, encoding="cp1251")

        if df.empty:
            return []

        rows = df.to_dict(orient="records")
        logger.info("Extracted %d rows from CSV", len(rows))
        return rows
