"""XLSX file importer."""

import logging
from pathlib import Path

import pandas as pd

from app.importers.base import BaseImporter

logger = logging.getLogger(__name__)


class XLSXImporter(BaseImporter):
    """Import raw rows from Excel files."""

    def __init__(self, file_path: Path, sheet_name: int | str = 0) -> None:
        super().__init__(file_path)
        self.sheet_name = sheet_name

    def extract(self) -> list[dict]:
        logger.info("Importing XLSX: %s", self.file_path)
        df = pd.read_excel(self.file_path, sheet_name=self.sheet_name, engine="openpyxl")
        if df.empty:
            return []

        rows = df.to_dict(orient="records")
        logger.info("Extracted %d rows from XLSX", len(rows))
        return rows
