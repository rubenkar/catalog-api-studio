"""PDF file importer — delegates to pdf/ pipeline."""

import logging

from app.importers.base import BaseImporter
from app.pdf.hybrid import HybridExtractor

logger = logging.getLogger(__name__)


class PDFImporter(BaseImporter):
    """Import table data from PDF files via hybrid extraction."""

    def extract(self) -> list[dict]:
        logger.info("Importing PDF: %s", self.file_path)
        extractor = HybridExtractor(self.file_path)
        raw_rows = extractor.extract()
        logger.info("Extracted %d raw rows from PDF", len(raw_rows))
        return raw_rows
