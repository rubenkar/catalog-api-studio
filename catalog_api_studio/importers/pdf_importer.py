"""PDF file importer — delegates to pdf/ pipeline."""

import logging
from pathlib import Path

from catalog_api_studio.importers.base import BaseImporter
from catalog_api_studio.models.schemas import ProductCreate
from catalog_api_studio.pdf.hybrid import HybridExtractor

logger = logging.getLogger(__name__)


class PDFImporter(BaseImporter):
    """Import products from PDF files via hybrid extraction pipeline."""

    def extract(self) -> list[ProductCreate]:
        logger.info("Importing PDF: %s", self.file_path)

        extractor = HybridExtractor(self.file_path)
        raw_rows = extractor.extract()

        logger.info("Extracted %d raw rows from PDF", len(raw_rows))

        from catalog_api_studio.extraction.pipeline import ExtractionPipeline

        pipeline = ExtractionPipeline(source=self.file_path.name)
        products = pipeline.process(raw_rows)

        logger.info("Produced %d products from PDF", len(products))
        return products
