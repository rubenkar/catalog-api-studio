"""Extraction pipeline — raw table rows from PDF/CSV/XLSX."""

import logging

logger = logging.getLogger(__name__)


class ExtractionPipeline:
    """Process raw extracted rows (dicts) from importers."""

    def __init__(self, source: str = "") -> None:
        self.source = source

    def process(self, raw_rows: list[dict]) -> list[dict]:
        """Process and return raw rows as-is."""
        logger.info("Pipeline received %d rows", len(raw_rows))
        return raw_rows
