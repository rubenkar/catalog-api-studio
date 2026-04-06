"""Hybrid PDF extraction — native first, bitmap fallback for weak pages."""

import logging
from pathlib import Path

from catalog_api_studio.pdf.bitmap import BitmapExtractor
from catalog_api_studio.pdf.native import NativeExtractor

logger = logging.getLogger(__name__)


class HybridExtractor:
    """Try native extraction first, fall back to bitmap for pages with low text."""

    def __init__(self, file_path: Path, char_threshold: int = 50) -> None:
        self.file_path = file_path
        self.char_threshold = char_threshold
        self.native = NativeExtractor(file_path)
        self.bitmap = BitmapExtractor(file_path)

    def extract(self) -> list[dict]:
        """Extract using hybrid strategy."""
        logger.info("Hybrid extraction: %s", self.file_path)

        if self.native.has_text_layer():
            logger.info("PDF has text layer, trying native extraction first")
            native_rows = self.native.extract()

            if native_rows:
                # Check if we got enough meaningful data
                text_rows = [r for r in native_rows if "_raw_text" not in r or len(r.get("_raw_text", "")) > self.char_threshold]
                if len(text_rows) >= len(native_rows) * 0.5:
                    logger.info("Native extraction sufficient: %d rows", len(native_rows))
                    return native_rows

            logger.info("Native extraction insufficient, supplementing with bitmap")
            bitmap_rows = self.bitmap.extract()
            return self._merge(native_rows, bitmap_rows)
        else:
            logger.info("No text layer, using bitmap extraction")
            return self.bitmap.extract()

    @staticmethod
    def _merge(native_rows: list[dict], bitmap_rows: list[dict]) -> list[dict]:
        """Merge native and bitmap results, preferring native for pages that have both."""
        native_pages = {r["_page"] for r in native_rows if "_page" in r}
        bitmap_only = [r for r in bitmap_rows if r.get("_page") not in native_pages]

        merged = native_rows + bitmap_only
        merged.sort(key=lambda r: r.get("_page", 0))

        logger.info(
            "Merged: %d native + %d bitmap-only = %d total",
            len(native_rows),
            len(bitmap_only),
            len(merged),
        )
        return merged
