"""Bitmap fallback PDF extraction — render pages to images, then OCR."""

import logging
from pathlib import Path

import fitz  # PyMuPDF
from PIL import Image

logger = logging.getLogger(__name__)


class BitmapExtractor:
    """Extract text from PDFs by rendering to bitmap and running OCR."""

    def __init__(self, file_path: Path, dpi: int = 200) -> None:
        self.file_path = file_path
        self.dpi = dpi

    def extract(self) -> list[dict]:
        """Render all pages to images, OCR each, return text rows."""
        logger.info("Bitmap extraction: %s", self.file_path)
        all_rows: list[dict] = []

        doc = fitz.open(self.file_path)

        for page_num in range(len(doc)):
            page = doc[page_num]
            pix = page.get_pixmap(dpi=self.dpi)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

            try:
                from catalog_api_studio.ocr.engine import OCREngine

                ocr = OCREngine()
                text_blocks = ocr.extract_text(img)

                # Group blocks into rows by Y-coordinate proximity
                rows = self._group_blocks_into_rows(text_blocks)
                for row in rows:
                    row["_page"] = page_num + 1
                    row["_method"] = "bitmap"
                    all_rows.append(row)

            except ImportError:
                logger.warning("OCR engine not available, extracting raw text only")
                text = page.get_text()
                if text.strip():
                    all_rows.append({
                        "_raw_text": text.strip(),
                        "_page": page_num + 1,
                        "_method": "bitmap_fallback",
                    })

        doc.close()
        logger.info("Bitmap extraction got %d rows", len(all_rows))
        return all_rows

    def render_page(self, page_num: int) -> Image.Image:
        """Render a single page to PIL Image."""
        doc = fitz.open(self.file_path)
        page = doc[page_num]
        pix = page.get_pixmap(dpi=self.dpi)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        doc.close()
        return img

    @staticmethod
    def _group_blocks_into_rows(
        text_blocks: list[dict], y_threshold: float = 10.0
    ) -> list[dict]:
        """Group OCR text blocks into rows based on Y-coordinate proximity."""
        if not text_blocks:
            return []

        sorted_blocks = sorted(text_blocks, key=lambda b: (b["bbox"][1], b["bbox"][0]))

        rows: list[list[dict]] = []
        current_row: list[dict] = [sorted_blocks[0]]
        current_y = sorted_blocks[0]["bbox"][1]

        for block in sorted_blocks[1:]:
            if abs(block["bbox"][1] - current_y) < y_threshold:
                current_row.append(block)
            else:
                rows.append(current_row)
                current_row = [block]
                current_y = block["bbox"][1]
        rows.append(current_row)

        result: list[dict] = []
        for row_blocks in rows:
            row_blocks.sort(key=lambda b: b["bbox"][0])
            cells = [b["text"] for b in row_blocks]
            row_dict = {f"col_{i}": cell for i, cell in enumerate(cells)}
            row_dict["_raw_text"] = " ".join(cells)
            avg_conf = sum(b.get("confidence", 0) for b in row_blocks) / len(row_blocks)
            row_dict["_confidence"] = avg_conf
            result.append(row_dict)

        return result
