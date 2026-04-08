"""Native PDF extraction using pdfplumber (text-layer PDFs)."""

import logging
from pathlib import Path

import pdfplumber

logger = logging.getLogger(__name__)


class NativeExtractor:
    """Extract text and tables from PDFs with native text layers."""

    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path

    def extract(self) -> list[dict]:
        """Extract all tables from PDF, returning rows as dicts."""
        logger.info("Native extraction: %s", self.file_path)
        all_rows: list[dict] = []

        with pdfplumber.open(self.file_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                tables = page.extract_tables()
                for table in tables:
                    if not table or len(table) < 2:
                        continue

                    headers = [str(h).strip() if h else f"col_{i}" for i, h in enumerate(table[0])]
                    for row in table[1:]:
                        if not any(cell for cell in row):
                            continue
                        row_dict = {
                            headers[i]: str(cell).strip() if cell else ""
                            for i, cell in enumerate(row)
                            if i < len(headers)
                        }
                        row_dict["_page"] = page_num
                        row_dict["_method"] = "native"
                        all_rows.append(row_dict)

                # If no tables found, try extracting text blocks
                if not tables:
                    text = page.extract_text()
                    if text and len(text.strip()) > 20:
                        all_rows.append({
                            "_raw_text": text.strip(),
                            "_page": page_num,
                            "_method": "native_text",
                        })

        logger.info("Native extraction got %d rows", len(all_rows))
        return all_rows

    def has_text_layer(self) -> bool:
        """Check if PDF has a usable native text layer."""
        try:
            with pdfplumber.open(self.file_path) as pdf:
                if not pdf.pages:
                    return False
                sample_pages = pdf.pages[:3]
                total_chars = sum(len(p.extract_text() or "") for p in sample_pages)
                return total_chars > 50 * len(sample_pages)
        except Exception:
            return False
