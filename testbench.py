"""Standalone Bbox Detection Testbench.

Opens the BboxDetectionDialog for a given PDF document and page.
Shares all detection methods, functions, and libraries with the main app.

Usage:
    python testbench.py <pdf_path> [page_number]

Examples:
    python testbench.py data/catalogs/FBJ.pdf
    python testbench.py data/catalogs/FBJ.pdf 4
    python testbench.py "C:/path/to/catalog.pdf" 1

Arguments:
    pdf_path      Path to PDF file
    page_number   Page number (1-based, default: 1)
"""

import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    pdf_path = Path(sys.argv[1])
    page_num = int(sys.argv[2]) if len(sys.argv) > 2 else 1

    if not pdf_path.exists():
        logger.error("File not found: %s", pdf_path)
        sys.exit(1)

    if not pdf_path.suffix.lower() == ".pdf":
        logger.error("Not a PDF file: %s", pdf_path)
        sys.exit(1)

    # Open PDF document
    import fitz

    doc = fitz.open(str(pdf_path))
    total_pages = len(doc)

    if page_num < 1 or page_num > total_pages:
        logger.error(
            "Page %d out of range. Document has %d pages.", page_num, total_pages
        )
        doc.close()
        sys.exit(1)

    page_idx = page_num - 1  # Convert 1-based to 0-based

    logger.info(
        "Opening testbench: %s — page %d/%d", pdf_path.name, page_num, total_pages
    )

    # Launch Qt app
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication(sys.argv)

    from app.ui.preview_view import BboxDetectionDialog

    dialog = BboxDetectionDialog(doc, page_idx)
    dialog.setWindowTitle(f"Bbox Testbench — {pdf_path.name} — Page {page_num}/{total_pages}")
    dialog.resize(1100, 850)
    dialog.show()

    exit_code = app.exec()

    doc.close()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
