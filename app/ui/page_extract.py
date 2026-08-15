"""Background worker for live single-page extraction (Recognition tab)."""

import json
import logging
from pathlib import Path

from PySide6.QtCore import QThread, Signal

from app.extraction.bearings.cli import DEFAULT_OUT, brand_from_filename, extract_page
from app.extraction.bearings.llm import DeepSeekClient, load_api_key

logger = logging.getLogger(__name__)


class SignalLogHandler(logging.Handler):
    """Forward pipeline log records to a Qt signal (status bars, consoles)."""

    def __init__(self, signal) -> None:
        super().__init__(level=logging.INFO)
        self._signal = signal

    def emit(self, record: logging.LogRecord) -> None:
        self._signal.emit(record.getMessage())


class PageExtractWorker(QThread):
    """Run live extraction of one PDF page in the background.

    ``finished`` emits either an Exception or a tuple
    ``(items, issues, manifest_dict | None)``.
    """

    finished = Signal(object)
    progress = Signal(str)

    def __init__(self, pdf_path: Path, page_no: int) -> None:
        super().__init__()
        self.pdf_path = pdf_path
        self.page_no = page_no

    def run(self) -> None:
        pipeline_logger = logging.getLogger("app.extraction.bearings")
        handler = SignalLogHandler(self.progress)
        pipeline_logger.addHandler(handler)
        try:
            client = DeepSeekClient(
                load_api_key(), DEFAULT_OUT / "cache" / self.pdf_path.stem
            )
            items, issues = extract_page(self.pdf_path, self.page_no, DEFAULT_OUT, client)
            brand = brand_from_filename(self.pdf_path).lower()
            manifest_file = DEFAULT_OUT / f"{brand}.manifest.json"
            manifest = (
                json.loads(manifest_file.read_text(encoding="utf-8"))
                if manifest_file.exists()
                else None
            )
            self.finished.emit((items, issues, manifest))
        except Exception as exc:  # noqa: BLE001 — показываем любую ошибку в UI
            logger.exception("Page extraction failed: %s p.%d", self.pdf_path, self.page_no)
            self.finished.emit(exc)
        finally:
            pipeline_logger.removeHandler(handler)
