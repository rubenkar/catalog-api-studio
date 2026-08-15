"""Import tab — file upload and import job management (grid view)."""

import logging
from datetime import datetime
from pathlib import Path

from PySide6.QtCore import QSize, Qt, QThread, Signal
from PySide6.QtGui import QColor, QIcon, QImage, QPainter, QPen, QPixmap
from PySide6.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QListView,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from app.db.engine import get_session
from app.db.models import ImportJob
from app.services.import_service import ImportService

logger = logging.getLogger(__name__)

THUMB_W = 160
THUMB_H = 220
CARD_W = 200
CARD_H = 340


def _format_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def _placeholder_pixmap(label: str) -> QPixmap:
    """Generate a simple placeholder pixmap for non-PDF files."""
    pm = QPixmap(THUMB_W, THUMB_H)
    pm.fill(QColor("#2b2b2b"))
    painter = QPainter(pm)
    painter.setPen(QPen(QColor("#555")))
    painter.drawRect(0, 0, THUMB_W - 1, THUMB_H - 1)
    painter.setPen(QPen(QColor("#ccc")))
    font = painter.font()
    font.setPointSize(28)
    font.setBold(True)
    painter.setFont(font)
    painter.drawText(pm.rect(), Qt.AlignmentFlag.AlignCenter, label.upper())
    painter.end()
    return pm


def _render_pdf_thumb(file_path: Path) -> QPixmap | None:
    """Render first page of a PDF to a QPixmap (thumbnail size)."""
    try:
        import fitz  # PyMuPDF
    except ImportError:
        return None
    try:
        doc = fitz.open(file_path)
        if len(doc) == 0:
            doc.close()
            return None
        page = doc[0]
        # Compute zoom to fit THUMB_W/THUMB_H
        rect = page.rect
        zoom = min(THUMB_W / rect.width, THUMB_H / rect.height)
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        img = QImage(pix.samples, pix.width, pix.height, pix.stride, QImage.Format.Format_RGB888)
        pm = QPixmap.fromImage(img.copy())
        page_count = len(doc)
        doc.close()
        # Draw a 1-px border
        bordered = QPixmap(pm.size())
        bordered.fill(Qt.GlobalColor.transparent)
        painter = QPainter(bordered)
        painter.drawPixmap(0, 0, pm)
        painter.setPen(QPen(QColor("#555")))
        painter.drawRect(0, 0, pm.width() - 1, pm.height() - 1)
        painter.end()
        bordered.setDevicePixelRatio(1.0)
        # Attach page count via property-like dict — return tuple
        return bordered, page_count
    except Exception as e:
        logger.warning("Thumbnail render failed for %s: %s", file_path, e)
        return None


class ThumbWorker(QThread):
    """Renders thumbnails in the background."""

    ready = Signal(int, QPixmap, int)  # job_id, pixmap, page_count

    def __init__(self, tasks: list[tuple[int, Path]]) -> None:
        super().__init__()
        self._tasks = tasks

    def run(self) -> None:
        for job_id, path in self._tasks:
            if not path.exists() or path.suffix.lower() != ".pdf":
                continue
            result = _render_pdf_thumb(path)
            if result is None:
                continue
            pm, page_count = result
            self.ready.emit(job_id, pm, page_count)


class ImportWorker(QThread):
    finished = Signal(object)
    progress = Signal(str)

    def __init__(self, file_path: Path) -> None:
        super().__init__()
        self.file_path = file_path

    def run(self) -> None:
        try:
            self.progress.emit(f"Importing {self.file_path.name}...")
            service = ImportService()
            job = service.import_file(self.file_path)
            self.finished.emit(job)
        except Exception as e:
            logger.error("Import failed: %s", e)
            self.finished.emit(e)


class ImportView(QWidget):
    """File import tab — grid view with PDF thumbnails."""

    preview_requested = Signal(str)  # file_path

    def __init__(self) -> None:
        super().__init__()
        self._worker: ImportWorker | None = None
        self._thumb_worker: ThumbWorker | None = None
        self._jobs: list[ImportJob] = []
        self._thumb_cache: dict[str, tuple[QPixmap, int]] = {}  # key: path::mtime
        self._setup_ui()
        self.refresh()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        top_bar = QHBoxLayout()
        self.import_btn = QPushButton("Import File")
        self.import_btn.setMinimumHeight(40)
        self.import_btn.clicked.connect(self._on_import)
        top_bar.addWidget(self.import_btn)

        self.preview_btn = QPushButton("Preview")
        self.preview_btn.setMinimumHeight(40)
        self.preview_btn.setEnabled(False)
        self.preview_btn.clicked.connect(self._on_preview)
        top_bar.addWidget(self.preview_btn)

        self.status_label = QLabel("Ready")
        top_bar.addWidget(self.status_label)
        top_bar.addStretch()

        layout.addLayout(top_bar)

        hint = QLabel("Supported formats: PDF, XLSX, CSV  |  Double-click a PDF to preview")
        hint.setStyleSheet("color: #666; font-size: 11px;")
        layout.addWidget(hint)

        # Grid (icon-mode list)
        self.grid = QListWidget()
        self.grid.setViewMode(QListView.ViewMode.IconMode)
        self.grid.setResizeMode(QListView.ResizeMode.Adjust)
        self.grid.setMovement(QListView.Movement.Static)
        self.grid.setIconSize(QSize(THUMB_W, THUMB_H))
        self.grid.setGridSize(QSize(CARD_W, CARD_H))
        self.grid.setSpacing(12)
        self.grid.setUniformItemSizes(True)
        self.grid.setWordWrap(True)
        self.grid.setStyleSheet(
            "QListWidget { background: #1a1a1a; }"
            "QListWidget::item { color: #ddd; border: 1px solid transparent; padding: 6px; }"
            "QListWidget::item:selected { border: 1px solid #4a90e2; background: #2a3a4a; }"
        )
        self.grid.currentItemChanged.connect(self._on_selection_changed)
        self.grid.itemDoubleClicked.connect(self._on_double_click)
        layout.addWidget(self.grid)

    # --- Actions ---

    def _on_import(self) -> None:
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Catalog File",
            "",
            "All Supported (*.pdf *.xlsx *.csv);;PDF (*.pdf);;Excel (*.xlsx);;CSV (*.csv)",
        )
        if not file_path:
            return

        self.import_btn.setEnabled(False)
        self.status_label.setText(f"Importing {Path(file_path).name}...")

        self._worker = ImportWorker(Path(file_path))
        self._worker.finished.connect(self._on_import_finished)
        self._worker.progress.connect(lambda msg: self.status_label.setText(msg))
        self._worker.start()

    def _on_import_finished(self, result: object) -> None:
        self.import_btn.setEnabled(True)
        if isinstance(result, Exception):
            self.status_label.setText("Import failed")
            QMessageBox.critical(self, "Import Error", str(result))
        else:
            job = result
            self.status_label.setText(f"Done: {job.filename}")
        self.refresh()

    def _current_job(self) -> ImportJob | None:
        item = self.grid.currentItem()
        if item is None:
            return None
        job_id = item.data(Qt.ItemDataRole.UserRole)
        for job in self._jobs:
            if job.id == job_id:
                return job
        return None

    def _on_selection_changed(self, *_args) -> None:
        job = self._current_job()
        self.preview_btn.setEnabled(bool(job and job.file_type.lower() == "pdf"))

    def _on_double_click(self, item: QListWidgetItem) -> None:
        job_id = item.data(Qt.ItemDataRole.UserRole)
        for job in self._jobs:
            if job.id == job_id:
                if job.file_type.lower() == "pdf" and Path(job.file_path).exists():
                    self.preview_requested.emit(job.file_path)
                return

    def _on_preview(self) -> None:
        job = self._current_job()
        if job and job.file_type.lower() == "pdf" and Path(job.file_path).exists():
            self.preview_requested.emit(job.file_path)

    # --- Data ---

    def refresh(self) -> None:
        """Reload import jobs and rebuild the grid."""
        session = get_session()
        try:
            self._jobs = session.query(ImportJob).order_by(ImportJob.created_at.desc()).all()
        finally:
            session.close()

        self.grid.clear()
        pending_thumbs: list[tuple[int, Path]] = []

        for job in self._jobs:
            path = Path(job.file_path)
            exists = path.exists()

            # File metadata
            size_str = "—"
            last_access_str = "—"
            page_count: int | None = None
            if exists:
                try:
                    st = path.stat()
                    size_str = _format_size(st.st_size)
                    last_access_str = datetime.fromtimestamp(st.st_atime).strftime(
                        "%Y-%m-%d %H:%M"
                    )
                except OSError:
                    pass

            # Thumbnail / cache
            file_type = job.file_type.lower()
            pm: QPixmap | None = None
            cache_key = None
            if exists:
                try:
                    cache_key = f"{path}::{path.stat().st_mtime}"
                except OSError:
                    cache_key = None
            if cache_key and cache_key in self._thumb_cache:
                pm, page_count = self._thumb_cache[cache_key]

            if pm is None:
                pm = _placeholder_pixmap(file_type or "?")
                if exists and file_type == "pdf":
                    pending_thumbs.append((job.id, path))

            # Label lines
            name = job.filename
            if len(name) > 28:
                name = name[:26] + "…"
            pages_part = f"{page_count} pages" if page_count else "— pages"
            label = (
                f"{name}\n"
                f"{size_str}  ·  {pages_part}\n"
                f"Accessed: {last_access_str}"
            )

            item = QListWidgetItem(QIcon(pm), label)
            item.setData(Qt.ItemDataRole.UserRole, job.id)
            item.setSizeHint(QSize(CARD_W, CARD_H))
            item.setTextAlignment(Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop)
            if not exists:
                item.setForeground(QColor("#888"))
                item.setToolTip(f"File missing:\n{path}")
            else:
                item.setToolTip(str(path))
            self.grid.addItem(item)

        # Kick off background thumbnail rendering for PDFs
        if pending_thumbs:
            if self._thumb_worker and self._thumb_worker.isRunning():
                self._thumb_worker.requestInterruption()
            self._thumb_worker = ThumbWorker(pending_thumbs)
            self._thumb_worker.ready.connect(self._on_thumb_ready)
            self._thumb_worker.start()

    def _on_thumb_ready(self, job_id: int, pm: QPixmap, page_count: int) -> None:
        """Update grid item thumbnail + page count once rendered."""
        # Cache by path+mtime
        for job in self._jobs:
            if job.id == job_id:
                path = Path(job.file_path)
                try:
                    key = f"{path}::{path.stat().st_mtime}"
                    self._thumb_cache[key] = (pm, page_count)
                except OSError:
                    pass
                break

        for i in range(self.grid.count()):
            item = self.grid.item(i)
            if item.data(Qt.ItemDataRole.UserRole) != job_id:
                continue
            item.setIcon(QIcon(pm))
            # Patch page count into the existing label
            lines = item.text().split("\n")
            if len(lines) >= 2:
                parts = lines[1].split("·")
                if len(parts) == 2:
                    lines[1] = f"{parts[0].strip()}  ·  {page_count} pages"
                    item.setText("\n".join(lines))
            break
