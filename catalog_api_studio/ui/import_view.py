"""Import tab — file upload and import job management."""

import logging
from pathlib import Path

from PySide6.QtCore import QThread, Signal
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from catalog_api_studio.db.engine import get_session
from catalog_api_studio.db.models import ImportJob
from catalog_api_studio.services.import_service import ImportService

logger = logging.getLogger(__name__)


class ImportWorker(QThread):
    """Background worker for file import."""

    finished = Signal(object)  # ImportJob or Exception
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
    """File import tab with drag-drop and import history."""

    preview_requested = Signal(str)  # file_path

    def __init__(self) -> None:
        super().__init__()
        self._worker: ImportWorker | None = None
        self._jobs: list[ImportJob] = []
        self._setup_ui()
        self.refresh()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        # Top bar
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

        # Supported formats hint
        hint = QLabel("Supported formats: PDF, XLSX, CSV  |  Select a PDF row and click Preview")
        hint.setStyleSheet("color: #666; font-size: 11px;")
        layout.addWidget(hint)

        # Import jobs table
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(
            ["ID", "Filename", "Type", "Status", "Products", "Created"]
        )
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.currentCellChanged.connect(lambda row, *_: self._on_row_changed(row))
        self.table.doubleClicked.connect(self._on_double_click)
        layout.addWidget(self.table)

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
            self.status_label.setText(
                f"Done: {job.filename} — {job.products_count} products"
            )

        self.refresh()

    def _on_row_changed(self, row: int) -> None:
        """Enable Preview button for PDF files."""
        if 0 <= row < len(self._jobs):
            job = self._jobs[row]
            is_pdf = job.file_type.lower() == "pdf"
            self.preview_btn.setEnabled(is_pdf)
        else:
            self.preview_btn.setEnabled(False)

    def _on_preview(self) -> None:
        """Open selected PDF in preview tab."""
        row = self.table.currentRow()
        if 0 <= row < len(self._jobs):
            job = self._jobs[row]
            if job.file_type.lower() == "pdf" and Path(job.file_path).exists():
                self.preview_requested.emit(job.file_path)

    def _on_double_click(self, index) -> None:
        """Double-click on PDF row opens preview."""
        row = index.row()
        if 0 <= row < len(self._jobs):
            job = self._jobs[row]
            if job.file_type.lower() == "pdf" and Path(job.file_path).exists():
                self.preview_requested.emit(job.file_path)

    def refresh(self) -> None:
        """Reload import jobs from database."""
        session = get_session()
        try:
            self._jobs = session.query(ImportJob).order_by(ImportJob.created_at.desc()).all()
            self.table.setRowCount(len(self._jobs))

            for row, job in enumerate(self._jobs):
                self.table.setItem(row, 0, QTableWidgetItem(str(job.id)))
                self.table.setItem(row, 1, QTableWidgetItem(job.filename))
                self.table.setItem(row, 2, QTableWidgetItem(job.file_type.upper()))

                status_item = QTableWidgetItem(job.status)
                color_map = {
                    "pending": "#888",
                    "processing": "#f0ad4e",
                    "done": "#5cb85c",
                    "error": "#d9534f",
                }
                color = color_map.get(job.status, "#333")
                status_item.setForeground(QColor(color))
                self.table.setItem(row, 3, status_item)

                self.table.setItem(row, 4, QTableWidgetItem(str(job.products_count)))
                self.table.setItem(
                    row, 5, QTableWidgetItem(job.created_at.strftime("%Y-%m-%d %H:%M"))
                )
        finally:
            session.close()
