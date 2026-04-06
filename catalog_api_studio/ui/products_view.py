"""Products tab — table view of all products."""

import logging

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
from catalog_api_studio.db.models import Product
from catalog_api_studio.services.export_service import ExportService

logger = logging.getLogger(__name__)

COLUMNS = ["ID", "SKU", "Brand", "Name", "Category", "Unit", "Price", "Confidence", "Reviewed"]


class ProductsView(QWidget):
    """Display all products in a table."""

    def __init__(self) -> None:
        super().__init__()
        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        # Top bar
        top_bar = QHBoxLayout()

        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.refresh)
        top_bar.addWidget(self.refresh_btn)

        self.count_label = QLabel("0 products")
        top_bar.addWidget(self.count_label)

        top_bar.addStretch()

        self.export_json_btn = QPushButton("Export JSON")
        self.export_json_btn.clicked.connect(lambda: self._export("json"))
        top_bar.addWidget(self.export_json_btn)

        self.export_csv_btn = QPushButton("Export CSV")
        self.export_csv_btn.clicked.connect(lambda: self._export("csv"))
        top_bar.addWidget(self.export_csv_btn)

        layout.addLayout(top_bar)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(len(COLUMNS))
        self.table.setHorizontalHeaderLabels(COLUMNS)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setSortingEnabled(True)
        layout.addWidget(self.table)

    def refresh(self) -> None:
        session = get_session()
        try:
            products = session.query(Product).order_by(Product.id).all()
            self.count_label.setText(f"{len(products)} products")
            self.table.setRowCount(len(products))

            for row, p in enumerate(products):
                self.table.setItem(row, 0, QTableWidgetItem(str(p.id)))
                self.table.setItem(row, 1, QTableWidgetItem(p.sku or ""))
                self.table.setItem(row, 2, QTableWidgetItem(p.brand or ""))
                self.table.setItem(row, 3, QTableWidgetItem(p.name or ""))
                self.table.setItem(row, 4, QTableWidgetItem(p.category or ""))
                self.table.setItem(row, 5, QTableWidgetItem(p.unit or ""))
                self.table.setItem(row, 6, QTableWidgetItem(
                    f"{p.price:.2f}" if p.price else ""
                ))
                self.table.setItem(row, 7, QTableWidgetItem(f"{p.confidence_score:.0%}"))
                self.table.setItem(row, 8, QTableWidgetItem("Yes" if p.reviewed else "No"))
        finally:
            session.close()

    def _export(self, format: str) -> None:
        if format == "json":
            path, _ = QFileDialog.getSaveFileName(self, "Export JSON", "products.json", "JSON (*.json)")
        else:
            path, _ = QFileDialog.getSaveFileName(self, "Export CSV", "products.csv", "CSV (*.csv)")

        if not path:
            return

        from pathlib import Path

        session = get_session()
        try:
            svc = ExportService(session)
            if format == "json":
                count = svc.export_json(Path(path))
            else:
                count = svc.export_csv(Path(path))
            QMessageBox.information(self, "Export", f"Exported {count} products")
        except Exception as e:
            QMessageBox.critical(self, "Export Error", str(e))
        finally:
            session.close()
