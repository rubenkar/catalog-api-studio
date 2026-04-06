"""Search tab — full-text search with filters."""

import logging

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from catalog_api_studio.db.engine import get_session
from catalog_api_studio.search.indexer import SearchIndexer
from catalog_api_studio.services.product_service import ProductService

logger = logging.getLogger(__name__)


class SearchView(QWidget):
    """Search interface with text search and facet filters."""

    def __init__(self) -> None:
        super().__init__()
        self._indexer: SearchIndexer | None = None
        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        # Search bar
        search_bar = QHBoxLayout()

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search by SKU, name, brand...")
        self.search_input.setMinimumHeight(36)
        self.search_input.returnPressed.connect(self._on_search)
        search_bar.addWidget(self.search_input)

        self.search_btn = QPushButton("Search")
        self.search_btn.setMinimumHeight(36)
        self.search_btn.clicked.connect(self._on_search)
        search_bar.addWidget(self.search_btn)

        layout.addLayout(search_bar)

        # Filters
        filter_bar = QHBoxLayout()

        filter_bar.addWidget(QLabel("Brand:"))
        self.brand_combo = QComboBox()
        self.brand_combo.setMinimumWidth(150)
        self.brand_combo.addItem("All")
        filter_bar.addWidget(self.brand_combo)

        filter_bar.addWidget(QLabel("Category:"))
        self.category_combo = QComboBox()
        self.category_combo.setMinimumWidth(150)
        self.category_combo.addItem("All")
        filter_bar.addWidget(self.category_combo)

        filter_bar.addStretch()

        self.result_label = QLabel("")
        filter_bar.addWidget(self.result_label)

        layout.addLayout(filter_bar)

        # Results table
        columns = ["ID", "SKU", "Brand", "Name", "Category", "Unit", "Price", "Confidence"]
        self.table = QTableWidget()
        self.table.setColumnCount(len(columns))
        self.table.setHorizontalHeaderLabels(columns)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        layout.addWidget(self.table)

    def refresh(self) -> None:
        """Reload filter options."""
        session = get_session()
        try:
            svc = ProductService(session)

            self.brand_combo.clear()
            self.brand_combo.addItem("All")
            for brand in svc.get_brands():
                self.brand_combo.addItem(brand)

            self.category_combo.clear()
            self.category_combo.addItem("All")
            for cat in svc.get_categories():
                self.category_combo.addItem(cat)
        finally:
            session.close()

    def _on_search(self) -> None:
        query = self.search_input.text().strip()
        brand = self.brand_combo.currentText()
        category = self.category_combo.currentText()

        if brand == "All":
            brand = None
        if category == "All":
            category = None

        if not self._indexer:
            self._indexer = SearchIndexer()

        session = get_session()
        try:
            products, total = self._indexer.search(
                query=query, session=session, brand=brand, category=category
            )
            self.result_label.setText(f"{total} results")
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
        finally:
            session.close()
