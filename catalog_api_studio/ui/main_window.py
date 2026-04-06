"""Main application window with tabbed interface."""

import logging

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QMainWindow, QTabWidget, QVBoxLayout, QWidget

from catalog_api_studio.ui.api_view import APIView
from catalog_api_studio.ui.import_view import ImportView
from catalog_api_studio.ui.products_view import ProductsView
from catalog_api_studio.ui.review_view import ReviewView
from catalog_api_studio.ui.search_view import SearchView

logger = logging.getLogger(__name__)


class MainWindow(QMainWindow):
    """Main application window with 5 tabs."""

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Catalog API Studio")
        self.resize(1200, 800)

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(0, 0, 0, 0)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        # Create tabs
        self.import_view = ImportView()
        self.products_view = ProductsView()
        self.review_view = ReviewView()
        self.search_view = SearchView()
        self.api_view = APIView()

        self.tabs.addTab(self.import_view, "Import")
        self.tabs.addTab(self.products_view, "Products")
        self.tabs.addTab(self.review_view, "Review")
        self.tabs.addTab(self.search_view, "Search")
        self.tabs.addTab(self.api_view, "API")

        # Refresh data when switching tabs
        self.tabs.currentChanged.connect(self._on_tab_changed)

        logger.info("Main window initialized")

    def _on_tab_changed(self, index: int) -> None:
        widget = self.tabs.widget(index)
        if hasattr(widget, "refresh"):
            widget.refresh()
