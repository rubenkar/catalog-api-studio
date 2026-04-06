"""Main application window with tabbed interface."""

import logging
from datetime import datetime
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from catalog_api_studio.ui.api_view import APIView
from catalog_api_studio.ui.import_view import ImportView
from catalog_api_studio.ui.preview_view import PreviewView
from catalog_api_studio.ui.products_view import ProductsView
from catalog_api_studio.ui.review_view import ReviewView
from catalog_api_studio.ui.search_view import SearchView

logger = logging.getLogger(__name__)

APP_VERSION = "0.1.1"


def _get_build_time() -> str:
    """Get latest modification time across UI source files."""
    try:
        ui_dir = Path(__file__).parent
        mtimes = [f.stat().st_mtime for f in ui_dir.glob("*.py")]
        if mtimes:
            latest = max(mtimes)
            return datetime.fromtimestamp(latest).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return "unknown"


class MainWindow(QMainWindow):
    """Main application window with 6 tabs."""

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Catalog API Studio")
        self.resize(1200, 800)

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(0, 0, 0, 0)

        # Top bar with version
        top_bar = QHBoxLayout()
        top_bar.setContentsMargins(8, 4, 8, 4)
        top_bar.addStretch()
        build_time = _get_build_time()
        version_label = QLabel(f"v{APP_VERSION} | {build_time}")
        version_label.setStyleSheet("color: #888; font-size: 11px;")
        top_bar.addWidget(version_label)
        layout.addLayout(top_bar)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        # Create tabs
        self.import_view = ImportView()
        self.preview_view = PreviewView()
        self.products_view = ProductsView()
        self.review_view = ReviewView()
        self.search_view = SearchView()
        self.api_view = APIView()

        self.tabs.addTab(self.import_view, "Import")
        self.tabs.addTab(self.preview_view, "Preview")
        self.tabs.addTab(self.products_view, "Products")
        self.tabs.addTab(self.review_view, "Review")
        self.tabs.addTab(self.search_view, "Search")
        self.tabs.addTab(self.api_view, "API")

        # Connect import → preview
        self.import_view.preview_requested.connect(self._open_preview)

        # Refresh data when switching tabs
        self.tabs.currentChanged.connect(self._on_tab_changed)

        logger.info("Main window initialized")

    def _on_tab_changed(self, index: int) -> None:
        widget = self.tabs.widget(index)
        if hasattr(widget, "refresh"):
            widget.refresh()

    def _open_preview(self, file_path: str) -> None:
        """Open a document in the Preview tab."""
        from pathlib import Path

        self.preview_view.load_document(Path(file_path))
        self.tabs.setCurrentWidget(self.preview_view)
