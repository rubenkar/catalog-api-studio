"""Main application window with tabbed interface."""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from PySide6.QtCore import QProcess, QSettings, Qt
from PySide6.QtGui import QAction
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMenuBar,
    QPushButton,
    QTabWidget,
    QVBoxLayout,
    QMessageBox,
    QWidget,
)

from app.ui.api_view import APIView
from app.ui.import_view import ImportView
from app.ui.preview_view import PreviewView
from app.ui.review_view import ReviewView
from app.ui.search_view import SearchView

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

        # Top bar with menus and version
        top_bar = QHBoxLayout()
        top_bar.setContentsMargins(8, 4, 8, 4)

        # Menu bar (embedded in top bar, not native)
        menu_bar = QMenuBar()
        menu_bar.setStyleSheet("QMenuBar { background: transparent; }")
        self._build_view_menu(menu_bar)
        top_bar.addWidget(menu_bar)

        self.progress_label = QLabel("")
        self.progress_label.setStyleSheet("color: #e8a020; font-size: 11px; font-weight: bold;")
        self.progress_label.setMinimumWidth(160)
        top_bar.addWidget(self.progress_label)

        top_bar.addStretch()
        build_time = _get_build_time()
        version_label = QLabel(f"v{APP_VERSION} | {build_time}")
        version_label.setStyleSheet("color: #888; font-size: 11px;")
        top_bar.addWidget(version_label)

        reload_btn = QPushButton("Reload")
        reload_btn.setFixedWidth(60)
        reload_btn.setStyleSheet("font-size: 11px;")
        reload_btn.clicked.connect(self._reload_app)
        top_bar.addWidget(reload_btn)

        layout.addLayout(top_bar)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        # Create tabs
        self.import_view = ImportView()
        self.preview_view = PreviewView()
        self.review_view = ReviewView()
        self.search_view = SearchView()
        self.api_view = APIView()

        self.tabs.addTab(self.import_view, "Import")
        self.tabs.addTab(self.preview_view, "Preview")
        self.tabs.addTab(self.review_view, "Review")
        self.tabs.addTab(self.search_view, "Search")
        self.tabs.addTab(self.api_view, "API")

        # Connect import → preview
        self.import_view.preview_requested.connect(self._open_preview)

        # Connect progress
        self.preview_view.progress.connect(self._on_progress)

        # Refresh data when switching tabs
        self.tabs.currentChanged.connect(self._on_tab_changed)

        # Restore saved state
        self._restore_state()

        logger.info("Main window initialized")

    def _build_view_menu(self, menu_bar: QMenuBar) -> None:
        """Build View menu with Show section."""
        view_menu = menu_bar.addMenu("View")

        # --- Show section ---
        show_label = view_menu.addAction("Show")
        show_label.setEnabled(False)
        view_menu.addSeparator()

        self.show_hidden_action = QAction("Show hidden", self)
        self.show_hidden_action.setCheckable(True)
        self.show_hidden_action.setChecked(False)
        self.show_hidden_action.triggered.connect(self._toggle_show_hidden)
        view_menu.addAction(self.show_hidden_action)

        self.show_template_action = QAction("Show template", self)
        self.show_template_action.setCheckable(True)
        self.show_template_action.setChecked(False)
        self.show_template_action.triggered.connect(self._toggle_show_template)
        view_menu.addAction(self.show_template_action)

    def _toggle_show_hidden(self, checked: bool) -> None:
        """Toggle visibility of hidden objects in preview."""
        self.preview_view.set_show_hidden(checked)

    def _toggle_show_template(self, checked: bool) -> None:
        """Toggle template overlay (placeholder for future use)."""
        pass

    def _on_tab_changed(self, index: int) -> None:
        widget = self.tabs.widget(index)
        if hasattr(widget, "refresh"):
            widget.refresh()

    def _open_preview(self, file_path: str) -> None:
        """Open a document in the Preview tab."""
        from pathlib import Path

        self.preview_view.load_document(Path(file_path))
        self.tabs.setCurrentWidget(self.preview_view)

    def _on_progress(self, text: str) -> None:
        """Update progress label from preview operations."""
        self.progress_label.setText(text)
        self.progress_label.repaint()

    def _reload_app(self) -> None:
        """Save state and restart the application process."""
        self.close()  # triggers closeEvent → saves state
        QProcess.startDetached(sys.executable, sys.argv, os.getcwd())

    # --- State persistence ---

    def _settings(self) -> QSettings:
        return QSettings("CatalogAPIStudio", "CatalogAPIStudio")

    def closeEvent(self, event) -> None:
        """Ask for confirmation, then save window and document state on close."""
        reply = QMessageBox.question(
            self,
            "Выход",
            "Вы уверены, что хотите закрыть приложение?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            event.ignore()
            return
        s = self._settings()

        # Window geometry
        s.setValue("window/geometry", self.saveGeometry())
        s.setValue("window/state", self.saveState())
        s.setValue("window/tab", self.tabs.currentIndex())

        # Preview state
        if self.preview_view._file_path:
            s.setValue("preview/file", str(self.preview_view._file_path))
            s.setValue("preview/page", self.preview_view.page_spin.value())
            s.setValue("preview/zoom", self.preview_view._zoom)

        # Show filters (bbox, table, text, photo, picture, drawing)
        for key, act in self.preview_view.bbox_filter_actions.items():
            s.setValue(f"show/{key}", act.isChecked())

        # View menu toggles
        s.setValue("show/hidden", self.show_hidden_action.isChecked())
        s.setValue("show/template", self.show_template_action.isChecked())

        s.sync()
        super().closeEvent(event)

    def _restore_state(self) -> None:
        """Restore window and document state from previous session."""
        s = self._settings()

        # Window geometry
        geom = s.value("window/geometry")
        if geom:
            self.restoreGeometry(geom)
        state = s.value("window/state")
        if state:
            self.restoreState(state)
        tab_idx = s.value("window/tab", 0, type=int)
        self.tabs.setCurrentIndex(tab_idx)

        # Show filters
        for key, act in self.preview_view.bbox_filter_actions.items():
            saved = s.value(f"show/{key}")
            if saved is not None:
                act.setChecked(saved == "true" or saved is True)
        self.preview_view._on_bbox_filter_changed()

        # View menu toggles
        show_hidden = s.value("show/hidden")
        if show_hidden is not None:
            checked = show_hidden == "true" or show_hidden is True
            self.show_hidden_action.setChecked(checked)
            self.preview_view.set_show_hidden(checked)

        show_template = s.value("show/template")
        if show_template is not None:
            self.show_template_action.setChecked(
                show_template == "true" or show_template is True
            )

        # Preview state
        file_path = s.value("preview/file", "")
        if file_path and Path(file_path).exists():
            zoom = s.value("preview/zoom", 1.0, type=float)
            page = s.value("preview/page", 1, type=int)
            self.preview_view.load_document(Path(file_path))
            self.preview_view._set_zoom(zoom)
            self.preview_view.page_spin.setValue(page)
            self.preview_view._scroll_to_page(page)
