"""Main application window with tabbed interface."""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from PySide6.QtCore import QProcess, QSettings, Qt
from PySide6.QtGui import QFont, QTextCursor, QColor, QTextCharFormat, QAction
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMenuBar,
    QPlainTextEdit,
    QPushButton,
    QSplitter,
    QTabWidget,
    QVBoxLayout,
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

        # Splitter: tabs on top, debug console on bottom
        self._splitter = QSplitter(Qt.Orientation.Vertical)
        self._splitter.setChildrenCollapsible(False)
        layout.addWidget(self._splitter)

        self.tabs = QTabWidget()
        self._splitter.addWidget(self.tabs)

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

        # --- Debug console bar (footer) ---
        self._debug_bar = QWidget()
        debug_layout = QVBoxLayout(self._debug_bar)
        debug_layout.setContentsMargins(0, 0, 0, 0)
        debug_layout.setSpacing(0)

        # Toggle header
        self._debug_toggle = QPushButton("▸ Console")
        self._debug_toggle.setFixedHeight(20)
        self._debug_toggle.setStyleSheet(
            "QPushButton { background: #1e1e1e; color: #888; font-size: 10px; "
            "border: none; text-align: left; padding-left: 6px; }"
            "QPushButton:hover { color: #ccc; }"
        )
        self._debug_toggle.clicked.connect(self._toggle_debug_bar)
        debug_layout.addWidget(self._debug_toggle)

        self._debug_console = QPlainTextEdit()
        self._debug_console.setReadOnly(True)
        self._debug_console.setFont(QFont("Consolas", 9))
        self._debug_console.setLineWrapMode(QPlainTextEdit.LineWrapMode.WidgetWidth)
        self._debug_console.setMinimumHeight(150)  # Minimum height to show multiple lines
        # Show vertical scrollbar always (visible)
        self._debug_console.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
        self._debug_console.setStyleSheet(
            "QPlainTextEdit { background: #1e1e1e; color: #00cc66; "
            "border: none; padding: 4px; }"
        )

        # Format for foreground tasks (cyan background)
        self._format_fg = QTextCharFormat()
        self._format_fg.setBackground(QColor(0, 180, 255, 100))  # Cyan with transparency

        # Format for background tasks (brown background)
        self._format_bg = QTextCharFormat()
        self._format_bg.setBackground(QColor(180, 120, 0, 100))  # Brown with transparency

        debug_layout.addWidget(self._debug_console, 1)  # stretch=1, fill available space

        self._splitter.addWidget(self._debug_bar)

        # Start collapsed: only toggle bar visible
        self._debug_expanded = False
        self._debug_console.setVisible(False)
        # Will expand to 250px when toggled on
        self._splitter.setSizes([1, 20])

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
        """Insert progress message at top of debug console (one message per line)."""
        if not text:
            return
        from datetime import datetime
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        # Clean text: replace embedded newlines with spaces to keep message on single line
        clean_text = text.replace("\n", " ").replace("\r", " ").strip()
        if not clean_text:
            return

        # Determine format based on task type
        line = f"[{ts}] {clean_text}\n"
        if "[foreground]" in clean_text:
            fmt = self._format_fg
        elif "[background]" in clean_text:
            fmt = self._format_bg
        else:
            fmt = QTextCharFormat()  # Default format (no background)

        # Check if scrollbar is at top (value near 0 = at top)
        sb = self._debug_console.verticalScrollBar()
        was_at_top = (sb.value() < 5)  # Small margin for floating point

        # Save current scroll position
        old_scroll_value = sb.value()
        old_scroll_max = sb.maximum()

        # Insert at top (beginning) with formatting
        cursor = self._debug_console.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.Start)
        self._debug_console.setTextCursor(cursor)
        cursor.insertText(line, fmt)

        # Keep cursor at top for next insertion
        cursor.movePosition(QTextCursor.MoveOperation.Start)
        self._debug_console.setTextCursor(cursor)

        # Restore scroll position
        if was_at_top:
            # If was at top, keep at top (value = 0)
            sb.setValue(0)
        else:
            # If was scrolled down, maintain reading position by shifting down
            # One new line added at top, so shift view down by line height
            line_height = self._debug_console.fontMetrics().lineSpacing()
            sb.setValue(old_scroll_value + line_height)

        # Update toggle text with latest message (truncate if too long)
        display_text = clean_text[:50] + "..." if len(clean_text) > 50 else clean_text
        self._debug_toggle.setText(f"▾ Console — {display_text}" if self._debug_console.isVisible()
                                   else f"▸ Console — {display_text}")

    def _toggle_debug_bar(self) -> None:
        """Expand/collapse the debug console."""
        self._debug_expanded = not self._debug_expanded
        self._debug_console.setVisible(self._debug_expanded)
        total = sum(self._splitter.sizes())
        if self._debug_expanded:
            # Default to 250px (large footer), at least 150px (console minimum)
            h = self._settings().value("debug/height", 250, type=int)
            h = max(150, min(h, total - 100))
            self._splitter.setSizes([total - h, h])
        else:
            # Collapsed: just show toggle button (20px)
            self._splitter.setSizes([total - 20, 20])
        arrow = "▾" if self._debug_expanded else "▸"
        current = self._debug_toggle.text()
        msg = current.split(" — ", 1)[1] if " — " in current else ""
        self._debug_toggle.setText(f"{arrow} Console — {msg}" if msg else f"{arrow} Console")

    def _reload_app(self) -> None:
        """Save state and restart the application process."""
        self.close()  # triggers closeEvent → saves state
        QProcess.startDetached(sys.executable, sys.argv, os.getcwd())

    # --- State persistence ---

    def _settings(self) -> QSettings:
        return QSettings("CatalogAPIStudio", "CatalogAPIStudio")

    def closeEvent(self, event) -> None:
        """Save window and document state on close."""
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

        # Content filters
        for key, cb in self.preview_view._content_checkboxes.items():
            s.setValue(f"content/{key}", cb.isChecked())

        # Debug bar
        s.setValue("debug/expanded", self._debug_expanded)
        if self._debug_expanded:
            s.setValue("debug/height", self._splitter.sizes()[1])

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

        # Debug bar
        debug_expanded = s.value("debug/expanded")
        if debug_expanded is not None:
            expanded = debug_expanded == "true" or debug_expanded is True
            self._debug_expanded = expanded
            self._debug_console.setVisible(expanded)
            self._debug_toggle.setText("▾ Console" if expanded else "▸ Console")
            if expanded:
                h = s.value("debug/height", 250, type=int)
                total = sum(self._splitter.sizes())
                h = max(150, min(h, total - 100))
                self._splitter.setSizes([total - h, h])

        # Show filters
        for key, act in self.preview_view.bbox_filter_actions.items():
            saved = s.value(f"show/{key}")
            if saved is not None:
                act.setChecked(saved == "true" or saved is True)
        self.preview_view._on_bbox_filter_changed()

        # Content filters
        for key, cb in self.preview_view._content_checkboxes.items():
            saved = s.value(f"content/{key}")
            if saved is not None:
                cb.setChecked(saved == "true" or saved is True)
        self.preview_view._on_content_filter_changed()

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
