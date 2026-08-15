"""Main application window with tabbed interface."""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from PySide6.QtCore import QByteArray, QProcess, QSettings, QSize, Qt, QThread, Signal
from PySide6.QtGui import QFont, QGuiApplication, QIcon, QImage, QPainter, QPixmap, QTextCursor, QColor, QTextCharFormat, QAction
from PySide6.QtSvg import QSvgRenderer
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMenuBar,
    QPlainTextEdit,
    QPushButton,
    QScrollBar,
    QSlider,
    QSplitter,
    QTabWidget,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from app.ui.api_view import APIView
from app.ui.import_view import ImportView
from app.ui.layers_view import LayersView
from app.ui.preview_view import LayoutView, PreviewView, RecognitionView
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


# ---------------------------------------------------------------------------
# Right-sidebar SVG icons (outline-only, #5A5A5A stroke, 24x24 viewBox)
# ---------------------------------------------------------------------------

_SB_STROKE = "#5A5A5A"


def _sb_svg(body: str, sw: float = 1.5) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" '
        f'fill="none" stroke="{_SB_STROKE}" stroke-width="{sw}" '
        f'stroke-linecap="round" stroke-linejoin="round">{body}</svg>'
    )


SVG_CHAT = _sb_svg(
    '<path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>'
)
SVG_BOOKMARK = _sb_svg(
    '<path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/>'
)
SVG_COPY = _sb_svg(
    '<rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>'
    '<path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>'
)
SVG_CHEVRON_UP = _sb_svg('<polyline points="18 15 12 9 6 15"/>', sw=2)
SVG_CHEVRON_DOWN = _sb_svg('<polyline points="6 9 12 15 18 9"/>', sw=2)
SVG_REFRESH = _sb_svg(
    # rotate-cw (Feather) — single curved arrow, rotation-clockwise semantic
    '<polyline points="23 4 23 10 17 10"/>'
    '<path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>'
)
SVG_PAGE_FIT = _sb_svg(
    '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>'
    '<polyline points="14 2 14 8 20 8"/>'
)
SVG_ZOOM_IN = _sb_svg(
    '<circle cx="11" cy="11" r="8"/>'
    '<line x1="21" y1="21" x2="16.65" y2="16.65"/>'
    '<line x1="11" y1="8" x2="11" y2="14"/>'
    '<line x1="8" y1="11" x2="14" y2="11"/>'
)
SVG_ZOOM_OUT = _sb_svg(
    '<circle cx="11" cy="11" r="8"/>'
    '<line x1="21" y1="21" x2="16.65" y2="16.65"/>'
    '<line x1="8" y1="11" x2="14" y2="11"/>'
)


SIDEBAR_QSS = """
#AcrobatSidebar {
    background: #F5F5F5;
    border: none;
    border-left: 1px solid #E0E0E0;
}
#AcrobatSidebar QToolButton {
    background: transparent;
    border: none;
    border-radius: 4px;
    min-width: 44px;
    max-width: 44px;
    min-height: 44px;
    max-height: 44px;
    padding: 0;
}
#AcrobatSidebar QToolButton:hover {
    background: #E8E8E8;
}
#AcrobatSidebar QToolButton:pressed {
    background: #DEDEDE;
}
#AcrobatSidebar QToolButton:disabled {
    background: transparent;
}
#AcrobatSidebar QToolButton::menu-indicator { image: none; width: 0; }
#AcrobatSidebar QToolButton#PageLabel {
    color: #4A4A4A;
    font-size: 11px;
    min-height: 22px;
    max-height: 22px;
}
"""


def _sb_svg_icon(svg_str: str, size: int = 20) -> QIcon:
    """Rasterize an SVG string into a crisp QIcon honoring the screen's DPR."""
    app = QGuiApplication.instance()
    screen = app.primaryScreen() if app is not None else None
    dpr = screen.devicePixelRatio() if screen else 1.0
    physical = max(1, int(round(size * dpr)))
    pm = QPixmap(physical, physical)
    pm.setDevicePixelRatio(dpr)
    pm.fill(Qt.GlobalColor.transparent)
    renderer = QSvgRenderer(QByteArray(svg_str.encode("utf-8")))
    painter = QPainter(pm)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
    painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)
    renderer.render(painter)
    painter.end()
    return QIcon(pm)


class ThumbnailRenderWorker(QThread):
    """Renders PDF-page thumbnails in the background."""

    ready = Signal(int, QImage)  # page_idx, image

    def __init__(self, file_path: str, page_count: int, thumb_w: int = 180, parent=None):
        super().__init__(parent)
        self._file_path = file_path
        self._page_count = page_count
        self._thumb_w = thumb_w
        self._cancel = False

    def cancel(self) -> None:
        self._cancel = True

    def run(self) -> None:
        try:
            import fitz  # PyMuPDF
        except ImportError:
            return
        try:
            doc = fitz.open(self._file_path)
        except Exception:
            return
        for i in range(min(self._page_count, len(doc))):
            if self._cancel:
                break
            try:
                page = doc[i]
                zoom = self._thumb_w / max(1.0, page.rect.width)
                pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
                img = QImage(pix.samples, pix.width, pix.height, pix.stride,
                             QImage.Format.Format_RGB888).copy()
                self.ready.emit(i, img)
            except Exception:
                continue
        doc.close()


class ThumbnailItem(QWidget):
    """One thumbnail entry: image + page number label, clickable with selection state."""

    clicked = Signal()

    def __init__(self, page_idx: int, target_w: int = 180, target_h: int = 240, parent=None):
        super().__init__(parent)
        self._page_idx = page_idx
        self._selected = False
        v = QVBoxLayout(self)
        v.setContentsMargins(4, 4, 4, 4)
        v.setSpacing(6)

        self.thumb_lbl = QLabel()
        self.thumb_lbl.setFixedSize(target_w, target_h)
        self.thumb_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.thumb_lbl.setStyleSheet(
            "background: #fafafa; border: 1px solid #d5d5d5; border-radius: 2px;"
        )
        v.addWidget(self.thumb_lbl, alignment=Qt.AlignmentFlag.AlignHCenter)

        self.num_lbl = QLabel(str(page_idx + 1))
        self.num_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.num_lbl.setStyleSheet("color: #3a3a3a; font-size: 12px;")
        v.addWidget(self.num_lbl)

        self.setCursor(Qt.CursorShape.PointingHandCursor)

    def set_image(self, image: QImage) -> None:
        pm = QPixmap.fromImage(image).scaled(
            self.thumb_lbl.width() - 4, self.thumb_lbl.height() - 4,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        self.thumb_lbl.setPixmap(pm)

    def set_selected(self, selected: bool) -> None:
        self._selected = selected
        if selected:
            self.thumb_lbl.setStyleSheet(
                "background: #fafafa; border: 2px solid #2B7FD9; border-radius: 2px;"
            )
            self.num_lbl.setStyleSheet("color: #2B7FD9; font-weight: bold; font-size: 12px;")
        else:
            self.thumb_lbl.setStyleSheet(
                "background: #fafafa; border: 1px solid #d5d5d5; border-radius: 2px;"
            )
            self.num_lbl.setStyleSheet("color: #3a3a3a; font-size: 12px;")

    def mousePressEvent(self, event) -> None:
        if event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit()
        super().mousePressEvent(event)


class PageThumbnailsPanel(QWidget):
    """Left-of-sidebar panel showing vertical list of page thumbnails (Acrobat style)."""

    page_clicked = Signal(int)  # 0-based page index
    close_clicked = Signal()

    PANEL_W = 230

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setObjectName("PageThumbsPanel")
        self.setFixedWidth(self.PANEL_W)
        self.setStyleSheet("""
            #PageThumbsPanel {
                background: #F7F7F7;
                border-left: 1px solid #E0E0E0;
            }
            #PageThumbsPanel #ThumbsHeader {
                background: #F7F7F7;
                border-bottom: 1px solid #E0E0E0;
            }
            #PageThumbsPanel #ThumbsHeader QLabel {
                color: #1a1a1a;
                font-size: 14px;
                font-weight: 600;
            }
            #PageThumbsPanel QToolButton {
                background: transparent;
                border: none;
                padding: 6px;
                color: #555;
                font-size: 14px;
                border-radius: 4px;
            }
            #PageThumbsPanel QToolButton:hover {
                background: #E8E8E8;
            }
            #PageThumbsPanel QScrollArea {
                background: #F7F7F7;
                border: none;
            }
            #PageThumbsPanel QScrollBar:vertical {
                background: transparent;
                width: 8px;
            }
            #PageThumbsPanel QScrollBar::handle:vertical {
                background: #C0C0C0;
                border-radius: 4px;
                min-height: 30px;
            }
            #PageThumbsPanel QScrollBar::add-line:vertical,
            #PageThumbsPanel QScrollBar::sub-line:vertical {
                height: 0;
            }
        """)

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        # Header row: close button + "Pages" title
        header = QWidget()
        header.setObjectName("ThumbsHeader")
        header.setFixedHeight(36)
        hh = QHBoxLayout(header)
        hh.setContentsMargins(8, 0, 8, 0)
        hh.setSpacing(4)
        close_btn = QToolButton()
        close_btn.setText("\u2715")  # ✕
        close_btn.setToolTip("Hide page thumbnails")
        close_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        close_btn.clicked.connect(self.close_clicked)
        hh.addWidget(close_btn)
        title = QLabel("Pages")
        hh.addWidget(title)
        hh.addStretch()
        outer.addWidget(header)

        # Scroll area with vertical list of thumbnails
        from PySide6.QtWidgets import QScrollArea, QFrame
        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.scroll.setFrameShape(QFrame.Shape.NoFrame)
        self.scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        self._content = QWidget()
        self._cv = QVBoxLayout(self._content)
        self._cv.setContentsMargins(12, 12, 12, 12)
        self._cv.setSpacing(14)
        self._cv.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.scroll.setWidget(self._content)
        outer.addWidget(self.scroll, 1)

        self._items: list[ThumbnailItem] = []
        self._current_idx: int = -1
        self._worker: ThumbnailRenderWorker | None = None
        self._loaded_path: str | None = None
        self._loaded_page_count: int = 0

    def set_document(self, doc, current_idx: int = 0) -> None:
        """Populate with thumbnails for every page of *doc* (a fitz.Document)."""
        if doc is None:
            self.clear()
            return

        # If already loaded for this doc, just update selection and return.
        file_path = getattr(doc, "name", None) or ""
        page_count = len(doc)
        if self._loaded_path == file_path and self._loaded_page_count == page_count:
            self.set_current(current_idx)
            return

        self.clear()
        self._loaded_path = file_path
        self._loaded_page_count = page_count

        # Compute per-page target size from first page aspect ratio
        first = doc[0]
        ratio = first.rect.height / max(1.0, first.rect.width)
        target_w = 180
        target_h = max(60, int(target_w * ratio))

        for i in range(page_count):
            item = ThumbnailItem(i, target_w, target_h)
            item.clicked.connect(lambda idx=i: self.page_clicked.emit(idx))
            self._cv.addWidget(item, alignment=Qt.AlignmentFlag.AlignHCenter)
            self._items.append(item)

        # Kick off background rendering
        if file_path:
            self._worker = ThumbnailRenderWorker(file_path, page_count, target_w)
            self._worker.ready.connect(self._on_thumb_ready)
            self._worker.start()

        self.set_current(current_idx)

    def clear(self) -> None:
        if self._worker and self._worker.isRunning():
            self._worker.cancel()
            self._worker = None
        for item in self._items:
            self._cv.removeWidget(item)
            item.deleteLater()
        self._items.clear()
        self._current_idx = -1
        self._loaded_path = None
        self._loaded_page_count = 0

    def set_current(self, idx: int) -> None:
        if 0 <= self._current_idx < len(self._items):
            self._items[self._current_idx].set_selected(False)
        if 0 <= idx < len(self._items):
            self._items[idx].set_selected(True)
            # Scroll to the selected item
            self.scroll.ensureWidgetVisible(self._items[idx], 0, 50)
        self._current_idx = idx

    def _on_thumb_ready(self, page_idx: int, image: QImage) -> None:
        if 0 <= page_idx < len(self._items):
            self._items[page_idx].set_image(image)


class MainWindow(QMainWindow):
    """Main application window with 6 tabs."""

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Catalog API Studio")
        self.setWindowFlag(Qt.WindowType.WindowMinimizeButtonHint, True)
        self.setWindowFlag(Qt.WindowType.WindowMaximizeButtonHint, True)

        # Minimum size — prevents the tab bar from collapsing into scroll arrows
        # when the corner widget (zoom + progress + menu + version + reload) is
        # too wide for the window.
        self.setMinimumSize(900, 600)

        screen = QGuiApplication.primaryScreen()
        avail = screen.availableGeometry() if screen else None
        if avail is not None:
            self.resize(min(1200, avail.width()), min(800, avail.height()))
        else:
            self.resize(1200, 800)

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(0, 0, 0, 0)

        # Splitter: tabs on top, debug console on bottom
        self._splitter = QSplitter(Qt.Orientation.Vertical)
        self._splitter.setChildrenCollapsible(False)
        layout.addWidget(self._splitter)

        # Content area: tabs on the left, vertical PDF-control sidebar on the right
        content_wrap = QWidget()
        content_h = QHBoxLayout(content_wrap)
        content_h.setContentsMargins(0, 0, 0, 0)
        content_h.setSpacing(0)

        self.tabs = QTabWidget()
        content_h.addWidget(self.tabs, 1)

        # Pages thumbnails panel (hidden by default; toggled via sidebar "pages" icon)
        self.thumbs_panel = PageThumbnailsPanel()
        self.thumbs_panel.setVisible(False)
        self.thumbs_panel.page_clicked.connect(self._on_thumb_clicked)
        self.thumbs_panel.close_clicked.connect(lambda: self.thumbs_panel.setVisible(False))
        content_h.addWidget(self.thumbs_panel)

        self._build_right_sidebar(content_h)

        self._splitter.addWidget(content_wrap)

        # Create tabs first so the header zoom/fit controls can reference the PDF views
        self.import_view = ImportView()
        self.preview_view = PreviewView()
        self.layers_view = LayersView()
        self.layout_view = LayoutView()
        self.recognition_view = RecognitionView()
        self.review_view = ReviewView()
        self.search_view = SearchView()
        self.api_view = APIView()

        self.tabs.addTab(self.import_view, "Import")
        self.tabs.addTab(self.preview_view, "Preview")
        self.tabs.addTab(self.layers_view, "Layers")
        self.tabs.addTab(self.layout_view, "Layout")
        self.tabs.addTab(self.recognition_view, "Recognition")
        self.tabs.addTab(self.review_view, "Review")
        self.tabs.addTab(self.search_view, "Search")
        self.tabs.addTab(self.api_view, "API")

        # Right corner of tab bar: progress + View menu + version + Reload
        # (zoom/fit/pagination live on the right sidebar now)
        right_corner = QWidget()
        right_layout = QHBoxLayout(right_corner)
        right_layout.setContentsMargins(4, 0, 6, 0)
        right_layout.setSpacing(6)

        # "last operation: <name>" label (right-aligned, left of progress bar)
        self.hdr_progress_name = QLabel("")
        self.hdr_progress_name.setMinimumWidth(220)
        self.hdr_progress_name.setMaximumWidth(360)
        self.hdr_progress_name.setStyleSheet("color: #aaa; font-size: 11px;")
        self.hdr_progress_name.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        right_layout.addWidget(self.hdr_progress_name)

        # Common progress bar — 0–100%, shared across Preview/Layout operations.
        from PySide6.QtWidgets import QProgressBar
        self.hdr_progress = QProgressBar()
        self.hdr_progress.setFixedSize(220, 18)
        self.hdr_progress.setRange(0, 100)
        self.hdr_progress.setValue(0)
        self.hdr_progress.setTextVisible(True)
        self.hdr_progress.setFormat("")
        self.hdr_progress.setStyleSheet("QProgressBar { font-size: 10px; }")
        right_layout.addWidget(self.hdr_progress)

        # Pause/resume toggle for background detection across PDF tabs.
        # "×" while running, "▶" while paused.
        self.hdr_detect_toggle = QToolButton()
        self.hdr_detect_toggle.setFixedSize(20, 18)
        self.hdr_detect_toggle.setCheckable(True)
        self.hdr_detect_toggle.setAutoRaise(True)
        self.hdr_detect_toggle.setStyleSheet(
            "QToolButton { font-weight: bold; font-size: 12px; padding: 0px; }"
        )
        self._detect_paused = False
        self._update_detect_toggle_ui()
        self.hdr_detect_toggle.clicked.connect(self._on_detect_toggle_clicked)
        right_layout.addWidget(self.hdr_detect_toggle)

        # Duration of the last operation (right of progress bar)
        self.hdr_progress_duration = QLabel("")
        self.hdr_progress_duration.setFixedWidth(56)
        self.hdr_progress_duration.setStyleSheet("color: #aaa; font-size: 11px;")
        self.hdr_progress_duration.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        right_layout.addWidget(self.hdr_progress_duration)

        # Push the meta block (menu/version/reload) to the far right so the
        # zoom + progress cluster sits flush-left in the corner widget.
        right_layout.addStretch()

        # Separator before meta controls
        sep = QLabel("|")
        sep.setStyleSheet("color: #555;")
        right_layout.addWidget(sep)

        menu_bar = QMenuBar()
        menu_bar.setStyleSheet("QMenuBar { background: transparent; }")
        self._build_view_menu(menu_bar)
        right_layout.addWidget(menu_bar)

        build_time = _get_build_time()
        version_label = QLabel(f"v{APP_VERSION} | {build_time}")
        version_label.setStyleSheet("color: #888; font-size: 11px;")
        right_layout.addWidget(version_label)

        reload_btn = QPushButton("Reload")
        reload_btn.setFixedWidth(60)
        reload_btn.setStyleSheet("font-size: 11px;")
        reload_btn.clicked.connect(self._reload_app)
        right_layout.addWidget(reload_btn)
        self.tabs.setCornerWidget(right_corner, Qt.Corner.TopRightCorner)

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
        self.layers_view.progress.connect(self._on_progress)
        self.layout_view.progress.connect(self._on_progress)
        self.recognition_view.progress.connect(self._on_progress)

        # Keep sidebar page/zoom labels in sync with active view
        self.preview_view.page_spin.valueChanged.connect(self._sync_page_from_active)
        self.layers_view.page_spin.valueChanged.connect(self._sync_page_from_active)
        self.layout_view.page_spin.valueChanged.connect(self._sync_page_from_active)
        self.recognition_view.page_spin.valueChanged.connect(self._sync_page_from_active)

        # Auto-save when any Layout-view-dropdown checkbox toggles
        for cb_name in ("_layout_pdf_cb", "_layout_show_labels_cb", "_layout_rulers_cb"):
            cb = getattr(self.layout_view, cb_name, None)
            if cb is not None:
                cb.toggled.connect(lambda _=False: self._schedule_save_state())
        # "Show objects" dropdown checkboxes → auto-save
        for cb in getattr(self.layout_view, "_layout_show_cbs", {}).values():
            cb.toggled.connect(lambda _=False: self._schedule_save_state())

        # Refresh data when switching tabs
        self.tabs.currentChanged.connect(self._on_tab_changed)

        # Restore saved state — deferred via QTimer so the empty window paints
        # first. Loading the previous document + rendering its first spread
        # can take seconds for large catalogs; deferring keeps the UI
        # responsive and gives instant visual feedback that the app launched.
        from PySide6.QtCore import QTimer
        QTimer.singleShot(0, self._restore_state)

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

    def _build_right_sidebar(self, parent_layout: QHBoxLayout) -> None:
        """Adobe-Acrobat clone: fixed 44px vertical sidebar with 3 zones
        (top icon group → middle scrollbar → bottom icon group).
        """
        from PySide6.QtWidgets import QMenu

        sidebar = QWidget()
        sidebar.setObjectName("AcrobatSidebar")
        sidebar.setFixedWidth(44)
        sidebar.setStyleSheet(SIDEBAR_QSS)

        v = QVBoxLayout(sidebar)
        v.setContentsMargins(0, 6, 0, 6)
        v.setSpacing(0)

        # ---------------- TOP icon group ----------------
        self.sb_chat_btn = self._sb_make_icon_btn(SVG_CHAT, "Comments",
                                                  lambda: logger.info("sidebar: chat"))
        v.addWidget(self.sb_chat_btn)
        self.sb_bookmark_btn = self._sb_make_icon_btn(SVG_BOOKMARK, "Bookmarks",
                                                      lambda: logger.info("sidebar: bookmarks"))
        v.addWidget(self.sb_bookmark_btn)
        self.sb_pages_btn = self._sb_make_icon_btn(SVG_COPY, "Page thumbnails",
                                                   self._toggle_thumbs_panel)
        v.addWidget(self.sb_pages_btn)

        # ---------------- MIDDLE stretch (empty, just pushes the two groups apart) ----------------
        v.addStretch(1)

        # ---------------- BOTTOM icon group ----------------
        # Two stacked page-number labels (current / total) — clickable for go-to-page
        self.sb_page_label = self._sb_make_page_button("—")
        self.sb_page_label.clicked.connect(self._sb_prompt_goto_page)
        v.addWidget(self.sb_page_label)

        self.sb_total_label = self._sb_make_page_button("—")
        self.sb_total_label.clicked.connect(self._sb_prompt_goto_page)
        v.addWidget(self.sb_total_label)

        # Chevrons + refresh + fit + zoom
        self.sb_page_prev = self._sb_make_icon_btn(
            SVG_CHEVRON_UP, "Previous page", lambda: self._nudge_page(-1)
        )
        v.addWidget(self.sb_page_prev)

        self.sb_page_next = self._sb_make_icon_btn(
            SVG_CHEVRON_DOWN, "Next page", lambda: self._nudge_page(+1)
        )
        v.addWidget(self.sb_page_next)

        self.sb_refresh = self._sb_make_icon_btn(
            SVG_REFRESH, "Rotate document right (90° CW)", self._sb_rotate_document_right
        )
        v.addWidget(self.sb_refresh)

        # Fit-to-page is a popup menu (Acrobat-style view options), not a plain button
        self.sb_fit_page = QToolButton()
        self.sb_fit_page.setAutoRaise(True)
        self.sb_fit_page.setIcon(_sb_svg_icon(SVG_PAGE_FIT, 20))
        self.sb_fit_page.setIconSize(QSize(20, 20))
        self.sb_fit_page.setToolTip("Page view options")
        self.sb_fit_page.setCursor(Qt.CursorShape.PointingHandCursor)
        self.sb_fit_page.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)

        fit_menu = QMenu(self.sb_fit_page)
        # View mode (informational — we only have two-page spread right now)
        self.sb_act_two_page = fit_menu.addAction("Two-page view")
        self.sb_act_two_page.setCheckable(True)
        self.sb_act_two_page.setChecked(True)
        self.sb_act_two_page.setEnabled(False)  # fixed mode for now

        self.sb_act_cover = fit_menu.addAction("Show cover page")
        self.sb_act_cover.setCheckable(True)
        self.sb_act_cover.setChecked(True)
        self.sb_act_cover.triggered.connect(self._sb_toggle_cover_page)

        fit_menu.addSeparator()

        fit_menu.addAction("Actual size").triggered.connect(
            lambda: self._apply_zoom_value(1.0))
        self.sb_act_zoom_page = fit_menu.addAction("Zoom to page level")
        self.sb_act_zoom_page.triggered.connect(self._apply_fit_height)
        fit_menu.addAction("Fit to width").triggered.connect(self._apply_fit_width)
        fit_menu.addAction("Fit height").triggered.connect(self._apply_fit_height)

        # Sync "Show cover page" checkmark with active view state whenever shown
        def _sync_fit_menu_state() -> None:
            view = self._active_pdf_view()
            if view is not None:
                self.sb_act_cover.setChecked(bool(getattr(view, "_first_is_cover", True)))
                self.sb_act_cover.setEnabled(True)
            else:
                self.sb_act_cover.setEnabled(False)
        fit_menu.aboutToShow.connect(_sync_fit_menu_state)

        # Forward wheel events landing outside the menu to the active PDF
        # view's scroll area so zoom / scroll keep working while the menu is
        # open.
        self._wire_menu_wheel_passthrough(fit_menu)

        self.sb_fit_page.setMenu(fit_menu)
        v.addWidget(self.sb_fit_page)

        # Zoom in — plain click, no menu
        self.sb_zoom_in = self._sb_make_icon_btn(
            SVG_ZOOM_IN, "Zoom in", lambda: self._apply_zoom_delta(+0.1)
        )
        v.addWidget(self.sb_zoom_in)

        self.sb_zoom_out = self._sb_make_icon_btn(
            SVG_ZOOM_OUT, "Zoom out", lambda: self._apply_zoom_delta(-0.1)
        )
        v.addWidget(self.sb_zoom_out)

        # Hidden zoom-% label — kept as an attribute so _sync_zoom_from_view() works
        # without rewriting its callers. Not part of the Acrobat layout.
        self.sb_zoom_label = QLabel("100%")
        self.sb_zoom_label.setVisible(False)

        # Alias used by _on_tab_changed's enable-list
        self.sb_view_btn = self.sb_zoom_in

        parent_layout.addWidget(sidebar)
        self._right_sidebar = sidebar

    def _sb_make_icon_btn(self, svg_str: str, tooltip: str, on_click) -> QToolButton:
        btn = QToolButton()
        btn.setAutoRaise(True)
        btn.setIcon(_sb_svg_icon(svg_str, 20))
        btn.setIconSize(QSize(20, 20))
        btn.setToolTip(tooltip)
        btn.setCursor(Qt.CursorShape.PointingHandCursor)
        btn.clicked.connect(on_click)
        return btn

    def _sb_make_page_button(self, text: str) -> QToolButton:
        btn = QToolButton()
        btn.setAutoRaise(True)
        btn.setObjectName("PageLabel")
        btn.setText(text)
        btn.setCursor(Qt.CursorShape.PointingHandCursor)
        return btn

    def _sb_prompt_goto_page(self) -> None:
        view = self._active_pdf_view()
        if view is None:
            return
        from PySide6.QtWidgets import QInputDialog
        cur = view.page_spin.value()
        mx = view.page_spin.maximum() or 1
        page, ok = QInputDialog.getInt(self, "Go to page", "Page:", cur, 1, mx, 1)
        if ok:
            view.page_spin.setValue(page)

    def _wire_menu_wheel_passthrough(self, menu) -> None:
        """Forward wheel events that land outside *menu* to the active PDF
        view's scroll-area viewport so zoom / scroll keep working while the
        menu is open (QMenu otherwise swallows wheel globally)."""
        from PySide6.QtCore import QEvent, QObject, QPointF
        from PySide6.QtGui import QWheelEvent
        from PySide6.QtWidgets import QApplication

        owner = self

        class _Forwarder(QObject):
            def __init__(self):
                super().__init__(menu)
                self._app = None
                menu.aboutToShow.connect(self._on_show)
                menu.aboutToHide.connect(self._on_hide)

            def _on_show(self):
                self._app = QApplication.instance()
                if self._app is not None:
                    self._app.installEventFilter(self)

            def _on_hide(self):
                if self._app is not None:
                    self._app.removeEventFilter(self)
                    self._app = None

            def eventFilter(self, obj, event):
                if event.type() != QEvent.Type.Wheel:
                    return False
                try:
                    gp = event.globalPosition().toPoint()
                except Exception:
                    return False
                if menu.rect().translated(menu.pos()).contains(gp):
                    return False
                view = owner._active_pdf_view()
                if view is None:
                    return False
                vp = view.scroll_area.viewport()
                vp_pos = vp.mapFromGlobal(gp)
                if not vp.rect().contains(vp_pos):
                    return False
                new_event = QWheelEvent(
                    QPointF(vp_pos), QPointF(gp),
                    event.pixelDelta(), event.angleDelta(),
                    event.buttons(), event.modifiers(),
                    event.phase(), event.inverted(),
                )
                QApplication.sendEvent(vp, new_event)
                return True

        _Forwarder()

    def _sb_rotate_document_right(self) -> None:
        """Rotate the active document 90° clockwise."""
        view = self._active_pdf_view()
        if view is None:
            return
        view.rotate_right()
        self._schedule_save_state()

    def _sb_toggle_cover_page(self, checked: bool) -> None:
        """Toggle 'first page is cover' on the active PDF view."""
        view = self._active_pdf_view()
        if view is None:
            return
        view._first_is_cover = bool(checked)
        view._rebuild_spreads()
        self._schedule_save_state()

    def _toggle_thumbs_panel(self) -> None:
        """Show/hide the Pages thumbnails panel, populating from the active view."""
        if self.thumbs_panel.isVisible():
            self.thumbs_panel.setVisible(False)
            self._schedule_save_state()
            return
        view = self._active_pdf_view()
        doc = getattr(view, "_doc", None) if view else None
        if doc is None:
            return
        current_idx = (view.page_spin.value() - 1) if view else 0
        self.thumbs_panel.set_document(doc, current_idx)
        self.thumbs_panel.setVisible(True)
        self._schedule_save_state()

    def _on_thumb_clicked(self, page_idx: int) -> None:
        """Scroll the active PDF view to the clicked thumbnail's page."""
        view = self._active_pdf_view()
        if view is None:
            return
        view.page_spin.setValue(page_idx + 1)
        self.thumbs_panel.set_current(page_idx)

    def _active_pdf_view(self):
        """Return the current PDF-rendering view (Preview / Layers / Layout / Recognition), or None."""
        widget = self.tabs.currentWidget()
        if widget in (self.preview_view,
                      getattr(self, "layers_view", None),
                      self.layout_view,
                      getattr(self, "recognition_view", None)):
            return widget
        return None

    def _nudge_page(self, delta: int) -> None:
        """Advance (delta=+1) or go back (delta=-1) one page on the active view."""
        view = self._active_pdf_view()
        if view is None:
            return
        new_page = view.page_spin.value() + delta
        new_page = max(view.page_spin.minimum(), min(view.page_spin.maximum(), new_page))
        view.page_spin.setValue(new_page)

    def _sync_page_from_active(self) -> None:
        view = self._active_pdf_view()
        if view is None:
            return
        current = view.page_spin.value()
        total = view.page_spin.maximum()
        self.sb_page_label.setText(str(current))
        self.sb_total_label.setText(str(total))
        # Mirror into the thumbnails panel if visible
        if self.thumbs_panel.isVisible():
            self.thumbs_panel.set_current(current - 1)

    def _apply_zoom_value(self, zoom: float) -> None:
        view = self._active_pdf_view()
        if view is None:
            return
        view._set_zoom(zoom)
        self._sync_zoom_from_view(view)

    def _apply_zoom_delta(self, delta: float) -> None:
        view = self._active_pdf_view()
        if view is None:
            return
        view._set_zoom(view._zoom + delta)
        self._sync_zoom_from_view(view)

    def _apply_fit_width(self) -> None:
        view = self._active_pdf_view()
        if view is None:
            return
        view._fit_to_width()
        self._sync_zoom_from_view(view)

    def _apply_fit_height(self) -> None:
        view = self._active_pdf_view()
        if view is None:
            return
        view._fit_to_height()
        self._sync_zoom_from_view(view)

    def _sync_zoom_from_view(self, view) -> None:
        """Reflect the view's current zoom + page in the sidebar labels."""
        percent = int(round(view._zoom * 100))
        self.sb_zoom_label.setText(f"{percent}%")
        try:
            current = view.page_spin.value()
            total = view.page_spin.maximum()
            self.sb_page_label.setText(str(current))
            self.sb_total_label.setText(str(total))
        except Exception:
            pass
        self._schedule_save_state()

    def _on_tab_changed(self, index: int) -> None:
        widget = self.tabs.widget(index)
        # Pause background detection when leaving the Layout / Recognition
        # tabs so large catalogs don't keep the main thread busy emitting
        # detect signals on other tabs.
        if widget is not self.layout_view:
            self.layout_view._cancel_detect_worker()
        if widget is not self.recognition_view:
            self.recognition_view._cancel_detect_worker()
        # Sync header zoom controls with the active PDF view (or disable them)
        pdf_view = widget if widget in (self.preview_view, self.layers_view,
                                        self.layout_view, self.recognition_view) else None
        for w in (self.sb_page_prev, self.sb_page_next, self.sb_refresh,
                  self.sb_fit_page, self.sb_zoom_in, self.sb_zoom_out,
                  self.sb_view_btn):
            w.setEnabled(pdf_view is not None)
        if pdf_view is not None:
            self._sync_zoom_from_view(pdf_view)
        # Lazy-load the previewed document into LayersView on first open
        if widget is self.layers_view:
            current = getattr(self.preview_view, "_original_path", None)
            loaded = getattr(self.layers_view, "_original_path", None)
            if current and current != loaded:
                from pathlib import Path
                from PySide6.QtCore import QTimer
                QTimer.singleShot(
                    0, lambda p=Path(str(current)): self.layers_view.load_document(p)
                )
        # Lazy-load the currently-previewed document into LayoutView on first open
        if widget is self.layout_view:
            current = getattr(self.preview_view, "_original_path", None)
            loaded = getattr(self.layout_view, "_original_path", None)
            if current and current != loaded:
                # Defer the heavy load/rebuild so the tab switch itself is
                # instant — user sees Layout first, rendering begins on the
                # next event-loop iteration.
                from pathlib import Path
                from PySide6.QtCore import QTimer
                def _load_and_restore(p=Path(str(current))) -> None:
                    self.layout_view.load_document(p)
                    # Apply saved layout page/zoom once the doc is loaded
                    page = getattr(self, "_pending_layout_page", None)
                    zoom = getattr(self, "_pending_layout_zoom", None)
                    if zoom:
                        self.layout_view._set_zoom(float(zoom))
                    if page:
                        self.layout_view.page_spin.setValue(int(page))
                        self.layout_view._scroll_to_page(int(page))
                    self._pending_layout_page = None
                    self._pending_layout_zoom = None
                    self._sync_zoom_from_view(self.layout_view)
                QTimer.singleShot(0, _load_and_restore)
            else:
                self.layout_view._detect_visible_pages()
        # Same lazy-load behavior for Recognition tab
        if widget is self.recognition_view:
            current = getattr(self.preview_view, "_original_path", None)
            loaded = getattr(self.recognition_view, "_original_path", None)
            if current and current != loaded:
                from pathlib import Path
                from PySide6.QtCore import QTimer
                QTimer.singleShot(
                    0, lambda p=Path(str(current)): self.recognition_view.load_document(p)
                )
            else:
                self.recognition_view._detect_visible_pages()
        if hasattr(widget, "refresh"):
            widget.refresh()
        self._schedule_save_state()

    def _update_detect_toggle_ui(self) -> None:
        """Sync the toggle button's glyph + tooltip to the current pause state."""
        if self._detect_paused:
            self.hdr_detect_toggle.setText("▶")
            self.hdr_detect_toggle.setToolTip("Resume background detection")
            self.hdr_detect_toggle.setChecked(True)
        else:
            self.hdr_detect_toggle.setText("×")
            self.hdr_detect_toggle.setToolTip("Pause background detection")
            self.hdr_detect_toggle.setChecked(False)

    def _on_detect_toggle_clicked(self) -> None:
        """Toggle background detection paused/running across all PDF views."""
        self._detect_paused = not self._detect_paused
        self._update_detect_toggle_ui()
        for view in (self.preview_view, self.layout_view, self.recognition_view):
            setter = getattr(view, "set_detection_paused", None)
            if callable(setter):
                setter(self._detect_paused)

    def _open_preview(self, file_path: str) -> None:
        """Open a document and switch to the tab after Import (Preview)."""
        from pathlib import Path

        # Switch first so the user sees the tab change immediately,
        # then the document load work happens on the visible tab.
        import_idx = self.tabs.indexOf(self.import_view)
        next_idx = import_idx + 1 if import_idx >= 0 else self.tabs.indexOf(self.preview_view)
        if 0 <= next_idx < self.tabs.count():
            self.tabs.setCurrentIndex(next_idx)
        self.preview_view.load_document(Path(file_path))

    def _on_progress(self, text: str) -> None:
        """Insert progress message at top of debug console (one message per line)."""
        # Keep the header progress bar in solid (determinate) mode at all times —
        # updating only when a new numeric ratio is available. Messages without a
        # ratio (e.g. single-page renders) leave the current value untouched, so
        # the bar doesn't flicker between indeterminate and determinate modes.
        self.hdr_progress.setRange(0, 100)

        clean_text = (text or "").replace("\n", " ").replace("\r", " ").strip()
        is_done = (not clean_text) or clean_text.lower().startswith("done") or " done " in clean_text.lower()

        # Lazy-init a debounce timer for "done" resets — panning fires many
        # short render/done cycles; resetting the bar each time would flicker.
        import time as _time
        if not hasattr(self, "_progress_reset_timer"):
            from PySide6.QtCore import QTimer
            self._progress_reset_timer = QTimer(self)
            self._progress_reset_timer.setSingleShot(True)
            self._progress_reset_timer.setInterval(800)
            self._progress_start_wall: float | None = None

            def _reset_bar() -> None:
                # Keep the "last operation: <name>" label + duration visible —
                # only the active-progress value/format is cleared. This way
                # the user always sees what just finished.
                self.hdr_progress.setRange(0, 100)
                self.hdr_progress.setValue(0)
                self.hdr_progress.setFormat("")
                self._progress_start_wall = None

            self._progress_reset_timer.timeout.connect(_reset_bar)

        import re

        if is_done:
            # Schedule reset — gets cancelled if another progress update
            # arrives within the debounce window.
            self._progress_reset_timer.start()
            # Freeze the duration label at the final operation time.
            if self._progress_start_wall is not None:
                dur = _time.perf_counter() - self._progress_start_wall
                self.hdr_progress_duration.setText(f"{dur:.1f}s")
        else:
            # Mark start of the full operation when coming out of idle.
            if self._progress_start_wall is None:
                self._progress_start_wall = _time.perf_counter()
            # Live-update the duration label with wall-clock elapsed from
            # the start of the current operation batch (covers 0→100% span).
            dur = _time.perf_counter() - self._progress_start_wall
            self.hdr_progress_duration.setText(f"{dur:.1f}s")
            # A real progress update cancels any pending reset.
            self._progress_reset_timer.stop()
            # Process label = full message minus the trailing "X.Ys" elapsed-time.
            name = re.sub(r"\s+\d+(?:[.,]\d+)?s\s*$", "", clean_text).strip()
            display = f"last operation: {name}" if name else ""
            # Elide from the left via font metrics so the rightmost part (nearest
            # the progress bar) is always visible.
            from PySide6.QtGui import QFontMetrics
            fm = QFontMetrics(self.hdr_progress_name.font())
            avail_w = self.hdr_progress_name.width() or self.hdr_progress_name.maximumWidth()
            elided = fm.elidedText(display, Qt.TextElideMode.ElideLeft, max(40, avail_w - 4))
            self.hdr_progress_name.setText(elided)
            self.hdr_progress_name.setToolTip(display if elided != display else "")

            # Try to extract an N/M ratio anywhere in the message.
            percent: int | None = None
            ratio_match = re.search(r"(\d+)\s*/\s*(\d+)", clean_text)
            if ratio_match:
                cur, total = int(ratio_match.group(1)), int(ratio_match.group(2))
                if total > 0:
                    percent = max(0, min(100, int(cur * 100 / total)))
            else:
                rem_match = re.search(r"\((\d+)\s+remaining\)", clean_text)
                if rem_match:
                    remaining = int(rem_match.group(1))
                    pdf_view = self._active_pdf_view()
                    total = getattr(pdf_view, "_page_count", 0) if pdf_view else 0
                    if total > 0:
                        done = max(0, total - remaining)
                        percent = max(0, min(100, int(done * 100 / total)))

            if percent is not None:
                self.hdr_progress.setValue(percent)
                self.hdr_progress.setFormat(f"{percent}%")
            # else: leave value/format as-is — no flicker between modes.

        if not text:
            return
        from datetime import datetime
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
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

    def _save_state(self) -> None:
        """Persist all UI state to QSettings. Safe to call any time."""
        s = self._settings()

        # Window geometry
        s.setValue("window/geometry", self.saveGeometry())
        s.setValue("window/state", self.saveState())
        s.setValue("window/tab", self.tabs.currentIndex())

        # Preview state
        if self.preview_view._file_path:
            s.setValue("preview/file", str(self.preview_view._original_path or self.preview_view._file_path))
            s.setValue("preview/page", self.preview_view.page_spin.value())
            s.setValue("preview/zoom", self.preview_view._zoom)

        # Show filters (bbox, table, text, photo, picture, drawing)
        for key, act in self.preview_view.bbox_filter_actions.items():
            if hasattr(act, "isChecked"):
                s.setValue(f"show/{key}", act.isChecked())

        # Content filters
        for key, cb in self.preview_view._content_checkboxes.items():
            s.setValue(f"content/{key}", cb.isChecked())

        # Debug bar
        s.setValue("debug/expanded", self._debug_expanded)
        if self._debug_expanded:
            sizes = self._splitter.sizes()
            if len(sizes) >= 2:
                s.setValue("debug/height", sizes[1])

        # View menu toggles
        s.setValue("show/hidden", self.show_hidden_action.isChecked())
        s.setValue("show/template", self.show_template_action.isChecked())

        # --- View states (covers, labels, rotation, thumbs panel) ---
        s.setValue("ui/thumbs_visible", self.thumbs_panel.isVisible())

        # Preview-view layout prefs
        s.setValue("preview/first_is_cover", bool(self.preview_view._first_is_cover))
        s.setValue("preview/last_is_back_cover", bool(
            getattr(self.preview_view, "_last_is_back_cover", True)))
        s.setValue("preview/rotation", self._get_doc_rotation(self.preview_view))

        # Layout-view layout prefs + filter checkboxes
        s.setValue("layout/first_is_cover", bool(self.layout_view._first_is_cover))
        s.setValue("layout/last_is_back_cover", bool(
            getattr(self.layout_view, "_last_is_back_cover", True)))
        s.setValue("layout/rotation", self._get_doc_rotation(self.layout_view))
        s.setValue("layout/show_pdf_objects", bool(
            getattr(self.layout_view, "_bboxes_visible", True)))
        s.setValue("layout/show_labels", bool(
            getattr(self.layout_view, "_show_object_labels", True)))
        s.setValue("layout/show_rulers", bool(
            getattr(self.layout_view, "_show_rulers", False)))

        # Per-view page + zoom (sidebar page/zoom labels reflect these)
        if self.layout_view._doc is not None:
            s.setValue("layout/page", self.layout_view.page_spin.value())
            s.setValue("layout/zoom", float(self.layout_view._zoom))

        # Per-type filter checkboxes from the Layout "View ▾" dropdown
        for name, cb in getattr(self.layout_view, "_layout_type_cbs", {}).items():
            s.setValue(f"layout/type_filter/{name}", bool(cb.isChecked()))

        # "Show objects" dropdown — content visibility per type (+ parent)
        for name, cb in getattr(self.layout_view, "_layout_show_cbs", {}).items():
            s.setValue(f"layout/show_obj/{name}", bool(cb.isChecked()))

        s.sync()

    def _get_doc_rotation(self, view) -> int:
        """Current rotation (0/90/180/270) of page 0 on *view*, or 0 if no doc."""
        doc = getattr(view, "_doc", None)
        if not doc or len(doc) == 0:
            return 0
        try:
            return int(doc[0].rotation) % 360
        except Exception:
            return 0

    def _schedule_save_state(self) -> None:
        """Debounced auto-save — persist state 700ms after the last change."""
        if not hasattr(self, "_save_state_timer"):
            from PySide6.QtCore import QTimer
            self._save_state_timer = QTimer(self)
            self._save_state_timer.setSingleShot(True)
            self._save_state_timer.setInterval(700)
            self._save_state_timer.timeout.connect(self._save_state)
        self._save_state_timer.start()

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._schedule_save_state()

    def moveEvent(self, event) -> None:
        super().moveEvent(event)
        self._schedule_save_state()

    def closeEvent(self, event) -> None:
        """Save window and document state on close."""
        self._save_state()
        super().closeEvent(event)

    def _restore_state(self) -> None:
        """Restore window and document state from previous session."""
        s = self._settings()

        # Window geometry
        geom = s.value("window/geometry")
        if geom:
            self.restoreGeometry(geom)
        # Clamp: if a previously saved geometry is smaller than our minimum
        # (e.g. from an older version without setMinimumSize), restore to a
        # sensible default instead of a broken mini-window.
        min_w, min_h = self.minimumWidth(), self.minimumHeight()
        if self.width() < min_w or self.height() < min_h:
            screen = QGuiApplication.primaryScreen()
            avail = screen.availableGeometry() if screen else None
            if avail is not None:
                self.resize(min(1200, avail.width()), min(800, avail.height()))
            else:
                self.resize(1200, 800)
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

        # --- View states: covers, rotation, layout-toggles (applied BEFORE load) ---
        def _bool(v, default: bool = False) -> bool:
            if v is None:
                return default
            return v == "true" or v is True or v == 1 or v == "1"

        # Preview cover/back defaults
        v = s.value("preview/first_is_cover")
        if v is not None:
            self.preview_view._first_is_cover = _bool(v, True)
        v = s.value("preview/last_is_back_cover")
        if v is not None:
            self.preview_view._last_is_back_cover = _bool(v, True)

        # Layout cover/back defaults + filter/label/ruler toggles.
        # Set the underlying attrs AND sync the dropdown checkboxes.
        v = s.value("layout/first_is_cover")
        if v is not None:
            self.layout_view._first_is_cover = _bool(v, True)
        v = s.value("layout/last_is_back_cover")
        if v is not None:
            self.layout_view._last_is_back_cover = _bool(v, True)

        v = s.value("layout/show_pdf_objects")
        if v is not None:
            val = _bool(v, True)
            self.layout_view._bboxes_visible = val
            cb = getattr(self.layout_view, "_layout_pdf_cb", None)
            if cb is not None and cb.isChecked() != val:
                cb.setChecked(val)  # toggled signal propagates to pages

        v = s.value("layout/show_labels")
        if v is not None:
            val = _bool(v, True)
            self.layout_view._show_object_labels = val
            cb = getattr(self.layout_view, "_layout_show_labels_cb", None)
            if cb is not None and cb.isChecked() != val:
                cb.setChecked(val)

        v = s.value("layout/show_rulers")
        if v is not None:
            val = _bool(v, False)
            self.layout_view._show_rulers = val
            cb = getattr(self.layout_view, "_layout_rulers_cb", None)
            if cb is not None and cb.isChecked() != val:
                cb.setChecked(val)

        # Per-type filter checkboxes
        type_cbs = getattr(self.layout_view, "_layout_type_cbs", {})
        for name, cb in type_cbs.items():
            v = s.value(f"layout/type_filter/{name}")
            if v is not None:
                val = _bool(v, True)
                if cb.isChecked() != val:
                    cb.setChecked(val)

        # "Show objects" dropdown — restore per-type + parent state
        show_cbs = getattr(self.layout_view, "_layout_show_cbs", {})
        for name, cb in show_cbs.items():
            v = s.value(f"layout/show_obj/{name}")
            if v is not None:
                val = _bool(v, True)
                if cb.isChecked() != val:
                    cb.setChecked(val)

        # Remember layout's saved page + zoom for later (applied after its doc loads)
        self._pending_layout_page = s.value("layout/page", None, type=int)
        self._pending_layout_zoom = s.value("layout/zoom", None, type=float)


        # Preview state — load document after view prefs are set so the first
        # render picks them up.
        file_path = s.value("preview/file", "")
        if file_path and Path(file_path).exists():
            zoom = s.value("preview/zoom", 1.0, type=float)
            page = s.value("preview/page", 1, type=int)
            self.preview_view.load_document(Path(file_path))
            # Re-apply saved rotation after load
            rot = s.value("preview/rotation", 0, type=int) % 360
            if rot and self.preview_view._doc:
                for i in range(len(self.preview_view._doc)):
                    self.preview_view._doc[i].set_rotation(rot)
                self.preview_view._pixmap_cache.clear()
                self.preview_view._rebuild_spreads()
            self.preview_view._set_zoom(zoom)
            self.preview_view.page_spin.setValue(page)
            self.preview_view._scroll_to_page(page)

        # Thumbs panel visibility
        if _bool(s.value("ui/thumbs_visible"), False):
            self._toggle_thumbs_panel()

        # If Layout was the active tab when the app closed, load the document
        # into LayoutView too — tab-change fired before preview_view had a
        # document, so the earlier _on_tab_changed call skipped the load.
        self._on_tab_changed(self.tabs.currentIndex())
