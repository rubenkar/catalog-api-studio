"""Preview tab — 2-page PDF viewer with zoom, continuous scroll, and bounding box overlay."""

import logging
import re
from pathlib import Path

import fitz  # PyMuPDF

# Dimension label patterns: single letters (h, d, B, D, etc.),
# letter+digit combos (s1, s2, r1), numbers with optional decimals,
# units (mm), diameter/radius symbols
_DIM_PATTERN = re.compile(
    r'^[a-zA-Z][0-9]?$'        # single letter or letter+digit (h, s1, D, r1)
    r'|^[0-9]+\.?[0-9]*$'      # numbers (12, 30.2, 0.5)
    r'|^Ø[0-9.]*$'             # diameter (Ø, Ø30)
    r'|^R[0-9.]*$'             # radius (R, R5)
    r'|^mm$'                   # unit
    r'|^M[0-9]+$'              # metric thread (M10, M14)
)
from PySide6.QtCore import QPoint, QPointF, QRectF, Qt, Signal
from PySide6.QtGui import QAction, QColor, QCursor, QFont, QImage, QPainter, QPen, QPixmap, QWheelEvent
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QMenu,
    QPushButton,
    QScrollArea,
    QSlider,
    QSpinBox,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from app.services.catalog_meta import (
    load_meta,
    merge_detected,
    save_meta,
    update_object,
)

logger = logging.getLogger(__name__)

# Colors for different bounding box types
BBOX_COLORS = {
    "table": QColor(0, 120, 215, 100),      # blue
    "text": QColor(76, 175, 80, 80),         # green
    "photo": QColor(255, 87, 34, 80),        # deep orange
    "picture": QColor(255, 152, 0, 80),      # orange
    "drawing": QColor(156, 39, 176, 80),     # purple
    "unknown": QColor(158, 158, 158, 80),    # gray
}

BBOX_BORDER_COLORS = {
    "table": QColor(0, 120, 215, 200),
    "text": QColor(76, 175, 80, 160),
    "photo": QColor(255, 87, 34, 160),
    "picture": QColor(255, 152, 0, 160),
    "drawing": QColor(156, 39, 176, 160),
    "unknown": QColor(158, 158, 158, 160),
}


class PageWidget(QWidget):
    """Renders a single PDF page with optional bounding box overlay and stats header."""

    HEADER_HEIGHT = 24
    bbox_selected = None  # class-level: shared selected bbox across pages

    HANDLE_SIZE = 7  # half-size of resize handles in px
    # Handle indices: 0=TL, 1=T, 2=TR, 3=R, 4=BR, 5=B, 6=BL, 7=L
    _HANDLE_CURSORS = [
        Qt.CursorShape.SizeFDiagCursor,  # TL
        Qt.CursorShape.SizeVerCursor,    # T
        Qt.CursorShape.SizeBDiagCursor,  # TR
        Qt.CursorShape.SizeHorCursor,    # R
        Qt.CursorShape.SizeFDiagCursor,  # BR
        Qt.CursorShape.SizeVerCursor,    # B
        Qt.CursorShape.SizeBDiagCursor,  # BL
        Qt.CursorShape.SizeHorCursor,    # L
    ]

    hide_requested = Signal(str, bool)        # (obj_id, hidden)
    type_changed = Signal(str, str)          # (obj_id, new_type)
    bbox_modified = Signal(str, tuple)       # (obj_id, new_pts)
    selection_changed = Signal(object)       # emits self when bbox selected

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._pixmap: QPixmap | None = None
        self._bboxes_pts: list[dict] = []  # bboxes in PDF points
        self._show_bboxes = False
        self._show_hidden = False
        self._page_stats: str = ""
        self._zoom_factor: float = 1.0
        self._selected_idx: int = -1
        self._page_index: int = -1  # 0-based page number
        self._visible_layers: dict[str, bool] = {
            "bbox": True, "table": True, "text": True,
            "photo": True, "picture": True, "drawing": True,
        }

        # Drag state
        self._drag_mode: str = ""       # "move", "resize"
        self._drag_handle: int = -1     # which handle (0-7) for resize
        self._drag_start_px: QPointF | None = None  # mouse pos at drag start (px)
        self._drag_orig_pts: tuple | None = None     # original bbox pts at drag start
        self.setMouseTracking(True)

    def set_pixmap(self, pixmap: QPixmap) -> None:
        self._pixmap = pixmap
        self.setFixedSize(pixmap.width(), pixmap.height() + self.HEADER_HEIGHT)
        self.update()

    def set_bboxes(self, bboxes_pts: list[dict], zoom_factor: float) -> None:
        """Set bounding boxes in PDF point coordinates."""
        self._bboxes_pts = bboxes_pts
        self._zoom_factor = zoom_factor
        self._selected_idx = -1
        self.update()

    def set_show_bboxes(self, show: bool) -> None:
        self._show_bboxes = show
        self.update()

    def set_page_stats(self, stats: str) -> None:
        self._page_stats = stats
        self.update()

    def set_show_hidden(self, show: bool) -> None:
        self._show_hidden = show
        self.update()

    def set_visible_layers(self, layers: dict[str, bool]) -> None:
        """Set which layers are visible: bbox, table, text, image, drawing."""
        self._visible_layers = layers
        self.update()

    def _handle_rects(self, r: QRectF) -> list[QRectF]:
        """Return 8 handle QRectFs for a bbox rect: TL, T, TR, R, BR, B, BL, L."""
        s = self.HANDLE_SIZE
        cx, cy = r.center().x(), r.center().y()
        return [
            QRectF(r.left() - s,  r.top() - s,    2*s, 2*s),   # 0 TL
            QRectF(cx - s,        r.top() - s,     2*s, 2*s),   # 1 T
            QRectF(r.right() - s, r.top() - s,     2*s, 2*s),   # 2 TR
            QRectF(r.right() - s, cy - s,          2*s, 2*s),   # 3 R
            QRectF(r.right() - s, r.bottom() - s,  2*s, 2*s),   # 4 BR
            QRectF(cx - s,        r.bottom() - s,  2*s, 2*s),   # 5 B
            QRectF(r.left() - s,  r.bottom() - s,  2*s, 2*s),   # 6 BL
            QRectF(r.left() - s,  cy - s,          2*s, 2*s),   # 7 L
        ]

    def _hit_handle(self, pos: QPointF) -> int:
        """Return handle index (0-7) if pos is over a handle of selected bbox, else -1."""
        if self._selected_idx < 0 or self._selected_idx >= len(self._bboxes_pts):
            return -1
        bbox = self._bboxes_pts[self._selected_idx]
        if bbox.get("hidden", False) and not self._show_hidden:
            return -1
        r = self._bbox_rect_px(bbox)
        if not r:
            return -1
        for i, hr in enumerate(self._handle_rects(r)):
            if hr.contains(pos):
                return i
        return -1

    def _px_to_pts(self, px: QPointF) -> tuple[float, float]:
        """Convert pixel position to PDF point coordinates."""
        zf = self._zoom_factor
        h = self.HEADER_HEIGHT
        return (px.x() / zf, (px.y() - h) / zf)

    def contextMenuEvent(self, event) -> None:
        """Right-click selects bbox under cursor (if any) and opens context menu."""
        if self._show_bboxes and self._bboxes_pts:
            click = event.pos()
            hits: list[tuple[int, float]] = []
            for i, bbox in enumerate(self._bboxes_pts):
                if bbox.get("hidden", False) and not self._show_hidden:
                    continue
                r = self._bbox_rect_px(bbox)
                if r and r.contains(QPoint(click.x(), click.y())):
                    hits.append((i, r.width() * r.height()))
            if hits:
                hits.sort(key=lambda h: h[1])  # smallest first
                self._selected_idx = hits[0][0]
                self.update()

        if self._selected_idx < 0 or self._selected_idx >= len(self._bboxes_pts):
            return super().contextMenuEvent(event)
        bbox = self._bboxes_pts[self._selected_idx]
        obj_id = bbox.get("id", "")
        if not obj_id:
            return super().contextMenuEvent(event)

        menu = QMenu(self)

        # Hide / Show
        if bbox.get("hidden"):
            action = menu.addAction("Show")
            action.triggered.connect(lambda: self.hide_requested.emit(obj_id, False))
        else:
            action = menu.addAction("Hide")
            action.triggered.connect(lambda: self.hide_requested.emit(obj_id, True))

        # Change type submenu
        type_menu = menu.addMenu("Change type")
        current_type = bbox.get("type", "unknown")
        for type_name in ("table", "text", "photo", "picture", "drawing"):
            act = type_menu.addAction(type_name.capitalize())
            act.setCheckable(True)
            act.setChecked(type_name == current_type)
            act.triggered.connect(
                lambda checked, t=type_name: self.type_changed.emit(obj_id, t)
            )

        menu.exec(event.globalPos())

    def _bbox_rect_px(self, bbox: dict) -> QRectF | None:
        """Convert bbox pts to pixel QRectF (with header offset)."""
        pts = bbox.get("pts")
        if not pts:
            return None
        zf = self._zoom_factor
        h = self.HEADER_HEIGHT
        return QRectF(
            pts[0] * zf, pts[1] * zf + h,
            (pts[2] - pts[0]) * zf, (pts[3] - pts[1]) * zf,
        )

    def mousePressEvent(self, event) -> None:
        if event.button() == Qt.MouseButton.RightButton:
            return super().mousePressEvent(event)
        if not self._show_bboxes or not self._bboxes_pts:
            return super().mousePressEvent(event)

        click = event.position()

        # Check if clicking a resize handle on the selected bbox
        handle = self._hit_handle(click)
        if handle >= 0:
            bbox = self._bboxes_pts[self._selected_idx]
            self._drag_mode = "resize"
            self._drag_handle = handle
            self._drag_start_px = click
            self._drag_orig_pts = tuple(bbox["pts"])
            return

        # Check if clicking inside the selected bbox → start move
        if self._selected_idx >= 0:
            bbox = self._bboxes_pts[self._selected_idx]
            r = self._bbox_rect_px(bbox)
            if r and r.contains(click):
                self._drag_mode = "move"
                self._drag_start_px = click
                self._drag_orig_pts = tuple(bbox["pts"])
                return

        # Otherwise: select bbox under cursor (cycle through overlapping)
        hits: list[tuple[int, float]] = []
        for i, bbox in enumerate(self._bboxes_pts):
            if bbox.get("hidden", False) and not self._show_hidden:
                continue
            r = self._bbox_rect_px(bbox)
            if r and r.contains(click):
                hits.append((i, r.width() * r.height()))

        if not hits:
            self._selected_idx = -1
            self.update()
            return

        hits.sort(key=lambda h: h[1])  # smallest first
        current_pos = -1
        for pos, (idx, _) in enumerate(hits):
            if idx == self._selected_idx:
                current_pos = pos
                break

        if current_pos == -1:
            self._selected_idx = hits[0][0]
        else:
            next_pos = current_pos + 1
            if next_pos < len(hits):
                self._selected_idx = hits[next_pos][0]
            else:
                self._selected_idx = -1

        if self._selected_idx >= 0:
            self.selection_changed.emit(self)
        self.update()

    def mouseMoveEvent(self, event) -> None:
        pos = event.position()

        # Active drag
        if self._drag_mode and self._drag_start_px and self._drag_orig_pts:
            zf = self._zoom_factor
            dx_pts = (pos.x() - self._drag_start_px.x()) / zf
            dy_pts = (pos.y() - self._drag_start_px.y()) / zf
            x0, y0, x1, y1 = self._drag_orig_pts

            if self._drag_mode == "move":
                new_pts = (x0 + dx_pts, y0 + dy_pts, x1 + dx_pts, y1 + dy_pts)
            else:  # resize
                h = self._drag_handle
                nx0, ny0, nx1, ny1 = x0, y0, x1, y1
                # Adjust edges based on handle
                if h in (0, 6, 7):   # left edge
                    nx0 = x0 + dx_pts
                if h in (2, 3, 4):   # right edge
                    nx1 = x1 + dx_pts
                if h in (0, 1, 2):   # top edge
                    ny0 = y0 + dy_pts
                if h in (4, 5, 6):   # bottom edge
                    ny1 = y1 + dy_pts
                # Enforce minimum size (5 pts)
                if nx1 - nx0 < 5:
                    nx1 = nx0 + 5
                if ny1 - ny0 < 5:
                    ny1 = ny0 + 5
                new_pts = (nx0, ny0, nx1, ny1)

            bbox = self._bboxes_pts[self._selected_idx]
            bbox["pts"] = new_pts
            # Update label with current dimensions
            btype = bbox.get("type", "unknown")
            bid = bbox.get("id", "")
            w = abs(new_pts[2] - new_pts[0])
            h = abs(new_pts[3] - new_pts[1])
            if btype != "table" and btype != "text":
                bbox["label"] = f"{bid} {btype} {w:.0f}x{h:.0f}pt"
            self.update()
            return

        # Hover cursor changes
        handle = self._hit_handle(pos)
        if handle >= 0:
            self.setCursor(QCursor(self._HANDLE_CURSORS[handle]))
            return

        # Check if hovering over selected bbox → move cursor
        if self._selected_idx >= 0 and self._selected_idx < len(self._bboxes_pts):
            bbox = self._bboxes_pts[self._selected_idx]
            r = self._bbox_rect_px(bbox)
            if r and r.contains(pos):
                self.setCursor(QCursor(Qt.CursorShape.SizeAllCursor))
                return

        self.unsetCursor()

    def mouseReleaseEvent(self, event) -> None:
        if self._drag_mode and self._selected_idx >= 0:
            bbox = self._bboxes_pts[self._selected_idx]
            obj_id = bbox.get("id", "")
            if obj_id:
                self.bbox_modified.emit(obj_id, tuple(bbox["pts"]))
        self._drag_mode = ""
        self._drag_handle = -1
        self._drag_start_px = None
        self._drag_orig_pts = None

    def paintEvent(self, event) -> None:
        if not self._pixmap:
            return
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        h = self.HEADER_HEIGHT
        zf = self._zoom_factor

        # Draw stats header
        if self._page_stats:
            painter.setBrush(QColor(0, 0, 0, 160))
            painter.setPen(Qt.PenStyle.NoPen)
            painter.drawRect(QRectF(0, 0, self._pixmap.width(), h))

            font = QFont("Consolas", 9)
            painter.setFont(font)
            painter.setPen(QColor(255, 255, 255))
            painter.drawText(int(6), int(h - 7), self._page_stats)

        # Draw page image below header
        painter.drawPixmap(0, h, self._pixmap)

        # Mask hidden objects — solid fill over PDF content, always active
        for i, bbox in enumerate(self._bboxes_pts):
            if not bbox.get("hidden", False):
                continue
            r = self._bbox_rect_px(bbox)
            if not r:
                continue
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(QColor(245, 245, 245))
            painter.drawRect(r)
            if self._show_hidden:
                # Show hidden indicator: dashed border + label
                pen = QPen(QColor(158, 158, 158, 160), 2, Qt.PenStyle.DashLine)
                painter.setBrush(Qt.BrushStyle.NoBrush)
                painter.setPen(pen)
                painter.drawRect(r)
                painter.setPen(QColor(120, 120, 120))
                font = QFont("Consolas", 9)
                painter.setFont(font)
                label = bbox.get("label", bbox.get("type", ""))
                painter.drawText(int(r.x() + 3), int(r.y() + 14), f"[hidden] {label}")
                # Selected accent border + handles
                if i == self._selected_idx:
                    accent = QPen(QColor(255, 255, 0, 240), 3)
                    painter.setBrush(Qt.BrushStyle.NoBrush)
                    painter.setPen(accent)
                    painter.drawRect(r.adjusted(-3, -3, 3, 3))
                    painter.setPen(QPen(QColor(80, 80, 80), 1))
                    painter.setBrush(QColor(255, 255, 255))
                    for hr in self._handle_rects(r):
                        painter.drawRect(hr)

        if self._show_bboxes and self._bboxes_pts:
            layers = self._visible_layers
            show_bbox = layers.get("bbox", True)

            # Layer 1: Bounding boxes — gray frame + semi-transparent gray fill
            # "Bounding Box" toggle controls this layer for ALL objects
            if show_bbox:
                bbox_fill = QColor(160, 160, 160, 40)
                bbox_border_color = QColor(130, 130, 130, 200)
                bbox_pen = QPen(bbox_border_color, 3)
                for bbox in self._bboxes_pts:
                    if bbox.get("hidden", False):
                        continue
                    r = self._bbox_rect_px(bbox)
                    if not r:
                        continue
                    painter.setBrush(bbox_fill)
                    painter.setPen(bbox_pen)
                    painter.drawRect(r)
                    # Label
                    painter.setPen(QPen(bbox_border_color, 1))
                    label = bbox.get("label", bbox.get("type", ""))
                    painter.drawText(int(r.x() + 3), int(r.y() + 14), label)

            # Layer 2: Content-type visualization (colored overlays, table grids)
            for i, bbox in enumerate(self._bboxes_pts):
                if bbox.get("hidden", False):
                    continue

                bbox_type = bbox.get("type", "unknown")
                r = self._bbox_rect_px(bbox)
                if not r:
                    continue

                show_content = layers.get(bbox_type, True)
                if show_content:
                    border = BBOX_BORDER_COLORS.get(bbox_type, BBOX_BORDER_COLORS["unknown"])
                    painter.setBrush(Qt.BrushStyle.NoBrush)
                    painter.setPen(QPen(border, 2))
                    painter.drawRect(r)
                    if bbox_type == "table":
                        self._paint_table_grid(painter, bbox, zf, h)

                # Selected: accent border + resize handles
                if i == self._selected_idx:
                    accent = QPen(QColor(255, 255, 0, 240), 3)
                    painter.setBrush(Qt.BrushStyle.NoBrush)
                    painter.setPen(accent)
                    painter.drawRect(r.adjusted(-3, -3, 3, 3))

                    painter.setPen(QPen(QColor(80, 80, 80), 1))
                    painter.setBrush(QColor(255, 255, 255))
                    for hr in self._handle_rects(r):
                        painter.drawRect(hr)

        painter.end()


    def _paint_table_grid(
        self, painter: QPainter, bbox: dict, zf: float, header_h: int
    ) -> None:
        """Draw exact PDF line segments for table grid."""
        pts = bbox.get("pts")
        if not pts:
            return

        h_segments: list[tuple] = bbox.get("h_segments", [])
        v_segments: list[tuple] = bbox.get("v_segments", [])

        if not h_segments and not v_segments:
            return

        line_pen = QPen(QColor(255, 255, 0, 220), 2)
        painter.setPen(line_pen)

        # Draw each horizontal segment exactly as in PDF
        for x0, x1, y in h_segments:
            painter.drawLine(
                int(x0 * zf), int(y * zf + header_h),
                int(x1 * zf), int(y * zf + header_h),
            )

        # Draw each vertical segment exactly as in PDF
        for x, y0, y1 in v_segments:
            painter.drawLine(
                int(x * zf), int(y0 * zf + header_h),
                int(x * zf), int(y1 * zf + header_h),
            )


class PageSpreadWidget(QWidget):
    """Displays a 2-page spread (left + right pages side by side)."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.left_page = PageWidget()
        self.right_page = PageWidget()

        layout.addWidget(self.left_page)
        layout.addWidget(self.right_page)


class PreviewView(QWidget):
    """PDF catalog preview with 2-page spread, zoom, continuous scroll, bounding boxes."""

    def __init__(self) -> None:
        super().__init__()
        self._doc: fitz.Document | None = None
        self._file_path: Path | None = None
        self._page_count = 0
        self._zoom = 1.0
        self._base_dpi = 150
        self._spreads: list[PageSpreadWidget] = []
        # Bboxes stored in PDF points (zoom-independent)
        self._bboxes_cache: dict[int, list[dict]] = {}
        self._stats_cache: dict[int, str] = {}
        self._catalog_meta: dict = {}
        self._show_hidden = False

        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Toolbar
        toolbar = QHBoxLayout()
        toolbar.setContentsMargins(8, 4, 8, 4)

        # File info
        self.file_label = QLabel("No document loaded")
        self.file_label.setStyleSheet("font-weight: bold;")
        toolbar.addWidget(self.file_label)

        toolbar.addStretch()

        # Page navigation
        toolbar.addWidget(QLabel("Page:"))
        self.page_spin = QSpinBox()
        self.page_spin.setMinimum(1)
        self.page_spin.setMaximum(1)
        self.page_spin.valueChanged.connect(self._scroll_to_page)
        toolbar.addWidget(self.page_spin)
        self.page_count_label = QLabel("/ 0")
        toolbar.addWidget(self.page_count_label)

        # Separator
        toolbar.addWidget(self._separator())

        # Zoom controls
        self.zoom_out_btn = QPushButton("-")
        self.zoom_out_btn.setFixedWidth(32)
        self.zoom_out_btn.clicked.connect(lambda: self._set_zoom(self._zoom - 0.1))
        toolbar.addWidget(self.zoom_out_btn)

        self.zoom_slider = QSlider(Qt.Orientation.Horizontal)
        self.zoom_slider.setMinimum(25)
        self.zoom_slider.setMaximum(400)
        self.zoom_slider.setValue(100)
        self.zoom_slider.setFixedWidth(120)
        self.zoom_slider.valueChanged.connect(lambda v: self._set_zoom(v / 100.0))
        toolbar.addWidget(self.zoom_slider)

        self.zoom_in_btn = QPushButton("+")
        self.zoom_in_btn.setFixedWidth(32)
        self.zoom_in_btn.clicked.connect(lambda: self._set_zoom(self._zoom + 0.1))
        toolbar.addWidget(self.zoom_in_btn)

        self.zoom_label = QLabel("100%")
        self.zoom_label.setFixedWidth(45)
        toolbar.addWidget(self.zoom_label)

        self.fit_btn = QPushButton("Fit Width")
        self.fit_btn.clicked.connect(self._fit_to_width)
        toolbar.addWidget(self.fit_btn)

        # Separator
        toolbar.addWidget(self._separator())

        self.bbox_filter_btn = QToolButton()
        self.bbox_filter_btn.setText("Show ▾")
        self.bbox_filter_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        filter_menu = QMenu(self.bbox_filter_btn)
        self.bbox_filter_actions: dict[str, QAction] = {}
        for label, type_key in [("Bounding Box", "bbox"),
                                ("Tables", "table"), ("Text", "text"),
                                ("Photos", "photo"), ("Pictures", "picture"),
                                ("Drawings", "drawing")]:
            if type_key == "bbox":
                act = QAction(label, filter_menu)
                act.setCheckable(True)
                act.setChecked(True)
                act.toggled.connect(self._on_bbox_filter_changed)
                filter_menu.addAction(act)
                self.bbox_filter_actions[type_key] = act
                filter_menu.addSeparator()
            else:
                act = QAction(label, filter_menu)
                act.setCheckable(True)
                act.setChecked(True)
                act.toggled.connect(self._on_bbox_filter_changed)
                filter_menu.addAction(act)
                self.bbox_filter_actions[type_key] = act
        self.bbox_filter_btn.setMenu(filter_menu)
        toolbar.addWidget(self.bbox_filter_btn)

        self.clear_bbox_btn = QPushButton("Clear && Re-detect")
        self.clear_bbox_btn.clicked.connect(self._clear_and_redetect)
        toolbar.addWidget(self.clear_bbox_btn)

        layout.addLayout(toolbar)

        # Scroll area for continuous pages
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
        self.scroll_area.verticalScrollBar().valueChanged.connect(self._on_scroll)

        self.pages_container = QWidget()
        self.pages_layout = QVBoxLayout(self.pages_container)
        self.pages_layout.setAlignment(Qt.AlignmentFlag.AlignHCenter)
        self.pages_layout.setSpacing(16)
        self.pages_layout.setContentsMargins(0, 8, 0, 8)

        self.scroll_area.setWidget(self.pages_container)
        layout.addWidget(self.scroll_area)

    def load_document(self, file_path: Path) -> None:
        """Load a PDF document for preview."""
        if self._doc:
            self._doc.close()

        self._file_path = file_path
        self._bboxes_cache.clear()
        self._stats_cache.clear()
        self._catalog_meta = load_meta(file_path)

        try:
            self._doc = fitz.open(str(file_path))
            self._page_count = len(self._doc)
        except Exception as e:
            logger.error("Failed to open PDF: %s", e)
            self.file_label.setText(f"Error: {e}")
            return

        self.file_label.setText(f"{file_path.name} ({self._page_count} pages)")
        self.page_spin.setMaximum(self._page_count)
        self.page_spin.setValue(1)
        self.page_count_label.setText(f"/ {self._page_count}")

        self._fit_to_width()
        if True:
            self._detect_visible_pages()
        logger.info("Loaded document: %s (%d pages)", file_path.name, self._page_count)

    def _current_zoom_factor(self) -> float:
        """Current points-to-pixels conversion factor."""
        return self._base_dpi * self._zoom / 72.0

    def _render_all_spreads(self) -> None:
        """Render all page spreads (2 pages per row)."""
        # Clear existing
        for spread in self._spreads:
            self.pages_layout.removeWidget(spread)
            spread.deleteLater()
        self._spreads.clear()

        if not self._doc:
            return

        zf = self._current_zoom_factor()
        dpi = zf * 72.0  # keep exact same factor for pixmap and bboxes

        # Create spreads: pages 0-1, 2-3, 4-5, etc.
        page_idx = 0
        while page_idx < self._page_count:
            spread = PageSpreadWidget()

            # Left page
            left_pixmap = self._render_page(page_idx, dpi)
            spread.left_page._page_index = page_idx
            spread.left_page.set_pixmap(left_pixmap)
            spread.left_page.set_show_bboxes(True)
            spread.left_page.set_show_hidden(self._show_hidden)
            spread.left_page.hide_requested.connect(self._on_hide_object)
            spread.left_page.selection_changed.connect(self._on_page_selection)

            spread.left_page.type_changed.connect(self._on_type_changed)
            spread.left_page.bbox_modified.connect(self._on_bbox_modified)
            if page_idx in self._bboxes_cache:
                spread.left_page.set_bboxes(
                    self._bboxes_cache[page_idx], zf
                )
            if page_idx in self._stats_cache:
                spread.left_page.set_page_stats(self._stats_cache[page_idx])

            # Right page
            if page_idx + 1 < self._page_count:
                right_pixmap = self._render_page(page_idx + 1, dpi)
                spread.right_page._page_index = page_idx + 1
                spread.right_page.set_pixmap(right_pixmap)
                spread.right_page.set_show_bboxes(True)
                spread.right_page.set_show_hidden(self._show_hidden)
                spread.right_page.hide_requested.connect(self._on_hide_object)
                spread.right_page.selection_changed.connect(self._on_page_selection)
                spread.right_page.type_changed.connect(self._on_type_changed)
                spread.right_page.bbox_modified.connect(self._on_bbox_modified)
                if page_idx + 1 in self._bboxes_cache:
                    spread.right_page.set_bboxes(
                        self._bboxes_cache[page_idx + 1], zf
                    )
                if page_idx + 1 in self._stats_cache:
                    spread.right_page.set_page_stats(self._stats_cache[page_idx + 1])
            else:
                # Odd page count — blank right side
                blank = QPixmap(left_pixmap.size())
                blank.fill(QColor(240, 240, 240))
                spread.right_page.set_pixmap(blank)

            # Apply current layer visibility to new spread
            cur_layers = {
                key: act.isChecked()
                for key, act in self.bbox_filter_actions.items()
            }
            spread.left_page.set_visible_layers(cur_layers)
            spread.right_page.set_visible_layers(cur_layers)

            self.pages_layout.addWidget(spread)
            self._spreads.append(spread)
            page_idx += 2

    def _render_page(self, page_num: int, dpi: int) -> QPixmap:
        """Render a single page to QPixmap."""
        page = self._doc[page_num]
        zoom_factor = dpi / 72.0
        mat = fitz.Matrix(zoom_factor, zoom_factor)
        pix = page.get_pixmap(matrix=mat)

        img = QImage(pix.samples, pix.width, pix.height, pix.stride, QImage.Format.Format_RGB888)
        return QPixmap.fromImage(img)

    def _set_zoom(self, zoom: float) -> None:
        """Set zoom level and re-render."""
        zoom = max(0.25, min(4.0, zoom))
        self._zoom = zoom

        self.zoom_slider.blockSignals(True)
        self.zoom_slider.setValue(int(zoom * 100))
        self.zoom_slider.blockSignals(False)

        self.zoom_label.setText(f"{int(zoom * 100)}%")
        self._render_all_spreads()

    def _fit_to_width(self) -> None:
        """Calculate zoom to fit 2-page spread within scroll area width."""
        if not self._doc or self._page_count == 0:
            return

        page = self._doc[0]
        page_width_pt = page.rect.width  # points (72 dpi)
        # Overhead: spread margins (10+10) + page spacing (8) + scrollbar (~18)
        # + PageWidget internal margins/rounding
        scrollbar_w = self.scroll_area.verticalScrollBar().width() if self.scroll_area.verticalScrollBar().isVisible() else 18
        spread_overhead = 10 + 10 + 8 + scrollbar_w + 4  # 4px safety
        available_width = self.scroll_area.viewport().width() - spread_overhead
        two_page_base_px = 2 * (page_width_pt * self._base_dpi / 72.0)
        if two_page_base_px > 0:
            new_zoom = available_width / two_page_base_px
            self._set_zoom(new_zoom)

    def _scroll_to_page(self, page_num: int) -> None:
        """Scroll to show the spread containing the given page."""
        if not self._spreads:
            return
        spread_idx = (page_num - 1) // 2
        if 0 <= spread_idx < len(self._spreads):
            spread = self._spreads[spread_idx]
            self.scroll_area.ensureWidgetVisible(spread, 0, 50)

    def _on_scroll(self) -> None:
        """Update page spinner and trigger lazy bbox detection."""
        if not self._spreads:
            return

        viewport_center_y = (
            self.scroll_area.verticalScrollBar().value()
            + self.scroll_area.viewport().height() // 2
        )

        for i, spread in enumerate(self._spreads):
            spread_y = spread.mapTo(self.pages_container, QPoint(0, 0)).y()
            spread_bottom = spread_y + spread.height()
            if spread_y <= viewport_center_y <= spread_bottom:
                page_num = i * 2 + 1
                self.page_spin.blockSignals(True)
                self.page_spin.setValue(min(page_num, self._page_count))
                self.page_spin.blockSignals(False)
                break

        # Lazy bbox detection on scroll
        if True:
            self._detect_visible_pages()

    def _clear_and_redetect(self) -> None:
        """Clear cached bboxes and saved metadata, then re-detect."""
        self._bboxes_cache.clear()
        self._stats_cache.clear()
        # Clear saved metadata for this document so detection starts fresh
        if self._file_path:
            meta = load_meta(self._file_path)
            meta["objects"] = {}
            save_meta(self._file_path, meta)
            self._catalog_meta = meta
        # Clear bboxes from all page widgets and repaint immediately
        for spread in self._spreads:
            spread.left_page.set_bboxes([], 1.0)
            spread.right_page.set_bboxes([], 1.0)
            spread.left_page.update()
            spread.right_page.update()
        # Force repaint before detection starts
        from PySide6.QtWidgets import QApplication
        QApplication.processEvents()
        # Re-detect
        self._detect_visible_pages()

    def _on_bbox_filter_changed(self, _checked: bool = False) -> None:
        """Update visible layers on all page widgets."""
        layers = {
            key: act.isChecked()
            for key, act in self.bbox_filter_actions.items()
        }
        for spread in self._spreads:
            spread.left_page.set_visible_layers(layers)
            spread.right_page.set_visible_layers(layers)

    def _apply_bboxes_to_spreads(self) -> None:
        """Apply cached bboxes to all spread widgets with current zoom."""
        zf = self._current_zoom_factor()
        for i, spread in enumerate(self._spreads):
            left_idx = i * 2
            right_idx = i * 2 + 1

            if left_idx in self._bboxes_cache:
                spread.left_page.set_bboxes(
                    self._bboxes_cache[left_idx], zf
                )
            if right_idx in self._bboxes_cache:
                spread.right_page.set_bboxes(
                    self._bboxes_cache[right_idx], zf
                )

    def _get_current_page(self) -> int:
        """Return the 0-based page index currently visible."""
        return max(0, self.page_spin.value() - 1)

    def _detect_visible_pages(self) -> None:
        """Detect objects only for visible pages (current ±2 pages).

        Skips pages already in the cache. Updates spread widgets
        for any newly detected pages.
        """
        if not self._doc:
            return

        current = self._get_current_page()
        # Current spread ± 2 pages (1 spread each side)
        start = max(0, current - 2)
        end = min(self._page_count, current + 4)  # +4 to cover 2 pages ahead

        detected_new = False
        for page_num in range(start, end):
            if page_num in self._bboxes_cache:
                continue  # already detected
            try:
                page = self._doc[page_num]
                bboxes, stats = self._detect_native_page(page)
                # Merge with saved metadata (assigns IDs, preserves user edits)
                if self._file_path:
                    bboxes = merge_detected(self._file_path, page_num, bboxes)
                    self._catalog_meta = load_meta(self._file_path)
                self._bboxes_cache[page_num] = bboxes
                self._stats_cache[page_num] = stats
                detected_new = True
                logger.info("Lazy detect page %d: %s", page_num + 1, stats)
            except Exception as e:
                logger.error("Detection failed for page %d: %s", page_num + 1, e)

        if detected_new:
            self._apply_bboxes_to_spreads()

    def _detect_native_page(self, page) -> tuple[list[dict], str]:
        """Detect objects on a page using PyMuPDF.

        Returns bboxes with 'pts' key = (x0, y0, x1, y1) in PDF points.
        """
        bboxes: list[dict] = []
        obj_id = 0

        # 1. Tables via find_tables() — lines-only mode (no header/footer/merges)
        words = page.get_text("words")
        table_fitz_rects: list[fitz.Rect] = []
        try:
            tables = page.find_tables()
            for table in tables.tables:
                tr = fitz.Rect(table.bbox)
                # Grow table bbox to include borderless columns
                y_words = [
                    w for w in words
                    if w[1] >= tr.y0 - 3 and w[3] <= tr.y1 + 3
                ]
                if y_words:
                    tr = fitz.Rect(
                        min(tr.x0, min(w[0] for w in y_words) - 2),
                        tr.y0,
                        max(tr.x1, max(w[2] for w in y_words) + 2),
                        tr.y1,
                    )
                table_fitz_rects.append(tr)

                # Extract raw line segments from PDF drawings
                h_segs, v_segs = self._extract_table_lines(page, tr)

                bboxes.append({
                    "type": "table",
                    "label": f"#{obj_id} table {table.row_count}x{table.col_count}",
                    "pts": (tr.x0, tr.y0, tr.x1, tr.y1),
                    "h_segments": h_segs,  # [(x0, x1, y), ...]
                    "v_segments": v_segs,  # [(x, y0, y1), ...]
                    "row_ys": [],
                    "col_xs": [],
                    "major_row_ys": [],
                    "header_rows": 0,
                    "footer_rows": 0,
                    "mid_headings": [],
                    "header_splits": [],
                    "header_merges": [],
                })
                obj_id += 1
        except Exception:
            pass

        # ── Page segmentation: collect ALL visual elements, cluster into objects ──
        page_rect = page.rect
        page_area = page_rect.width * page_rect.height

        # Exclusion rects: tables + margin
        exclude_rects = [
            fitz.Rect(tr.x0 - 5, tr.y0 - 5, tr.x1 + 5, tr.y1 + 5)
            for tr in table_fitz_rects
        ]

        # Collect all non-table elements: drawings, text words, images
        elements: list[tuple[float, float, float, float]] = []

        # Vector drawings (clip to page, skip fills/frames/strips)
        for d in page.get_drawings():
            r = d.get("rect")
            if not r or (r.width < 1 and r.height < 1):
                continue
            # Skip large filled shapes (decorative backgrounds/bars)
            # but keep small filled elements (dimension arrows, markers)
            if d.get("fill") is not None:
                cr_check = r & page_rect
                if not cr_check.is_empty and cr_check.width * cr_check.height > 500:
                    continue
            cr = r & page_rect
            if cr.is_empty or cr.width < 1 or cr.height < 1:
                continue
            if cr.width * cr.height > page_area * 0.3:
                continue
            aspect = max(cr.width, cr.height) / max(min(cr.width, cr.height), 0.1)
            if aspect > 15 and max(cr.width, cr.height) > 100:
                continue
            nr = fitz.Rect(cr.x0 - 1, cr.y0 - 1, cr.x1 + 1, cr.y1 + 1)
            if any(tr.intersects(nr) for tr in exclude_rects):
                continue
            elements.append((cr.x0, cr.y0, cr.x1, cr.y1))

        # Text words (exclude those inside tables)
        for w in words:
            wr = fitz.Rect(w[0], w[1], w[2], w[3])
            if any(tr.contains(wr) for tr in exclude_rects):
                continue
            elements.append((w[0], w[1], w[2], w[3]))

        # Embedded raster images
        for img_info in page.get_images(full=True):
            xref = img_info[0]
            for rect in page.get_image_rects(xref):
                elements.append((rect.x0, rect.y0, rect.x1, rect.y1))

        # Cluster all elements into objects (gap=5pt)
        object_clusters = self._cluster_rects(elements, gap=5)
        object_clusters = [
            c for c in object_clusters
            if (c[2] - c[0]) > 20 and (c[3] - c[1]) > 20
        ]

        # Each cluster = one bounding box (no type recognition at this stage)
        for c in object_clusters:
            w, h = c[2] - c[0], c[3] - c[1]
            bboxes.append({
                "type": "unknown",
                "label": f"#{obj_id} {w:.0f}x{h:.0f}pt",
                "pts": (c[0], c[1], c[2], c[3]),
            })
            obj_id += 1

        n_tables = sum(1 for b in bboxes if b["type"] == "table")
        n_text = sum(1 for b in bboxes if b["type"] == "text")
        n_photos = sum(1 for b in bboxes if b["type"] == "photo")
        n_pictures = sum(1 for b in bboxes if b["type"] == "picture")
        n_drawings = sum(1 for b in bboxes if b["type"] == "drawing")

        stats = (
            f"P{page.number + 1}  |  "
            f"T:{n_tables}  Txt:{n_text}  "
            f"Ph:{n_photos}  Pic:{n_pictures}  Drw:{n_drawings}  "
            f"Total:{len(bboxes)}"
        )

        return bboxes, stats

    @staticmethod
    def _classify_raster(doc, xref: int, img_w: int, img_h: int) -> str:
        """Classify embedded raster: 'drawing', 'picture', or 'photo'.

        - drawing: B/W line art, 1-2 colors, masks, icons
        - picture: color illustration with limited palette
        - photo: real photograph with continuous tones
        """
        try:
            img_dict = doc.extract_image(xref)
            if not img_dict:
                return "photo"

            colorspace = img_dict.get("colorspace", 0)  # components count
            bpc = img_dict.get("bpc", 8)  # bits per component
            is_gray = colorspace <= 1

            # 1-bit images are always drawings (line art, masks)
            if bpc <= 1:
                return "drawing"

            # Very small images — icons/logos → drawing
            if img_w < 64 and img_h < 64:
                return "drawing"

            # Analyze actual pixel data via sampling
            image_bytes = img_dict.get("image")
            if not image_bytes:
                return "photo"

            n_components = max(colorspace, 1)
            stride = n_components * (bpc // 8 or 1)
            total_pixels = len(image_bytes) // stride if stride > 0 else 0

            if total_pixels == 0:
                return "photo"

            # Sample up to 2000 evenly-spaced pixels
            sample_count = min(total_pixels, 2000)
            step = max(total_pixels // sample_count, 1)
            unique_colors: set[bytes] = set()
            for i in range(0, total_pixels, step):
                offset = i * stride
                pixel = image_bytes[offset:offset + stride]
                unique_colors.add(pixel)
                if len(unique_colors) > 500:
                    break  # clearly a photo, stop early

            n_unique = len(unique_colors)

            # B/W drawing: grayscale with very few colors
            if n_unique <= 8:
                return "drawing"
            # Grayscale with moderate colors — still drawing
            if is_gray and n_unique <= 32:
                return "drawing"
            # Color but limited palette → picture (illustration)
            if n_unique <= 200:
                return "picture"
            # High diversity → photo
            return "photo"
        except Exception:
            return "photo"

    def _detect_header_footer(
        self, table, row_ys: list[float], col_xs: list[float],
        page=None,
    ) -> tuple[int, int, list[int]]:
        """Detect header/footer using text style patterns (font, bold, content type).

        For each row in row_ys, builds a style signature:
        - bold_ratio: fraction of bold spans
        - numeric_ratio: fraction of numeric-content spans
        - font set

        Header rows: high bold ratio, low numeric ratio (column names).
        Data rows: low bold ratio, high numeric ratio (values).
        Footer rows: may differ in style from data (totals, notes).

        Falls back to cell-border analysis if font info unavailable.

        Returns (header_row_count, footer_row_count, mid_headings).
        """
        if len(row_ys) < 4:
            return (0, 0, [])

        n_rows = len(row_ys) - 1

        # Get text spans with font info from page
        spans: list[dict] = []
        if page is not None:
            try:
                blocks = page.get_text("dict")["blocks"]
                for b in blocks:
                    if b["type"] != 0:
                        continue
                    for line in b["lines"]:
                        for span in line["spans"]:
                            bb = span["bbox"]
                            text = span["text"].strip()
                            if not text:
                                continue
                            spans.append({
                                "x0": bb[0], "y0": bb[1],
                                "x1": bb[2], "y1": bb[3],
                                "bold": bool(span["flags"] & 16),
                                "font": span["font"],
                                "size": span["size"],
                                "color": span["color"],
                                "numeric": any(c.isdigit() for c in text),
                            })
            except Exception:
                pass

        # Filter spans to table region
        table_bbox = table.bbox
        table_spans = [
            s for s in spans
            if s["y0"] >= table_bbox[1] - 2 and s["y1"] <= table_bbox[3] + 2
            and s["x0"] >= table_bbox[0] - 5 and s["x1"] <= table_bbox[2] + 5
        ]

        if not table_spans:
            return (0, 0, [])

        # Build style signature per row
        row_styles: list[dict] = []
        for ri in range(n_rows):
            ry0, ry1 = row_ys[ri], row_ys[ri + 1]
            rs = [s for s in table_spans if ry0 - 1 <= (s["y0"] + s["y1"]) / 2 <= ry1 + 1]
            if not rs:
                row_styles.append({"bold": 0.0, "numeric": 0.0, "n": 0,
                                   "fonts": frozenset()})
                continue
            n = len(rs)
            bold_r = sum(1 for s in rs if s["bold"]) / n
            num_r = sum(1 for s in rs if s["numeric"]) / n
            fonts = frozenset(s["font"] for s in rs)
            row_styles.append({"bold": bold_r, "numeric": num_r, "n": n,
                               "fonts": fonts})

        # Find majority data pattern by looking at the most common
        # (span_count, font_set, numeric_ratio_bucket) signature.
        # Data rows repeat with same structure; header rows are unique.
        from collections import Counter

        sigs = []
        for i, rs in enumerate(row_styles):
            if rs["n"] == 0:
                sigs.append(None)
                continue
            # Quantize: span count, numeric ratio to 10% bucket, font set
            sig = (rs["n"], round(rs["numeric"] * 10), rs["fonts"])
            sigs.append(sig)

        # Count signatures — most common = data pattern
        sig_counter = Counter(s for s in sigs if s is not None)
        if not sig_counter:
            return (0, 0, [])

        data_sig, data_freq = sig_counter.most_common(1)[0]
        data_n, data_num_bucket, data_fonts = data_sig

        # A row matches data pattern if:
        # - similar span count (±30%)
        # - similar numeric ratio (±2 buckets)
        # - same or overlapping font set
        def is_data_row(rs: dict) -> bool:
            if rs["n"] == 0:
                return False
            n_ok = abs(rs["n"] - data_n) <= max(3, data_n * 0.3)
            num_ok = abs(round(rs["numeric"] * 10) - data_num_bucket) <= 2
            font_ok = bool(rs["fonts"] & data_fonts)  # any overlap
            return n_ok and num_ok and font_ok

        data_indices = [i for i in range(n_rows) if is_data_row(row_styles[i])]

        if not data_indices:
            return (0, 0, [])

        # Header: consecutive non-data rows from top
        first_data = data_indices[0]
        header_count = first_data

        # Footer: consecutive non-data rows from bottom
        last_data = data_indices[-1]
        footer_count = n_rows - 1 - last_data

        # Intermediate headings: non-data rows between header and footer
        data_set = set(data_indices)
        mid_headings: list[int] = []
        for i in range(header_count, n_rows - footer_count):
            if i not in data_set and row_styles[i]["n"] > 0:
                mid_headings.append(i)

        return (header_count, footer_count, mid_headings)

    @staticmethod
    def _detect_text_rows(words: list, table_rect: "fitz.Rect") -> list[float]:
        """Detect row boundaries from word Y positions within a table region.

        Returns sorted list of Y coordinates (top of each row + bottom of last).
        Detects individual data rows by word baseline gaps, not grid lines.
        """
        # Words in table region
        tw = [
            w for w in words
            if w[1] >= table_rect.y0 - 2 and w[3] <= table_rect.y1 + 2
            and w[0] >= table_rect.x0 - 5 and w[2] <= table_rect.x1 + 5
        ]
        if not tw:
            return []

        # Collect all word top-Y values, sort
        y_tops = sorted(w[1] for w in tw)
        y_bottoms = sorted(w[3] for w in tw)

        if not y_tops:
            return []

        # Group words into rows by Y proximity (words within 3pt = same row)
        row_groups: list[list[float]] = [[y_tops[0]]]
        for y in y_tops[1:]:
            if y - row_groups[-1][-1] < 3:
                row_groups[-1].append(y)
            else:
                row_groups.append([y])

        # Row boundaries: midpoints between consecutive row groups
        row_centers = [sum(g) / len(g) for g in row_groups]
        row_ys: list[float] = []

        # Top of first row
        first_top = min(w[1] for w in tw)
        row_ys.append(first_top - 1)

        # Midpoints between consecutive rows
        for i in range(len(row_centers) - 1):
            # Find bottom of current row words and top of next row words
            curr_bottoms = [w[3] for w in tw if abs(w[1] - row_centers[i]) < 3]
            next_tops = [w[1] for w in tw if abs(w[1] - row_centers[i + 1]) < 3]
            if curr_bottoms and next_tops:
                mid = (max(curr_bottoms) + min(next_tops)) / 2
            else:
                mid = (row_centers[i] + row_centers[i + 1]) / 2
            row_ys.append(mid)

        # Bottom of last row
        last_bottom = max(w[3] for w in tw)
        row_ys.append(last_bottom + 1)

        return row_ys

    @staticmethod
    def _detect_header_splits(
        page, hdr_y0: float, hdr_y1: float, col_xs: list[float],
    ) -> tuple[list[tuple[float, float, float]], list[tuple[float, float, float, float]]]:
        """Find horizontal lines inside header cells and detect spanning.

        1. Find h-lines that split header cells into sub-rows.
        2. For each sub-row created, check column separators for vertical
           borders. Where no vertical border exists, adjacent cells are
           merged (spanning).

        Returns:
          splits: list of (x0, x1, y) — horizontal line segments per cell
          merges: list of (x0, x1, y0, y1) — merged cell ranges where
                  column lines should be suppressed
        """
        if not col_xs:
            return [], []

        drawings = page.get_drawings()

        # Collect horizontal lines inside header zone
        h_lines: list[tuple[float, float, float]] = []
        for d in drawings:
            r = d.get("rect")
            if not r:
                continue
            if r.height < 2 and r.width > 10:
                if hdr_y0 + 1 < r.y0 < hdr_y1 - 1:
                    h_lines.append((r.x0, r.x1, r.y0))

        if not h_lines:
            return [], []

        # Detect splits: which column cells have h-lines through them
        splits: list[tuple[float, float, float]] = []
        for i in range(len(col_xs) - 1):
            cx0, cx1 = col_xs[i], col_xs[i + 1]
            if cx1 - cx0 < 5:
                continue
            for lx0, lx1, ly in h_lines:
                if lx0 <= cx0 + 3 and lx1 >= cx1 - 3:
                    splits.append((cx0, cx1, ly))

        if not splits:
            return [], []

        # Collect vertical lines in header for border checking
        v_lines: list[tuple[float, float, float]] = []  # (x, y0, y1)
        for d in drawings:
            r = d.get("rect")
            if not r:
                continue
            if r.width < 2 and r.height > 3:
                if r.y0 < hdr_y1 and r.y1 > hdr_y0:
                    v_lines.append((r.x0, r.y0, r.y1))

        # For each unique split Y, find sub-rows and detect merges
        split_ys = sorted(set(s[2] for s in splits))
        merges: list[tuple[float, float, float, float]] = []

        for sy in split_ys:
            # Which columns are split at this Y?
            split_cols: set[int] = set()
            for s in splits:
                if s[2] == sy:
                    for i in range(len(col_xs) - 1):
                        if abs(col_xs[i] - s[0]) < 5:
                            split_cols.add(i)

            if not split_cols:
                continue

            # Top sub-row: y = hdr_y0 to sy
            # Check each column separator within split zone for vertical border
            min_sc = min(split_cols)
            max_sc = max(split_cols) + 1  # +1 because split_cols are left-edge indices

            # For each separator between split cells in top sub-row,
            # check if vertical border exists in y range [hdr_y0, sy]
            no_border_seps: list[int] = []
            for ci in range(min_sc + 1, max_sc):
                cx = col_xs[ci]
                has_vline = any(
                    abs(vx - cx) < 3 and vy0 <= hdr_y0 + 3 and vy1 >= sy - 3
                    for vx, vy0, vy1 in v_lines
                )
                if not has_vline:
                    no_border_seps.append(ci)

            # Build merge ranges from consecutive no-border separators
            if no_border_seps:
                # Find runs of consecutive indices to build merge spans
                runs: list[list[int]] = [[no_border_seps[0]]]
                for ci in no_border_seps[1:]:
                    if ci == runs[-1][-1] + 1:
                        runs[-1].append(ci)
                    else:
                        runs.append([ci])

                for run in runs:
                    # Merge from column before first no-border sep
                    # to column after last no-border sep (inclusive)
                    merge_x0 = col_xs[run[0] - 1] if run[0] > 0 else col_xs[0]
                    merge_x1 = col_xs[run[-1] + 1] if run[-1] + 1 < len(col_xs) else col_xs[-1]
                    merges.append((merge_x0, merge_x1, hdr_y0, sy))

        return splits, merges

    @staticmethod
    def _detect_vertical_borders(
        page, table_rect: "fitz.Rect",
    ) -> list[float]:
        """Find vertical border lines from PDF drawing path segments.

        Extracts individual 'l' (line) items from all drawings,
        keeps vertical segments (|dx| < 2) within table bounds.
        Clusters by X, keeps clusters with total height >= 30% of table.
        Returns X positions including table edges.
        """
        tr = table_rect
        table_h = tr.y1 - tr.y0
        min_h = table_h * 0.3

        v_segs: list[tuple[float, float, float]] = []  # (x, y_min, y_max)
        for d in page.get_drawings():
            for item in d.get("items", []):
                if item[0] != "l":
                    continue
                p1, p2 = item[1], item[2]
                dx = abs(p1.x - p2.x)
                dy = abs(p1.y - p2.y)
                if dx >= 2 or dy < 3:
                    continue
                x = (p1.x + p2.x) / 2
                y0, y1 = min(p1.y, p2.y), max(p1.y, p2.y)
                if x < tr.x0 - 5 or x > tr.x1 + 5:
                    continue
                if y0 > tr.y1 + 5 or y1 < tr.y0 - 5:
                    continue
                v_segs.append((x, y0, y1))

        if not v_segs:
            return []

        # Cluster by X (within 3pt)
        v_segs.sort(key=lambda s: s[0])
        clusters: list[list[tuple[float, float, float]]] = [[v_segs[0]]]
        for seg in v_segs[1:]:
            if seg[0] - clusters[-1][-1][0] < 3:
                clusters[-1].append(seg)
            else:
                clusters.append([seg])

        result: list[float] = []
        for cluster in clusters:
            total_h = sum(s[2] - s[1] for s in cluster)
            if total_h >= min_h:
                result.append(sum(s[0] for s in cluster) / len(cluster))

        return result

    @staticmethod
    def _extract_table_lines(
        page, table_rect: "fitz.Rect",
    ) -> tuple[list[tuple[float, float, float]], list[tuple[float, float, float]]]:
        """Extract exact horizontal and vertical line segments from PDF drawings.

        Returns:
          h_segments: list of (x0, x1, y) — horizontal line segments
          v_segments: list of (x, y0, y1) — vertical line segments

        Filters out drawing/icon noise by grouping segments by position
        and only keeping positions with significant total coverage.
        """
        from collections import defaultdict

        tr = table_rect
        table_w = tr.x1 - tr.x0
        table_h = tr.y1 - tr.y0

        # Collect all raw segments
        raw_h: dict[float, list[tuple[float, float]]] = defaultdict(list)
        raw_v: dict[float, list[tuple[float, float]]] = defaultdict(list)

        for d in page.get_drawings():
            for item in d.get("items", []):
                if item[0] != "l":
                    continue
                p1, p2 = item[1], item[2]
                dx = abs(p1.x - p2.x)
                dy = abs(p1.y - p2.y)

                # Horizontal segment
                if dy < 2 and dx > 5:
                    y = (p1.y + p2.y) / 2
                    if y < tr.y0 - 5 or y > tr.y1 + 5:
                        continue
                    x0, x1 = min(p1.x, p2.x), max(p1.x, p2.x)
                    if x0 > tr.x1 + 5 or x1 < tr.x0 - 5:
                        continue
                    ry = round(y * 2) / 2
                    raw_h[ry].append((x0, x1))

                # Vertical segment
                elif dx < 2 and dy > 3:
                    x = (p1.x + p2.x) / 2
                    if x < tr.x0 - 5 or x > tr.x1 + 5:
                        continue
                    y0, y1 = min(p1.y, p2.y), max(p1.y, p2.y)
                    if y0 > tr.y1 + 5 or y1 < tr.y0 - 5:
                        continue
                    rx = round(x * 2) / 2
                    raw_v[rx].append((y0, y1))

        # Filter H: keep Y positions where longest segment >= 10% of table width
        h_segs: list[tuple[float, float, float]] = []
        for ry in sorted(raw_h.keys()):
            segs = raw_h[ry]
            max_w = max(s[1] - s[0] for s in segs)
            if max_w >= table_w * 0.1:
                for x0, x1 in segs:
                    h_segs.append((x0, x1, ry))

        # Filter V: cluster nearby X (within 3pt), keep tight clusters
        # (spread < 5pt) where total height >= 30% of table height
        v_segs: list[tuple[float, float, float]] = []
        sorted_rxs = sorted(raw_v.keys())
        if sorted_rxs:
            x_clusters: list[list[float]] = [[sorted_rxs[0]]]
            for rx in sorted_rxs[1:]:
                if rx - x_clusters[-1][-1] < 3:
                    x_clusters[-1].append(rx)
                else:
                    x_clusters.append([rx])

            for cluster_xs in x_clusters:
                # Reject wide clusters — real borders are tight
                if cluster_xs[-1] - cluster_xs[0] > 5:
                    continue
                all_segs = []
                for rx in cluster_xs:
                    all_segs.extend(raw_v[rx])
                total_h = sum(s[1] - s[0] for s in all_segs)
                if total_h >= table_h * 0.3:
                    for rx in cluster_xs:
                        for y0, y1 in raw_v[rx]:
                            v_segs.append((rx, y0, y1))

        return h_segs, v_segs

    @staticmethod
    def _detect_horizontal_borders(
        page, table_rect: "fitz.Rect",
    ) -> list[float]:
        """Find horizontal border lines from PDF drawing path segments.

        Extracts individual 'l' (line) items from all drawings,
        keeps horizontal segments (|dy| < 2, width > 5) within table bounds.
        Groups by Y, sums segment widths, keeps Y where coverage >= 50%.
        """
        from collections import defaultdict

        tr = table_rect
        table_w = tr.x1 - tr.x0

        y_segs: dict[float, list[tuple[float, float]]] = defaultdict(list)
        for d in page.get_drawings():
            for item in d.get("items", []):
                if item[0] != "l":
                    continue
                p1, p2 = item[1], item[2]
                dx = abs(p1.x - p2.x)
                dy = abs(p1.y - p2.y)
                if dy >= 2 or dx < 5:
                    continue
                y = (p1.y + p2.y) / 2
                if y < tr.y0 - 5 or y > tr.y1 + 5:
                    continue
                x0, x1 = min(p1.x, p2.x), max(p1.x, p2.x)
                if x0 > tr.x1 + 5 or x1 < tr.x0 - 5:
                    continue
                ry = round(y * 2) / 2
                y_segs[ry].append((x0, x1))

        if not y_segs:
            return []

        result_ys: list[float] = []
        for ry in sorted(y_segs.keys()):
            segs = y_segs[ry]
            total_w = sum(s[1] - s[0] for s in segs)
            if total_w >= table_w * 0.2:
                result_ys.append(ry)

        if not result_ys:
            return []

        # Cluster nearby Y values (within 3pt)
        clusters: list[list[float]] = [[result_ys[0]]]
        for y in result_ys[1:]:
            if y - clusters[-1][-1] < 3:
                clusters[-1].append(y)
            else:
                clusters.append([y])

        return [sum(c) / len(c) for c in clusters]

    @staticmethod
    def _detect_text_columns(words: list, table_rect: "fitz.Rect") -> list[float]:
        """Detect column separators from text alignment within a table region.

        Only keeps separators that appear consistently across multiple rows
        (at least 30% of rows must have a gap at that position).
        """
        from collections import defaultdict

        # Words in table X+Y range
        tw = [
            w for w in words
            if w[1] >= table_rect.y0 - 5 and w[3] <= table_rect.y1 + 5
            and w[0] >= table_rect.x0 - 5 and w[2] <= table_rect.x1 + 5
        ]
        if not tw:
            return []

        # Group words by row (quantize Y)
        rows: dict[int, list] = defaultdict(list)
        for w in tw:
            row_y = round(w[1] / 5) * 5
            rows[row_y].append(w)

        n_rows = len(rows)
        if n_rows == 0:
            return []

        # Find gaps between consecutive words in each row,
        # tracking which row each gap came from
        all_gaps: list[tuple[float, int]] = []  # (gap_x, row_key)
        for row_y, row_words in rows.items():
            sorted_w = sorted(row_words, key=lambda w: w[0])
            for i in range(len(sorted_w) - 1):
                gap_start = sorted_w[i][2]
                gap_end = sorted_w[i + 1][0]
                if gap_end - gap_start > 3:
                    all_gaps.append(((gap_start + gap_end) / 2, row_y))

        if not all_gaps:
            return []

        # Cluster gap positions (within 15pt = same separator)
        all_gaps.sort(key=lambda g: g[0])
        clusters: list[list[tuple[float, int]]] = [[all_gaps[0]]]
        for g in all_gaps[1:]:
            if g[0] - clusters[-1][-1][0] < 15:
                clusters[-1].append(g)
            else:
                clusters.append([g])

        # Keep only separators that appear in >= 30% of rows
        min_row_count = max(2, n_rows * 0.3)
        col_seps: list[float] = []
        for cluster in clusters:
            unique_rows = len(set(g[1] for g in cluster))
            if unique_rows >= min_row_count:
                avg_x = sum(g[0] for g in cluster) / len(cluster)
                col_seps.append(avg_x)

        return col_seps

    @staticmethod
    def _grow_bbox(
        seed: list[float],
        all_rects: list[tuple[float, float, float, float]],
        margin: float = 2,
    ) -> list[float]:
        """Grow a bbox outward from seed until margin px free space on all sides.

        Starts from seed bbox, iteratively includes any drawing element
        within `margin` of the current border, expanding the bbox.
        Stops when no more elements are within margin on any side.
        """
        x0, y0, x1, y1 = seed
        for _ in range(500):
            grown = False
            for r in all_rects:
                # Check if element is within margin of current bbox border
                if (r[0] <= x1 + margin and r[2] >= x0 - margin
                        and r[1] <= y1 + margin and r[3] >= y0 - margin):
                    # Element touches or overlaps — expand if needed
                    if r[0] < x0 or r[1] < y0 or r[2] > x1 or r[3] > y1:
                        x0 = min(x0, r[0])
                        y0 = min(y0, r[1])
                        x1 = max(x1, r[2])
                        y1 = max(y1, r[3])
                        grown = True
            if not grown:
                break
        return [x0, y0, x1, y1]

    @staticmethod
    def _cluster_rects(
        rects: list[tuple[float, float, float, float]], gap: float = 5
    ) -> list[list[float]]:
        """Merge overlapping/nearby rectangles into clusters."""
        if not rects:
            return []
        clusters = [list(rects[0])]
        for r in rects[1:]:
            merged = False
            for c in clusters:
                if (r[0] <= c[2] + gap and r[2] >= c[0] - gap
                        and r[1] <= c[3] + gap and r[3] >= c[1] - gap):
                    c[0] = min(c[0], r[0])
                    c[1] = min(c[1], r[1])
                    c[2] = max(c[2], r[2])
                    c[3] = max(c[3], r[3])
                    merged = True
                    break
            if not merged:
                clusters.append(list(r))
        # Re-merge until stable
        changed = True
        while changed:
            changed = False
            new_clusters: list[list[float]] = []
            used: set[int] = set()
            for i, a in enumerate(clusters):
                if i in used:
                    continue
                for j, b in enumerate(clusters):
                    if j <= i or j in used:
                        continue
                    if (a[0] <= b[2] + gap and a[2] >= b[0] - gap
                            and a[1] <= b[3] + gap and a[3] >= b[1] - gap):
                        a[0] = min(a[0], b[0])
                        a[1] = min(a[1], b[1])
                        a[2] = max(a[2], b[2])
                        a[3] = max(a[3], b[3])
                        used.add(j)
                        changed = True
                new_clusters.append(a)
            clusters = new_clusters
        return clusters

    def _separator(self) -> QLabel:
        sep = QLabel("|")
        sep.setStyleSheet("color: #ccc; margin: 0 4px;")
        return sep

    def wheelEvent(self, event: QWheelEvent) -> None:
        """Ctrl+Wheel to zoom."""
        if event.modifiers() & Qt.KeyboardModifier.ControlModifier:
            delta = event.angleDelta().y()
            step = 0.1 if delta > 0 else -0.1
            self._set_zoom(self._zoom + step)
            event.accept()
        else:
            super().wheelEvent(event)

    def _on_page_selection(self, source_page: PageWidget) -> None:
        """Clear selection on all pages except the one that just selected."""
        for spread in self._spreads:
            for page in (spread.left_page, spread.right_page):
                if page is not source_page and page._selected_idx >= 0:
                    page._selected_idx = -1
                    page.update()

    def _on_hide_object(self, obj_id: str, hidden: bool) -> None:
        """Toggle hidden state on object by ID."""
        if not self._file_path:
            return
        update_object(self._file_path, obj_id, hidden=hidden)
        self._catalog_meta = load_meta(self._file_path)
        # Update in-memory cache
        for bboxes in self._bboxes_cache.values():
            for bbox in bboxes:
                if bbox.get("id") == obj_id:
                    bbox["hidden"] = hidden
        self._apply_bboxes_to_spreads()

    def _on_type_changed(self, obj_id: str, new_type: str) -> None:
        """Change object type by ID."""
        if not self._file_path:
            return
        update_object(self._file_path, obj_id, user_type=new_type, type=new_type)
        self._catalog_meta = load_meta(self._file_path)
        for bboxes in self._bboxes_cache.values():
            for bbox in bboxes:
                if bbox.get("id") == obj_id:
                    bbox["type"] = new_type
                    bbox["user_type"] = new_type
        self._apply_bboxes_to_spreads()

    def _on_bbox_modified(self, obj_id: str, new_pts: tuple) -> None:
        """Update bbox position by ID after move/resize, then re-detect internal structure."""
        if not self._file_path or not self._doc:
            return
        update_object(self._file_path, obj_id, user_pts=list(new_pts), pts=list(new_pts))
        self._catalog_meta = load_meta(self._file_path)

        # Find the bbox in cache and re-detect its internal content
        for page_idx, bboxes in self._bboxes_cache.items():
            for bbox in bboxes:
                if bbox.get("id") != obj_id:
                    continue
                bbox["pts"] = new_pts
                self._redetect_bbox_content(page_idx, bbox)
                self._apply_bboxes_to_spreads()
                return

    def _redetect_bbox_content(self, page_idx: int, bbox: dict) -> None:
        """Re-analyze content inside a bbox after move/resize.

        For tables: re-detect rows, columns, headers, footers.
        For text: re-scan words inside the new bounds.
        For images/drawings: update dimensions label.
        """
        if not self._doc or page_idx >= len(self._doc):
            return

        page = self._doc[page_idx]
        pts = bbox.get("pts", ())
        if len(pts) != 4:
            return
        x0, y0, x1, y1 = pts
        w, h = x1 - x0, y1 - y0
        bbox_type = bbox.get("user_type") or bbox.get("type", "unknown")

        if bbox_type == "table":
            # Re-detect table structure within new bounds
            rect = fitz.Rect(x0, y0, x1, y1)
            try:
                tables = page.find_tables(clip=rect)
                if tables.tables:
                    table = tables.tables[0]
                    tr = table.bbox

                    # Row/column detection
                    words = page.get_text("words")
                    table_words = [
                        w for w in words
                        if fitz.Rect(w[0], w[1], w[2], w[3]).intersects(rect)
                    ]

                    # Vertical borders and column detection
                    v_borders = self._detect_vertical_borders(page, rect)
                    col_xs = sorted(set([x0] + v_borders + [x1]))

                    if not v_borders:
                        text_cols = self._detect_text_columns(table_words, x0, x1)
                        if text_cols:
                            col_xs = sorted(set([x0] + text_cols + [x1]))

                    # Horizontal borders for rows
                    h_borders = self._detect_horizontal_borders(page, rect)
                    text_rows = self._detect_text_rows(table_words, y0, y1)
                    row_ys = sorted(set([y0] + h_borders + text_rows + [y1]))

                    major_row_ys = list(h_borders)

                    # Header/footer detection
                    header_count, footer_count, mid_headings = self._detect_header_footer(
                        table, row_ys, col_xs, page
                    )

                    # Header splits and merges
                    hdr_y0 = row_ys[0]
                    hdr_y1 = row_ys[header_count] if header_count < len(row_ys) else row_ys[-1]
                    header_splits, header_merges = self._detect_header_splits(
                        page, hdr_y0, hdr_y1, col_xs,
                    )

                    bbox["row_ys"] = row_ys
                    bbox["col_xs"] = col_xs
                    bbox["major_row_ys"] = major_row_ys
                    bbox["header_rows"] = header_count
                    bbox["footer_rows"] = footer_count
                    bbox["mid_headings"] = mid_headings
                    bbox["header_splits"] = header_splits
                    bbox["header_merges"] = header_merges
                    n_rows = len(row_ys) - 1
                    n_cols = len(col_xs) - 1
                    bbox["label"] = f"{bbox['id']} table {n_rows}x{n_cols}"
            except Exception as e:
                logger.error("Table re-detection failed: %s", e)

        elif bbox_type == "text":
            # Re-scan words inside bounds
            words = page.get_text("words")
            rect = fitz.Rect(x0, y0, x1, y1)
            inside = [
                w for w in words
                if rect.contains(fitz.Rect(w[0], w[1], w[2], w[3]))
            ]
            if inside:
                text_preview = " ".join(w[4] for w in inside[:4])[:30]
                bbox["label"] = f"{bbox['id']} {text_preview}"

        else:
            # Image/drawing — update dimension label
            bbox["label"] = f"{bbox['id']} {bbox_type} {w:.0f}x{h:.0f}pt"

    def set_show_hidden(self, show: bool) -> None:
        """Toggle visibility of hidden objects."""
        self._show_hidden = show
        for spread in self._spreads:
            spread.left_page.set_show_hidden(show)
            spread.right_page.set_show_hidden(show)

    def refresh(self) -> None:
        pass
