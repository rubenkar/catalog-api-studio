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
from PySide6.QtCore import QPoint, QPointF, QRectF, QThread, Qt, Signal
from PySide6.QtGui import QAction, QColor, QCursor, QFont, QImage, QPainter, QPen, QPixmap, QWheelEvent
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMenu,
    QPushButton,
    QScrollArea,
    QSlider,
    QSpinBox,
    QSplitter,
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
    "template": QColor(200, 0, 200, 40),     # magenta
    "unknown": QColor(158, 158, 158, 80),    # gray
}

BBOX_BORDER_COLORS = {
    "table": QColor(0, 120, 215, 200),
    "text": QColor(76, 175, 80, 160),
    "photo": QColor(255, 87, 34, 160),
    "picture": QColor(255, 152, 0, 160),
    "drawing": QColor(156, 39, 176, 160),
    "template": QColor(200, 0, 200, 160),
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
        self._page_size_pt: tuple[float, float] = (612, 792)  # (width, height) in PDF points
        self._visible_layers: dict[str, bool] = {
            "bbox": True, "table": True, "text": True,
            "photo": True, "picture": True, "drawing": True,
        }

        # Cropping overlays (fractions 0..1)
        self._crop_h_lines: list[float] = []
        self._crop_v_lines: list[float] = []
        self._crop_boxes: list[list[float]] = []

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
            bbox_type = bbox.get("type", "unknown")
            if not self._visible_layers.get(bbox_type, True):
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
            if show_bbox:
                bbox_fill = QColor(160, 160, 160, 40)
                bbox_border_color = QColor(130, 130, 130, 200)
                bbox_pen = QPen(bbox_border_color, 3)
                for bbox in self._bboxes_pts:
                    if bbox.get("hidden", False):
                        continue
                    bbox_type = bbox.get("type", "unknown")
                    if not layers.get(bbox_type, True):
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

                show_content = show_bbox and layers.get(bbox_type, False)
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

        # Cropping — solid white over cropped margins and template boxes
        if self._crop_h_lines or self._crop_v_lines or self._crop_boxes:
            page_w_pt, page_h_pt = self._page_size_pt
            white = QColor(255, 255, 255)
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(white)
            h_sorted = sorted(self._crop_h_lines)
            v_sorted = sorted(self._crop_v_lines)
            if h_sorted:
                ty = int(h_sorted[0] * page_h_pt * zf) + h
                painter.drawRect(QRectF(0, h, self.width(), ty - h))
                by = int(h_sorted[-1] * page_h_pt * zf) + h
                painter.drawRect(QRectF(0, by, self.width(), self.height() - by))
            if v_sorted:
                lx = int(v_sorted[0] * page_w_pt * zf)
                painter.drawRect(QRectF(0, h, lx, page_h_pt * zf))
                rx = int(v_sorted[-1] * page_w_pt * zf)
                painter.drawRect(QRectF(rx, h, self.width() - rx, page_h_pt * zf))
            # Template boxes — solid white
            for box in self._crop_boxes:
                bx0 = int(box[0] * page_w_pt * zf)
                by0 = int(box[1] * page_h_pt * zf) + h
                bx1 = int(box[2] * page_w_pt * zf)
                by1 = int(box[3] * page_h_pt * zf) + h
                painter.drawRect(QRectF(bx0, by0, bx1 - bx0, by1 - by0))

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


# ---------------------------------------------------------------------------
# Cropping dialog — semi-manual template definition via rulers and guide lines
# ---------------------------------------------------------------------------

RULER_SIZE = 20  # px width/height of rulers


class CropPreviewWidget(QWidget):
    """Page preview with rulers, guide lines, and drawable rectangles."""

    lines_changed = Signal()  # emitted when any line/box added/moved/deleted

    HANDLE = 6  # half-size of resize handles

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._pixmap: QPixmap | None = None
        self._page_w_pt: float = 612
        self._page_h_pt: float = 792
        self._h_lines: list[float] = []  # fractions [0..1] of page height
        self._v_lines: list[float] = []  # fractions [0..1] of page width
        # Boxes: list of [x0, y0, x1, y1] in fractions [0..1]
        self._boxes: list[list[float]] = []
        self._selected_box: int = -1

        # Drag state
        self._dragging: str = ""  # "h", "v", "box_new", "box_move", "box_resize"
        self._drag_idx: int = -1
        self._drag_pos: float = 0.0
        # For box drawing/moving
        self._box_drag_start: tuple[float, float] = (0, 0)
        self._box_drag_cur: tuple[float, float] = (0, 0)
        self._box_drag_handle: int = -1  # 0-7 resize handle, -1 = move
        self._box_drag_orig: list[float] = []
        self.setMouseTracking(True)
        self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
        self.setMinimumSize(200, 200)

    def set_page(self, pixmap: QPixmap, page_w_pt: float, page_h_pt: float) -> None:
        self._pixmap = pixmap
        self._page_w_pt = page_w_pt
        self._page_h_pt = page_h_pt
        self.update()

    def h_lines(self) -> list[float]:
        return sorted(self._h_lines)

    def v_lines(self) -> list[float]:
        return sorted(self._v_lines)

    def boxes(self) -> list[list[float]]:
        return list(self._boxes)

    def _pos_to_frac(self, px_x: float, px_y: float) -> tuple[float, float]:
        ix, iy, iw, ih = self._img_rect()
        fx = max(0.0, min(1.0, (px_x - ix) / iw)) if iw else 0.0
        fy = max(0.0, min(1.0, (px_y - iy) / ih)) if ih else 0.0
        return fx, fy

    def _frac_to_px(self, fx: float, fy: float) -> tuple[int, int]:
        ix, iy, iw, ih = self._img_rect()
        return int(ix + fx * iw), int(iy + fy * ih)

    def _box_rect_px(self, box: list[float]) -> QRectF:
        x0, y0 = self._frac_to_px(box[0], box[1])
        x1, y1 = self._frac_to_px(box[2], box[3])
        return QRectF(x0, y0, x1 - x0, y1 - y0)

    def _box_handles(self, r: QRectF) -> list[QRectF]:
        s = self.HANDLE
        cx, cy = r.center().x(), r.center().y()
        return [
            QRectF(r.left() - s, r.top() - s, 2*s, 2*s),
            QRectF(cx - s, r.top() - s, 2*s, 2*s),
            QRectF(r.right() - s, r.top() - s, 2*s, 2*s),
            QRectF(r.right() - s, cy - s, 2*s, 2*s),
            QRectF(r.right() - s, r.bottom() - s, 2*s, 2*s),
            QRectF(cx - s, r.bottom() - s, 2*s, 2*s),
            QRectF(r.left() - s, r.bottom() - s, 2*s, 2*s),
            QRectF(r.left() - s, cy - s, 2*s, 2*s),
        ]

    def _img_rect(self) -> tuple[int, int, int, int]:
        """Return (x, y, w, h) of the page image area (inside rulers)."""
        if not self._pixmap:
            return (RULER_SIZE, RULER_SIZE, self.width() - 2 * RULER_SIZE, self.height() - 2 * RULER_SIZE)
        r = RULER_SIZE
        avail_w = self.width() - 2 * r
        avail_h = self.height() - 2 * r
        scale = min(avail_w / self._pixmap.width(), avail_h / self._pixmap.height())
        iw = int(self._pixmap.width() * scale)
        ih = int(self._pixmap.height() * scale)
        ix = r + (avail_w - iw) // 2
        iy = r + (avail_h - ih) // 2
        return (ix, iy, iw, ih)

    def paintEvent(self, event) -> None:
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        r = RULER_SIZE
        w, h = self.width(), self.height()

        # Background
        p.fillRect(0, 0, w, h, QColor(60, 60, 60))

        # Rulers
        ruler_color = QColor(200, 200, 200)
        p.fillRect(r, 0, w - 2 * r, r, ruler_color)       # top
        p.fillRect(r, h - r, w - 2 * r, r, ruler_color)    # bottom
        p.fillRect(0, r, r, h - 2 * r, ruler_color)        # left
        p.fillRect(w - r, r, r, h - 2 * r, ruler_color)    # right

        # Page image
        ix, iy, iw, ih = self._img_rect()
        if self._pixmap:
            scaled = self._pixmap.scaled(iw, ih, Qt.AspectRatioMode.KeepAspectRatio,
                                         Qt.TransformationMode.SmoothTransformation)
            p.drawPixmap(ix, iy, scaled)

        # Red mask: area between ruler and first/last lines
        h_sorted = sorted(self._h_lines)
        v_sorted = sorted(self._v_lines)
        mask_color = QColor(200, 0, 0, 60)

        if h_sorted:
            # Top mask: ruler → first h_line
            top_y = iy + int(h_sorted[0] * ih)
            p.fillRect(ix, iy, iw, top_y - iy, mask_color)
            # Bottom mask: last h_line → bottom
            bot_y = iy + int(h_sorted[-1] * ih)
            p.fillRect(ix, bot_y, iw, iy + ih - bot_y, mask_color)

        if v_sorted:
            # Left mask: ruler → first v_line
            left_x = ix + int(v_sorted[0] * iw)
            p.fillRect(ix, iy, left_x - ix, ih, mask_color)
            # Right mask: last v_line → right
            right_x = ix + int(v_sorted[-1] * iw)
            p.fillRect(right_x, iy, ix + iw - right_x, ih, mask_color)

        # Draw guide lines
        line_pen = QPen(QColor(0, 120, 255), 2, Qt.PenStyle.DashLine)
        p.setPen(line_pen)
        for frac in self._h_lines:
            ly = iy + int(frac * ih)
            p.drawLine(ix, ly, ix + iw, ly)
        for frac in self._v_lines:
            lx = ix + int(frac * iw)
            p.drawLine(lx, iy, lx, iy + ih)

        # Drag preview for lines
        if self._dragging == "h":
            ly = iy + int(self._drag_pos * ih)
            p.setPen(QPen(QColor(255, 200, 0), 2))
            p.drawLine(ix, ly, ix + iw, ly)
        elif self._dragging == "v":
            lx = ix + int(self._drag_pos * iw)
            p.setPen(QPen(QColor(255, 200, 0), 2))
            p.drawLine(lx, iy, lx, iy + ih)

        # Draw boxes
        for i, box in enumerate(self._boxes):
            br = self._box_rect_px(box)
            is_sel = (i == self._selected_box)
            # Fill
            p.setBrush(QColor(200, 0, 0, 40))
            p.setPen(QPen(QColor(0, 180, 0, 200) if is_sel else QColor(0, 120, 200, 180), 2))
            p.drawRect(br)
            # Handles on selected
            if is_sel:
                p.setPen(QPen(QColor(80, 80, 80), 1))
                p.setBrush(QColor(255, 255, 255))
                for hr in self._box_handles(br):
                    p.drawRect(hr)

        # Box drawing preview
        if self._dragging == "box_new":
            x0, y0 = self._frac_to_px(*self._box_drag_start)
            x1, y1 = self._frac_to_px(*self._box_drag_cur)
            p.setBrush(QColor(200, 0, 0, 30))
            p.setPen(QPen(QColor(0, 120, 200), 2, Qt.PenStyle.DashLine))
            p.drawRect(QRectF(min(x0, x1), min(y0, y1), abs(x1-x0), abs(y1-y0)))

        p.end()

    def mousePressEvent(self, event) -> None:
        pos = event.position()
        ix, iy, iw, ih = self._img_rect()
        r = RULER_SIZE
        x, y = pos.x(), pos.y()
        in_image = ix <= x <= ix + iw and iy <= y <= iy + ih

        # 1. Check resize handles on selected box
        if self._selected_box >= 0 and self._selected_box < len(self._boxes):
            br = self._box_rect_px(self._boxes[self._selected_box])
            for hi, hr in enumerate(self._box_handles(br)):
                if hr.contains(pos):
                    self._dragging = "box_resize"
                    self._drag_idx = self._selected_box
                    self._box_drag_handle = hi
                    self._box_drag_orig = list(self._boxes[self._selected_box])
                    self._box_drag_start = (x, y)
                    return

        # 2. Check click on existing box (select it)
        for i in range(len(self._boxes) - 1, -1, -1):  # top-most first
            br = self._box_rect_px(self._boxes[i])
            if br.contains(pos):
                self._selected_box = i
                self._dragging = "box_move"
                self._drag_idx = i
                self._box_drag_orig = list(self._boxes[i])
                self._box_drag_start = self._pos_to_frac(x, y)
                self.update()
                return

        # 3. Check existing line drag
        for i, frac in enumerate(self._h_lines):
            ly = iy + int(frac * ih)
            if abs(y - ly) < 6 and ix <= x <= ix + iw:
                self._dragging = "h"
                self._drag_idx = i
                self._drag_pos = frac
                self._selected_box = -1
                self.update()
                return
        for i, frac in enumerate(self._v_lines):
            lx = ix + int(frac * iw)
            if abs(x - lx) < 6 and iy <= y <= iy + ih:
                self._dragging = "v"
                self._drag_idx = i
                self._drag_pos = frac
                self._selected_box = -1
                self.update()
                return

        # 4. Drag from ruler → new line
        if y < r and ix <= x <= ix + iw:
            self._dragging = "h"
            self._drag_idx = -1
            self._drag_pos = 0.0
            self._selected_box = -1
        elif y > self.height() - r and ix <= x <= ix + iw:
            self._dragging = "h"
            self._drag_idx = -1
            self._drag_pos = 1.0
            self._selected_box = -1
        elif x < r and iy <= y <= iy + ih:
            self._dragging = "v"
            self._drag_idx = -1
            self._drag_pos = 0.0
            self._selected_box = -1
        elif x > self.width() - r and iy <= y <= iy + ih:
            self._dragging = "v"
            self._drag_idx = -1
            self._drag_pos = 1.0
            self._selected_box = -1
        elif in_image:
            # 5. Start drawing new box
            self._dragging = "box_new"
            self._selected_box = -1
            self._box_drag_start = self._pos_to_frac(x, y)
            self._box_drag_cur = self._box_drag_start
        else:
            self._selected_box = -1

        self.update()

    def mouseMoveEvent(self, event) -> None:
        if not self._dragging:
            return
        pos = event.position()
        x, y = pos.x(), pos.y()
        ix, iy, iw, ih = self._img_rect()

        if self._dragging == "h" and ih > 0:
            self._drag_pos = max(0.0, min(1.0, (y - iy) / ih))
        elif self._dragging == "v" and iw > 0:
            self._drag_pos = max(0.0, min(1.0, (x - ix) / iw))
        elif self._dragging == "box_new":
            self._box_drag_cur = self._pos_to_frac(x, y)
        elif self._dragging == "box_move" and self._drag_idx >= 0:
            fx, fy = self._pos_to_frac(x, y)
            sx, sy = self._box_drag_start
            dx, dy = fx - sx, fy - sy
            orig = self._box_drag_orig
            bw, bh = orig[2] - orig[0], orig[3] - orig[1]
            nx0 = max(0.0, min(1.0 - bw, orig[0] + dx))
            ny0 = max(0.0, min(1.0 - bh, orig[1] + dy))
            self._boxes[self._drag_idx] = [nx0, ny0, nx0 + bw, ny0 + bh]
        elif self._dragging == "box_resize" and self._drag_idx >= 0:
            fx, fy = self._pos_to_frac(x, y)
            box = list(self._box_drag_orig)
            h = self._box_drag_handle
            # TL=0, T=1, TR=2, R=3, BR=4, B=5, BL=6, L=7
            if h in (0, 6, 7): box[0] = min(fx, box[2] - 0.01)
            if h in (0, 1, 2): box[1] = min(fy, box[3] - 0.01)
            if h in (2, 3, 4): box[2] = max(fx, box[0] + 0.01)
            if h in (4, 5, 6): box[3] = max(fy, box[1] + 0.01)
            self._boxes[self._drag_idx] = [max(0, v) for v in box]

        self.update()

    def mouseReleaseEvent(self, event) -> None:
        if not self._dragging:
            return
        pos = event.position()
        r = RULER_SIZE

        if self._dragging in ("h", "v"):
            on_ruler = (pos.y() < r or pos.y() > self.height() - r or
                        pos.x() < r or pos.x() > self.width() - r)
            lines = self._h_lines if self._dragging == "h" else self._v_lines
            if on_ruler:
                if 0 <= self._drag_idx < len(lines):
                    lines.pop(self._drag_idx)
            else:
                if 0 <= self._drag_idx < len(lines):
                    lines[self._drag_idx] = self._drag_pos
                else:
                    lines.append(self._drag_pos)

        elif self._dragging == "box_new":
            sx, sy = self._box_drag_start
            ex, ey = self._box_drag_cur
            x0, x1 = min(sx, ex), max(sx, ex)
            y0, y1 = min(sy, ey), max(sy, ey)
            if (x1 - x0) > 0.01 and (y1 - y0) > 0.01:
                self._boxes.append([x0, y0, x1, y1])
                self._selected_box = len(self._boxes) - 1

        # box_move and box_resize already updated in mouseMoveEvent

        self._dragging = ""
        self._drag_idx = -1
        self._box_drag_handle = -1
        self.update()
        self.lines_changed.emit()


class CroppingDialog(QWidget):
    """Semi-manual cropping dialog: preview with rulers + page thumbnails.

    Supports 1-page mode (single template) and 2-page mode (left/right mirrored templates).
    """

    accepted = Signal(dict)   # emitted on OK with cropping data
    cancelled = Signal()      # emitted on Cancel

    def __init__(self, doc: "fitz.Document", page_count: int,
                 file_path: "Path | None" = None, parent=None) -> None:
        super().__init__(parent, Qt.WindowType.Window)
        self.setWindowTitle("Cropping — Template Definition")
        self.resize(1200, 700)

        self._doc = doc
        self._page_count = page_count
        self._file_path = file_path
        self._current_page: int = 0
        self._two_page_mode: bool = False
        self._first_is_cover: bool = False
        self._mirrored: bool = False

        root = QVBoxLayout(self)
        root.setContentsMargins(4, 2, 4, 4)
        root.setSpacing(2)

        # Toolbar (compact)
        toolbar = QHBoxLayout()
        toolbar.setContentsMargins(0, 0, 0, 0)
        self._mode_btn = QPushButton("1-page template")
        self._mode_btn.setCheckable(True)
        self._mode_btn.setFixedWidth(140)
        self._mode_btn.setFixedHeight(24)
        self._mode_btn.clicked.connect(self._toggle_mode)
        toolbar.addWidget(self._mode_btn)
        self._cover_cb = QCheckBox("1st page is cover")
        self._cover_cb.setStyleSheet("font-size: 10px;")
        self._cover_cb.hide()
        self._cover_cb.toggled.connect(self._on_cover_toggled)
        toolbar.addWidget(self._cover_cb)
        self._mirror_cb = QCheckBox("Mirrored")
        self._mirror_cb.setStyleSheet("font-size: 10px;")
        self._mirror_cb.hide()
        self._mirror_cb.toggled.connect(self._on_mirror_toggled)
        toolbar.addWidget(self._mirror_cb)
        self._mode_label = QLabel("")
        self._mode_label.setStyleSheet("color: #888; font-size: 10px;")
        toolbar.addWidget(self._mode_label)
        toolbar.addStretch()

        cancel_btn = QPushButton("Cancel")
        cancel_btn.setFixedWidth(70)
        cancel_btn.setFixedHeight(24)
        cancel_btn.clicked.connect(self._on_cancel)
        toolbar.addWidget(cancel_btn)
        ok_btn = QPushButton("OK")
        ok_btn.setFixedWidth(70)
        ok_btn.setFixedHeight(24)
        ok_btn.setStyleSheet("font-weight: bold;")
        ok_btn.clicked.connect(self._on_ok)
        toolbar.addWidget(ok_btn)

        root.addLayout(toolbar)

        self._splitter = QSplitter(Qt.Orientation.Horizontal)
        root.addWidget(self._splitter, stretch=1)

        # Preview panel (holds 1 or 2 CropPreviewWidgets)
        self._preview_container = QWidget()
        self._preview_layout = QHBoxLayout(self._preview_container)
        self._preview_layout.setContentsMargins(0, 0, 0, 0)
        self._preview_layout.setSpacing(4)

        self._preview_left = CropPreviewWidget()
        self._preview_left.lines_changed.connect(self._on_lines_changed)
        self._preview_layout.addWidget(self._preview_left)

        self._preview_right = CropPreviewWidget()
        self._preview_right.lines_changed.connect(self._on_lines_changed)
        self._preview_right.hide()  # hidden in 1-page mode

        self._preview_layout.addWidget(self._preview_right)
        self._splitter.addWidget(self._preview_container)

        right_scroll = QScrollArea()
        right_scroll.setWidgetResizable(True)
        right_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        right_container = QWidget()
        self._grid = QGridLayout(right_container)
        self._grid.setSpacing(4)
        self._grid.setContentsMargins(4, 4, 4, 4)

        self._thumb_labels: list[QLabel] = []
        self._thumb_pixmaps: list[QPixmap] = []

        for pi in range(self._page_count):
            row, col = pi // 2, pi % 2
            lbl = QLabel()
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            lbl.setStyleSheet("border: 2px solid #444; background: #222;")
            lbl.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
            lbl.mousePressEvent = lambda e, p=pi: self._on_thumb_click(p)
            self._grid.addWidget(lbl, row, col)
            self._thumb_labels.append(lbl)

        right_scroll.setWidget(right_container)
        self._splitter.addWidget(right_scroll)
        self._splitter.setSizes([800, 300])

        # Restore saved cropping, render thumbnails and initial preview
        self._load_cropping()
        self._render_thumbnails()
        self._load_preview(self._current_page)

    def _toggle_mode(self) -> None:
        self._two_page_mode = not self._two_page_mode
        if self._two_page_mode:
            self._mode_btn.setText("2-page template")
            self._mode_label.setText("Left = odd pages, Right = even pages")
            self._cover_cb.show()
            self._mirror_cb.show()
            self._preview_right.show()
        else:
            self._mode_btn.setText("1-page template")
            self._mode_label.setText("")
            self._cover_cb.hide()
            self._mirror_cb.hide()
            self._preview_right.hide()
        self._load_preview(self._current_page)
        self._on_lines_changed()  # updates thumbs + saves

    def _on_cover_toggled(self, checked: bool) -> None:
        self._first_is_cover = checked
        self._rebuild_thumbs_grid()
        self._on_lines_changed()

    def _on_mirror_toggled(self, checked: bool) -> None:
        self._mirrored = checked
        if checked:
            self._apply_mirror()
        self._on_lines_changed()

    def _apply_mirror(self) -> None:
        """Copy left template to right, mirroring horizontal positions."""
        # h_lines stay the same (top/bottom are symmetric)
        self._preview_right._h_lines = list(self._preview_left._h_lines)
        # v_lines: mirror (1.0 - x)
        self._preview_right._v_lines = [1.0 - v for v in self._preview_left._v_lines]
        # boxes: mirror x coordinates
        self._preview_right._boxes = [
            [1.0 - b[2], b[1], 1.0 - b[0], b[3]] for b in self._preview_left._boxes
        ]
        self._preview_right.update()

    def _rebuild_thumbs_grid(self) -> None:
        """Reposition thumbnails in grid based on cover setting."""
        # Remove all from grid (don't delete widgets)
        for lbl in self._thumb_labels:
            self._grid.removeWidget(lbl)
        # Re-add with offset
        offset = 1 if (self._two_page_mode and self._first_is_cover) else 0
        for pi in range(self._page_count):
            shifted = pi + offset
            row, col = shifted // 2, shifted % 2
            self._grid.addWidget(self._thumb_labels[pi], row, col)

    def _is_left_page(self, page_idx: int) -> bool:
        """Determine if a page uses the left template."""
        if not self._two_page_mode:
            return True
        shifted = page_idx + (1 if self._first_is_cover else 0)
        return shifted % 2 == 0  # even shifted index = left column

    def _preview_for_page(self, page_idx: int) -> CropPreviewWidget:
        """Return the appropriate preview widget for a given page index."""
        if not self._two_page_mode:
            return self._preview_left
        return self._preview_left if self._is_left_page(page_idx) else self._preview_right

    def _render_page_pixmap(self, page_idx: int, dpi: int = 100) -> QPixmap:
        page = self._doc[page_idx]
        mat = fitz.Matrix(dpi / 72.0, dpi / 72.0)
        pix = page.get_pixmap(matrix=mat)
        img = QImage(pix.samples, pix.width, pix.height, pix.stride,
                     QImage.Format.Format_RGB888)
        return QPixmap.fromImage(img)

    def _render_thumbnails(self) -> None:
        self._thumb_pixmaps.clear()
        for pi in range(self._page_count):
            pix = self._render_page_pixmap(pi, dpi=48)
            self._thumb_pixmaps.append(pix)
            self._update_thumb(pi)

    def _update_thumb(self, pi: int) -> None:
        """Update a single thumbnail with crop mask from its corresponding template."""
        if pi >= len(self._thumb_pixmaps) or pi >= len(self._thumb_labels):
            return
        pix = self._thumb_pixmaps[pi]
        lbl = self._thumb_labels[pi]
        preview = self._preview_for_page(pi)

        thumb_w = 140
        scaled = pix.scaledToWidth(thumb_w, Qt.TransformationMode.FastTransformation)

        result = QPixmap(scaled)
        painter = QPainter(result)
        iw, ih = result.width(), result.height()
        mask_color = QColor(200, 0, 0, 80)

        h_lines = preview.h_lines()
        v_lines = preview.v_lines()

        if h_lines:
            top_y = int(h_lines[0] * ih)
            painter.fillRect(0, 0, iw, top_y, mask_color)
            bot_y = int(h_lines[-1] * ih)
            painter.fillRect(0, bot_y, iw, ih - bot_y, mask_color)
        if v_lines:
            left_x = int(v_lines[0] * iw)
            painter.fillRect(0, 0, left_x, ih, mask_color)
            right_x = int(v_lines[-1] * iw)
            painter.fillRect(right_x, 0, iw - right_x, ih, mask_color)

        for box in preview.boxes():
            bx0, by0 = int(box[0] * iw), int(box[1] * ih)
            bx1, by1 = int(box[2] * iw), int(box[3] * ih)
            painter.fillRect(bx0, by0, bx1 - bx0, by1 - by0, mask_color)

        # Page number + side label
        painter.setPen(QColor(255, 255, 255))
        font = QFont("Consolas", 8, QFont.Weight.Bold)
        painter.setFont(font)
        side = ""
        if self._two_page_mode:
            side = " L" if self._is_left_page(pi) else " R"
        painter.drawText(3, 12, f"P.{pi + 1}{side}")
        painter.end()

        border = "2px solid #0af" if pi == self._current_page else "2px solid #444"
        lbl.setStyleSheet(f"border: {border}; background: #222;")
        lbl.setPixmap(result)

    def _page_pair(self, page_idx: int) -> tuple[int, int]:
        """Return (left_page, right_page) indices for a spread pair.

        With cover: page 0 alone (right), then pairs (1,2), (3,4), ...
        Without cover: pairs (0,1), (2,3), (4,5), ...
        """
        offset = 1 if self._first_is_cover else 0
        shifted = page_idx + offset
        # Find the pair start (even shifted index)
        pair_start_shifted = (shifted // 2) * 2
        left_pi = pair_start_shifted - offset
        right_pi = pair_start_shifted - offset + 1
        return (left_pi, right_pi)

    def _load_preview(self, page_idx: int) -> None:
        old_page = self._current_page
        self._current_page = page_idx

        if not self._two_page_mode:
            # Single mode: load clicked page into left preview
            pix = self._render_page_pixmap(page_idx, dpi=150)
            page = self._doc[page_idx]
            self._preview_left.set_page(pix, page.rect.width, page.rect.height)
        else:
            # 2-page mode: load the paired spread
            left_pi, right_pi = self._page_pair(page_idx)

            if 0 <= left_pi < self._page_count:
                pix = self._render_page_pixmap(left_pi, dpi=150)
                pg = self._doc[left_pi]
                self._preview_left.set_page(pix, pg.rect.width, pg.rect.height)

            if 0 <= right_pi < self._page_count:
                pix = self._render_page_pixmap(right_pi, dpi=150)
                pg = self._doc[right_pi]
                self._preview_right.set_page(pix, pg.rect.width, pg.rect.height)

        # Update thumb borders — highlight both pages of the pair
        highlight = set()
        if self._two_page_mode:
            lp, rp = self._page_pair(page_idx)
            if 0 <= lp < self._page_count:
                highlight.add(lp)
            if 0 <= rp < self._page_count:
                highlight.add(rp)
        else:
            highlight.add(page_idx)

        for pi in set([old_page, page_idx]) | highlight:
            if 0 <= pi < len(self._thumb_labels):
                border = "2px solid #0af" if pi in highlight else "2px solid #444"
                self._thumb_labels[pi].setStyleSheet(f"border: {border}; background: #222;")

    def _on_thumb_click(self, page_idx: int) -> None:
        self._load_preview(page_idx)

    def _on_ok(self) -> None:
        """Save cropping data and close."""
        self._save_cropping()
        data = self._build_cropping_data()
        self.accepted.emit(data)
        self.close()

    def _on_cancel(self) -> None:
        """Discard changes and close."""
        self.cancelled.emit()
        self.close()

    def _build_cropping_data(self) -> dict:
        """Build cropping data dict for consumption by main window."""
        return {
            "two_page": self._two_page_mode,
            "first_is_cover": self._first_is_cover,
            "mirrored": self._mirrored,
            "left": {
                "h_lines": self._preview_left._h_lines,
                "v_lines": self._preview_left._v_lines,
                "boxes": self._preview_left._boxes,
            },
            "right": {
                "h_lines": self._preview_right._h_lines,
                "v_lines": self._preview_right._v_lines,
                "boxes": self._preview_right._boxes,
            },
        }

    def _on_lines_changed(self) -> None:
        """Update all thumbnails when guide lines/boxes change."""
        if self._mirrored:
            self._apply_mirror()
        for pi in range(self._page_count):
            self._update_thumb(pi)

    def _save_cropping(self) -> None:
        """Persist cropping data to catalog meta."""
        if not self._file_path:
            return
        meta = load_meta(self._file_path)
        meta["cropping"] = {
            "two_page": self._two_page_mode,
            "first_is_cover": self._first_is_cover,
            "mirrored": self._mirrored,
            "left": {
                "h_lines": self._preview_left._h_lines,
                "v_lines": self._preview_left._v_lines,
                "boxes": self._preview_left._boxes,
            },
            "right": {
                "h_lines": self._preview_right._h_lines,
                "v_lines": self._preview_right._v_lines,
                "boxes": self._preview_right._boxes,
            },
        }
        save_meta(self._file_path, meta)

    def _load_cropping(self) -> None:
        """Restore cropping data from catalog meta."""
        if not self._file_path:
            return
        meta = load_meta(self._file_path)
        crop = meta.get("cropping")
        if not crop:
            return
        self._two_page_mode = crop.get("two_page", False)
        self._first_is_cover = crop.get("first_is_cover", False)
        self._mirrored = crop.get("mirrored", False)
        if self._two_page_mode:
            self._mode_btn.setText("2-page template")
            self._mode_btn.setChecked(True)
            self._preview_right.show()
            self._cover_cb.show()
            self._cover_cb.setChecked(self._first_is_cover)
            self._mirror_cb.show()
            self._mirror_cb.setChecked(self._mirrored)
            self._mode_label.setText("Left = odd pages, Right = even pages")
            self._rebuild_thumbs_grid()
        left = crop.get("left", {})
        self._preview_left._h_lines = left.get("h_lines", [])
        self._preview_left._v_lines = left.get("v_lines", [])
        self._preview_left._boxes = left.get("boxes", [])
        right = crop.get("right", {})
        self._preview_right._h_lines = right.get("h_lines", [])
        self._preview_right._v_lines = right.get("v_lines", [])
        self._preview_right._boxes = right.get("boxes", [])


class PreviewView(QWidget):
    """PDF catalog preview with 2-page spread, zoom, continuous scroll, bounding boxes."""

    progress = Signal(str)  # "operation 45% 0.3s" or "" to clear

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
        self._first_is_cover: bool = False



        self._op_start: float = 0.0
        self._setup_ui()

    def _emit_progress(self, op: str, current: int, total: int) -> None:
        """Emit progress signal — skip if template worker owns the header."""
        import time
        elapsed = time.perf_counter() - self._op_start
        pct = int(current / total * 100) if total > 0 else 0
        self.progress.emit(f"{op} {pct}% {elapsed:.1f}s")

    def _emit_done(self) -> None:
        """Clear progress — but don't overwrite if template worker is active."""
        import time
        elapsed = time.perf_counter() - self._op_start
        self.progress.emit(f"done {elapsed:.1f}s")

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


        self.crop_btn = QPushButton("Cropping")
        self.crop_btn.clicked.connect(self._open_cropping_dialog)
        toolbar.addWidget(self.crop_btn)

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

        # Load cropping settings
        crop = self._catalog_meta.get("cropping", {})
        self._first_is_cover = crop.get("first_is_cover", False)
        self._cropping_data = crop

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
        """Render all page spreads (2 pages per row) with progress."""
        import time
        self._op_start = time.perf_counter()

        # Clear existing
        for spread in self._spreads:
            self.pages_layout.removeWidget(spread)
            spread.deleteLater()
        self._spreads.clear()

        if not self._doc:
            return

        zf = self._current_zoom_factor()
        dpi = zf * 72.0

        cur_layers = {
            key: act.isChecked()
            for key, act in self.bbox_filter_actions.items()
        }

        # Build list of spread pairs: [(left_page_idx or -1, right_page_idx or -1), ...]
        spread_pairs: list[tuple[int, int]] = []
        if self._first_is_cover and self._page_count > 0:
            # First spread: blank left, page 0 on right (cover)
            spread_pairs.append((-1, 0))
            pi = 1
        else:
            pi = 0
        while pi < self._page_count:
            left_pi = pi
            right_pi = pi + 1 if pi + 1 < self._page_count else -1
            spread_pairs.append((left_pi, right_pi))
            pi += 2

        total_spreads = len(spread_pairs)
        for spread_num, (left_pi, right_pi) in enumerate(spread_pairs):
            self._emit_progress("Render", spread_num, total_spreads)
            spread = PageSpreadWidget()

            # Left page
            if left_pi >= 0:
                left_pixmap = self._render_page(left_pi, dpi)
                spread.left_page._page_index = left_pi
                page = self._doc[left_pi]
                spread.left_page._page_size_pt = (page.rect.width, page.rect.height)
                spread.left_page.set_pixmap(left_pixmap)
                spread.left_page.set_show_bboxes(True)
                spread.left_page.set_show_hidden(self._show_hidden)
                spread.left_page.hide_requested.connect(self._on_hide_object)
                spread.left_page.selection_changed.connect(self._on_page_selection)
                spread.left_page.type_changed.connect(self._on_type_changed)
                spread.left_page.bbox_modified.connect(self._on_bbox_modified)
                spread.left_page.set_visible_layers(cur_layers)
                if left_pi in self._bboxes_cache:
                    spread.left_page.set_bboxes(self._bboxes_cache[left_pi], zf)
                if left_pi in self._stats_cache:
                    spread.left_page.set_page_stats(self._stats_cache[left_pi])
            else:
                # Blank left page (cover mode)
                if right_pi >= 0:
                    ref_pix = self._render_page(right_pi, dpi)
                    blank = QPixmap(ref_pix.size())
                else:
                    blank = QPixmap(100, 100)
                blank.fill(QColor(240, 240, 240))
                spread.left_page.set_pixmap(blank)

            # Right page
            if right_pi >= 0:
                right_pixmap = self._render_page(right_pi, dpi)
                spread.right_page._page_index = right_pi
                rpage = self._doc[right_pi]
                spread.right_page._page_size_pt = (rpage.rect.width, rpage.rect.height)
                spread.right_page.set_pixmap(right_pixmap)
                spread.right_page.set_show_bboxes(True)
                spread.right_page.set_show_hidden(self._show_hidden)
                spread.right_page.hide_requested.connect(self._on_hide_object)
                spread.right_page.selection_changed.connect(self._on_page_selection)
                spread.right_page.type_changed.connect(self._on_type_changed)
                spread.right_page.bbox_modified.connect(self._on_bbox_modified)
                if right_pi in self._bboxes_cache:
                    spread.right_page.set_bboxes(self._bboxes_cache[right_pi], zf)
                if right_pi in self._stats_cache:
                    spread.right_page.set_page_stats(self._stats_cache[right_pi])
            else:
                if left_pi >= 0:
                    ref_pix = self._render_page(left_pi, dpi)
                    blank = QPixmap(ref_pix.size())
                else:
                    blank = QPixmap(100, 100)
                blank.fill(QColor(240, 240, 240))
                spread.right_page.set_pixmap(blank)

            spread.left_page.set_visible_layers(cur_layers)
            spread.right_page.set_visible_layers(cur_layers)

            self.pages_layout.addWidget(spread)
            self._spreads.append(spread)

        # Restore overlays after re-creating spreads
        if hasattr(self, "_cropping_data") and self._cropping_data:
            self._apply_cropping_to_spreads()
        self._emit_done()

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
        self._detect_visible_pages()

    def _clear_and_redetect(self) -> None:
        """Clear cached bboxes and saved metadata, then re-detect."""
        self._bboxes_cache.clear()
        self._stats_cache.clear()
        if self._file_path:
            meta = load_meta(self._file_path)
            meta["objects"] = {}
            save_meta(self._file_path, meta)
            self._catalog_meta = meta
        for spread in self._spreads:
            spread.left_page.set_bboxes([], 1.0)
            spread.right_page.set_bboxes([], 1.0)
            spread.left_page.update()
            spread.right_page.update()
        from PySide6.QtWidgets import QApplication
        QApplication.processEvents()
        self._detect_visible_pages()

    def _open_cropping_dialog(self) -> None:
        """Open the semi-manual cropping dialog."""
        if not self._doc or self._page_count < 3:
            return
        dlg = CroppingDialog(self._doc, self._page_count,
                             file_path=self._file_path, parent=self)
        dlg.accepted.connect(self._on_cropping_accepted)
        dlg.show()

    def _on_cropping_accepted(self, data: dict) -> None:
        """Apply cropping from dialog to main view."""
        self._first_is_cover = data.get("first_is_cover", False)
        self._cropping_data = data
        self._catalog_meta["cropping"] = data
        if self._file_path:
            save_meta(self._file_path, self._catalog_meta)
        # Re-render spreads with updated cover/crop settings
        self._render_all_spreads()
        self._apply_cropping_to_spreads()

    def _cropping_for_page(self, page_idx: int) -> dict:
        """Return the cropping side data (h_lines, v_lines, boxes) for a page."""
        data = getattr(self, "_cropping_data", None)
        if not data:
            data = self._catalog_meta.get("cropping", {})
        if not data:
            return {}
        two_page = data.get("two_page", False)
        cover = data.get("first_is_cover", False)
        if not two_page:
            return data.get("left", {})
        shifted = page_idx + (1 if cover else 0)
        return data.get("left", {}) if shifted % 2 == 0 else data.get("right", {})

    def _apply_cropping_to_spreads(self) -> None:
        """Apply cropping overlays to all page widgets."""
        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                pi = pw._page_index
                if pi < 0:
                    continue
                side = self._cropping_for_page(pi)
                pw._crop_h_lines = side.get("h_lines", [])
                pw._crop_v_lines = side.get("v_lines", [])
                pw._crop_boxes = side.get("boxes", [])
                pw.update()

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

        import time

        current = self._get_current_page()
        # Current spread ± 2 pages (1 spread each side)
        start = max(0, current - 2)
        end = min(self._page_count, current + 4)  # +4 to cover 2 pages ahead

        to_detect = [p for p in range(start, end) if p not in self._bboxes_cache]
        if not to_detect:
            return

        self._op_start = time.perf_counter()
        detected_new = False
        for step, page_num in enumerate(to_detect):
            self._emit_progress("Detect", step, len(to_detect))
            try:
                page = self._doc[page_num]
                crop_side = self._cropping_for_page(page_num)
                bboxes, stats = self._detect_native_page(page, crop_side)
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

        self._emit_done()

    def _detect_native_page(
        self, page, crop_side: dict | None = None,
    ) -> tuple[list[dict], str]:
        """Detect object bounding boxes via rasterization.

        Renders the page to bitmap, finds connected content regions
        within the cropping area.
        Returns bboxes with 'pts' = (x0, y0, x1, y1) in PDF points.
        Type recognition is a separate stage.
        """
        import numpy as np

        page_area = page.rect.width * page.rect.height

        # Render at 72 DPI (1 pixel = 1 PDF point)
        pix = page.get_pixmap(dpi=72)
        img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(
            pix.height, pix.width, pix.n
        )

        # Grayscale → binary (content = dark pixels < 240)
        gray = np.mean(img[:, :, :3], axis=2)
        binary = (gray < 240).astype(np.uint8)

        # Apply cropping mask — zero out pixels outside crop area
        if crop_side:
            h_lines = sorted(crop_side.get("h_lines", []))
            v_lines = sorted(crop_side.get("v_lines", []))
            h_img, w_img = binary.shape
            # Horizontal crop: mask above first h_line and below last h_line
            if h_lines:
                top_px = int(h_lines[0] * h_img)
                bot_px = int(h_lines[-1] * h_img)
                binary[:top_px, :] = 0
                binary[bot_px:, :] = 0
            # Vertical crop: mask left of first v_line and right of last v_line
            if v_lines:
                left_px = int(v_lines[0] * w_img)
                right_px = int(v_lines[-1] * w_img)
                binary[:, :left_px] = 0
                binary[:, right_px:] = 0
            # Mask crop boxes (exclusion zones)
            for box in crop_side.get("boxes", []):
                bx0 = int(box[0] * w_img)
                by0 = int(box[1] * h_img)
                bx1 = int(box[2] * w_img)
                by1 = int(box[3] * h_img)
                binary[by0:by1, bx0:bx1] = 0

        # Dilate to bridge small gaps (~2pt)
        for _ in range(2):
            padded = np.pad(binary, 1, mode="constant")
            binary = (
                padded[:-2, 1:-1] | padded[2:, 1:-1]
                | padded[1:-1, :-2] | padded[1:-1, 2:]
                | binary
            ).astype(np.uint8)

        # Connected components via flood fill
        h_img, w_img = binary.shape
        visited = np.zeros_like(binary, dtype=bool)
        bboxes: list[dict] = []
        obj_id = 0

        for start_y in range(h_img):
            for start_x in range(w_img):
                if not binary[start_y, start_x] or visited[start_y, start_x]:
                    continue
                stack = [(start_y, start_x)]
                visited[start_y, start_x] = True
                min_x, min_y = start_x, start_y
                max_x, max_y = start_x, start_y
                count = 0
                while stack:
                    cy, cx = stack.pop()
                    count += 1
                    if cx < min_x: min_x = cx
                    if cx > max_x: max_x = cx
                    if cy < min_y: min_y = cy
                    if cy > max_y: max_y = cy
                    for dy, dx in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                        ny, nx = cy + dy, cx + dx
                        if (0 <= ny < h_img and 0 <= nx < w_img
                                and binary[ny, nx] and not visited[ny, nx]):
                            visited[ny, nx] = True
                            stack.append((ny, nx))

                bw = max_x - min_x
                bh = max_y - min_y
                # Skip noise (<50 pixels) and tiny regions (<20pt)
                if count < 50 or bw < 20 or bh < 20:
                    continue
                # Skip full-page frames (>80% of page)
                if bw * bh > page_area * 0.8:
                    continue

                bboxes.append({
                    "type": "unknown",
                    "label": f"#{obj_id} {bw}x{bh}pt",
                    "pts": (float(min_x), float(min_y),
                            float(max_x), float(max_y)),
                })
                obj_id += 1

        # Remove nested bboxes (fully contained inside a larger one)
        filtered: list[dict] = []
        for i, a in enumerate(bboxes):
            ap = a["pts"]
            nested = False
            for j, b in enumerate(bboxes):
                if i == j:
                    continue
                bp = b["pts"]
                if (ap[0] >= bp[0] and ap[1] >= bp[1]
                        and ap[2] <= bp[2] and ap[3] <= bp[3]):
                    nested = True
                    break
            if not nested:
                filtered.append(a)
        bboxes = filtered

        stats = f"P{page.number + 1}  |  Obj:{len(bboxes)}"
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
