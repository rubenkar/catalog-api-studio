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
from PySide6.QtCore import QPoint, QPointF, QRectF, QThread, QTimer, Qt, Signal
from PySide6.QtGui import QAction, QColor, QCursor, QFont, QImage, QPainter, QPen, QPixmap, QWheelEvent
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
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
    QWidgetAction,
)

from app.services.catalog_meta import (
    load_meta,
    merge_detected,
    save_meta,
    update_object,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Background PDF page renderer
# ---------------------------------------------------------------------------

class DetectWorker(QThread):
    """Runs object detection for one page in a background thread."""

    page_detected = Signal(int, list, str, list)  # page_idx, bboxes, stats, pdf_objects

    def __init__(self, preview: "PreviewView", page_idx: int, parent=None):
        super().__init__(parent)
        self._preview = preview
        self._page_idx = page_idx
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        try:
            doc = fitz.open(str(self._preview._file_path))
            # Apply same cropbox as main doc
            main_doc = self._preview._doc
            if main_doc:
                for pi in range(len(doc)):
                    if pi < len(main_doc):
                        doc[pi].set_cropbox(main_doc[pi].rect)
            page = doc[self._page_idx]
            if self._cancelled:
                doc.close()
                return
            bboxes, stats = self._preview._detect_native_page(page)
            if self._cancelled:
                doc.close()
                return
            pdf_objects = self._preview._extract_pdf_objects(page)
            doc.close()
            if not self._cancelled:
                self.page_detected.emit(self._page_idx, bboxes, stats, pdf_objects)
        except Exception as e:
            logger.error("DetectWorker page %d failed: %s", self._page_idx, e)


class PageRenderWorker(QThread):
    """Renders PDF pages in a background thread.

    Emits *page_ready* with (page_index, QImage, dpi) for each completed page.
    The caller must convert QImage → QPixmap on the main thread.
    """

    page_ready = Signal(int, QImage, float)  # page_idx, image, dpi
    all_done = Signal()

    def __init__(self, doc_path: str, requests: list[tuple[int, float]], parent=None):
        super().__init__(parent)
        self._doc_path = doc_path
        self._requests = requests  # [(page_idx, dpi), ...]
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        try:
            doc = fitz.open(self._doc_path)
        except Exception:
            return
        try:
            for page_idx, dpi in self._requests:
                if self._cancelled:
                    break
                try:
                    page = doc[page_idx]
                    mat = fitz.Matrix(dpi / 72.0, dpi / 72.0)
                    pix = page.get_pixmap(matrix=mat)
                    img = QImage(
                        pix.samples, pix.width, pix.height,
                        pix.stride, QImage.Format.Format_RGB888,
                    ).copy()  # .copy() — prevent dangling pointer after pix freed
                    if not self._cancelled:
                        self.page_ready.emit(page_idx, img, dpi)
                except Exception as exc:
                    logger.error("Background render page %d failed: %s", page_idx, exc)
        finally:
            doc.close()
        if not self._cancelled:
            self.all_done.emit()


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
    bbox_testbench = Signal(int)              # (page_index,)
    object_stats_requested = Signal(dict, int, str)  # (bbox_dict, page_index, file_path)

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
        self._file_path: Path | None = None  # path to PDF for rendering in dialogs
        self._page_size_pt: tuple[float, float] = (612, 792)  # (width, height) in PDF points
        self._visible_layers: dict[str, bool] = {
            "bbox": True, "table": True, "text": True,
            "photo": True, "picture": True, "drawing": True,
            "template": False,
            "pdf_objects": False,
            "pdf_text": False, "pdf_image": False, "pdf_table": False,
        }
        self._pdf_objects: list[dict] = []  # native PDF object rects
        self._content_mask: dict[str, bool] = {}  # types to mask (hide content)

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

    def set_file_path(self, file_path: Path) -> None:
        """Set the PDF file path (needed for rendering in dialogs)."""
        self._file_path = file_path

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

    def set_pdf_objects(self, objects: list[dict], zoom_factor: float) -> None:
        """Set native PDF object rects (pts coords)."""
        self._pdf_objects = objects
        self._zoom_factor = zoom_factor
        self.update()

    def set_content_mask(self, mask: dict[str, bool]) -> None:
        """Set which object types should have their content masked."""
        self._content_mask = mask
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
        """Right-click: always offer Grow Test + bbox context menu if selected."""
        click = event.pos()
        x_pt, y_pt = self._px_to_pts(QPointF(click))

        # Select bbox under cursor — selectable if bbox layer OR type layer visible
        if self._show_bboxes and self._bboxes_pts:
            show_bbox = self._visible_layers.get("bbox", True)
            hits: list[tuple[int, float]] = []
            for i, bbox in enumerate(self._bboxes_pts):
                if bbox.get("hidden", False) and not self._show_hidden:
                    continue
                bbox_type = bbox.get("type", "unknown")
                type_visible = self._visible_layers.get(bbox_type, False)
                if not show_bbox and not type_visible:
                    continue
                # Skip if content is hidden
                if self._content_mask.get(bbox_type, False):
                    continue
                if bbox.get("is_template") and self._content_mask.get("template", False):
                    continue
                r = self._bbox_rect_px(bbox)
                if r and r.contains(QPoint(click.x(), click.y())):
                    hits.append((i, r.width() * r.height()))
            if hits:
                hits.sort(key=lambda h: h[1])
                self._selected_idx = hits[0][0]
                self.update()

        menu = QMenu(self)
        testbench_action = menu.addAction("Bbox Testbench")
        testbench_action.triggered.connect(
            lambda: self.bbox_testbench.emit(self._page_index)
        )
        menu.addSeparator()

        if self._selected_idx < 0 or self._selected_idx >= len(self._bboxes_pts):
            menu.exec(event.globalPos())
            return
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
        show_bbox = self._visible_layers.get("bbox", True)
        for i, bbox in enumerate(self._bboxes_pts):
            if bbox.get("hidden", False) and not self._show_hidden:
                continue
            bbox_type = bbox.get("type", "unknown")
            type_visible = self._visible_layers.get(bbox_type, False)
            if not show_bbox and not type_visible:
                continue
            # Skip if content is hidden
            if self._content_mask.get(bbox_type, False):
                continue
            if bbox.get("is_template") and self._content_mask.get("template", False):
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

    def mouseDoubleClickEvent(self, event) -> None:
        """Double-click on bbox: open stats dialog."""
        if self._selected_idx < 0 or self._selected_idx >= len(self._bboxes_pts):
            return
        bbox = self._bboxes_pts[self._selected_idx]
        file_path = str(self._file_path) if self._file_path else ""
        self.object_stats_requested.emit(bbox, self._page_index, file_path)

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

        # Content hiding — fill hidden object areas with white
        if self._content_mask and self._bboxes_pts:
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(QColor(255, 255, 255))
            for bbox in self._bboxes_pts:
                bbox_type = bbox.get("type", "unknown")
                is_tpl = bbox.get("is_template", False)
                hide_by_type = self._content_mask.get(bbox_type, False)
                hide_by_tpl = is_tpl and self._content_mask.get("template", False)
                if not hide_by_type and not hide_by_tpl:
                    continue
                r = self._bbox_rect_px(bbox)
                if not r:
                    continue
                painter.drawRect(r)

        # Layer 0: Native PDF objects — dashed rectangles, colored by type
        _PDF_OBJ_COLORS = {
            "pdf_text":  QColor(76, 175, 80, 180),    # green
            "pdf_image": QColor(255, 87, 34, 180),     # orange
            "pdf_table": QColor(0, 120, 215, 180),     # blue
        }
        if self._visible_layers.get("pdf_objects", False) and self._pdf_objects:
            font = QFont("Consolas", 7)
            painter.setFont(font)
            for obj in self._pdf_objects:
                pdf_type = obj.get("pdf_type", "pdf_text")
                if not self._visible_layers.get(pdf_type, True):
                    continue
                pts = obj.get("pts")
                if not pts:
                    continue
                color = _PDF_OBJ_COLORS.get(pdf_type, QColor(150, 150, 150, 180))
                fill = QColor(color)
                fill.setAlpha(30)
                x0, y0, x1, y1 = pts
                r = QRectF(x0 * zf, y0 * zf + h, (x1 - x0) * zf, (y1 - y0) * zf)
                painter.setBrush(fill)
                painter.setPen(QPen(color, 2, Qt.PenStyle.DashLine))
                painter.drawRect(r)
                lbl = obj.get("label", "")
                if lbl:
                    painter.setPen(color)
                    painter.drawText(int(r.x() + 2), int(r.y() + 10), lbl)

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
            # Layer 1: Bounding boxes — dark blue border, 80% transparent fill
            if show_bbox:
                bbox_fill = QColor(20, 40, 80, 13)         # ~95% transparent dark blue
                bbox_border = QColor(20, 40, 120, 220)    # dark blue
                bbox_pen = QPen(bbox_border, 1)
                label_font = QFont("Consolas", 8)
                label_bg = QColor(20, 30, 60, 200)
                label_fg = QColor(255, 255, 255)
                show_template = layers.get("template", False)
                for bbox in self._bboxes_pts:
                    if bbox.get("hidden", False):
                        continue
                    if bbox.get("is_template") and not show_template:
                        continue
                    r = self._bbox_rect_px(bbox)
                    if not r:
                        continue
                    # Template objects: hatch fill
                    if bbox.get("is_template"):
                        hatch_color = QColor(180, 0, 180, 60)
                        painter.setPen(QPen(QColor(180, 0, 180, 120), 1))
                        painter.setBrush(Qt.BrushStyle.NoBrush)
                        painter.drawRect(r)
                        # Draw diagonal hatch lines
                        painter.setPen(QPen(hatch_color, 1))
                        step = 8
                        rx, ry = int(r.x()), int(r.y())
                        rw, rh = int(r.width()), int(r.height())
                        painter.setClipRect(r)
                        for d in range(-rh, rw, step):
                            painter.drawLine(rx + d, ry, rx + d + rh, ry + rh)
                        painter.setClipping(False)
                    else:
                        painter.setBrush(bbox_fill)
                    painter.setPen(bbox_pen)
                    painter.drawRect(r)
                    # Label on outer top side: dark background, white text
                    label = bbox.get("label", bbox.get("type", ""))
                    if label:
                        painter.setFont(label_font)
                        fm = painter.fontMetrics()
                        tw = fm.horizontalAdvance(label) + 6
                        th = fm.height() + 2
                        lx = int(r.x())
                        ly = int(r.y() - th)
                        painter.setPen(Qt.PenStyle.NoPen)
                        painter.setBrush(label_bg)
                        painter.drawRect(lx, ly, tw, th)
                        painter.setPen(label_fg)
                        painter.drawText(lx + 3, ly + fm.ascent() + 1, label)

            # Layer 2: Content-type visualization (colored overlays, table grids)
            for i, bbox in enumerate(self._bboxes_pts):
                if bbox.get("hidden", False):
                    continue

                bbox_type = bbox.get("type", "unknown")
                r = self._bbox_rect_px(bbox)
                if not r:
                    continue

                show_content = layers.get(bbox_type, False)
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

        # Cropping is handled by PDF cropbox — no visual overlay needed

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
        # Spread split mode
        self._spread_mode: bool = False
        self._split_pos: float = 0.5   # center of split
        self._split_gap: float = 0.01  # half-gap width
        self._dragging_split: bool = False

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

        # Spread split lines + gap
        if self._spread_mode:
            left_edge = self._split_pos - self._split_gap
            right_edge = self._split_pos + self._split_gap
            lx_left = ix + int(left_edge * iw)
            lx_right = ix + int(right_edge * iw)
            # Gap fill
            p.setPen(Qt.PenStyle.NoPen)
            p.setBrush(QColor(255, 255, 255, 160))
            p.drawRect(QRectF(lx_left, iy, lx_right - lx_left, ih))
            # Split lines (red, solid)
            split_pen = QPen(QColor(220, 0, 0), 2)
            p.setPen(split_pen)
            p.drawLine(lx_left, iy, lx_left, iy + ih)
            p.drawLine(lx_right, iy, lx_right, iy + ih)
            # Center marker
            cx = ix + int(self._split_pos * iw)
            p.setPen(QPen(QColor(220, 0, 0, 100), 1, Qt.PenStyle.DotLine))
            p.drawLine(cx, iy, cx, iy + ih)

        p.end()

    def mousePressEvent(self, event) -> None:
        pos = event.position()
        ix, iy, iw, ih = self._img_rect()
        r = RULER_SIZE
        x, y = pos.x(), pos.y()
        in_image = ix <= x <= ix + iw and iy <= y <= iy + ih

        # 0. Check split line drag in spread mode
        if self._spread_mode and in_image:
            left_edge = self._split_pos - self._split_gap
            right_edge = self._split_pos + self._split_gap
            lx_l = ix + int(left_edge * iw)
            lx_r = ix + int(right_edge * iw)
            if abs(x - lx_l) < 8 or abs(x - lx_r) < 8:
                self._dragging_split = True
                self._dragging = "split"
                self._drag_pos = self._split_pos
                return

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

        if self._dragging == "split" and iw > 0:
            # Moving split line — adjust gap symmetrically
            new_x = max(0.1, min(0.9, (x - ix) / iw))
            # Which side was closer at press? Compute new gap from movement
            old_gap = self._split_gap
            old_center = self._split_pos
            # The drag moves the edge; gap = distance from center to edge
            delta = abs(new_x - old_center)
            self._split_gap = max(0.005, delta)
            self.update()
            self.lines_changed.emit()
            return
        elif self._dragging == "h" and ih > 0:
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

        if self._dragging == "split":
            self._dragging_split = False
            self._dragging = ""
            self.update()
            self.lines_changed.emit()
            return

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


def _scanline_v1_core(
    binary, h: int, w: int, margin: int = 3, min_obj: int = 20,
    skip_rects: list | None = None,
) -> tuple[list[tuple[int, int, int, int]], list[tuple[int, int, int, int]]]:
    """Shared scanline v1: grow-from-seed on binary image.

    Returns (bboxes, artifacts). Skips skip_rects areas.
    Full-page frames (>80% area) are erased and skipped.
    """
    page_area = h * w
    all_scan = list(skip_rects or [])
    bboxes: list[tuple[int, int, int, int]] = []
    artifacts: list[tuple[int, int, int, int]] = []
    y = 0
    while y < h:
        x = 0
        while x < w:
            skipped = False
            for (bx0, by0, bx1, by1) in all_scan:
                if bx0 - 2 <= x <= bx1 + 2 and by0 - 2 <= y <= by1 + 2:
                    x = int(bx1) + 3; skipped = True; break
            if skipped:
                continue
            if binary[y, x]:
                gx0, gy0 = x, y
                gx1, gy1 = min(w, x + 1), min(h, y + 1)
                for _ in range(4000):
                    grown = False
                    t = max(0, gy0 - margin)
                    if t < gy0 and binary[t:gy0, gx0:gx1].any():
                        gy0 = max(0, gy0 - 1); grown = True
                    b = min(h, gy1 + margin)
                    if b > gy1 and binary[gy1:b, gx0:gx1].any():
                        gy1 = min(h, gy1 + 1); grown = True
                    l = max(0, gx0 - margin)
                    if l < gx0 and binary[gy0:gy1, l:gx0].any():
                        gx0 = max(0, gx0 - 1); grown = True
                    r = min(w, gx1 + margin)
                    if r > gx1 and binary[gy0:gy1, gx1:r].any():
                        gx1 = min(w, gx1 + 1); grown = True
                    if not grown:
                        break
                bw, bh = gx1 - gx0, gy1 - gy0
                bt = (gx0, gy0, gx1, gy1)
                if bw * bh > page_area * 0.8:
                    bd = 4
                    binary[gy0:gy0+bd, gx0:gx1] = 0
                    binary[max(gy0, gy1-bd):gy1, gx0:gx1] = 0
                    binary[gy0:gy1, gx0:gx0+bd] = 0
                    binary[gy0:gy1, max(gx0, gx1-bd):gx1] = 0
                    x += 1; continue
                all_scan.append(bt)
                if bw < min_obj and bh < min_obj:
                    artifacts.append(bt)
                else:
                    bboxes.append(bt)
                x = gx1 + 3
            else:
                x += 1
        y += 1
    # Remove nested
    if len(bboxes) > 1:
        keep = []
        for i, a in enumerate(bboxes):
            a_area = max((a[2]-a[0])*(a[3]-a[1]), 1)
            nested = False
            for j, b in enumerate(bboxes):
                if i == j: continue
                b_area = (b[2]-b[0])*(b[3]-b[1])
                if b_area <= a_area: continue
                ix0 = max(a[0],b[0]); iy0 = max(a[1],b[1])
                ix1 = min(a[2],b[2]); iy1 = min(a[3],b[3])
                if ix0<ix1 and iy0<iy1 and (ix1-ix0)*(iy1-iy0)/a_area >= 0.9:
                    nested = True; break
            if not nested:
                keep.append(a)
        bboxes = keep
    return bboxes, artifacts


class ScanlineTestDialog(QWidget):
    """Debug dialog: object detection algorithm comparison bench."""

    _ALGOS = [
        "Scanline v1 (original)",
        "Scanline v2 (visited + fast grow)",
        "Scanline v3 (adaptive margin)",
        "OpenCV CCA",
        "OpenCV CCA + dilation",
        "MSER text zones",
        "Scanline + MSER split",
        "PDF text + Scanline",
        "Hybrid 2-pass",
        "PDF objects only",
    ]

    def __init__(self, doc, page_idx: int, parent=None):
        super().__init__(parent)
        import numpy as np
        self.setWindowTitle(f"Bbox Testbench — Page {page_idx+1}")
        self.setWindowFlags(Qt.WindowType.Window)
        self.resize(900, 750)

        page = doc[page_idx]
        self._page = page
        self._dpi = 90
        pix = page.get_pixmap(dpi=self._dpi)
        self._page_img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(
            pix.height, pix.width, pix.n
        ).copy()
        self._h, self._w = pix.height, pix.width
        self._scale = self._dpi / 72  # px per pt

        gray = np.mean(self._page_img[:, :, :3], axis=2)
        self._binary = (gray < 240).astype(np.uint8)

        # Save unmasked binary for hybrid 2-pass
        self._binary_raw = self._binary.copy()

        # Extract native PDF text blocks and mask them from binary
        self._pdf_text_blocks: list[tuple[int, int, int, int]] = []
        blocks = page.get_text("blocks")
        for b in blocks:
            if b[6] == 0:  # text block
                x0 = int(b[0] * self._scale)
                y0 = int(b[1] * self._scale)
                x1 = int(b[2] * self._scale)
                y1 = int(b[3] * self._scale)
                x0 = max(0, min(x0, self._w))
                y0 = max(0, min(y0, self._h))
                x1 = max(0, min(x1, self._w))
                y1 = max(0, min(y1, self._h))
                if x1 > x0 and y1 > y0:
                    self._pdf_text_blocks.append((x0, y0, x1, y1))
                    self._binary[y0:y1, x0:x1] = 0  # mask out text

        # UI
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)

        top_bar = QHBoxLayout()
        top_bar.addWidget(QLabel("Algorithm:"))
        self._algo_combo = QComboBox()
        self._algo_combo.addItems(self._ALGOS)
        self._algo_combo.setFixedHeight(24)
        self._algo_combo.currentIndexChanged.connect(self._on_algo_changed)
        top_bar.addWidget(self._algo_combo)
        self._merge_cb = QCheckBox("Merge rows")
        self._merge_cb.setStyleSheet("font-size: 10px;")
        self._merge_cb.toggled.connect(lambda: self._on_algo_changed(self._algo_combo.currentIndex()))
        top_bar.addWidget(self._merge_cb)
        self._show_bboxes_cb = QCheckBox("Bboxes")
        self._show_bboxes_cb.setChecked(True)
        self._show_bboxes_cb.setStyleSheet("font-size: 10px;")
        self._show_bboxes_cb.toggled.connect(self._render)
        top_bar.addWidget(self._show_bboxes_cb)
        top_bar.addStretch()
        layout.addLayout(top_bar)

        self._info = QLabel()
        self._info.setStyleSheet("font: 11px Consolas;")
        layout.addWidget(self._info)

        self._image_label = QLabel()
        self._image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        scroll = QScrollArea()
        scroll.setWidget(self._image_label)
        scroll.setWidgetResizable(True)
        layout.addWidget(scroll)

        # Animation slider
        anim_bar = QHBoxLayout()
        self._anim_slider = QSlider(Qt.Orientation.Horizontal)
        self._anim_slider.setMinimum(0)
        self._anim_slider.setMaximum(0)
        self._anim_slider.valueChanged.connect(self._on_anim_step)
        anim_bar.addWidget(self._anim_slider)
        self._anim_label = QLabel("—")
        self._anim_label.setFixedWidth(120)
        self._anim_label.setStyleSheet("font: 10px Consolas;")
        anim_bar.addWidget(self._anim_label)
        layout.addLayout(anim_bar)

        self._step_label = QLabel()
        self._step_label.setStyleSheet("font: 11px Consolas;")
        layout.addWidget(self._step_label)

        self._bboxes: list[tuple[int, int, int, int]] = []
        self._artifacts: list[tuple[int, int, int, int]] = []
        self._detection_steps: list[tuple[
            list[tuple[int, int, int, int]],
            list[tuple[int, int, int, int]],
            int,
        ]] = []  # (bboxes_so_far, artifacts_so_far, scan_y)
        self._on_algo_changed(0)
        self.show()

    # ── Algorithm dispatcher ─────────────────────────────────────

    def _on_algo_changed(self, idx: int) -> None:
        import time
        t0 = time.perf_counter()
        runners = [
            self._run_v1_original,
            self._run_v2_visited_fast,
            self._run_v3_adaptive,
            lambda: self._run_opencv_cca(dilate=False),
            lambda: self._run_opencv_cca(dilate=True),
            self._run_mser_text,
            self._run_scanline_mser_split,
            self._run_pdf_text_scanline,
            self._run_hybrid_2pass,
        ]
        self._bboxes, self._artifacts = runners[idx]()
        if self._merge_cb.isChecked():
            self._bboxes = self._merge_text_rows(self._bboxes)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        extra = ""
        if idx == 2:
            extra = f"  margin={self._last_margin}"
        self._info.setText(
            f"Page: {self._w}x{self._h}px ({self._dpi} DPI)  "
            f"Objects: {len(self._bboxes)}  Artifacts: {len(self._artifacts)}  "
            f"Time: {elapsed_ms:.1f}ms{extra}"
        )
        # Update animation slider
        n_steps = len(self._detection_steps)
        if n_steps > 0:
            self._anim_slider.setMaximum(n_steps - 1)
            self._anim_slider.setValue(n_steps - 1)
        else:
            self._anim_slider.setMaximum(0)
        self._render()

    # ── v1: Original scanline (margin=3, 1px steps, O(n²) skip) ──

    def _run_v1_original(self):
        self._detection_steps = []
        min_obj = int(10 * self._scale)
        bboxes, artifacts = _scanline_v1_core(
            self._binary, self._h, self._w, margin=3, min_obj=min_obj,
        )
        self._last_margin = 3
        return bboxes, artifacts

    def _grow_1px(self, cx: int, cy: int, margin: int = 3):
        """Original grow: expand 1px per iteration."""
        x0, y0 = cx, cy
        x1, y1 = min(self._w, cx + 1), min(self._h, cy + 1)
        for _ in range(4000):
            grown = False
            t = max(0, y0 - margin)
            if t < y0 and self._binary[t:y0, x0:x1].any():
                y0 = max(0, y0 - 1); grown = True
            b = min(self._h, y1 + margin)
            if b > y1 and self._binary[y1:b, x0:x1].any():
                y1 = min(self._h, y1 + 1); grown = True
            l = max(0, x0 - margin)
            if l < x0 and self._binary[y0:y1, l:x0].any():
                x0 = max(0, x0 - 1); grown = True
            r = min(self._w, x1 + margin)
            if r > x1 and self._binary[y0:y1, x1:r].any():
                x1 = min(self._w, x1 + 1); grown = True
            if not grown:
                break
        return (x0, y0, x1, y1)

    # ── v2: Visited mask + fast grow (jump to content) ───────────

    def _run_v2_visited_fast(self):
        import numpy as np
        min_obj = int(10 * self._scale)
        visited = np.zeros((self._h, self._w), dtype=bool)
        bboxes: list[tuple[int, int, int, int]] = []
        artifacts: list[tuple[int, int, int, int]] = []

        for y in range(self._h):
            x = 0
            while x < self._w:
                if visited[y, x] or not self._binary[y, x]:
                    x += 1
                    continue
                bbox = self._grow_fast(x, y, margin=3)
                x0, y0, x1, y1 = bbox
                visited[y0:y1, x0:x1] = True
                if (x1 - x0) < min_obj and (y1 - y0) < min_obj:
                    artifacts.append(bbox)
                else:
                    bboxes.append(bbox)
                x = x1 + 1
        self._last_margin = 3
        return bboxes, artifacts

    def _grow_fast(self, cx: int, cy: int, margin: int = 3):
        """Fast grow: jump directly to the nearest/farthest content pixel."""
        import numpy as np
        x0, y0 = cx, cy
        x1, y1 = min(self._w, cx + 1), min(self._h, cy + 1)
        binary = self._binary
        for _ in range(2000):
            grown = False
            # Top: check margin strip above, jump to topmost content row
            t = max(0, y0 - margin)
            if t < y0:
                strip = binary[t:y0, x0:x1]
                if strip.any():
                    rows = np.where(strip.any(axis=1))[0]
                    y0 = t + rows[0]; grown = True
            # Bottom: jump to bottommost content row
            b = min(self._h, y1 + margin)
            if b > y1:
                strip = binary[y1:b, x0:x1]
                if strip.any():
                    rows = np.where(strip.any(axis=1))[0]
                    y1 = y1 + rows[-1] + 1; grown = True
            # Left: jump to leftmost content column
            l = max(0, x0 - margin)
            if l < x0:
                strip = binary[y0:y1, l:x0]
                if strip.any():
                    cols = np.where(strip.any(axis=0))[0]
                    x0 = l + cols[0]; grown = True
            # Right: jump to rightmost content column
            r = min(self._w, x1 + margin)
            if r > x1:
                strip = binary[y0:y1, x1:r]
                if strip.any():
                    cols = np.where(strip.any(axis=0))[0]
                    x1 = x1 + cols[-1] + 1; grown = True
            if not grown:
                break
        return (x0, y0, x1, y1)

    # ── v3: Adaptive margin from projection profile ──────────────

    def _run_v3_adaptive(self):
        import numpy as np
        min_obj = int(10 * self._scale)
        margin = self._compute_adaptive_margin()
        self._last_margin = margin
        visited = np.zeros((self._h, self._w), dtype=bool)
        bboxes: list[tuple[int, int, int, int]] = []
        artifacts: list[tuple[int, int, int, int]] = []

        for y in range(self._h):
            x = 0
            while x < self._w:
                if visited[y, x] or not self._binary[y, x]:
                    x += 1
                    continue
                bbox = self._grow_fast(x, y, margin=margin)
                x0, y0, x1, y1 = bbox
                visited[y0:y1, x0:x1] = True
                if (x1 - x0) < min_obj and (y1 - y0) < min_obj:
                    artifacts.append(bbox)
                else:
                    bboxes.append(bbox)
                x = x1 + 1
        return bboxes, artifacts

    def _compute_adaptive_margin(self) -> int:
        """Derive margin from horizontal projection profile gaps (RLSA-inspired).

        Computes median whitespace gap between content rows. Margin = ~40%
        of that median, clamped to [2, 15]. Larger gaps on sparse pages
        yield bigger margins; dense text pages yield smaller ones.
        """
        import numpy as np
        h_proj = self._binary.sum(axis=1)
        threshold = self._w * 0.01  # row is "empty" if <1% pixels dark
        is_gap = h_proj < threshold
        # Measure gap run lengths
        gaps: list[int] = []
        gap_len = 0
        for v in is_gap:
            if v:
                gap_len += 1
            else:
                if gap_len > 2:
                    gaps.append(gap_len)
                gap_len = 0
        if not gaps:
            return 3
        median_gap = int(np.median(gaps))
        return max(2, min(int(median_gap * 0.4), 15))

    # ── OpenCV CCA ───────────────────────────────────────────────

    def _run_opencv_cca(self, dilate: bool = False):
        import numpy as np
        try:
            import cv2
        except ImportError:
            logging.warning("OpenCV not installed — CCA unavailable")
            self._last_margin = 0
            return [], []
        min_obj = int(10 * self._scale)
        page_area = self._w * self._h

        binary = (self._binary * 255).astype(np.uint8)
        if dilate:
            kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
            binary = cv2.dilate(binary, kernel, iterations=2)

        n_labels, _labels, stats, _centroids = cv2.connectedComponentsWithStats(
            binary, connectivity=8,
        )

        bboxes: list[tuple[int, int, int, int]] = []
        artifacts: list[tuple[int, int, int, int]] = []
        for i in range(1, n_labels):  # skip background
            x = stats[i, cv2.CC_STAT_LEFT]
            y = stats[i, cv2.CC_STAT_TOP]
            w = stats[i, cv2.CC_STAT_WIDTH]
            h = stats[i, cv2.CC_STAT_HEIGHT]
            area = stats[i, cv2.CC_STAT_AREA]
            if area < 50:
                continue
            if w * h > page_area * 0.8:
                continue
            bbox = (x, y, x + w, y + h)
            if w < min_obj and h < min_obj:
                artifacts.append(bbox)
            else:
                bboxes.append(bbox)
        self._last_margin = 0
        return bboxes, artifacts

    # ── MSER text zone detection ───────────────────────────────

    def _run_mser_text(self):
        """Detect text regions via MSER + geometric filtering + row grouping.

        MSER finds stable extremal regions (character candidates).
        Filter by size/aspect → group into text lines → merge into zones.
        Non-text objects detected via CCA on the remaining pixels.
        """
        import numpy as np
        try:
            import cv2
        except ImportError:
            logging.warning("OpenCV not installed — MSER unavailable")
            self._last_margin = 0
            return [], []

        min_obj = int(10 * self._scale)
        gray = (255 - self._binary * 255).astype(np.uint8)  # inverted: text=white bg
        # Proper grayscale from page image
        gray_img = np.mean(self._page_img[:, :, :3], axis=2).astype(np.uint8)

        # MSER detection
        mser = cv2.MSER_create()
        mser.setDelta(5)
        mser.setMinArea(20)
        mser.setMaxArea(int(self._h * self._w * 0.01))
        mser.setMaxVariation(0.25)
        regions, _ = mser.detectRegions(gray_img)

        # Extract bounding rects and filter for text-like shapes
        char_bboxes: list[tuple[int, int, int, int]] = []
        for region in regions:
            x, y, w, h = cv2.boundingRect(region)
            if w < 3 or h < 3:
                continue
            aspect = w / h
            area = w * h
            # Text characters: reasonable aspect ratio, not too large
            if 0.1 < aspect < 10 and h < 80 and area < 3000:
                char_bboxes.append((x, y, x + w, y + h))

        if not char_bboxes:
            self._last_margin = 0
            return [], []

        # Group characters into text lines by Y proximity
        # Sort by vertical center
        char_bboxes.sort(key=lambda b: (b[1] + b[3]) / 2)
        heights = [b[3] - b[1] for b in char_bboxes]
        med_h = max(int(np.median(heights)), 3)

        # Row grouping
        rows: list[list[tuple[int, int, int, int]]] = []
        current_row = [char_bboxes[0]]
        row_y0 = char_bboxes[0][1]
        row_y1 = char_bboxes[0][3]
        for b in char_bboxes[1:]:
            b_cy = (b[1] + b[3]) / 2
            if row_y0 - med_h * 0.5 <= b_cy <= row_y1 + med_h * 0.5:
                current_row.append(b)
                row_y0 = min(row_y0, b[1])
                row_y1 = max(row_y1, b[3])
            else:
                rows.append(current_row)
                current_row = [b]
                row_y0 = b[1]
                row_y1 = b[3]
        rows.append(current_row)

        # Merge chars within each row into text line bboxes
        # Then merge close lines vertically into text zones
        line_bboxes: list[tuple[int, int, int, int]] = []
        for row in rows:
            row.sort(key=lambda b: b[0])
            # Merge horizontally close chars (gap < med_h * 2)
            gx0, gy0, gx1, gy1 = row[0]
            for b in row[1:]:
                if b[0] - gx1 <= med_h * 2:
                    gx0 = min(gx0, b[0])
                    gy0 = min(gy0, b[1])
                    gx1 = max(gx1, b[2])
                    gy1 = max(gy1, b[3])
                else:
                    line_bboxes.append((gx0, gy0, gx1, gy1))
                    gx0, gy0, gx1, gy1 = b
            line_bboxes.append((gx0, gy0, gx1, gy1))

        # Merge vertically adjacent lines (same column, gap < med_h)
        merged = True
        while merged:
            merged = False
            new_lines: list[list[int]] = [list(line_bboxes[0])]
            for b in line_bboxes[1:]:
                absorbed = False
                for a in new_lines:
                    # X overlap check
                    ox = max(0, min(a[2], b[2]) - max(a[0], b[0]))
                    min_w = min(a[2] - a[0], b[2] - b[0])
                    if min_w > 0 and ox / min_w > 0.3:
                        v_gap = max(b[1] - a[3], a[1] - b[3])
                        if 0 <= v_gap <= med_h * 1.2:
                            a[0] = min(a[0], b[0])
                            a[1] = min(a[1], b[1])
                            a[2] = max(a[2], b[2])
                            a[3] = max(a[3], b[3])
                            absorbed = True
                            merged = True
                            break
                if not absorbed:
                    new_lines.append(list(b))
            line_bboxes = [tuple(l) for l in new_lines]

        # Merge overlapping bboxes (IoU > 0 or containment)
        changed = True
        while changed:
            changed = False
            result: list[list[int]] = []
            used_m: set[int] = set()
            for i, a in enumerate(line_bboxes):
                if i in used_m:
                    continue
                acc = list(a)
                for j, b in enumerate(line_bboxes):
                    if j <= i or j in used_m:
                        continue
                    # Check overlap
                    ox0 = max(acc[0], b[0]); oy0 = max(acc[1], b[1])
                    ox1 = min(acc[2], b[2]); oy1 = min(acc[3], b[3])
                    if ox0 < ox1 and oy0 < oy1:
                        acc[0] = min(acc[0], b[0])
                        acc[1] = min(acc[1], b[1])
                        acc[2] = max(acc[2], b[2])
                        acc[3] = max(acc[3], b[3])
                        used_m.add(j)
                        changed = True
                result.append(acc)
            line_bboxes = [tuple(r) for r in result]

        # Remove nested (>=80% area inside a larger bbox)
        filtered: list[tuple[int, int, int, int]] = []
        for i, a in enumerate(line_bboxes):
            a_area = max((a[2] - a[0]) * (a[3] - a[1]), 1)
            nested = False
            for j, b in enumerate(line_bboxes):
                if i == j:
                    continue
                b_area = (b[2] - b[0]) * (b[3] - b[1])
                if b_area <= a_area:
                    continue
                ix0 = max(a[0], b[0]); iy0 = max(a[1], b[1])
                ix1 = min(a[2], b[2]); iy1 = min(a[3], b[3])
                if ix0 < ix1 and iy0 < iy1:
                    inter = (ix1 - ix0) * (iy1 - iy0)
                    if inter / a_area >= 0.8:
                        nested = True
                        break
            if not nested:
                filtered.append(a)
        line_bboxes = filtered

        # Separate into objects vs artifacts
        bboxes: list[tuple[int, int, int, int]] = []
        artifacts: list[tuple[int, int, int, int]] = []
        for b in line_bboxes:
            w, h = b[2] - b[0], b[3] - b[1]
            if w < min_obj and h < min_obj:
                artifacts.append(b)
            else:
                bboxes.append(b)

        self._last_margin = 0
        return bboxes, artifacts

    # ── Combined: Scanline + MSER split ────────────────────────

    def _run_scanline_mser_split(self):
        """Scanline detects all, non-text excluded, MSER on remainder.

        1. Scanline v1 → all bboxes
        2. Classify non-text by density/shape (images, drawings, bars)
        3. Mask out non-text areas from grayscale image
        4. MSER on masked image → clean text zones
        5. Result: non-text (red) + MSER text zones (green)
        """
        import numpy as np
        import cv2

        # Step 1: Scanline → all objects
        scan_bboxes, scan_artifacts = self._run_v1_original()
        binary = self._binary

        # Step 2: Classify scanline bboxes as non-text by density/shape
        nontext: list[tuple[int, int, int, int]] = []
        for sb in scan_bboxes:
            bw, bh = sb[2] - sb[0], sb[3] - sb[1]
            region = binary[sb[1]:sb[3], sb[0]:sb[2]]
            density = float(region.sum()) / max(region.size, 1)
            aspect = bw / max(bh, 1)
            # Non-text criteria:
            #   - Dense + large square-ish block (image/drawing)
            #   - Very dense horizontal bar (colored bar)
            is_nontext = False
            if bh > 35 and bw > 35 and aspect < 2.0 and density > 0.10:
                is_nontext = True   # image / drawing
            if bh < 25 and bw > 100 and density > 0.60:
                is_nontext = True   # dense horizontal bar
            if density > 0.70:
                is_nontext = True   # very dense fill (any shape)
            if is_nontext:
                nontext.append(sb)

        # Step 3: Mask out non-text areas from grayscale
        gray_img = np.mean(self._page_img[:, :, :3], axis=2).astype(np.uint8)
        masked = gray_img.copy()
        for nt in nontext:
            masked[nt[1]:nt[3], nt[0]:nt[2]] = 255  # white out

        # Step 4: MSER on masked image → text zones only
        mser = cv2.MSER_create()
        mser.setDelta(5)
        mser.setMinArea(20)
        mser.setMaxArea(int(self._h * self._w * 0.01))
        mser.setMaxVariation(0.25)
        regions, _ = mser.detectRegions(masked)

        # Extract char bboxes
        char_bboxes: list[tuple[int, int, int, int]] = []
        for region in regions:
            x, y, w, h = cv2.boundingRect(region)
            if w < 3 or h < 3:
                continue
            ar = w / h
            if 0.1 < ar < 10 and h < 80 and w * h < 3000:
                char_bboxes.append((x, y, x + w, y + h))

        # Group chars → lines → paragraphs
        text_bboxes: list[tuple[int, int, int, int]] = []
        if char_bboxes:
            char_bboxes.sort(key=lambda b: ((b[1] + b[3]) / 2, b[0]))
            heights = [b[3] - b[1] for b in char_bboxes]
            med_h = max(int(np.median(heights)), 3)

            # Row grouping
            rows: list[list[tuple[int, int, int, int]]] = []
            cur_row = [char_bboxes[0]]
            ry0, ry1 = char_bboxes[0][1], char_bboxes[0][3]
            for b in char_bboxes[1:]:
                bcy = (b[1] + b[3]) / 2
                if ry0 - med_h * 0.5 <= bcy <= ry1 + med_h * 0.5:
                    cur_row.append(b)
                    ry0 = min(ry0, b[1]); ry1 = max(ry1, b[3])
                else:
                    rows.append(cur_row)
                    cur_row = [b]; ry0 = b[1]; ry1 = b[3]
            rows.append(cur_row)

            # H-merge within rows (generous gap to bridge word spaces)
            h_gap = max(med_h * 4, 20)
            lines: list[tuple[int, int, int, int]] = []
            for row in rows:
                row.sort(key=lambda b: b[0])
                gx0, gy0, gx1, gy1 = row[0]
                for b in row[1:]:
                    if b[0] - gx1 <= h_gap:
                        gx0 = min(gx0, b[0]); gy0 = min(gy0, b[1])
                        gx1 = max(gx1, b[2]); gy1 = max(gy1, b[3])
                    else:
                        lines.append((gx0, gy0, gx1, gy1))
                        gx0, gy0, gx1, gy1 = b
                lines.append((gx0, gy0, gx1, gy1))

            # Expand-merge: similar-height bboxes expand L/R by own width,
            # merge if overlapping with similar-sized neighbor → chains into lines
            for _pass in range(10):
                merged_any = False
                lines.sort(key=lambda b: (b[0], b[1]))
                new_lines: list[tuple[int, int, int, int]] = []
                skip: set[int] = set()
                for i in range(len(lines)):
                    if i in skip:
                        continue
                    a = list(lines[i])
                    aw, ah = a[2] - a[0], a[3] - a[1]
                    # Expand horizontally by own width (capped to page)
                    exp_x0 = max(0, a[0] - aw)
                    exp_x1 = min(self._w, a[2] + aw)
                    for j in range(i + 1, len(lines)):
                        if j in skip:
                            continue
                        b = lines[j]
                        bw, bh = b[2] - b[0], b[3] - b[1]
                        # Similar height? (within 80% tolerance)
                        if ah > 0 and bh > 0:
                            ratio = min(ah, bh) / max(ah, bh)
                            if ratio < 0.4:
                                continue
                        # Y overlap (same row)?
                        oy = max(0, min(a[3], b[3]) - max(a[1], b[1]))
                        if oy < min(ah, bh) * 0.4:
                            continue
                        # Does expanded A overlap with B?
                        if exp_x0 <= b[2] and exp_x1 >= b[0]:
                            a[0] = min(a[0], b[0])
                            a[1] = min(a[1], b[1])
                            a[2] = max(a[2], b[2])
                            a[3] = max(a[3], b[3])
                            aw, ah = a[2] - a[0], a[3] - a[1]
                            exp_x0 = max(0, a[0] - aw)
                            exp_x1 = min(self._w, a[2] + aw)
                            skip.add(j)
                            merged_any = True
                    new_lines.append(tuple(a))
                lines = new_lines
                if not merged_any:
                    break

            # Remove overlap + nested
            ch = True
            while ch:
                ch = False
                res: list[list[int]] = []
                us: set[int] = set()
                for i, a in enumerate(lines):
                    if i in us: continue
                    acc = list(a)
                    for j, b in enumerate(lines):
                        if j <= i or j in us: continue
                        if max(acc[0], b[0]) < min(acc[2], b[2]) and max(acc[1], b[1]) < min(acc[3], b[3]):
                            acc[0] = min(acc[0], b[0]); acc[1] = min(acc[1], b[1])
                            acc[2] = max(acc[2], b[2]); acc[3] = max(acc[3], b[3])
                            us.add(j); ch = True
                    res.append(acc)
                lines = [tuple(r) for r in res]

            min_obj = int(10 * self._scale)
            text_bboxes = [b for b in lines if (b[2]-b[0]) >= min_obj or (b[3]-b[1]) >= min_obj]

        self._text_bboxes = text_bboxes
        self._nontext_bboxes = nontext
        self._last_margin = 3
        return text_bboxes + nontext, scan_artifacts

    # ── Hybrid 2-pass: with and without PDF text mask ──────────

    def _run_hybrid_2pass(self):
        """Two-pass detection to handle tables spanning text areas.

        Pass 1: masked binary (PDF text removed) → non-text objects
        Pass 2: raw binary (no mask) → full connected components
        Merge: pass-2 bbox covering multiple pass-1 bboxes → single table
        Result: PDF text (green) + merged non-text (red)
        """
        import numpy as np

        # Pass 1: scanline on masked binary (text removed)
        pass1_bboxes, pass1_artifacts = self._run_v1_original()

        # Pass 2: scanline on raw binary (with text)
        orig = self._binary
        self._binary = self._binary_raw.copy()
        pass2_bboxes, _ = self._run_v1_original()
        self._binary = orig

        # For each pass-2 bbox: find how many pass-1 bboxes it contains
        merged: list[tuple[int, int, int, int]] = []
        used_p1: set[int] = set()

        for p2 in pass2_bboxes:
            contained: list[int] = []
            for i, p1 in enumerate(pass1_bboxes):
                if i in used_p1:
                    continue
                # Check if p1 is mostly inside p2 (>=70%)
                p1_area = max((p1[2]-p1[0]) * (p1[3]-p1[1]), 1)
                ix0 = max(p1[0], p2[0]); iy0 = max(p1[1], p2[1])
                ix1 = min(p1[2], p2[2]); iy1 = min(p1[3], p2[3])
                if ix0 < ix1 and iy0 < iy1:
                    inter = (ix1-ix0) * (iy1-iy0)
                    if inter / p1_area >= 0.7:
                        contained.append(i)

            if len(contained) >= 2:
                # Multiple pass-1 bboxes inside one pass-2 → merge as table
                merged.append(p2)
                used_p1.update(contained)

        # Keep unmerged pass-1 bboxes
        remaining_p1 = [b for i, b in enumerate(pass1_bboxes) if i not in used_p1]

        self._text_bboxes = list(self._pdf_text_blocks)
        self._nontext_bboxes = remaining_p1 + merged
        self._last_margin = 3
        all_bboxes = self._text_bboxes + self._nontext_bboxes
        return all_bboxes, pass1_artifacts

    # ── PDF text blocks + Scanline for non-text ────────────────

    def _run_pdf_text_scanline(self):
        """Extract text blocks from PDF native layer, scanline for the rest.

        1. page.get_text("blocks") → native text bboxes (green)
        2. Mask text areas on binary image (white out)
        3. Scanline v1 on masked image → non-text objects (red)
        """
        import numpy as np

        # Step 1: Extract native PDF text blocks
        blocks = self._page.get_text("blocks")
        scale = self._dpi / 72  # pt → px
        text_bboxes: list[tuple[int, int, int, int]] = []
        for b in blocks:
            # b = (x0, y0, x1, y1, text_or_img, block_no, block_type)
            # block_type: 0=text, 1=image
            if b[6] == 0:  # text block
                x0 = int(b[0] * scale)
                y0 = int(b[1] * scale)
                x1 = int(b[2] * scale)
                y1 = int(b[3] * scale)
                # Clamp to page
                x0 = max(0, min(x0, self._w))
                y0 = max(0, min(y0, self._h))
                x1 = max(0, min(x1, self._w))
                y1 = max(0, min(y1, self._h))
                if x1 > x0 and y1 > y0:
                    text_bboxes.append((x0, y0, x1, y1))

        # Step 2: Mask text areas on binary image
        binary_masked = self._binary.copy()
        for tb in text_bboxes:
            binary_masked[tb[1]:tb[3], tb[0]:tb[2]] = 0  # clear text pixels

        # Step 3: Scanline on masked binary → non-text only
        orig_binary = self._binary
        self._binary = binary_masked
        nontext_bboxes, artifacts = self._run_v1_original()
        self._binary = orig_binary  # restore

        self._text_bboxes = text_bboxes
        self._nontext_bboxes = nontext_bboxes
        self._last_margin = 3
        return text_bboxes + nontext_bboxes, artifacts

    # ── Post-processing: merge letters/words into text rows ────

    def _merge_text_rows(
        self,
        bboxes: list[tuple[int, int, int, int]],
    ) -> list[tuple[int, int, int, int]]:
        """Expand-merge: similar-height bboxes expand L/R, chain into lines.

        For each small bbox: expand horizontally by own width (100%).
        If expanded overlaps a similar-height neighbor on same row → merge.
        Chains: A+B → AB+C → ABC+D → full text line.
        Large/dense bboxes (images, tables) are kept as-is.
        """
        import numpy as np
        if len(bboxes) < 2:
            return bboxes

        heights = [b[3] - b[1] for b in bboxes]
        med_h = max(int(np.median(heights)), 3)

        # Separate: small text-like vs large non-text
        text: list[list[int]] = []
        non_text: list[tuple[int, int, int, int]] = []
        for b in bboxes:
            bw, bh = b[2] - b[0], b[3] - b[1]
            if bh <= med_h * 2.5 and bh < 60:
                text.append(list(b))
            else:
                non_text.append(b)

        if len(text) < 2:
            return non_text + [tuple(t) for t in text]

        # Expand-merge passes
        for _pass in range(15):
            merged_any = False
            text.sort(key=lambda b: (b[0], b[1]))
            new_text: list[list[int]] = []
            skip: set[int] = set()
            for i in range(len(text)):
                if i in skip:
                    continue
                a = text[i]
                aw, ah = a[2] - a[0], a[3] - a[1]
                # Expand L/R by own width
                exp_x0 = max(0, a[0] - aw)
                exp_x1 = min(self._w, a[2] + aw)
                for j in range(i + 1, len(text)):
                    if j in skip:
                        continue
                    b = text[j]
                    bh = b[3] - b[1]
                    # Similar height? (within 60% tolerance)
                    if ah > 0 and bh > 0:
                        ratio = min(ah, bh) / max(ah, bh)
                        if ratio < 0.4:
                            continue
                    # Same row? (Y overlap > 40% of smaller)
                    oy = max(0, min(a[3], b[3]) - max(a[1], b[1]))
                    if oy < min(ah, bh) * 0.4:
                        continue
                    # Expanded A overlaps B?
                    if exp_x0 <= b[2] and exp_x1 >= b[0]:
                        a[0] = min(a[0], b[0])
                        a[1] = min(a[1], b[1])
                        a[2] = max(a[2], b[2])
                        a[3] = max(a[3], b[3])
                        aw, ah = a[2] - a[0], a[3] - a[1]
                        exp_x0 = max(0, a[0] - aw)
                        exp_x1 = min(self._w, a[2] + aw)
                        skip.add(j)
                        merged_any = True
                new_text.append(a)
            text = new_text
            if not merged_any:
                break

        return non_text + [tuple(t) for t in text]

    # ── Rendering ────────────────────────────────────────────────

    def _on_anim_step(self, step: int) -> None:
        """Render at a specific detection step."""
        if not self._detection_steps:
            return
        step = min(step, len(self._detection_steps) - 1)
        bboxes, artifacts, scan_y = self._detection_steps[step]
        self._anim_label.setText(
            f"Step {step+1}/{len(self._detection_steps)}"
        )
        self._render_frame(bboxes, artifacts, scan_y)

    def _render(self, _=None) -> None:
        """Render final result (all bboxes)."""
        is_split = self._algo_combo.currentIndex() in (6, 7, 8)
        if is_split and hasattr(self, '_text_bboxes'):
            self._render_frame(
                self._text_bboxes, self._artifacts, -1,
                nontext=self._nontext_bboxes,
            )
            self._step_label.setText(
                f"Text: {len(self._text_bboxes)}  Non-text: {len(self._nontext_bboxes)}  "
                f"Artifacts: {len(self._artifacts)}  "
                f"PDF blocks: {len(self._pdf_text_blocks)}"
            )
        else:
            self._render_frame(self._bboxes, self._artifacts, -1)
            self._step_label.setText(
                f"Objects: {len(self._bboxes)}  Artifacts: {len(self._artifacts)}  "
                f"PDF blocks: {len(self._pdf_text_blocks)}"
            )

    def _render_frame(
        self,
        bboxes: list[tuple[int, int, int, int]],
        artifacts: list[tuple[int, int, int, int]],
        scan_y: int = -1,
        nontext: list[tuple[int, int, int, int]] | None = None,
    ) -> None:
        vis = self._page_img.copy()
        show = self._show_bboxes_cb.isChecked()

        # PDF text blocks in blue
        for (x0, y0, x1, y1) in self._pdf_text_blocks:
            self._draw_rect(vis, x0, y0, x1, y1, [50, 100, 220], 1)

        if show:
            for (x0, y0, x1, y1) in bboxes:
                self._draw_rect(vis, x0, y0, x1, y1, [0, 180, 0], 2)
            if nontext:
                for (x0, y0, x1, y1) in nontext:
                    self._draw_rect(vis, x0, y0, x1, y1, [220, 50, 50], 2)
            for (x0, y0, x1, y1) in artifacts:
                self._draw_rect(vis, x0, y0, x1, y1, [160, 160, 160], 1)

        # Scanline indicator (red horizontal line)
        if 0 <= scan_y < self._h:
            ch = vis.shape[2]
            vis[scan_y, :] = [255, 0, 0] + [255] * (ch - 3)
        h, w, ch = vis.shape
        fmt = QImage.Format.Format_RGB888 if ch == 3 else QImage.Format.Format_RGBA8888
        qimg = QImage(vis.data, w, h, w * ch, fmt)
        self._image_label.setPixmap(QPixmap.fromImage(qimg))

    def _draw_rect(self, vis, x0, y0, x1, y1, color, thickness):
        ch = vis.shape[2]
        c = color + [255] * (ch - 3)
        for t in range(thickness):
            if y0 + t < self._h:
                vis[y0 + t, max(0, x0):min(self._w, x1)] = c
            if y1 - 1 - t >= 0:
                vis[y1 - 1 - t, max(0, x0):min(self._w, x1)] = c
            if x0 + t < self._w:
                vis[max(0, y0):min(self._h, y1), x0 + t] = c
            if x1 - 1 - t >= 0:
                vis[max(0, y0):min(self._h, y1), x1 - 1 - t] = c


class BboxStatsWidget(QWidget):
    """Custom widget displaying bbox preview with overlayed stats, scrollable and zoomable."""

    def __init__(
        self, bbox: dict, page_index: int, file_path: str
    ) -> None:
        super().__init__()
        self._bbox = bbox
        self._page_index = page_index
        self._file_path = file_path
        self._pixmap = QPixmap()
        self._margin = 12
        self._padding = 10
        self._last_render_width = 0
        self._nested_objects: list[dict] = []
        self._zoom = 1.0  # Zoom factor (1.0 = 100%)
        self._scroll_offset_x = 0  # Pan offset for scrolling
        self._scroll_offset_y = 0
        self.setMinimumSize(600, 400)

        # Generate stats text
        self._stats_text = self._generate_stats_text()

        # Initial render
        self._render_at_optimal_dpi()

    def set_zoom(self, zoom: float) -> None:
        """Set zoom factor and re-render."""
        self._zoom = max(0.1, min(zoom, 5.0))  # Clamp between 10% and 500%
        self.update()

    def wheelEvent(self, event: QWheelEvent) -> None:
        """Handle mouse wheel for zooming."""
        delta = event.angleDelta().y()
        zoom_step = 1.1 if delta > 0 else 0.9
        self.set_zoom(self._zoom * zoom_step)
        event.accept()

    def resizeEvent(self, event) -> None:
        """Re-render object at optimal DPI on resize."""
        super().resizeEvent(event)
        self._render_at_optimal_dpi()

    def _generate_stats_text(self) -> str:
        """Generate stats text including nested object count."""
        lines = []

        lines.append("═" * 50)
        lines.append(f"PAGE: {self._page_index + 1}")
        lines.append("═" * 50)
        lines.append("")

        # Basic info
        bbox_type = self._bbox.get("type", "unknown")
        bbox_label = self._bbox.get("label", "—")
        lines.append(f"Type:        {bbox_type.upper()}")
        lines.append(f"Label:       {bbox_label}")
        lines.append("")

        # Coordinates
        pts = self._bbox.get("pts")
        if pts:
            x0, y0, x1, y1 = pts
            width = x1 - x0
            height = y1 - y0
            lines.append("COORDINATES (PDF points)")
            lines.append("─" * 50)
            lines.append(f"X0:          {x0:.2f} pt")
            lines.append(f"Y0:          {y0:.2f} pt")
            lines.append(f"X1:          {x1:.2f} pt")
            lines.append(f"Y1:          {y1:.2f} pt")
            lines.append("")
            lines.append("DIMENSIONS")
            lines.append("─" * 50)
            lines.append(f"Width:       {width:.2f} pt ({width/72*25.4:.2f} mm)")
            lines.append(f"Height:      {height:.2f} pt ({height/72*25.4:.2f} mm)")
            lines.append(f"Area:        {width*height:.0f} pt² ({width*height/72**2*25.4**2:.2f} mm²)")
            lines.append("")

        # Additional metadata
        lines.append("METADATA")
        lines.append("─" * 50)

        if self._bbox.get("hidden"):
            lines.append("Status:      HIDDEN")
        else:
            lines.append("Status:      VISIBLE")

        if self._bbox.get("is_template"):
            lines.append("Template:    YES")

        if self._bbox.get("id"):
            lines.append(f"ID:          {self._bbox['id']}")

        # Content hash if present
        if self._bbox.get("_chash"):
            chash_hex = self._bbox["_chash"].hex()[:16]
            lines.append(f"Content Hash: {chash_hex}...")

        # Table-specific info
        if bbox_type == "table":
            h_segs = self._bbox.get("h_segments", [])
            v_segs = self._bbox.get("v_segments", [])
            if h_segs or v_segs:
                lines.append("")
                lines.append("TABLE STRUCTURE")
                lines.append("─" * 50)
                lines.append(f"Horizontal segments: {len(h_segs)}")
                lines.append(f"Vertical segments:   {len(v_segs)}")

        # Nested PDF objects
        lines.append("")
        lines.append("NESTED PDF OBJECTS")
        lines.append("─" * 50)
        total_nested = len(self._nested_objects)
        text_count = sum(1 for o in self._nested_objects if o["type"] == "text")
        img_count = sum(1 for o in self._nested_objects if o["type"] == "image")
        table_count = sum(1 for o in self._nested_objects if o["type"] == "table")
        lines.append(f"Total:       {total_nested}")
        lines.append(f"Text:        {text_count}")
        lines.append(f"Images:      {img_count}")
        lines.append(f"Tables:      {table_count}")

        lines.append("")
        lines.append("═" * 50)

        return "\n".join(lines)

    def _render_at_optimal_dpi(self) -> None:
        """Render bbox at optimal DPI with nested PDF objects highlighted."""
        if not self._bbox or not self._file_path:
            return

        available_width = self.width() - 2 * self._margin
        available_height = self.height() - 2 * self._margin

        if available_width <= 0 or available_height <= 0:
            return

        # Only re-render if width changed significantly (>10px to avoid excessive re-renders)
        if abs(available_width - self._last_render_width) < 10:
            return

        self._last_render_width = available_width

        try:
            doc = fitz.open(str(self._file_path))
            if self._page_index >= len(doc):
                doc.close()
                return

            page = doc[self._page_index]
            pts = self._bbox.get("pts")
            if not pts or len(pts) != 4:
                doc.close()
                return

            x0, y0, x1, y1 = pts
            bbox_width_pt = x1 - x0
            bbox_height_pt = y1 - y0

            if bbox_width_pt <= 0 or bbox_height_pt <= 0:
                doc.close()
                return

            # Calculate optimal DPI to fill available width
            dpi = (available_width * 72.0) / bbox_width_pt
            # Cap DPI for performance (max 200 DPI)
            dpi = min(dpi, 200.0)

            # Render at calculated DPI
            clip_rect = fitz.Rect(x0, y0, x1, y1)
            mat = fitz.Matrix(dpi / 72.0, dpi / 72.0)
            pix = page.get_pixmap(clip=clip_rect, matrix=mat)

            # Convert to QPixmap
            img_data = pix.tobytes("ppm")
            qimg = QImage()
            qimg.loadFromData(img_data, "PPM")
            base_pixmap = QPixmap.fromImage(qimg)

            # Now draw nested PDF objects on top
            painter = QPainter(base_pixmap)
            painter.setRenderHint(QPainter.RenderHint.Antialiasing)

            # Extract PDF objects within this bbox
            self._nested_objects = self._get_nested_pdf_objects(page, clip_rect)

            # Draw bboxes for each PDF object
            for obj in self._nested_objects:
                obj_type = obj["type"]
                obj_pts = obj["pts"]

                # Transform to clipped coordinates
                ox0, oy0, ox1, oy1 = obj_pts
                # Translate to clip origin
                ox0_rel = (ox0 - x0) * dpi / 72.0
                oy0_rel = (oy0 - y0) * dpi / 72.0
                ox1_rel = (ox1 - x0) * dpi / 72.0
                oy1_rel = (oy1 - y0) * dpi / 72.0

                # Color by type
                if obj_type == "text":
                    color = QColor(0, 200, 100, 180)  # Green
                    thickness = 1
                elif obj_type == "image":
                    color = QColor(255, 150, 0, 180)  # Orange
                    thickness = 2
                elif obj_type == "table":
                    color = QColor(0, 150, 255, 180)  # Blue
                    thickness = 2
                else:
                    color = QColor(200, 200, 200, 150)  # Gray
                    thickness = 1

                pen = QPen(color, thickness)
                pen.setStyle(Qt.PenStyle.SolidLine)
                painter.setPen(pen)
                painter.drawRect(
                    int(ox0_rel), int(oy0_rel),
                    int(ox1_rel - ox0_rel), int(oy1_rel - oy0_rel)
                )

            painter.end()
            self._pixmap = base_pixmap

            # Update stats text with nested objects count
            self._stats_text = self._generate_stats_text()

            doc.close()
            self.update()

        except Exception as e:
            logger.error("Failed to render bbox: %s", e)

    def _get_nested_pdf_objects(self, page, clip_rect: fitz.Rect) -> list[dict]:
        """Extract all PDF objects (text, image, table) within clip region."""
        objects = []
        x0, y0, x1, y1 = clip_rect

        # Extract text blocks
        try:
            text_dict = page.get_text("dict")
            for block in text_dict.get("blocks", []):
                if block.get("type") == 0:  # text block
                    bbox = block.get("bbox")
                    if bbox:
                        bx0, by0, bx1, by1 = bbox
                        # Check if intersects with clip rect
                        if bx1 > x0 and bx0 < x1 and by1 > y0 and by0 < y1:
                            objects.append({
                                "type": "text",
                                "pts": (bx0, by0, bx1, by1),
                                "label": "text"
                            })
                elif block.get("type") == 1:  # image block
                    bbox = block.get("bbox")
                    if bbox:
                        bx0, by0, bx1, by1 = bbox
                        if bx1 > x0 and bx0 < x1 and by1 > y0 and by0 < y1:
                            objects.append({
                                "type": "image",
                                "pts": (bx0, by0, bx1, by1),
                                "label": "image"
                            })
        except Exception:
            pass

        # Extract tables
        try:
            tables = page.find_tables(clip=clip_rect)
            for table in tables.tables:
                bbox = table.bbox
                if bbox:
                    objects.append({
                        "type": "table",
                        "pts": tuple(bbox),
                        "label": f"table {table.row_count}x{table.col_count}"
                    })
        except Exception:
            pass

        return objects

    def sizeHint(self):
        """Return hint size based on pixmap and zoom."""
        if self._pixmap.isNull():
            return super().sizeHint()
        return int(self._pixmap.width() * self._zoom + 2 * self._margin), int(self._pixmap.height() * self._zoom + 2 * self._margin)

    def paintEvent(self, event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        # Fill background
        painter.fillRect(self.rect(), QColor(240, 240, 240))

        # Draw pixmap scaled by zoom factor
        if not self._pixmap.isNull():
            scaled_width = int(self._pixmap.width() * self._zoom)
            scaled_height = int(self._pixmap.height() * self._zoom)
            scaled_pixmap = self._pixmap.scaledToWidth(scaled_width, Qt.TransformationMode.SmoothTransformation)

            # Center within available space
            x = (self.width() - scaled_width) // 2
            y = (self.height() - scaled_height) // 2

            # Clamp to valid drawing area
            if x < 0:
                x = self._margin
            if y < 0:
                y = self._margin

            painter.drawPixmap(x, y, scaled_pixmap)

        # Draw semi-transparent overlay panel with stats (bottom-left)
        padding = self._padding
        max_text_width = 350

        # Measure text
        font = QFont("Consolas", 8)
        painter.setFont(font)
        fm = painter.fontMetrics()

        lines = self._stats_text.split("\n")
        line_height = fm.lineSpacing()
        text_height = len(lines) * line_height + 2 * padding
        text_width = min(max_text_width, max((fm.horizontalAdvance(line) for line in lines), default=100) + 2 * padding)

        # Position: bottom-left (adjust for scrolling if needed)
        panel_x = self._margin
        panel_y = self.height() - text_height - self._margin

        # Draw semi-transparent background
        panel_color = QColor(30, 30, 30, 220)
        painter.fillRect(panel_x, panel_y, text_width, text_height, panel_color)

        # Draw border
        painter.setPen(QPen(QColor(100, 100, 100), 1))
        painter.drawRect(panel_x, panel_y, text_width, text_height)

        # Draw text
        painter.setPen(QColor(200, 220, 200))
        text_x = panel_x + padding
        text_y = panel_y + padding + fm.ascent()

        for line in lines:
            painter.drawText(text_x, text_y, line)
            text_y += line_height

        painter.end()


class BboxStatsDialog(QDialog):
    """Modal dialog showing detailed stats for a selected bounding box."""

    def __init__(self, bbox: dict, page_index: int, file_path: str, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Object Statistics")
        self.setModal(True)
        self.setMinimumWidth(800)
        self.setMinimumHeight(650)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        # Zoom control bar
        zoom_bar = QHBoxLayout()

        zoom_btn_minus = QPushButton("−")
        zoom_btn_minus.setMaximumWidth(40)
        zoom_btn_minus.setToolTip("Zoom out (Ctrl+Scroll)")

        self._zoom_slider = QSlider(Qt.Orientation.Horizontal)
        self._zoom_slider.setMinimum(10)   # 10% (0.1)
        self._zoom_slider.setMaximum(500)  # 500% (5.0)
        self._zoom_slider.setValue(100)    # 100% (1.0)
        self._zoom_slider.setToolTip("Adjust zoom level")

        zoom_btn_plus = QPushButton("+")
        zoom_btn_plus.setMaximumWidth(40)
        zoom_btn_plus.setToolTip("Zoom in (Ctrl+Scroll)")

        self._zoom_label = QLabel("100%")
        self._zoom_label.setMinimumWidth(40)

        zoom_bar.addWidget(QLabel("Zoom:"))
        zoom_bar.addWidget(zoom_btn_minus)
        zoom_bar.addWidget(self._zoom_slider, 1)
        zoom_bar.addWidget(zoom_btn_plus)
        zoom_bar.addWidget(self._zoom_label)

        layout.addLayout(zoom_bar)

        # Custom widget with adaptive rendering in scrollable area
        self._stats_widget = BboxStatsWidget(bbox, page_index, file_path)

        scroll_area = QScrollArea()
        scroll_area.setWidget(self._stats_widget)
        scroll_area.setWidgetResizable(True)
        scroll_area.setStyleSheet("QScrollArea { border: none; }")

        layout.addWidget(scroll_area, 1)

        # Close button
        close_btn = QPushButton("Close")
        close_btn.setMinimumHeight(32)
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn)

        # Connect zoom controls
        zoom_btn_minus.clicked.connect(lambda: self._set_zoom_slider(self._zoom_slider.value() - 10))
        zoom_btn_plus.clicked.connect(lambda: self._set_zoom_slider(self._zoom_slider.value() + 10))
        self._zoom_slider.valueChanged.connect(self._on_zoom_changed)

    def _set_zoom_slider(self, value: int) -> None:
        """Set zoom slider and emit change."""
        self._zoom_slider.setValue(max(10, min(500, value)))

    def _on_zoom_changed(self, value: int) -> None:
        """Handle zoom slider change."""
        zoom = value / 100.0  # Convert from 0-500 scale to 0.1-5.0
        self._stats_widget.set_zoom(zoom)
        self._zoom_label.setText(f"{value}%")


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
        self._mode: str = "1-page"  # "1-page", "2-page", "spread"
        self._first_is_cover: bool = False
        self._mirrored: bool = False
        self._split_pos: float = 0.5   # center split position (fraction 0..1) for spread mode
        self._split_gap: float = 0.01  # half-gap width (fraction) for spread mode

        root = QVBoxLayout(self)
        root.setContentsMargins(4, 2, 4, 4)
        root.setSpacing(2)

        # Toolbar (compact)
        toolbar = QHBoxLayout()
        toolbar.setContentsMargins(0, 0, 0, 0)
        self._mode_combo = QComboBox()
        self._mode_combo.addItems(["1-page", "2-page", "2-page spread"])
        self._mode_combo.setFixedWidth(130)
        self._mode_combo.setFixedHeight(24)
        self._mode_combo.currentTextChanged.connect(self._on_mode_changed)
        toolbar.addWidget(self._mode_combo)
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

        reset_btn = QPushButton("Reset")
        reset_btn.setFixedWidth(70)
        reset_btn.setFixedHeight(24)
        reset_btn.clicked.connect(self._on_reset)
        toolbar.addWidget(reset_btn)
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

    def _on_mode_changed(self, text: str) -> None:
        mode_map = {"1-page": "1-page", "2-page": "2-page", "2-page spread": "spread"}
        self._mode = mode_map.get(text, "1-page")

        if self._mode == "2-page":
            self._mode_label.setText("Left = odd pages, Right = even pages")
            self._cover_cb.show()
            self._mirror_cb.show()
            self._preview_right.show()
            self._preview_left._spread_mode = False
        elif self._mode == "spread":
            self._mode_label.setText("Split each PDF page into left + right")
            self._cover_cb.hide()
            self._mirror_cb.hide()
            self._preview_right.hide()
            self._preview_left._spread_mode = True
            self._preview_left._split_pos = self._split_pos
            self._preview_left._split_gap = self._split_gap
        else:
            self._mode_label.setText("")
            self._cover_cb.hide()
            self._mirror_cb.hide()
            self._preview_right.hide()
            self._preview_left._spread_mode = False

        self._load_preview(self._current_page)
        self._on_lines_changed()

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
        offset = 1 if ((self._mode == "2-page") and self._first_is_cover) else 0
        for pi in range(self._page_count):
            shifted = pi + offset
            row, col = shifted // 2, shifted % 2
            self._grid.addWidget(self._thumb_labels[pi], row, col)

    def _is_left_page(self, page_idx: int) -> bool:
        """Determine if a page uses the left template."""
        if not (self._mode == "2-page"):
            return True
        shifted = page_idx + (1 if self._first_is_cover else 0)
        return shifted % 2 == 0  # even shifted index = left column

    def _preview_for_page(self, page_idx: int) -> CropPreviewWidget:
        """Return the appropriate preview widget for a given page index."""
        if not (self._mode == "2-page"):
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
        if (self._mode == "2-page"):
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

        if not (self._mode == "2-page"):
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
        if (self._mode == "2-page"):
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

    def _on_reset(self) -> None:
        """Clear all cropping lines and boxes."""
        self._preview_left._h_lines.clear()
        self._preview_left._v_lines.clear()
        self._preview_left._boxes.clear()
        self._preview_left.update()
        self._preview_right._h_lines.clear()
        self._preview_right._v_lines.clear()
        self._preview_right._boxes.clear()
        self._preview_right.update()
        # Emit empty cropping data to reset cropbox
        self.accepted.emit({
            "two_page": self._two_page_mode,
            "first_is_cover": self._first_is_cover,
            "mirrored": False,
            "left": {"h_lines": [], "v_lines": [], "boxes": []},
            "right": {"h_lines": [], "v_lines": [], "boxes": []},
        })

    def _on_cancel(self) -> None:
        """Discard changes and close."""
        self.cancelled.emit()
        self.close()

    def _build_cropping_data(self) -> dict:
        """Build cropping data dict for consumption by main window."""
        return {
            "mode": self._mode,
            "two_page": self._mode == "2-page",
            "first_is_cover": self._first_is_cover,
            "mirrored": self._mirrored,
            "split_pos": self._split_pos,
            "split_gap": self._split_gap,
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
            "two_page": (self._mode == "2-page"),
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
        self._mode = crop.get("mode", "2-page" if crop.get("two_page") else "1-page")
        self._first_is_cover = crop.get("first_is_cover", False)
        self._mirrored = crop.get("mirrored", False)
        self._split_pos = crop.get("split_pos", 0.5)
        self._split_gap = crop.get("split_gap", 0.01)

        # Restore combo selection (triggers _on_mode_changed)
        mode_labels = {"1-page": "1-page", "2-page": "2-page", "spread": "2-page spread"}
        self._mode_combo.blockSignals(True)
        self._mode_combo.setCurrentText(mode_labels.get(self._mode, "1-page"))
        self._mode_combo.blockSignals(False)
        # Apply mode UI
        self._on_mode_changed(self._mode_combo.currentText())

        if self._mode == "2-page":
            self._cover_cb.setChecked(self._first_is_cover)
            self._mirror_cb.setChecked(self._mirrored)
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
        # Pan mode (space + drag)
        self._pan_active = False
        self._pan_dragging = False
        self._pan_start: QPoint | None = None
        self._pan_scroll_start: tuple[int, int] = (0, 0)
        self._spreads: list[PageSpreadWidget] = []
        # Bboxes stored in PDF points (zoom-independent)
        self._bboxes_cache: dict[int, list[dict]] = {}
        self._stats_cache: dict[int, str] = {}
        self._pdf_objects_cache: dict[int, list[dict]] = {}
        self._catalog_meta: dict = {}
        self._show_hidden = False
        self._first_is_cover: bool = False

        # --- Pixmap cache & background renderer ---
        # page_idx → (QPixmap, dpi_at_which_it_was_rendered)
        self._pixmap_cache: dict[int, tuple[QPixmap, float]] = {}
        self._render_worker: PageRenderWorker | None = None
        self._target_dpi: float = 0.0  # DPI that we want visible pages at

        self._op_start: float = 0.0
        self._setup_ui()

    def _emit_progress(self, msg: str) -> None:
        """Emit progress with elapsed time."""
        import time
        elapsed = time.perf_counter() - self._op_start
        self.progress.emit(f"{msg}  {elapsed:.1f}s")

    def _emit_done(self, op: str = "") -> None:
        """Emit completion message."""
        import time
        elapsed = time.perf_counter() - self._op_start
        self.progress.emit(f"{op} done {elapsed:.1f}s" if op else f"done {elapsed:.1f}s")

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
        self._filter_checkboxes: dict[str, QCheckBox] = {}
        _PDF_SUB_TYPES = [("pdf_text", "Text"), ("pdf_image", "Image"), ("pdf_table", "Table")]
        for label, type_key in [("Bounding Box", "bbox"),
                                ("Tables", "table"), ("Text", "text"),
                                ("Photos", "photo"), ("Pictures", "picture"),
                                ("Drawings", "drawing"),
                                ("Template", "template"),
                                ("PDF Objects", "pdf_objects")]:
            # Widget with checkbox + "Only" button
            widget = QWidget()
            row = QHBoxLayout(widget)
            row.setContentsMargins(4, 1, 4, 1)
            cb = QCheckBox(label)
            cb.setChecked(type_key not in ("pdf_objects", "template"))  # off by default
            cb.toggled.connect(self._on_bbox_filter_changed)
            row.addWidget(cb)
            row.addStretch()
            only_btn = QPushButton("Only")
            only_btn.setFixedSize(36, 18)
            only_btn.setStyleSheet("font-size: 9px; padding: 0;")
            only_btn.clicked.connect(lambda _=False, k=type_key: self._on_show_only(k))
            row.addWidget(only_btn)
            wa = QWidgetAction(filter_menu)
            wa.setDefaultWidget(widget)
            filter_menu.addAction(wa)
            # Store checkbox for filter state
            self._filter_checkboxes[type_key] = cb
            # Wrap as QAction-like for compatibility
            act = type("_CbProxy", (), {
                "isChecked": cb.isChecked,
                "setChecked": cb.setChecked,
            })()
            self.bbox_filter_actions[type_key] = act
            if type_key == "bbox":
                filter_menu.addSeparator()
            if type_key == "template":
                filter_menu.addSeparator()
            # Add PDF sub-type checkboxes indented under "PDF Objects"
            if type_key == "pdf_objects":
                for sub_key, sub_label in _PDF_SUB_TYPES:
                    sw = QWidget()
                    sr = QHBoxLayout(sw)
                    sr.setContentsMargins(20, 1, 4, 1)  # indented
                    scb = QCheckBox(sub_label)
                    scb.setChecked(False)
                    scb.toggled.connect(self._on_bbox_filter_changed)
                    sr.addWidget(scb)
                    sr.addStretch()
                    swa = QWidgetAction(filter_menu)
                    swa.setDefaultWidget(sw)
                    filter_menu.addAction(swa)
                    self._filter_checkboxes[sub_key] = scb
                    sub_act = type("_CbProxy", (), {
                        "isChecked": scb.isChecked,
                        "setChecked": scb.setChecked,
                    })()
                    self.bbox_filter_actions[sub_key] = sub_act

        self.bbox_filter_btn.setMenu(filter_menu)
        toolbar.addWidget(self.bbox_filter_btn)

        # Content visibility dropdown — hide/show actual object content
        self.content_filter_btn = QToolButton()
        self.content_filter_btn.setText("Content ▾")
        self.content_filter_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        content_menu = QMenu(self.content_filter_btn)
        self._content_checkboxes: dict[str, QCheckBox] = {}
        for label, type_key in [("Tables", "table"), ("Text", "text"),
                                ("Photos", "photo"), ("Pictures", "picture"),
                                ("Drawings", "drawing"),
                                ("Template", "template")]:
            cw = QWidget()
            cr = QHBoxLayout(cw)
            cr.setContentsMargins(4, 1, 4, 1)
            ccb = QCheckBox(label)
            ccb.setChecked(False)  # not hidden by default
            ccb.toggled.connect(self._on_content_filter_changed)
            cr.addWidget(ccb)
            cr.addStretch()
            cwa = QWidgetAction(content_menu)
            cwa.setDefaultWidget(cw)
            content_menu.addAction(cwa)
            self._content_checkboxes[type_key] = ccb
        self.content_filter_btn.setMenu(content_menu)
        toolbar.addWidget(self.content_filter_btn)

        self._detect_method_combo = QComboBox()
        self._detect_method_combo.addItems(ScanlineTestDialog._ALGOS)
        self._detect_method_combo.setCurrentIndex(8)  # Hybrid 2-pass default
        self._detect_method_combo.setFixedHeight(24)
        self._detect_method_combo.setFixedWidth(180)
        toolbar.addWidget(self._detect_method_combo)

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

        # Install event filter for pan mode on scroll viewport
        self.scroll_area.viewport().installEventFilter(self)
        self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)

    def keyPressEvent(self, event) -> None:
        if event.key() == Qt.Key.Key_Space and not event.isAutoRepeat():
            self._pan_active = True
            self.scroll_area.viewport().setCursor(QCursor(Qt.CursorShape.OpenHandCursor))
        super().keyPressEvent(event)

    def keyReleaseEvent(self, event) -> None:
        if event.key() == Qt.Key.Key_Space and not event.isAutoRepeat():
            self._pan_active = False
            self._pan_dragging = False
            self.scroll_area.viewport().unsetCursor()
        super().keyReleaseEvent(event)

    def eventFilter(self, obj, event) -> bool:
        """Handle pan via Space+drag or middle mouse drag on scroll viewport."""
        if obj is not self.scroll_area.viewport():
            return False

        etype = event.type()

        # Middle button press → start pan
        if etype == event.Type.MouseButtonPress and event.button() == Qt.MouseButton.MiddleButton:
            self._pan_dragging = True
            self._pan_start = event.globalPosition().toPoint()
            self._pan_scroll_start = (
                self.scroll_area.horizontalScrollBar().value(),
                self.scroll_area.verticalScrollBar().value(),
            )
            self.scroll_area.viewport().setCursor(QCursor(Qt.CursorShape.ClosedHandCursor))
            return True

        # Space held + left click → start pan
        if etype == event.Type.MouseButtonPress and self._pan_active and event.button() == Qt.MouseButton.LeftButton:
            self._pan_dragging = True
            self._pan_start = event.globalPosition().toPoint()
            self._pan_scroll_start = (
                self.scroll_area.horizontalScrollBar().value(),
                self.scroll_area.verticalScrollBar().value(),
            )
            self.scroll_area.viewport().setCursor(QCursor(Qt.CursorShape.ClosedHandCursor))
            return True

        # Drag
        if etype == event.Type.MouseMove and self._pan_dragging and self._pan_start:
            delta = event.globalPosition().toPoint() - self._pan_start
            self.scroll_area.horizontalScrollBar().setValue(
                self._pan_scroll_start[0] - delta.x()
            )
            self.scroll_area.verticalScrollBar().setValue(
                self._pan_scroll_start[1] - delta.y()
            )
            return True

        # Release
        if etype == event.Type.MouseButtonRelease and self._pan_dragging:
            self._pan_dragging = False
            self._pan_start = None
            if self._pan_active:
                self.scroll_area.viewport().setCursor(QCursor(Qt.CursorShape.OpenHandCursor))
            else:
                self.scroll_area.viewport().unsetCursor()
            return True

        return False

    def load_document(self, file_path: Path) -> None:
        """Load a PDF document for preview."""
        self.progress.emit(f"Loading {file_path.name}...")
        if self._doc:
            self._doc.close()

        self._cancel_render_worker()
        self._cancel_detect_worker()
        self._detect_method_idx = self._detect_method_combo.currentIndex()
        self._file_path = file_path
        self._bboxes_cache.clear()
        self._stats_cache.clear()
        self._pdf_objects_cache.clear()
        self._pixmap_cache.clear()
        self._catalog_meta = load_meta(file_path)

        # Load cropping settings
        crop = self._catalog_meta.get("cropping", {})
        self._first_is_cover = crop.get("first_is_cover", False)
        self._cropping_data = crop

        try:
            self._doc = fitz.open(str(file_path))
            self._page_count = len(self._doc)
            # Apply saved cropbox if cropping data exists
            if crop and (crop.get("left") or crop.get("right")):
                self._apply_pdf_cropbox()
        except Exception as e:
            logger.error("Failed to open PDF: %s", e)
            self.file_label.setText(f"Error: {e}")
            return

        self.file_label.setText(f"{file_path.name} ({self._page_count} pages)")
        self.page_spin.setMaximum(self._page_count)
        self.page_spin.setValue(1)
        self.page_count_label.setText(f"/ {self._page_count}")

        self._fit_to_width()   # sets self._zoom, calls _set_zoom → _update_spreads_zoom (no-op: no spreads yet)
        self._rebuild_spreads()  # create spread widgets + kick off background render
        self._detect_visible_pages()
        logger.info("Loaded document: %s (%d pages)", file_path.name, self._page_count)

    def _current_zoom_factor(self) -> float:
        """Current points-to-pixels conversion factor."""
        return self._base_dpi * self._zoom / 72.0

    # ------------------------------------------------------------------
    # Spread layout helpers
    # ------------------------------------------------------------------

    def _build_spread_pairs(self) -> list[tuple[int, int]]:
        """Return [(left_page_idx | -1, right_page_idx | -1), ...]."""
        pairs: list[tuple[int, int]] = []
        if self._first_is_cover and self._page_count > 0:
            pairs.append((-1, 0))
            pi = 1
        else:
            pi = 0
        while pi < self._page_count:
            right = pi + 1 if pi + 1 < self._page_count else -1
            pairs.append((pi, right))
            pi += 2
        return pairs

    # ------------------------------------------------------------------
    # Rebuild spreads (structure only — called on load / cover toggle)
    # ------------------------------------------------------------------

    def _rebuild_spreads(self) -> None:
        """Create spread widgets with correct page assignments.

        The first spread is rendered synchronously so the user sees content
        immediately.  Remaining visible pages are rendered in a background
        thread; off-screen pages get a correctly-sized placeholder.
        """
        # Tear down old widgets
        for spread in self._spreads:
            self.pages_layout.removeWidget(spread)
            spread.deleteLater()
        self._spreads.clear()

        if not self._doc:
            return

        zf = self._current_zoom_factor()
        target_dpi = zf * 72.0
        cur_layers = {k: a.isChecked() for k, a in self.bbox_filter_actions.items()}
        cur_content_mask = {k: cb.isChecked() for k, cb in self._content_checkboxes.items()}
        spread_pairs = self._build_spread_pairs()

        import time
        self._op_start = time.perf_counter()

        # Synchronously render the first spread for instant feedback
        first_sync_pages = set()
        if spread_pairs:
            lp, rp = spread_pairs[0]
            if lp >= 0:
                first_sync_pages.add(lp)
            if rp >= 0:
                first_sync_pages.add(rp)
        for pi in first_sync_pages:
            self._emit_progress(f"Render p.{pi+1} sync @ {int(target_dpi)} DPI")
            self._render_page_sync(pi, target_dpi)

        total_spreads = len(spread_pairs)
        for si, (left_pi, right_pi) in enumerate(spread_pairs):
            pages_str = f"p.{left_pi+1}" if left_pi >= 0 else ""
            if right_pi >= 0:
                pages_str += f"+{right_pi+1}" if pages_str else f"p.{right_pi+1}"
            self._emit_progress(f"Layout spread {si+1}/{total_spreads} ({pages_str})")
            spread = PageSpreadWidget()

            # --- helper: configure one PageWidget side ---
            def _setup_page(pw: "PageWidget", pi: int) -> None:
                page = self._doc[pi]
                pw._page_index = pi
                pw._page_size_pt = (page.rect.width, page.rect.height)
                pw.set_pixmap(self._pixmap_for_page(pi, zf))
                pw.set_show_bboxes(True)
                pw.set_show_hidden(self._show_hidden)
                pw.set_file_path(self._file_path)
                pw.hide_requested.connect(self._on_hide_object)
                pw.selection_changed.connect(self._on_page_selection)
                pw.type_changed.connect(self._on_type_changed)
                pw.bbox_modified.connect(self._on_bbox_modified)
                pw.bbox_testbench.connect(self._on_bbox_testbench)
                pw.object_stats_requested.connect(self._on_object_stats_requested)
                pw.set_visible_layers(cur_layers)
                pw.set_content_mask(cur_content_mask)
                if pi in self._bboxes_cache:
                    pw.set_bboxes(self._bboxes_cache[pi], zf)
                if pi in self._pdf_objects_cache:
                    pw.set_pdf_objects(self._pdf_objects_cache[pi], zf)
                if pi in self._stats_cache:
                    pw.set_page_stats(self._stats_cache[pi])

            def _blank_like(ref_pi: int) -> QPixmap:
                if ref_pi >= 0:
                    p = self._doc[ref_pi]
                    w = int(p.rect.width * zf)
                    h = int(p.rect.height * zf)
                else:
                    w, h = 100, 100
                blank = QPixmap(w, h)
                blank.fill(QColor(240, 240, 240))
                return blank

            if left_pi >= 0:
                _setup_page(spread.left_page, left_pi)
            else:
                spread.left_page.set_pixmap(_blank_like(right_pi))

            if right_pi >= 0:
                _setup_page(spread.right_page, right_pi)
            else:
                spread.right_page.set_pixmap(_blank_like(left_pi))

            self.pages_layout.addWidget(spread)
            self._spreads.append(spread)

        # Cropping is applied via PDF cropbox, no visual overlay needed
        self._emit_done("Layout")

        # Kick off background render for remaining visible pages
        self._schedule_hires_render()

    # ------------------------------------------------------------------
    # Pixmap helpers
    # ------------------------------------------------------------------

    def _pixmap_for_page(self, page_idx: int, zf: float) -> QPixmap:
        """Return the best available pixmap for *page_idx* at zoom-factor *zf*.

        If we already have a cached render (possibly at a different DPI),
        scale it to the expected size so the layout is immediate.
        """
        target_w = int(self._doc[page_idx].rect.width * zf)
        target_h = int(self._doc[page_idx].rect.height * zf)

        cached = self._pixmap_cache.get(page_idx)
        if cached:
            pix, _cached_dpi = cached
            if pix.width() == target_w and pix.height() == target_h:
                return pix
            # Scale existing render to target size (fast, may be slightly blurry)
            return pix.scaled(
                target_w, target_h,
                Qt.AspectRatioMode.IgnoreAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )

        # No cache at all — grey placeholder
        blank = QPixmap(target_w, target_h)
        blank.fill(QColor(245, 245, 245))
        return blank

    def _render_page_sync(self, page_num: int, dpi: float) -> QPixmap:
        """Render a single page synchronously (used for initial load)."""
        page = self._doc[page_num]
        zoom_factor = dpi / 72.0
        mat = fitz.Matrix(zoom_factor, zoom_factor)
        pix = page.get_pixmap(matrix=mat)
        img = QImage(pix.samples, pix.width, pix.height, pix.stride, QImage.Format.Format_RGB888)
        qpix = QPixmap.fromImage(img)
        self._pixmap_cache[page_num] = (qpix, dpi)
        return qpix

    # ------------------------------------------------------------------
    # Visible-page detection
    # ------------------------------------------------------------------

    def _visible_page_indices(self) -> list[int]:
        """Return page indices whose spread widgets are in/near the viewport."""
        if not self._spreads:
            return []
        vp = self.scroll_area.viewport()
        vp_top = self.scroll_area.verticalScrollBar().value()
        vp_bot = vp_top + vp.height()
        margin = vp.height()  # pre-render 1 viewport ahead/behind
        indices: list[int] = []
        for spread in self._spreads:
            sy = spread.mapTo(self.pages_container, QPoint(0, 0)).y()
            sh = spread.height()
            if sy + sh < vp_top - margin:
                continue
            if sy > vp_bot + margin:
                break
            for pw in (spread.left_page, spread.right_page):
                pi = getattr(pw, "_page_index", -1)
                if pi >= 0:
                    indices.append(pi)
        return indices

    # ------------------------------------------------------------------
    # Background hi-res rendering
    # ------------------------------------------------------------------

    def _cancel_render_worker(self) -> None:
        if self._render_worker is not None:
            self._render_worker.cancel()
            self._render_worker.page_ready.disconnect(self._on_page_rendered)
            self._render_worker.all_done.disconnect(self._on_render_done)
            self._render_worker = None

    def _cancel_detect_worker(self) -> None:
        if hasattr(self, "_detect_worker") and self._detect_worker is not None:
            self._detect_worker.cancel()
            self._detect_worker = None
        if hasattr(self, "_detect_timer"):
            self._detect_timer.stop()

    def _schedule_hires_render(self) -> None:
        """Queue background rendering for visible pages at the correct DPI."""
        if not self._doc or not self._file_path:
            return
        self._cancel_render_worker()

        target_dpi = self._current_zoom_factor() * 72.0
        self._target_dpi = target_dpi

        visible = self._visible_page_indices()
        # Only request pages whose cache doesn't already match target DPI
        requests: list[tuple[int, float]] = []
        for pi in visible:
            cached = self._pixmap_cache.get(pi)
            if cached and abs(cached[1] - target_dpi) < 1.0:
                continue  # already sharp
            requests.append((pi, target_dpi))

        if not requests:
            return

        worker = PageRenderWorker(str(self._file_path), requests, parent=self)
        worker.page_ready.connect(self._on_page_rendered)
        worker.all_done.connect(self._on_render_done)
        self._render_worker = worker
        worker.start()

    def _on_page_rendered(self, page_idx: int, image: QImage, dpi: float) -> None:
        """Slot: background worker delivered a rendered page."""
        # Stale result? (user changed zoom while rendering)
        if abs(dpi - self._target_dpi) > 1.0:
            return

        self.progress.emit(f"HiRes p.{page_idx+1} @ {int(dpi)} DPI")
        qpix = QPixmap.fromImage(image)
        self._pixmap_cache[page_idx] = (qpix, dpi)

        # Push to the correct PageWidget
        zf = self._current_zoom_factor()
        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                if getattr(pw, "_page_index", -1) == page_idx:
                    pw.set_pixmap(qpix)
                    if page_idx in self._bboxes_cache:
                        pw.set_bboxes(self._bboxes_cache[page_idx], zf)
                    return

    def _on_render_done(self) -> None:
        """All queued pages rendered."""
        self._render_worker = None
        self.progress.emit("")

    # ------------------------------------------------------------------
    # Zoom — instant scale + deferred hi-res
    # ------------------------------------------------------------------

    def _update_spreads_zoom(self) -> None:
        """Instantly rescale cached pixmaps to new zoom & update bboxes."""
        if not self._doc:
            return
        self.progress.emit(f"Zoom {int(self._zoom * 100)}%")
        zf = self._current_zoom_factor()
        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                pi = getattr(pw, "_page_index", -1)
                if pi >= 0:
                    pw.set_pixmap(self._pixmap_for_page(pi, zf))
                    if pi in self._bboxes_cache:
                        pw.set_bboxes(self._bboxes_cache[pi], zf)
                else:
                    # Blank page — resize to match sibling
                    sibling = spread.right_page if pw is spread.left_page else spread.left_page
                    spi = getattr(sibling, "_page_index", -1)
                    if spi >= 0:
                        p = self._doc[spi]
                        w = int(p.rect.width * zf)
                        h = int(p.rect.height * zf)
                    else:
                        w, h = 100, 100
                    blank = QPixmap(w, h)
                    blank.fill(QColor(240, 240, 240))
                    pw.set_pixmap(blank)

    def _set_zoom(self, zoom: float, anchor: QPointF | None = None) -> None:
        """Set zoom level — instantly rescale, then render sharp in background.

        *anchor* is the position in the **viewport** that should stay fixed
        (typically the mouse cursor).  If ``None``, the viewport centre is used.
        """
        zoom = max(0.25, min(4.0, zoom))
        if abs(zoom - self._zoom) < 0.001:
            return

        old_zoom = self._zoom
        self._zoom = zoom

        self.zoom_slider.blockSignals(True)
        self.zoom_slider.setValue(int(zoom * 100))
        self.zoom_slider.blockSignals(False)
        self.zoom_label.setText(f"{int(zoom * 100)}%")

        # --- anchor-aware scroll adjustment ---
        vbar = self.scroll_area.verticalScrollBar()
        hbar = self.scroll_area.horizontalScrollBar()
        vp = self.scroll_area.viewport()

        if anchor is None:
            anchor = QPointF(vp.width() / 2.0, vp.height() / 2.0)

        # Content-space position of the anchor before zoom
        cx_before = hbar.value() + anchor.x()
        cy_before = vbar.value() + anchor.y()

        ratio = zoom / old_zoom

        # 1) Instant: scale cached pixmaps to new size
        self._update_spreads_zoom()

        # 2) Adjust scroll so anchor stays under the cursor
        hbar.setValue(int(cx_before * ratio - anchor.x()))
        vbar.setValue(int(cy_before * ratio - anchor.y()))

        # 3) Deferred: re-render visible pages at correct DPI in background
        self._schedule_hires_render()

    # Keep old name as alias for callers that still reference it
    def _render_all_spreads(self) -> None:
        self._pixmap_cache.clear()
        self._rebuild_spreads()

    def _fit_to_width(self) -> None:
        """Calculate zoom to fit a single page width, then center current page."""
        if not self._doc or self._page_count == 0:
            return

        current = self._get_current_page()
        page = self._doc[current]
        page_width_pt = page.rect.width  # points (72 dpi)
        scrollbar_w = self.scroll_area.verticalScrollBar().width() if self.scroll_area.verticalScrollBar().isVisible() else 18
        overhead = 10 + 10 + scrollbar_w + 4  # spread margins + safety
        available_width = self.scroll_area.viewport().width() - overhead
        one_page_base_px = page_width_pt * self._base_dpi / 72.0
        if one_page_base_px > 0:
            new_zoom = available_width / one_page_base_px
            self._set_zoom(new_zoom)
            self._scroll_to_page(current + 1)

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

        # Render newly-visible pages at correct DPI
        self._schedule_hires_render()

    def _clear_and_redetect(self) -> None:
        """Clear caches and re-detect. Visible pages first, rest in background."""
        # Capture selected method before detection starts
        self._detect_method_idx = self._detect_method_combo.currentIndex()
        self._cancel_detect_worker()
        self._bboxes_cache.clear()
        self._stats_cache.clear()
        self._pdf_objects_cache.clear()
        if self._file_path:
            meta = load_meta(self._file_path)
            meta["objects"] = []
            save_meta(self._file_path, meta)
            self._catalog_meta = meta
        # Clear all visible bboxes and repaint
        zf = self._current_zoom_factor()
        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                pw.set_bboxes([], zf)
                pw.set_pdf_objects([], zf)
                pw.update()
        # Kick off lazy background detection for visible pages
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
        """Apply real PDF cropping, transform object coordinates, re-render."""
        old_crop_data = getattr(self, "_cropping_data", {}) or {}
        new_crop_data = data

        self._first_is_cover = data.get("first_is_cover", False)
        self._cropping_data = data
        self._catalog_meta["cropping"] = data

        # Transform saved object coordinates from old crop space → new crop space
        if self._doc and self._file_path:
            self._transform_objects_for_crop(old_crop_data, new_crop_data)

        if self._file_path:
            save_meta(self._file_path, self._catalog_meta)

        # Apply real PDF cropbox
        if self._doc:
            self._apply_pdf_cropbox()

        # Clear caches and re-render with new crop
        self._bboxes_cache.clear()
        self._stats_cache.clear()
        self._pixmap_cache.clear()
        self._render_all_spreads()
        self._detect_visible_pages()

    # ------------------------------------------------------------------
    # Coordinate transformation when crop changes
    # ------------------------------------------------------------------

    @staticmethod
    def _side_for_page(page_idx: int, crop_data: dict) -> dict:
        """Return the crop side dict for *page_idx* given arbitrary *crop_data*."""
        if not crop_data:
            return {}
        two_page = crop_data.get("two_page", False)
        cover = crop_data.get("first_is_cover", False)
        if not two_page:
            return crop_data.get("left", {})
        shifted = page_idx + (1 if cover else 0)
        return crop_data.get("left", {}) if shifted % 2 == 0 else crop_data.get("right", {})

    def _crop_origin_for_page(self, page_idx: int, crop_data: dict) -> tuple[float, float]:
        """Return (x0, y0) of the crop area in MediaBox coordinates.

        If no crop is defined the MediaBox origin is returned (typically 0, 0),
        meaning bbox coords are in full-page space.
        """
        page = self._doc[page_idx]
        mb = page.mediabox

        side = self._side_for_page(page_idx, crop_data)
        if not side:
            return (mb.x0, mb.y0)

        h_lines = sorted(side.get("h_lines", []))
        v_lines = sorted(side.get("v_lines", []))
        x0 = mb.x0 + v_lines[0] * mb.width if v_lines else mb.x0
        y0 = mb.y0 + h_lines[0] * mb.height if h_lines else mb.y0
        return (x0, y0)

    def _crop_rect_for_page(self, page_idx: int, crop_data: dict) -> "fitz.Rect":
        """Return the crop Rect in MediaBox coordinates for *page_idx*."""
        page = self._doc[page_idx]
        mb = page.mediabox

        side = self._side_for_page(page_idx, crop_data)
        if not side:
            return fitz.Rect(mb)

        h_lines = sorted(side.get("h_lines", []))
        v_lines = sorted(side.get("v_lines", []))
        if not h_lines and not v_lines:
            return fitz.Rect(mb)

        x0 = mb.x0 + v_lines[0] * mb.width if v_lines else mb.x0
        x1 = mb.x0 + v_lines[-1] * mb.width if v_lines else mb.x1
        y0 = mb.y0 + h_lines[0] * mb.height if h_lines else mb.y0
        y1 = mb.y0 + h_lines[-1] * mb.height if h_lines else mb.y1
        return fitz.Rect(x0, y0, x1, y1)

    def _transform_objects_for_crop(
        self, old_crop_data: dict, new_crop_data: dict,
    ) -> None:
        """Transform saved object coordinates from old crop space to new crop space.

        Bbox coords are 0-based within the crop area.  When the crop changes
        the origin shifts, so every saved coordinate must be adjusted::

            new_xy = old_xy + old_origin - new_origin

        Objects that end up completely outside the new crop area are removed.
        """
        if not self._file_path or not self._doc:
            return

        # Reset all pages to MediaBox so mediabox dims are reliable
        for pi in range(self._page_count):
            self._doc[pi].set_cropbox(self._doc[pi].mediabox)

        meta = self._catalog_meta
        objects = meta.get("objects", [])
        if not objects:
            return

        transformed: list[dict] = []
        for obj in objects:
            pi = obj.get("page", 0)
            if pi >= self._page_count:
                transformed.append(obj)
                continue

            old_ox, old_oy = self._crop_origin_for_page(pi, old_crop_data)
            new_ox, new_oy = self._crop_origin_for_page(pi, new_crop_data)
            new_rect = self._crop_rect_for_page(pi, new_crop_data)
            crop_w = new_rect.width
            crop_h = new_rect.height

            dx = old_ox - new_ox
            dy = old_oy - new_oy

            pts = obj.get("pts", [])
            if len(pts) != 4:
                transformed.append(obj)
                continue

            new_pts = [pts[0] + dx, pts[1] + dy, pts[2] + dx, pts[3] + dy]

            # Skip objects completely outside the new crop area
            if new_pts[2] <= 0 or new_pts[3] <= 0 or new_pts[0] >= crop_w or new_pts[1] >= crop_h:
                continue

            # Clamp to crop bounds
            new_pts[0] = max(0.0, new_pts[0])
            new_pts[1] = max(0.0, new_pts[1])
            new_pts[2] = min(crop_w, new_pts[2])
            new_pts[3] = min(crop_h, new_pts[3])
            obj["pts"] = new_pts

            # Transform user_pts if present
            u = obj.get("user_pts")
            if u and len(u) == 4:
                obj["user_pts"] = [u[0] + dx, u[1] + dy, u[2] + dx, u[3] + dy]

            # Transform table grid segments: h_seg=(x0, x1, y), v_seg=(x, y0, y1)
            if obj.get("h_segments"):
                obj["h_segments"] = [
                    [s[0] + dx, s[1] + dx, s[2] + dy] for s in obj["h_segments"]
                ]
            if obj.get("v_segments"):
                obj["v_segments"] = [
                    [s[0] + dx, s[1] + dy, s[2] + dy] for s in obj["v_segments"]
                ]

            transformed.append(obj)

        meta["objects"] = transformed
        logger.info(
            "Transformed %d objects for crop change (%d removed)",
            len(transformed), len(objects) - len(transformed),
        )

    def _apply_pdf_cropbox(self) -> None:
        """Set CropBox on each page from cropping guide lines.

        Guide lines are fractional (0..1) relative to the original MediaBox.
        Content between first and last h/v lines is kept.
        """
        if not self._doc:
            return
        for page_idx in range(self._page_count):
            page = self._doc[page_idx]
            # Always reset to full page first
            page.set_cropbox(page.mediabox)

            side = self._cropping_for_page(page_idx)
            if not side:
                continue
            h_lines = sorted(side.get("h_lines", []))
            v_lines = sorted(side.get("v_lines", []))
            if not h_lines and not v_lines:
                continue

            mb = page.mediabox
            x0 = mb.x0 + v_lines[0] * mb.width if v_lines else mb.x0
            x1 = mb.x0 + v_lines[-1] * mb.width if v_lines else mb.x1
            y0 = mb.y0 + h_lines[0] * mb.height if h_lines else mb.y0
            y1 = mb.y0 + h_lines[-1] * mb.height if h_lines else mb.y1

            page.set_cropbox(fitz.Rect(x0, y0, x1, y1))

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

    def _on_content_filter_changed(self, _checked: bool = False) -> None:
        """Update content mask on all page widgets."""
        mask = {
            key: cb.isChecked()
            for key, cb in self._content_checkboxes.items()
        }
        for spread in self._spreads:
            spread.left_page.set_content_mask(mask)
            spread.right_page.set_content_mask(mask)

    def _on_show_only(self, type_key: str) -> None:
        """Show only the selected type, uncheck all others."""
        for key, cb in self._filter_checkboxes.items():
            cb.blockSignals(True)
            cb.setChecked(key == type_key or key == "bbox")
            cb.blockSignals(False)
        self._on_bbox_filter_changed()

    def _on_bbox_filter_changed(self, _checked: bool = False) -> None:
        """Update visible layers on all page widgets."""
        # Sync parent "PDF Objects" → toggle all sub-types together
        pdf_parent = self._filter_checkboxes.get("pdf_objects")
        if pdf_parent:
            pdf_subs = ["pdf_text", "pdf_image", "pdf_table"]
            parent_on = pdf_parent.isChecked()
            for sk in pdf_subs:
                scb = self._filter_checkboxes.get(sk)
                if scb:
                    scb.blockSignals(True)
                    if not parent_on:
                        scb.setChecked(False)
                    elif not any(self._filter_checkboxes[s].isChecked() for s in pdf_subs):
                        scb.setChecked(True)  # turning parent on → enable all subs
                    scb.setEnabled(parent_on)
                    scb.blockSignals(False)

        layers = {
            key: cb.isChecked()
            for key, cb in self._filter_checkboxes.items()
        }
        for spread in self._spreads:
            spread.left_page.set_visible_layers(layers)
            spread.right_page.set_visible_layers(layers)

    def _apply_bboxes_to_spreads(self) -> None:
        """Apply cached bboxes and pdf objects to all spread widgets with current zoom."""
        zf = self._current_zoom_factor()
        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                pi = pw._page_index
                if pi < 0:
                    continue
                if pi in self._bboxes_cache:
                    pw.set_bboxes(self._bboxes_cache[pi], zf)
                if pi in self._pdf_objects_cache:
                    pw.set_pdf_objects(self._pdf_objects_cache[pi], zf)

    def _get_current_page(self) -> int:
        """Return the 0-based page index currently visible."""
        return max(0, self.page_spin.value() - 1)

    def _detect_visible_pages(self) -> None:
        """Schedule background detection for pages near the viewport."""
        if not self._doc or not self._file_path:
            return
        if not hasattr(self, "_detect_timer"):
            self._detect_timer = QTimer(self)
            self._detect_timer.setSingleShot(True)
            self._detect_timer.timeout.connect(self._start_next_detect)
            self._detect_worker: DetectWorker | None = None
        # Debounce rapid scroll
        self._detect_timer.start(50)

    def _start_next_detect(self) -> None:
        """Find the next undetected page and launch a background worker."""
        if not self._doc or not self._file_path:
            return
        # Don't start a new worker if one is already running
        if self._detect_worker and self._detect_worker.isRunning():
            return

        current = self._get_current_page()
        start = max(0, current - 2)
        end = min(self._page_count, current + 4)

        page_num = None
        for p in range(start, end):
            if p not in self._bboxes_cache:
                page_num = p
                break
        if page_num is None:
            self.progress.emit("")
            return

        import time
        self._op_start = time.perf_counter()
        remaining = sum(1 for p in range(start, end) if p not in self._bboxes_cache)
        self._emit_progress(f"Detect p.{page_num+1} ({remaining} remaining)")

        worker = DetectWorker(self, page_num, parent=self)
        worker.page_detected.connect(self._on_page_detected)
        worker.finished.connect(self._on_detect_worker_done)
        self._detect_worker = worker
        worker.start()

    def _on_page_detected(self, page_num: int, bboxes: list, stats: str, pdf_objects: list) -> None:
        """Slot: background detection finished for one page."""
        pn = page_num + 1
        self._emit_progress(f"Detect p.{pn} — {len(bboxes)} objects, merge")

        for b in bboxes:
            b["page"] = page_num
        if self._file_path:
            bboxes = merge_detected(self._file_path, page_num, bboxes)
            self._catalog_meta = load_meta(self._file_path)

        self._bboxes_cache[page_num] = bboxes
        self._stats_cache[page_num] = stats
        self._pdf_objects_cache[page_num] = pdf_objects
        self._apply_bboxes_to_spreads()

        # Run template detection after 3+ pages cached
        if len(self._bboxes_cache) >= 3:
            self._detect_template_objects()
            self._apply_bboxes_to_spreads()

        self._emit_done(f"Detect p.{pn}")
        logger.info("Detect page %d: %s", pn, stats)

    def _detect_template_objects(self) -> None:
        """Compare bboxes across cached pages to find template objects.

        Template = same type + same position (±5pt) + same size (±10%)
        appearing on 3+ pages. Marks matching bboxes with is_template=True.
        """
        if len(self._bboxes_cache) < 3:
            return

        # Collect all bboxes with page info
        all_bboxes: list[tuple[int, dict]] = []  # (page, bbox)
        for page_num, page_bboxes in self._bboxes_cache.items():
            for bbox in page_bboxes:
                all_bboxes.append((page_num, bbox))

        # Clear old template marks
        for _, bbox in all_bboxes:
            bbox["is_template"] = False

        # Group by similar position + size
        pos_tol = 5    # pt tolerance for position
        size_tol = 0.1  # 10% tolerance for size

        # Find template candidates: for each bbox, count matches on other pages
        templates_found = 0
        for i, (pi, bi) in enumerate(all_bboxes):
            if bi.get("is_template"):
                continue
            pts_i = bi["pts"]
            wi = pts_i[2] - pts_i[0]
            hi = pts_i[3] - pts_i[1]
            if wi < 5 or hi < 5:
                continue

            matching_pages: set[int] = {pi}
            matching_indices: list[int] = [i]

            for j, (pj, bj) in enumerate(all_bboxes):
                if j == i or pj == pi:
                    continue
                if pj in matching_pages:
                    continue
                pts_j = bj["pts"]
                # Position match (within tolerance)
                pos_match = (abs(pts_i[0] - pts_j[0]) <= pos_tol
                             and abs(pts_i[1] - pts_j[1]) <= pos_tol
                             and abs(pts_i[2] - pts_j[2]) <= pos_tol
                             and abs(pts_i[3] - pts_j[3]) <= pos_tol)
                # Content hash match
                hash_i = bi.get("_chash", b"")
                hash_j = bj.get("_chash", b"")
                hash_match = (hash_i and hash_j and hash_i == hash_j)
                # Both position AND content must match
                if pos_match and hash_match:
                    matching_pages.add(pj)
                    matching_indices.append(j)

            if len(matching_pages) >= 3:
                for idx in matching_indices:
                    if all_bboxes[idx][1].get("type") != "table":
                        all_bboxes[idx][1]["is_template"] = True
                templates_found += 1

        if templates_found:
            logger.info("Template detection: %d template groups found", templates_found)

    def _on_detect_worker_done(self) -> None:
        """Worker finished — schedule next page if needed."""
        self._detect_worker = None
        # Check if more pages need detection
        self._detect_timer.start(10)

    def _extract_pdf_objects(self, page) -> list[dict]:
        """Extract native PDF object bounding boxes from a page.

        Includes text, images, tables, and graphics (shapes, lines, curves).
        """
        objects: list[dict] = []
        logger.info(f"_extract_pdf_objects: page {page.number + 1}")
        try:
            # get_text("dict") returns text and images
            td = page.get_text("dict")
            blocks = td["blocks"]
            text_count = 0
            img_count = 0
            for b in blocks:
                bbox = b["bbox"]
                if b["type"] == 0:
                    pdf_type = "pdf_text"
                    text_count += 1
                    lines = b.get("lines", [])
                    preview = ""
                    for ln in lines[:2]:
                        for sp in ln.get("spans", []):
                            preview += sp.get("text", "") + " "
                    preview = preview.strip()[:30]
                    label = f"text: {preview}" if preview else "text"
                elif b["type"] == 1:
                    pdf_type = "pdf_image"
                    img_count += 1
                    w = int(bbox[2] - bbox[0])
                    h = int(bbox[3] - bbox[1])
                    label = f"image {w}x{h}pt"
                else:
                    pdf_type = "pdf_text"
                    label = f"block-{b['type']}"
                objects.append({
                    "pdf_type": pdf_type,
                    "label": label,
                    "pts": (bbox[0], bbox[1], bbox[2], bbox[3]),
                })
            logger.info(f"  get_text blocks: {text_count} text, {img_count} images, total blocks: {len(blocks)}")
        except Exception as e:
            logger.error(f"  get_text error: {e}")
        try:
            tables = page.find_tables()
            logger.info(f"  tables: {len(tables.tables) if tables else 0}")
            if tables:
                for t in tables.tables:
                    objects.append({
                        "pdf_type": "pdf_table",
                        "label": f"table {t.row_count}x{t.col_count}",
                        "pts": tuple(t.bbox),
                    })
        except Exception as e:
            logger.error(f"  find_tables error: {e}")
        try:
            # Extract graphics (shapes, lines, curves)
            drawings = page.get_drawings()
            logger.info(f"  drawings: {len(drawings)}")
            for drw in drawings:
                # drw is a dict with "rect" field (not an object with .bbox attribute)
                rect = drw.get("rect")
                if not rect:
                    continue
                x0, y0, x1, y1 = rect
                if x1 <= x0 or y1 <= y0:
                    continue
                w = int(x1 - x0)
                h = int(y1 - y0)
                objects.append({
                    "pdf_type": "pdf_image",  # Graphics treated as image-like objects
                    "label": f"graphics {w}x{h}pt",
                    "pts": (x0, y0, x1, y1),
                })
        except Exception as e:
            logger.error(f"  get_drawings error: {e}")
        try:
            # Extract all embedded images (covers inline images, XObjects, etc.)
            image_list = page.get_images()
            logger.info(f"  images: {len(image_list)}")
            for img_idx in image_list:
                xref = img_idx[0]
                try:
                    rect = page.get_image_bbox(img_idx)
                    if not rect.is_empty:
                        objects.append({
                            "pdf_type": "pdf_image",
                            "label": f"image xref={xref}",
                            "pts": (rect.x0, rect.y0, rect.x1, rect.y1),
                        })
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"  get_images error: {e}")
        logger.info(f"  total objects detected: {len(objects)}")
        return objects

    def _detect_native_page(self, page) -> tuple[list[dict], str]:
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

        h_img, w_img = binary.shape

        # Save raw binary (unmasked) for grow operations
        binary_raw = binary.copy()

        # ── Extract PDF text blocks and mask them ────────────────
        pdf_text_bboxes: list[dict] = []
        text_blocks = page.get_text("blocks")
        for tb in text_blocks:
            if tb[6] == 0:  # text block
                tx0 = max(0, int(tb[0]))
                ty0 = max(0, int(tb[1]))
                tx1 = min(w_img, int(tb[2]))
                ty1 = min(h_img, int(tb[3]))
                if tx1 > tx0 and ty1 > ty0:
                    binary[ty0:ty1, tx0:tx1] = 0
                    pdf_text_bboxes.append({
                        "type": "text",
                        "label": f"text {tx1-tx0}x{ty1-ty0}pt",
                        "pts": (float(tx0), float(ty0),
                                float(tx1), float(ty1)),
                    })

        # ── Detection using selected method ──────────────────────
        method_idx = getattr(self, '_detect_method_idx', 8)  # default: Hybrid 2-pass

        # PDF objects only method: extract native PDF objects and map types
        if method_idx == 9:
            pdf_objs = self._extract_pdf_objects(page)
            type_map = {
                "pdf_text": "text",
                "pdf_image": "photo",
                "pdf_table": "table",
            }
            bboxes: list[dict] = []
            for obj in pdf_objs:
                pdf_type = obj.get("pdf_type", "pdf_text")
                mapped_type = type_map.get(pdf_type, "unknown")
                bboxes.append({
                    "type": mapped_type,
                    "label": obj.get("label", ""),
                    "pts": obj.get("pts"),
                })
            # Generate stats
            n_t = sum(1 for b in bboxes if b["type"] == "table")
            n_d = sum(1 for b in bboxes if b["type"] == "drawing")
            n_x = sum(1 for b in bboxes if b["type"] == "text")
            n_p = sum(1 for b in bboxes if b["type"] in ("photo", "picture"))
            stats = (
                f"P{page.number + 1}  |  "
                f"T:{n_t} Drw:{n_d} Txt:{n_x} Img:{n_p}  "
                f"Total:{len(bboxes)}"
            )
            return bboxes, stats

        min_obj_size = 20

        # PDF text skip rects for scanline
        text_skip = [tb["pts"] for tb in pdf_text_bboxes]

        # Pass 1: scanline on masked binary (grow on raw)
        pass1_raw, _ = _scanline_v1_core(
            binary_raw.copy(), h_img, w_img, margin=3,
            min_obj=min_obj_size, skip_rects=text_skip,
        )

        # Filter out bboxes mostly covered by PDF text (≥80%)
        pass1_filtered: list[tuple[int, int, int, int]] = []
        for bt in pass1_raw:
            bbox_area = max((bt[2]-bt[0]) * (bt[3]-bt[1]), 1)
            text_cover = 0.0
            for tb in pdf_text_bboxes:
                tp = tb["pts"]
                ix0 = max(bt[0], tp[0]); iy0 = max(bt[1], tp[1])
                ix1 = min(bt[2], tp[2]); iy1 = min(bt[3], tp[3])
                if ix0 < ix1 and iy0 < iy1:
                    text_cover += (ix1-ix0) * (iy1-iy0)
            if text_cover / bbox_area < 0.8:
                pass1_filtered.append(bt)

        # Hybrid pass 2 (if selected): merge fragmented tables
        if method_idx == 8:  # Hybrid 2-pass
            binary_full = (gray < 240).astype(np.uint8)
            pass2_raw, _ = _scanline_v1_core(
                binary_full, h_img, w_img, margin=3, min_obj=min_obj_size,
            )
            used_p1: set[int] = set()
            merged_pts: list[tuple[int, int, int, int]] = []
            for p2 in pass2_raw:
                contained: list[int] = []
                for i, p1 in enumerate(pass1_filtered):
                    if i in used_p1:
                        continue
                    p1a = max((p1[2]-p1[0])*(p1[3]-p1[1]), 1)
                    ix0 = max(p1[0],p2[0]); iy0 = max(p1[1],p2[1])
                    ix1 = min(p1[2],p2[2]); iy1 = min(p1[3],p2[3])
                    if ix0<ix1 and iy0<iy1 and (ix1-ix0)*(iy1-iy0)/p1a >= 0.7:
                        contained.append(i)
                if len(contained) >= 2:
                    merged_pts.append(p2)
                    used_p1.update(contained)
            final_pts = [p for i, p in enumerate(pass1_filtered) if i not in used_p1]
            final_pts.extend(merged_pts)
        else:
            final_pts = pass1_filtered

        # Build bbox dicts with content hash
        bboxes: list[dict] = []
        for i, bt in enumerate(final_pts):
            bw, bh = bt[2]-bt[0], bt[3]-bt[1]
            chash = self._bbox_content_hash(gray, bt[1], bt[0], bt[3], bt[2])
            bboxes.append({
                "type": "unknown",
                "label": f"#{i} {bw}x{bh}pt",
                "pts": (float(bt[0]), float(bt[1]), float(bt[2]), float(bt[3])),
                "_chash": chash,
            })

        # ── Classify untyped bboxes (skip already classified) ─────
        words = page.get_text("words")
        for bbox in bboxes:
            if bbox["type"] == "unknown":
                bbox["type"] = self._classify_bbox(bbox, page, img, words)

        # ── Absorb fully nested PDF text blocks into detected bboxes ──
        # If a PDF text block is fully inside a detected bbox → merge it.
        # Detected bbox with absorbed text → classify as "table" immediately.
        absorbed_text: set[int] = set()
        for bbox in bboxes:
            bp = bbox["pts"]
            has_text = False
            for ti, tb in enumerate(pdf_text_bboxes):
                if ti in absorbed_text:
                    continue
                tp = tb["pts"]
                t_area = max((tp[2]-tp[0]) * (tp[3]-tp[1]), 1)
                ix0 = max(bp[0], tp[0]); iy0 = max(bp[1], tp[1])
                ix1 = min(bp[2], tp[2]); iy1 = min(bp[3], tp[3])
                if ix0 < ix1 and iy0 < iy1:
                    inter = (ix1-ix0) * (iy1-iy0)
                    if inter / t_area >= 0.9:
                        absorbed_text.add(ti)
                        has_text = True
            if has_text:
                bbox["type"] = "table"

        # Add standalone PDF text blocks (not absorbed by any detected bbox)
        for ti, tb in enumerate(pdf_text_bboxes):
            if ti not in absorbed_text:
                bboxes.append(tb)

        # Update labels
        for i, bbox in enumerate(bboxes):
            pts = bbox["pts"]
            w, h = pts[2] - pts[0], pts[3] - pts[1]
            bbox["label"] = f"#{i} {bbox['type']} {w:.0f}x{h:.0f}pt"

        n_t = sum(1 for b in bboxes if b["type"] == "table")
        n_d = sum(1 for b in bboxes if b["type"] == "drawing")
        n_x = sum(1 for b in bboxes if b["type"] == "text")
        n_p = sum(1 for b in bboxes if b["type"] in ("photo", "picture"))
        stats = (
            f"P{page.number + 1}  |  "
            f"T:{n_t} Drw:{n_d} Txt:{n_x} Img:{n_p}  "
            f"Total:{len(bboxes)}"
        )
        return bboxes, stats

    @staticmethod
    def _bbox_content_hash(gray, y0: int, x0: int, y1: int, x1: int) -> bytes:
        """Compute perceptual hash of bbox region (16x16 average hash).

        Resize region to 16x16 via block averaging, binarize at mean → 32 bytes.
        """
        import numpy as np
        region = gray[y0:y1, x0:x1]
        if region.size == 0:
            return b"\x00" * 32
        h, w = region.shape
        # Block average to 16x16
        bh = max(h // 16, 1)
        bw = max(w // 16, 1)
        thumb = np.zeros((16, 16), dtype=np.float32)
        for ty in range(16):
            for tx in range(16):
                sy = min(ty * bh, h - 1)
                ey = min(sy + bh, h)
                sx = min(tx * bw, w - 1)
                ex = min(sx + bw, w)
                thumb[ty, tx] = region[sy:ey, sx:ex].mean()
        # Binarize at mean
        mean_val = thumb.mean()
        bits = (thumb > mean_val).flatten()
        # Pack 256 bits into 32 bytes
        result = bytearray(32)
        for i, bit in enumerate(bits):
            if bit:
                result[i // 8] |= (1 << (i % 8))
        return bytes(result)

    def _classify_bbox(
        self, bbox: dict, page, img_arr, words: list,
    ) -> str:
        """Classify a bbox by analyzing its content.

        Checks color map, text structure, dimension patterns.
        Returns: 'drawing', 'photo', 'picture', 'table', or 'text'.
        """
        import numpy as np

        pts = bbox["pts"]
        x0, y0, x1, y1 = pts[0], pts[1], pts[2], pts[3]

        # Crop the rendered image region (72 DPI, 1px = 1pt)
        crop = img_arr[int(y0):int(y1), int(x0):int(x1)]
        if crop.size == 0:
            return "unknown"

        # ── Color analysis ──────────────────────────────────────────
        h, w = crop.shape[:2]
        # Sample up to 2000 pixels for color diversity
        total_px = h * w
        step = max(total_px // 2000, 1)
        flat = crop.reshape(-1, crop.shape[2])
        sampled = flat[::step, :3]
        unique_colors = len(set(map(bytes, sampled)))

        is_bw = unique_colors <= 8
        is_limited_palette = unique_colors <= 64
        is_photo_palette = unique_colors > 200

        # ── Text words inside bbox ──────────────────────────────────
        bbox_words = []
        for wd in words:
            wx0, wy0, wx1, wy1 = float(wd[0]), float(wd[1]), float(wd[2]), float(wd[3])
            if wx0 >= x0 - 2 and wy0 >= y0 - 2 and wx1 <= x1 + 2 and wy1 <= y1 + 2:
                bbox_words.append(wd)

        n_words = len(bbox_words)

        # ── Dimension pattern check ─────────────────────────────────
        dim_count = 0
        if bbox_words:
            for wd in bbox_words:
                if _DIM_PATTERN.match(wd[4]):
                    dim_count += 1

        # ── Row structure analysis (table detection) ────────────────
        is_table = False
        if n_words >= 6:
            # Group words by Y proximity (3pt threshold)
            word_ys = sorted(set(round(float(wd[1]) / 3) * 3 for wd in bbox_words))
            n_rows = len(word_ys)

            if n_rows >= 3:
                # Count words per row
                row_word_counts = []
                for ry in word_ys:
                    row_words = [
                        wd for wd in bbox_words
                        if abs(float(wd[1]) - ry) < 4
                    ]
                    row_word_counts.append(len(row_words))

                # Table: multiple rows with similar word count (>=3 cols)
                cols_per_row = [c for c in row_word_counts if c >= 3]
                if len(cols_per_row) >= 3:
                    # Check column alignment: X positions repeat across rows
                    all_x_starts = []
                    for ry in word_ys:
                        row_words = sorted(
                            [wd for wd in bbox_words if abs(float(wd[1]) - ry) < 4],
                            key=lambda w: float(w[0]),
                        )
                        all_x_starts.append([round(float(w[0]) / 5) * 5 for w in row_words])

                    # Count how many X positions appear in multiple rows
                    from collections import Counter
                    x_counts = Counter()
                    for xs in all_x_starts:
                        for x in set(xs):
                            x_counts[x] += 1
                    aligned_cols = sum(1 for x, cnt in x_counts.items() if cnt >= n_rows * 0.3)
                    if aligned_cols >= 3:
                        is_table = True

        # ── Classification decision ─────────────────────────────────
        if is_table:
            return "table"

        # Drawing: has dimension annotations OR narrow color palette with few words
        has_dimensions = dim_count >= 2 or (
            n_words > 0 and dim_count / max(n_words, 1) > 0.3
        )
        if has_dimensions:
            return "drawing"
        if (is_bw or is_limited_palette) and n_words < 20:
            return "drawing"

        if is_photo_palette and n_words < 5:
            return "photo"

        if not is_limited_palette and n_words < 10:
            return "picture"

        if n_words >= 3:
            return "text"

        return "unknown"

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
        """Ctrl+Wheel to zoom, anchored at mouse position."""
        if event.modifiers() & Qt.KeyboardModifier.ControlModifier:
            delta = event.angleDelta().y()
            step = 0.1 if delta > 0 else -0.1
            # Map mouse position to viewport coordinates
            vp_pos = self.scroll_area.viewport().mapFromGlobal(event.globalPosition().toPoint())
            self._set_zoom(self._zoom + step, anchor=QPointF(vp_pos))
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

    def _on_bbox_testbench(self, page_idx: int) -> None:
        """Open scanline detection test dialog for the given page."""
        if not self._doc or page_idx < 0:
            return
        dlg = ScanlineTestDialog(self._doc, page_idx, parent=self)
        dlg.show()

    def _on_object_stats_requested(self, bbox: dict, page_index: int, file_path: str) -> None:
        """Open stats dialog for double-clicked bbox."""
        dlg = BboxStatsDialog(bbox, page_index, file_path, parent=self)
        dlg.exec()

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
