"""Preview tab — 2-page PDF viewer with zoom, continuous scroll, and bounding box overlay."""

import logging
from pathlib import Path

import fitz  # PyMuPDF
from PySide6.QtCore import QPoint, QRectF, Qt
from PySide6.QtGui import QBrush, QColor, QFont, QImage, QPainter, QPen, QPixmap, QWheelEvent
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QSlider,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

logger = logging.getLogger(__name__)

# Colors for different bounding box types
BBOX_COLORS = {
    "table": QColor(0, 120, 215, 100),      # blue
    "text": QColor(76, 175, 80, 80),         # green
    "image": QColor(255, 152, 0, 80),        # orange
    "drawing": QColor(156, 39, 176, 80),     # purple
    "unknown": QColor(158, 158, 158, 80),    # gray
}

BBOX_BORDER_COLORS = {
    "table": QColor(0, 120, 215, 200),
    "text": QColor(76, 175, 80, 160),
    "image": QColor(255, 152, 0, 160),
    "drawing": QColor(156, 39, 176, 160),
    "unknown": QColor(158, 158, 158, 160),
}


class PageWidget(QWidget):
    """Renders a single PDF page with optional bounding box overlay and stats header."""

    HEADER_HEIGHT = 24

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._pixmap: QPixmap | None = None
        self._bboxes: list[dict] = []
        self._show_bboxes = False
        self._page_stats: str = ""

    def set_pixmap(self, pixmap: QPixmap) -> None:
        self._pixmap = pixmap
        self.setFixedSize(pixmap.width(), pixmap.height() + self.HEADER_HEIGHT)
        self.update()

    def set_bboxes(self, bboxes: list[dict]) -> None:
        self._bboxes = bboxes
        self.update()

    def set_show_bboxes(self, show: bool) -> None:
        self._show_bboxes = show
        self.update()

    def set_page_stats(self, stats: str) -> None:
        self._page_stats = stats
        self.update()

    def paintEvent(self, event) -> None:
        if not self._pixmap:
            return
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        h = self.HEADER_HEIGHT

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

        if self._show_bboxes and self._bboxes:
            for bbox in self._bboxes:
                bbox_type = bbox.get("type", "unknown")
                rect = bbox.get("rect")
                if not rect:
                    continue

                x, y, w, bh = rect
                y += h  # offset by header
                fill = BBOX_COLORS.get(bbox_type, BBOX_COLORS["unknown"])
                border = BBOX_BORDER_COLORS.get(bbox_type, BBOX_BORDER_COLORS["unknown"])

                painter.setBrush(fill)
                painter.setPen(QPen(border, 2))
                painter.drawRect(QRectF(x, y, w, bh))

                # Label
                painter.setPen(QPen(border, 1))
                label = bbox.get("label", bbox_type)
                painter.drawText(int(x + 3), int(y + 14), label)

        painter.end()


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
        self._bboxes_cache: dict[int, list[dict]] = {}
        self._stats_cache: dict[int, str] = {}

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

        # Bounding boxes toggle
        self.bbox_check = QCheckBox("Bounding Boxes")
        self.bbox_check.toggled.connect(self._toggle_bboxes)
        toolbar.addWidget(self.bbox_check)

        self.bbox_type_combo = QComboBox()
        self.bbox_type_combo.addItems(["All", "Tables", "Text", "Images", "Drawings"])
        self.bbox_type_combo.currentTextChanged.connect(self._on_bbox_filter_changed)
        toolbar.addWidget(self.bbox_type_combo)

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

        self._detect_objects()
        self._render_all_spreads()
        logger.info("Loaded document: %s (%d pages)", file_path.name, self._page_count)

    def _render_all_spreads(self) -> None:
        """Render all page spreads (2 pages per row)."""
        # Clear existing
        for spread in self._spreads:
            self.pages_layout.removeWidget(spread)
            spread.deleteLater()
        self._spreads.clear()

        if not self._doc:
            return

        dpi = int(self._base_dpi * self._zoom)

        # Create spreads: pages 0-1, 2-3, 4-5, etc.
        page_idx = 0
        while page_idx < self._page_count:
            spread = PageSpreadWidget()

            # Left page
            left_pixmap = self._render_page(page_idx, dpi)
            spread.left_page.set_pixmap(left_pixmap)
            spread.left_page.set_show_bboxes(self.bbox_check.isChecked())
            if page_idx in self._bboxes_cache:
                spread.left_page.set_bboxes(
                    self._filter_bboxes(self._bboxes_cache[page_idx])
                )
            if page_idx in self._stats_cache:
                spread.left_page.set_page_stats(self._stats_cache[page_idx])

            # Right page
            if page_idx + 1 < self._page_count:
                right_pixmap = self._render_page(page_idx + 1, dpi)
                spread.right_page.set_pixmap(right_pixmap)
                spread.right_page.set_show_bboxes(self.bbox_check.isChecked())
                if page_idx + 1 in self._bboxes_cache:
                    spread.right_page.set_bboxes(
                        self._filter_bboxes(self._bboxes_cache[page_idx + 1])
                    )
                if page_idx + 1 in self._stats_cache:
                    spread.right_page.set_page_stats(self._stats_cache[page_idx + 1])
            else:
                # Odd page count — blank right side
                blank = QPixmap(left_pixmap.size())
                blank.fill(QColor(240, 240, 240))
                spread.right_page.set_pixmap(blank)

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
        """Calculate zoom to fit 2 pages within scroll area width."""
        if not self._doc or self._page_count == 0:
            return

        page = self._doc[0]
        page_width_pt = page.rect.width  # points (72 dpi)
        two_page_width_px = 2 * (page_width_pt * self._base_dpi / 72.0)

        available_width = self.scroll_area.viewport().width() - 40  # margins
        if two_page_width_px > 0:
            new_zoom = available_width / two_page_width_px
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
        """Update page spinner based on current scroll position."""
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

    def _toggle_bboxes(self, checked: bool) -> None:
        """Show or hide bounding box overlay on all pages."""
        for spread in self._spreads:
            spread.left_page.set_show_bboxes(checked)
            spread.right_page.set_show_bboxes(checked)

    def _on_bbox_filter_changed(self, text: str) -> None:
        """Re-apply filtered bboxes to all pages."""
        self._apply_bboxes_to_spreads()

    def _filter_bboxes(self, bboxes: list[dict]) -> list[dict]:
        """Filter bboxes by selected type."""
        selected = self.bbox_type_combo.currentText().lower()
        if selected == "all":
            return bboxes
        type_map = {"tables": "table", "text": "text", "images": "image", "drawings": "drawing"}
        filter_type = type_map.get(selected, selected)
        return [b for b in bboxes if b.get("type") == filter_type]

    def _apply_bboxes_to_spreads(self) -> None:
        """Apply cached bboxes to all spread widgets."""
        for i, spread in enumerate(self._spreads):
            left_idx = i * 2
            right_idx = i * 2 + 1

            if left_idx in self._bboxes_cache:
                spread.left_page.set_bboxes(
                    self._filter_bboxes(self._bboxes_cache[left_idx])
                )
            if right_idx in self._bboxes_cache:
                spread.right_page.set_bboxes(
                    self._filter_bboxes(self._bboxes_cache[right_idx])
                )

    def _detect_objects(self) -> None:
        """Detect native PDF objects on all pages using PyMuPDF only."""
        if not self._doc:
            return

        logger.info("Detecting objects on %d pages...", self._page_count)

        try:
            zoom_factor = self._base_dpi * self._zoom / 72.0

            for page_num in range(self._page_count):
                page = self._doc[page_num]
                bboxes, stats = self._detect_native_page(page, zoom_factor)
                self._bboxes_cache[page_num] = bboxes
                self._stats_cache[page_num] = stats
                logger.info("Page %d: %s", page_num + 1, stats)

            logger.info("Detection complete")

        except Exception as e:
            logger.error("Object detection failed: %s", e)

    def _detect_native_page(self, page, zoom_factor: float) -> tuple[list[dict], str]:
        """Detect objects on a page using PyMuPDF. Returns (bboxes, stats_string)."""
        bboxes: list[dict] = []
        obj_id = 0

        # 1. Tables via find_tables()
        words = page.get_text("words")  # (x0, y0, x1, y1, word, block_no, line_no, word_no)
        table_fitz_rects: list[fitz.Rect] = []
        try:
            tables = page.find_tables()
            for table in tables.tables:
                tr = fitz.Rect(table.bbox)
                table_fitz_rects.append(tr)
                bboxes.append({
                    "type": "table",
                    "label": f"#{obj_id} table {table.row_count}x{table.col_count}",
                    "rect": self._pts_to_px(table.bbox, zoom_factor),
                })
                obj_id += 1
        except Exception:
            pass

        # 2. Text — use words for precise line-level bboxes
        # Expanded table rects for filtering (catch text just outside table lines)
        table_filter_rects = [
            fitz.Rect(tr.x0 - 60, tr.y0 - 15, tr.x1 + 60, tr.y1 + 15)
            for tr in table_fitz_rects
        ]
        if words:
            # Group words into text lines
            lines: dict[tuple[int, int], list] = {}
            for w in words:
                key = (w[5], w[6])  # block_no, line_no
                lines.setdefault(key, []).append(w)

            for key, line_words in lines.items():
                x0 = min(w[0] for w in line_words)
                y0 = min(w[1] for w in line_words)
                x1 = max(w[2] for w in line_words)
                y1 = max(w[3] for w in line_words)
                text_preview = " ".join(w[4] for w in line_words[:4])[:30]

                # Skip text lines inside or near table areas
                line_rect = fitz.Rect(x0, y0, x1, y1)
                in_table = any(tr.contains(line_rect) for tr in table_filter_rects)
                if in_table:
                    continue

                bboxes.append({
                    "type": "text",
                    "label": f"#{obj_id} {text_preview}",
                    "rect": self._pts_to_px((x0, y0, x1, y1), zoom_factor),
                })
                obj_id += 1

        # 3. Embedded raster images via get_images()
        for img_info in page.get_images(full=True):
            xref = img_info[0]
            img_w, img_h = img_info[2], img_info[3]
            for rect in page.get_image_rects(xref):
                bboxes.append({
                    "type": "image",
                    "label": f"#{obj_id} image {img_w}x{img_h}px",
                    "rect": self._pts_to_px(
                        (rect.x0, rect.y0, rect.x1, rect.y1), zoom_factor
                    ),
                })
                obj_id += 1

        # 4. Vector drawings — cluster into regions, skip table-area lines
        drawings = page.get_drawings()
        if drawings:
            diagram_rects: list[tuple[float, float, float, float]] = []
            for d in drawings:
                r = d.get("rect")
                if not r or (r.width < 3 and r.height < 3):
                    continue
                # Normalize degenerate (zero-width/height) rects for intersection test
                nr = fitz.Rect(r.x0 - 1, r.y0 - 1, r.x1 + 1, r.y1 + 1)
                # Skip drawings that overlap with table areas
                if any(tr.intersects(nr) for tr in table_filter_rects):
                    continue
                diagram_rects.append((r.x0, r.y0, r.x1, r.y1))

            # Cluster nearby drawings into diagram regions
            regions = self._cluster_rects(diagram_rects, gap=5)
            # Filter: keep only regions that are significant (not thin lines)
            for c in regions:
                w, h = c[2] - c[0], c[3] - c[1]
                if w > 30 and h > 30:
                    bboxes.append({
                        "type": "drawing",
                        "label": f"#{obj_id} diagram {w:.0f}x{h:.0f}pt",
                        "rect": self._pts_to_px(
                            (c[0], c[1], c[2], c[3]), zoom_factor
                        ),
                    })
                    obj_id += 1

        n_tables = sum(1 for b in bboxes if b["type"] == "table")
        n_text = sum(1 for b in bboxes if b["type"] == "text")
        n_images = sum(1 for b in bboxes if b["type"] == "image")
        n_drawings = sum(1 for b in bboxes if b["type"] == "drawing")

        stats = (
            f"P{page.number + 1}  |  "
            f"T:{n_tables}  Txt:{n_text}  Img:{n_images}  Drw:{n_drawings}  "
            f"Total:{len(bboxes)}"
        )

        return bboxes, stats

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

    @staticmethod
    def _pts_to_px(bbox: tuple, zoom_factor: float) -> tuple[float, float, float, float]:
        """Convert (x0, y0, x1, y1) in points to (x, y, w, h) in pixels."""
        x0, y0, x1, y1 = bbox
        return (
            x0 * zoom_factor,
            y0 * zoom_factor,
            (x1 - x0) * zoom_factor,
            (y1 - y0) * zoom_factor,
        )

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

    def refresh(self) -> None:
        pass
