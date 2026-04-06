"""Preview tab — 2-page PDF viewer with zoom, continuous scroll, and bounding box overlay."""

import logging
from pathlib import Path

import fitz  # PyMuPDF
from PySide6.QtCore import QPoint, QRectF, Qt
from PySide6.QtGui import QColor, QImage, QPainter, QPen, QPixmap, QWheelEvent
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
    """Renders a single PDF page with optional bounding box overlay."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._pixmap: QPixmap | None = None
        self._bboxes: list[dict] = []
        self._show_bboxes = False

    def set_pixmap(self, pixmap: QPixmap) -> None:
        self._pixmap = pixmap
        self.setFixedSize(pixmap.size())
        self.update()

    def set_bboxes(self, bboxes: list[dict]) -> None:
        self._bboxes = bboxes
        self.update()

    def set_show_bboxes(self, show: bool) -> None:
        self._show_bboxes = show
        self.update()

    def paintEvent(self, event) -> None:
        if not self._pixmap:
            return
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.drawPixmap(0, 0, self._pixmap)

        if self._show_bboxes and self._bboxes:
            for bbox in self._bboxes:
                bbox_type = bbox.get("type", "unknown")
                rect = bbox.get("rect")
                if not rect:
                    continue

                x, y, w, h = rect
                fill = BBOX_COLORS.get(bbox_type, BBOX_COLORS["unknown"])
                border = BBOX_BORDER_COLORS.get(bbox_type, BBOX_BORDER_COLORS["unknown"])

                painter.setBrush(fill)
                painter.setPen(QPen(border, 2))
                painter.drawRect(QRectF(x, y, w, h))

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

            # Right page
            if page_idx + 1 < self._page_count:
                right_pixmap = self._render_page(page_idx + 1, dpi)
                spread.right_page.set_pixmap(right_pixmap)
                spread.right_page.set_show_bboxes(self.bbox_check.isChecked())
                if page_idx + 1 in self._bboxes_cache:
                    spread.right_page.set_bboxes(
                        self._filter_bboxes(self._bboxes_cache[page_idx + 1])
                    )
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
                bboxes = self._detect_native_page(page, zoom_factor)
                self._bboxes_cache[page_num] = bboxes
                logger.info("Page %d: %d objects", page_num + 1, len(bboxes))

            logger.info("Detection complete")

        except Exception as e:
            logger.error("Object detection failed: %s", e)

    def _detect_native_page(self, page, zoom_factor: float) -> list[dict]:
        """Detect objects on a native text-layer page using PyMuPDF."""
        bboxes: list[dict] = []

        # Detect tables
        try:
            tables = page.find_tables()
            for table in tables.tables:
                bboxes.append({
                    "type": "table",
                    "label": f"table ({table.row_count}x{table.col_count})",
                    "rect": self._pts_to_px(table.bbox, zoom_factor),
                })
        except Exception:
            pass

        # Detect text and image blocks
        text_dict = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)
        for block in text_dict.get("blocks", []):
            block_type = block.get("type", 0)
            bbox = block.get("bbox", (0, 0, 0, 0))

            if block_type == 0:  # text
                text_preview = ""
                for line in block.get("lines", [])[:1]:
                    for span in line.get("spans", [])[:1]:
                        text_preview = span.get("text", "")[:30]
                bboxes.append({
                    "type": "text",
                    "label": f"text: {text_preview}",
                    "rect": self._pts_to_px(bbox, zoom_factor),
                })
            elif block_type == 1:  # image
                bboxes.append({
                    "type": "image",
                    "label": "image",
                    "rect": self._pts_to_px(bbox, zoom_factor),
                })

        # Detect drawings
        drawings = page.get_drawings()
        if drawings:
            for d in drawings[:50]:
                rect = d.get("rect")
                if rect and (rect.width > 10 and rect.height > 10):
                    bboxes.append({
                        "type": "drawing",
                        "label": "drawing",
                        "rect": self._pts_to_px(
                            (rect.x0, rect.y0, rect.x1, rect.y1), zoom_factor
                        ),
                    })

        return bboxes

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
