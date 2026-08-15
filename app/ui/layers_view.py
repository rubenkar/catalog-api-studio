"""Layers tab: experimental rendering via the pdf_layers subsystem.

Subclass of PreviewView in view-only mode. Hides the default preview
toolbar and builds a new structure:

  header:  [ Method ▾ ]   <stretch>   status: types / render: Xms / flags
  body:    [ left sidebar (types + counts) | pages scroll area ]

Each type row in the sidebar shows a checkbox, the type label, and the
object count on the current spread. Counts refresh on page change and on
document load.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from PySide6.QtCore import QBuffer, QEvent, QIODevice, QSettings, Qt, Signal
from PySide6.QtGui import QAction, QBrush, QColor, QImage, QPainter, QPixmap
from PySide6.QtWidgets import (
    QCheckBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QMenu,
    QPushButton,
    QScrollArea,
    QToolButton,
    QToolTip,
    QVBoxLayout,
    QWidget,
)

from app.services.pdf_layers import (
    LAYER_TYPE_GROUPS,
    METHOD_REGISTRY,
    META_FLAGS,
    LayerType,
    RenderMethod,
    method_supports_type,
)
from app.services.pdf_layers.methods import default_method, method_info
from app.services.pdf_layers.types import DISPLAY_LABELS, default_enabled_types
from app.ui.preview_view import PreviewView, _install_menu_wheel_passthrough

logger = logging.getLogger(__name__)


_SETTINGS_PREFIX = "layers_tab"


BG_MODES: tuple[tuple[str, str], ...] = (
    ("transparent", "Trans"),
    ("white",       "White"),
    ("black",       "Black"),
    ("checker",     "Checker"),
)


@dataclass
class LayersUIState:
    """In-memory snapshot of user selections on the Layers tab."""

    enabled_types: set[LayerType]
    method: RenderMethod
    meta_flags: dict[str, bool]
    bkg_mode: str = "white"   # one of BG_MODES keys


@dataclass(frozen=True)
class ObjectRef:
    """One addressable object on the current spread, shown as a subitem.

    Extra fields for per-object filter dispatch:
        xobject_name — for IMAGE_XOBJECT / FORM_XOBJECT: the /Name used in
                       `Do /Name` content-stream operator
        op_index     — positional index within the object's class, used by
                       the filter's per-class counter or (for annotations)
                       by `page.delete_annot(annots_list[i])`
    """

    id: str                                  # e.g. "img#12", "stroke#3"
    label: str                                # brief human-readable summary
    bbox: tuple[float, float, float, float] | None  # PDF user space, y-up
    page_idx: int                             # 0-based
    xobject_name: str | None = None          # /Name for Do — images/forms
    op_index: int | None = None              # 0-based positional index


def _maybe_bbox(raw) -> tuple[float, float, float, float] | None:
    """Best-effort coerce a fitz.Rect / sequence into a 4-tuple of floats."""
    if raw is None:
        return None
    # fitz.Rect has .x0/.y0/.x1/.y1
    x0 = getattr(raw, "x0", None)
    if x0 is not None:
        try:
            return (float(raw.x0), float(raw.y0), float(raw.x1), float(raw.y1))
        except Exception:
            return None
    # Sequence / tuple
    try:
        t = tuple(raw)
        if len(t) >= 4:
            return (float(t[0]), float(t[1]), float(t[2]), float(t[3]))
    except Exception:
        pass
    return None


def _size_label(bbox: tuple[float, float, float, float] | None) -> str:
    """Compact 'W×H' string for display; '—' if bbox missing."""
    if bbox is None:
        return "—"
    w = bbox[2] - bbox[0]
    h = bbox[3] - bbox[1]
    return f"{w:.0f}×{h:.0f}"


def _flip_y(
    bbox: tuple[float, float, float, float],
    page_height: float,
) -> tuple[float, float, float, float]:
    """Flip Y axis of a bbox between PDF y-UP and widget y-DOWN.

    PDF's native coordinate system puts the origin at the page's
    BOTTOM-LEFT, y increasing upward. Qt/widget coords use TOP-LEFT, y
    increasing downward. PyMuPDF APIs sometimes return one, sometimes the
    other, so we flip explicitly where the source is known y-UP.

    (x0, y0_up, x1, y1_up) → (x0, page_h - y1_up, x1, page_h - y0_up)
    """
    x0, y0, x1, y1 = bbox
    return (x0, page_height - y1, x1, page_height - y0)


def _is_stroke_visible(drw: dict) -> bool:
    """Filter out phantom strokes — get_drawings returns entries for `S`
    operators even when the stroke produces no visible output (zero line
    width, no color, or full transparency). These bloat the sidebar with
    rows that don't correspond to anything a user can see on the render."""
    width = drw.get("width")
    if width is None or width <= 0.01:
        return False
    color = drw.get("color")
    if color is None:
        return False
    opacity = drw.get("stroke_opacity")
    if opacity is not None and opacity <= 0.01:
        return False
    return True


def _is_fill_visible(drw: dict) -> bool:
    """Skip fills with no paint color or full transparency."""
    fill = drw.get("fill")
    if fill is None:
        return False
    opacity = drw.get("fill_opacity")
    if opacity is not None and opacity <= 0.01:
        return False
    return True


def _parse_xobject_resources(doc, xref: int) -> dict[str, int]:
    """Read the /Resources/XObject mapping from an xref's object string.

    Returns {local_name: target_xref}. Empty dict if no /XObject sub-dict
    or the xref isn't readable.

    Used to build the "via X" parent-context hint shown next to form
    xobject subitems — so the user can tell apart multiple forms that
    share the same local name /FmN in different resource dicts.
    """
    import re

    if not xref:
        return {}
    try:
        obj_str = doc.xref_object(xref)
    except Exception:
        return {}
    if not obj_str:
        return {}

    # Find /Resources << ... >> block (non-greedy; handle nested dicts naively)
    # Simpler: scan for /XObject << ... >> directly anywhere in the object.
    xobj_open = obj_str.find("/XObject")
    if xobj_open < 0:
        return {}
    # Find opening << after /XObject
    search_from = xobj_open + len("/XObject")
    dd = obj_str.find("<<", search_from)
    if dd < 0:
        return {}
    # Find matching >> at depth 0
    depth = 1
    i = dd + 2
    n = len(obj_str)
    while i < n - 1 and depth > 0:
        if obj_str[i:i + 2] == "<<":
            depth += 1
            i += 2
        elif obj_str[i:i + 2] == ">>":
            depth -= 1
            if depth == 0:
                break
            i += 2
        else:
            i += 1
    if depth != 0:
        return {}
    xobj_block = obj_str[dd + 2:i]

    out: dict[str, int] = {}
    for m in re.finditer(r"/([A-Za-z0-9_.+\-]+)\s+(\d+)\s+0\s+R", xobj_block):
        try:
            out[m.group(1)] = int(m.group(2))
        except Exception:
            continue
    return out


def _extract_text_operand(raw: bytes, op: bytes) -> str:
    """Best-effort decode of text inside a Tj / TJ / ' / " command.

    Parses literal strings `(…)` and hex strings `<…>` from the operand
    portion of `raw` (everything before the operator keyword) and
    concatenates their bytes as latin-1. Accurate for standard-encoded
    Latin fonts; CID / custom encodings will show unreadable bytes —
    acceptable for a subitem label since truncated anyway.
    """
    # Slice away the trailing operator + any surrounding whitespace.
    # `raw` ends with the operator bytes (per _parse_content_commands).
    operand = raw[:len(raw) - len(op)].rstrip()

    text_chars: list[str] = []
    i = 0
    n = len(operand)
    while i < n:
        c = operand[i:i + 1]
        if c == b"(":
            # Literal string — handle nested parens and \ escapes.
            i += 1
            depth = 1
            while i < n and depth > 0:
                ch = operand[i:i + 1]
                if ch == b"\\":
                    # Skip escape sequence byte (don't emit the backslash)
                    if i + 1 < n:
                        text_chars.append(
                            operand[i + 1:i + 2].decode("latin-1", errors="replace")
                        )
                        i += 2
                    else:
                        i += 1
                    continue
                if ch == b"(":
                    depth += 1
                    text_chars.append("(")
                    i += 1
                    continue
                if ch == b")":
                    depth -= 1
                    if depth == 0:
                        i += 1
                        break
                    text_chars.append(")")
                    i += 1
                    continue
                text_chars.append(ch.decode("latin-1", errors="replace"))
                i += 1
        elif c == b"<":
            # Hex string — pairs of hex digits until '>'
            j = operand.find(b">", i)
            if j < 0:
                break
            hex_str = operand[i + 1:j].replace(b" ", b"").replace(b"\n", b"")
            try:
                if len(hex_str) % 2:
                    hex_str += b"0"
                hex_bytes = bytes.fromhex(hex_str.decode("ascii"))
                text_chars.append(hex_bytes.decode("latin-1", errors="replace"))
            except Exception:
                pass
            i = j + 1
        else:
            i += 1
    return "".join(text_chars)


class LayersView(PreviewView):
    """PDF view with Type × Method selection for layer-rendering experiments."""

    # Emitted whenever Types, Method, or meta flags change. Commit 2+ will
    # listen and re-render visible pages via the selected method.
    selection_changed = Signal()

    def __init__(self) -> None:
        super().__init__(view_only=True)

        # Layers is a pure render tab: no bbox overlays, no type labels.
        # PreviewView defaults _show_object_labels=True; its paintEvent has an
        # "independent labels pass" that draws type labels regardless of
        # whether bbox outlines are visible, so we must turn both off.
        self._show_object_labels = False
        self._bboxes_visible = False

        # Initialize state from defaults, then override from QSettings.
        self._ui_state = LayersUIState(
            enabled_types=set(default_enabled_types()),
            method=default_method(),
            meta_flags={key: False for key, _ in META_FLAGS},
        )

        # Hide the inherited preview toolbar — Layers uses its own compact header.
        self._toolbar_scroll.setVisible(False)

        # Widget refs populated by header + sidebar builders.
        self._type_checkboxes: dict[LayerType, QCheckBox] = {}
        self._type_count_labels: dict[LayerType, QLabel] = {}
        self._type_toggle_btns: dict[LayerType, QToolButton] = {}
        self._type_child_containers: dict[LayerType, QWidget] = {}
        self._type_child_layouts: dict[LayerType, QVBoxLayout] = {}
        self._expanded_types: set[LayerType] = set()
        self._current_objects: dict[LayerType, list[ObjectRef]] = {}
        # Per-object hide: set of ObjectRef.id strings that are currently
        # unchecked (hidden). Cleared on document change; preserved across
        # page changes (re-applied when the same IDs reappear).
        self._hidden_object_ids: set[str] = set()
        # Preview tooltip cache: obj.id → rich-text HTML with base64 PNG of
        # that object. Built lazily on first hover via eventFilter.
        self._preview_tooltip_cache: dict[str, str] = {}
        # Fast lookup: obj.id → ObjectRef for tooltip event resolution
        self._obj_by_id: dict[str, ObjectRef] = {}
        self._meta_checkboxes: dict[str, QCheckBox] = {}
        self._method_action_group: list[tuple[RenderMethod, QAction]] = []
        self._status_label: QLabel | None = None
        self._method_btn: QToolButton | None = None

        # Render timing — wall-clock from re-render trigger to first visible
        # page completion. Stays as "—" until the first render finishes.
        self._last_render_ms: int | None = None
        self._render_start_wall: float | None = None

        # Build header (top) + restructure body (sidebar on left).
        self._build_layers_header()
        self._restructure_with_sidebar()
        self._restore_state()
        self._apply_method_capabilities()
        self._refresh_buttons_text()
        self._refresh_status()

        # Listen to our own selection signal to drive re-renders.
        self.selection_changed.connect(self._trigger_rerender)
        # Tap the progress signal to freeze render time when "done" arrives.
        self.progress.connect(self._observe_progress_for_timing)
        # Refresh per-type counts when the user navigates to a different page
        # (spread) or the viewport scrolls to new pages.
        self.page_spin.valueChanged.connect(self._refresh_type_counts)
        self.scroll_area.verticalScrollBar().valueChanged.connect(
            self._refresh_type_counts
        )

    def load_document(self, file_path) -> None:
        """Extend PreviewView.load_document to refresh the sidebar counts.

        page_spin.setValue(1) won't fire valueChanged if the spin was already
        at 1 — so we refresh explicitly once the doc is open.
        """
        super().load_document(file_path)
        self._refresh_type_counts()
        # Propagate current bg mode to freshly-built page widgets.
        self._apply_bkg_mode_to_pages()

    # ------------------------------------------------------------------
    # Header bar
    # ------------------------------------------------------------------

    def _build_layers_header(self) -> None:
        header = QWidget()
        h = QHBoxLayout(header)
        h.setContentsMargins(8, 4, 8, 4)
        h.setSpacing(8)

        self._method_btn = self._build_method_dropdown()
        h.addWidget(self._method_btn)

        h.addStretch()

        self._status_label = QLabel("")
        self._status_label.setStyleSheet("color: #666; font-size: 11px;")
        self._status_label.setAlignment(
            Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        )
        self._status_label.setMinimumWidth(360)
        h.addWidget(self._status_label)

        # Insert at the top of the view's vertical layout, above the (hidden)
        # preview toolbar and the pages scroll area.
        self.layout().insertWidget(0, header)

    def _build_method_dropdown(self) -> QToolButton:
        btn = QToolButton()
        btn.setText("Method ▾")
        btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        btn.setToolTip("Rendering method. One is active at a time.")
        menu = QMenu(btn)

        # One QAction per method, mutually exclusive (single-select). All
        # methods are selectable for UI testing; unimplemented ones carry
        # "[not yet implemented]" in the tooltip. Rendering dispatch (commit
        # 2+) is free to fall back to baseline if the selected method isn't
        # wired up yet.
        for info in METHOD_REGISTRY:
            label = info.label if info.implemented else f"{info.label}  · TBD"
            act = QAction(label, menu)
            act.setCheckable(True)
            act.setChecked(info.method == self._ui_state.method)
            tooltip = info.subtitle
            if info.notes:
                tooltip += f"\n\n{info.notes}"
            if not info.implemented:
                tooltip += "\n\n[not yet implemented — will render via baseline fallback]"
            act.setToolTip(tooltip)
            act.triggered.connect(
                lambda _checked=False, m=info.method: self._on_method_selected(m)
            )
            menu.addAction(act)
            self._method_action_group.append((info.method, act))

        btn.setMenu(menu)
        _install_menu_wheel_passthrough(menu, self.scroll_area)
        return btn

    # ------------------------------------------------------------------
    # Left sidebar — grouped types with live object counts
    # ------------------------------------------------------------------

    SIDEBAR_WIDTH = 260

    def _restructure_with_sidebar(self) -> None:
        """Wrap the pages scroll area in a horizontal container with the
        type sidebar on the left. Inherited QVBoxLayout becomes:

            [header]                         (added by _build_layers_header)
            [toolbar_scroll]  (hidden)
            [hbox: sidebar | scroll_area]    (added here)
        """
        main_layout = self.layout()
        # Find scroll_area's slot in the inherited vertical layout.
        idx = None
        for i in range(main_layout.count()):
            if main_layout.itemAt(i).widget() is self.scroll_area:
                idx = i
                break
        if idx is None:
            return  # inherited layout unexpectedly changed

        main_layout.removeWidget(self.scroll_area)

        sidebar = self._build_layers_sidebar()

        container = QWidget()
        h = QHBoxLayout(container)
        h.setContentsMargins(0, 0, 0, 0)
        h.setSpacing(0)
        h.addWidget(sidebar)
        h.addWidget(self.scroll_area, 1)

        main_layout.insertWidget(idx, container, 1)

    def _build_layers_sidebar(self) -> QWidget:
        sidebar = QWidget()
        sidebar.setObjectName("LayersSidebar")
        sidebar.setFixedWidth(self.SIDEBAR_WIDTH)
        sidebar.setStyleSheet(
            """
            #LayersSidebar { background: #F7F7F7; border-right: 1px solid #E0E0E0; }
            #LayersSidebar QLabel#SectionTitle {
                color: #888; font-size: 10px; font-weight: bold;
                padding: 8px 8px 2px 8px;
                text-transform: uppercase; letter-spacing: 0.5px;
            }
            #LayersSidebar QLabel#TypeCount {
                color: #888; font-size: 10px;
                font-family: Consolas, "Courier New", monospace;
            }
            #LayersSidebar QCheckBox { font-size: 11px; }
            #LayersSidebar QScrollArea { background: transparent; border: none; }
            #LayersSidebar QPushButton {
                font-size: 10px; padding: 2px 8px; min-height: 20px;
            }
            """
        )

        outer = QVBoxLayout(sidebar)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        # Background mode selector — 4 toggle-style buttons, one active
        self._bg_mode_buttons: dict[str, QPushButton] = {}
        bg_row = QWidget()
        bgr = QHBoxLayout(bg_row)
        bgr.setContentsMargins(6, 4, 6, 2)
        bgr.setSpacing(2)
        for key, label in BG_MODES:
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setChecked(key == self._ui_state.bkg_mode)
            btn.clicked.connect(
                lambda _checked=False, k=key: self._on_bkg_mode_selected(k)
            )
            bgr.addWidget(btn)
            self._bg_mode_buttons[key] = btn
        outer.addWidget(bg_row)

        # All / None / Invert control row
        ctrl = QWidget()
        cr = QHBoxLayout(ctrl)
        cr.setContentsMargins(6, 2, 6, 4)
        cr.setSpacing(4)
        for text, handler in (
            ("All",    lambda: self._bulk_set_types(True)),
            ("None",   lambda: self._bulk_set_types(False)),
            ("Invert", self._invert_types),
        ):
            b = QPushButton(text)
            b.clicked.connect(handler)
            cr.addWidget(b)
        cr.addStretch()
        outer.addWidget(ctrl)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet("color: #E0E0E0;")
        outer.addWidget(sep)

        # Scrollable list of sections + rows
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        content = QWidget()
        cv = QVBoxLayout(content)
        cv.setContentsMargins(0, 0, 0, 4)
        cv.setSpacing(0)
        cv.setAlignment(Qt.AlignmentFlag.AlignTop)

        for section in LAYER_TYPE_GROUPS:
            cv.addWidget(self._make_section_title(section.title))
            for layer_type in section.types:
                cv.addWidget(self._make_sidebar_row(layer_type))

        cv.addWidget(self._make_section_title("Render flags"))
        for key, label in META_FLAGS:
            cv.addWidget(self._make_sidebar_meta_row(key, label))

        scroll.setWidget(content)
        outer.addWidget(scroll, 1)

        return sidebar

    def _make_section_title(self, text: str) -> QWidget:
        lbl = QLabel(text)
        lbl.setObjectName("SectionTitle")
        return lbl

    def _make_sidebar_row(self, layer_type: LayerType) -> QWidget:
        """One expandable row:

            ▸ [x] Label  .....  N
               obj#1  label
               obj#2  label
               …

        Clicking the ▸/▾ toggle shows/hides the subitem list.
        """
        container = QWidget()
        cv = QVBoxLayout(container)
        cv.setContentsMargins(0, 0, 0, 0)
        cv.setSpacing(0)

        # --- Main row ---
        main = QWidget()
        h = QHBoxLayout(main)
        h.setContentsMargins(2, 0, 10, 0)
        h.setSpacing(2)

        toggle_btn = QToolButton()
        toggle_btn.setText("▸")
        toggle_btn.setFixedSize(16, 18)
        toggle_btn.setStyleSheet(
            "QToolButton { border: none; color: #888; font-size: 9px; padding: 0; }"
            "QToolButton:hover { color: #333; }"
            "QToolButton:disabled { color: #DDD; }"
        )
        toggle_btn.clicked.connect(lambda _=False, t=layer_type: self._toggle_expand(t))
        h.addWidget(toggle_btn)

        cb = QCheckBox(DISPLAY_LABELS[layer_type])
        cb.setChecked(layer_type in self._ui_state.enabled_types)
        cb.toggled.connect(
            lambda checked, t=layer_type: self._on_type_toggled(t, checked)
        )
        h.addWidget(cb)
        h.addStretch()

        count_lbl = QLabel("—")
        count_lbl.setObjectName("TypeCount")
        count_lbl.setMinimumWidth(30)
        count_lbl.setAlignment(
            Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        )
        h.addWidget(count_lbl)

        cv.addWidget(main)

        # --- Child container (subitems) ---
        child = QWidget()
        child.setVisible(False)
        child_lay = QVBoxLayout(child)
        child_lay.setContentsMargins(30, 0, 10, 2)  # indent to align under label
        child_lay.setSpacing(0)
        cv.addWidget(child)

        self._type_checkboxes[layer_type] = cb
        self._type_count_labels[layer_type] = count_lbl
        self._type_toggle_btns[layer_type] = toggle_btn
        self._type_child_containers[layer_type] = child
        self._type_child_layouts[layer_type] = child_lay
        return container

    def _toggle_expand(self, layer_type: LayerType) -> None:
        container = self._type_child_containers.get(layer_type)
        btn = self._type_toggle_btns.get(layer_type)
        if container is None or btn is None:
            return
        expand = not container.isVisible()
        container.setVisible(expand)
        btn.setText("▾" if expand else "▸")
        if expand:
            self._expanded_types.add(layer_type)
        else:
            self._expanded_types.discard(layer_type)

    def _on_subitem_toggled(self, obj_id: str, visible: bool) -> None:
        """Handle a per-object checkbox click: add/remove obj_id from the
        hidden set and trigger a re-render at the current layer key."""
        if visible:
            self._hidden_object_ids.discard(obj_id)
        else:
            self._hidden_object_ids.add(obj_id)
        self.progress.emit(f"object {obj_id} {'ON' if visible else 'OFF'}")
        self.selection_changed.emit()

    # ------------------------------------------------------------------
    # Per-subitem hover preview tooltip
    # ------------------------------------------------------------------

    PREVIEW_MAX_DIM = 220  # pixels — tooltip preview max size

    def eventFilter(self, obj, event) -> bool:
        """Intercept hover events on subitem labels:

        - ToolTip → lazily build + show an HTML tooltip with a PNG preview
          (cached per obj.id)
        - Enter   → draw a yellow bbox overlay around the object on the page
        - Leave   → clear the overlay
        """
        etype = event.type()
        if etype == QEvent.Type.ToolTip:
            obj_id = obj.property("layer_obj_id")
            if isinstance(obj_id, str) and obj_id:
                html = self._preview_tooltip_cache.get(obj_id)
                if html is None:
                    ref = self._obj_by_id.get(obj_id)
                    if ref is not None:
                        html = self._build_preview_tooltip(ref)
                        if html:
                            self._preview_tooltip_cache[obj_id] = html
                if html:
                    QToolTip.showText(event.globalPos(), html, obj)
                    return True
                ref = self._obj_by_id.get(obj_id)
                if ref is not None:
                    QToolTip.showText(
                        event.globalPos(), self._format_bbox_tooltip(ref), obj
                    )
                    return True
        elif etype == QEvent.Type.Enter:
            obj_id = obj.property("layer_obj_id")
            if isinstance(obj_id, str) and obj_id:
                ref = self._obj_by_id.get(obj_id)
                if ref is not None:
                    self._show_page_highlight(ref)
        elif etype == QEvent.Type.Leave:
            obj_id = obj.property("layer_obj_id")
            if isinstance(obj_id, str) and obj_id:
                self._clear_page_highlight()
        return super().eventFilter(obj, event)

    def _show_page_highlight(self, ref: ObjectRef) -> None:
        """Set the yellow pulsing overlay on the PageWidget(s) that render
        `ref.page_idx` and clear it on every other page widget."""
        if ref.bbox is None:
            self._clear_page_highlight()
            return
        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                pi = getattr(pw, "_page_index", -1)
                if pi == ref.page_idx:
                    pw.set_layers_highlight(ref.bbox)
                elif getattr(pw, "_layers_highlight_bbox", None) is not None:
                    pw.set_layers_highlight(None)

    def _clear_page_highlight(self) -> None:
        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                if getattr(pw, "_layers_highlight_bbox", None) is not None:
                    pw.set_layers_highlight(None)

    def _build_preview_tooltip(self, obj: ObjectRef) -> str | None:
        """Render a small PNG preview of `obj`, return HTML tooltip markup.
        Returns None if rendering isn't feasible (no bbox, no extract path)."""
        pm = self._render_object_preview(obj)
        if pm is None or pm.isNull():
            return None

        # PNG → base64 data URI
        buffer = QBuffer()
        buffer.open(QIODevice.OpenModeFlag.WriteOnly)
        if not pm.save(buffer, "PNG"):
            return None
        b64 = bytes(buffer.data().toBase64()).decode("ascii")
        buffer.close()

        bbox_line = ""
        if obj.bbox is not None:
            x0, y0, x1, y1 = obj.bbox
            bbox_line = (
                f"<div style='color:#888;font-size:10px;'>"
                f"bbox: ({x0:.0f}, {y0:.0f}) → ({x1:.0f}, {y1:.0f})"
                f"  |  {(x1 - x0):.0f}×{(y1 - y0):.0f} pt"
                f"</div>"
            )

        return (
            f"<div>"
            f"<div style='font-family:Consolas,monospace;font-size:11px;color:#444;'>"
            f"{obj.id}</div>"
            f"<div style='font-family:Consolas,monospace;font-size:11px;color:#666;"
            f"margin-bottom:4px;'>{obj.label}</div>"
            f"<img src='data:image/png;base64,{b64}'/>"
            f"{bbox_line}"
            f"</div>"
        )

    def _render_object_preview(self, obj: ObjectRef) -> QPixmap | None:
        """Best-effort render of a single object into a small QPixmap.

        Strategy:
            IMAGE_XOBJECT — doc.extract_image(xref) returns original bytes
            anything with bbox — page.get_pixmap(clip=bbox) crops just that
                                 rectangle at 2x zoom (fast, no full render)
            otherwise (e.g. shading without bbox) — None
        """
        if self._doc is None:
            return None

        try:
            import fitz
        except ImportError:
            return None

        pm: QPixmap | None = None

        # --- Images: extract original encoded bytes ---
        if obj.id.startswith("img#"):
            try:
                xref_str = obj.id[len("img#"):]
                xref = int(xref_str)
                info = self._doc.extract_image(xref)
                if info and "image" in info:
                    img = QImage.fromData(info["image"])
                    if not img.isNull():
                        pm = QPixmap.fromImage(img)
            except Exception:
                pm = None

        # --- Anything with bbox: clipped page render ---
        if pm is None and obj.bbox is not None:
            try:
                page = self._doc[obj.page_idx]
                zoom = 2.0
                mat = fitz.Matrix(zoom, zoom)
                clip = fitz.Rect(*obj.bbox)
                if clip.is_empty or clip.is_infinite:
                    return None
                pix = page.get_pixmap(matrix=mat, clip=clip, alpha=False)
                img = QImage(
                    pix.samples, pix.width, pix.height, pix.stride,
                    QImage.Format.Format_RGB888,
                ).copy()
                if not img.isNull():
                    pm = QPixmap.fromImage(img)
            except Exception:
                pm = None

        if pm is None or pm.isNull():
            return None

        # Scale down for tooltip if needed
        if pm.width() > self.PREVIEW_MAX_DIM or pm.height() > self.PREVIEW_MAX_DIM:
            pm = pm.scaled(
                self.PREVIEW_MAX_DIM, self.PREVIEW_MAX_DIM,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )
        return pm

    def _build_hidden_ops_spec(self):
        """Build HiddenOpSet from hidden_object_ids using the cached object
        lists from the last _refresh_type_counts call.
        """
        from app.ui.preview_view import HiddenOpSet

        if not self._hidden_object_ids or not self._current_objects:
            return HiddenOpSet()

        xobj_names: set[str] = set()
        text_idx: set[int] = set()
        stroke_idx: set[int] = set()
        fill_idx: set[int] = set()
        stroke_fill_idx: set[int] = set()
        shading_idx: set[int] = set()
        annot_idx: set[int] = set()

        for layer_type, objs in self._current_objects.items():
            for obj in objs:
                if obj.id not in self._hidden_object_ids:
                    continue
                if layer_type in (LayerType.IMAGE_XOBJECT, LayerType.FORM_XOBJECT):
                    if obj.xobject_name:
                        xobj_names.add(obj.xobject_name)
                elif obj.op_index is None:
                    continue
                elif layer_type == LayerType.TEXT:
                    text_idx.add(obj.op_index)
                elif layer_type == LayerType.PATH_STROKE:
                    stroke_idx.add(obj.op_index)
                elif layer_type == LayerType.PATH_FILL:
                    fill_idx.add(obj.op_index)
                elif layer_type == LayerType.PATH_STROKE_FILL:
                    stroke_fill_idx.add(obj.op_index)
                elif layer_type == LayerType.SHADING:
                    shading_idx.add(obj.op_index)
                elif layer_type in self._ANNOTATIONS_BUCKET:
                    annot_idx.add(obj.op_index)

        return HiddenOpSet(
            xobject_names=frozenset(xobj_names),
            text_indices=frozenset(text_idx),
            stroke_indices=frozenset(stroke_idx),
            fill_indices=frozenset(fill_idx),
            stroke_fill_indices=frozenset(stroke_fill_idx),
            shading_indices=frozenset(shading_idx),
            annotation_indices=frozenset(annot_idx),
        )

    def _make_sidebar_meta_row(self, key: str, label: str) -> QWidget:
        row = QWidget()
        h = QHBoxLayout(row)
        h.setContentsMargins(10, 0, 10, 0)
        h.setSpacing(4)

        cb = QCheckBox(label)
        cb.setChecked(self._ui_state.meta_flags.get(key, False))
        cb.toggled.connect(lambda checked, k=key: self._on_meta_toggled(k, checked))
        h.addWidget(cb)
        h.addStretch()

        self._meta_checkboxes[key] = cb
        return row

    # ------------------------------------------------------------------
    # State changes
    # ------------------------------------------------------------------

    def _on_type_toggled(self, layer_type: LayerType, checked: bool) -> None:
        if checked:
            self._ui_state.enabled_types.add(layer_type)
        else:
            self._ui_state.enabled_types.discard(layer_type)
        self._save_state()
        self._refresh_buttons_text()
        self._refresh_status()
        self.progress.emit(f"layers: {layer_type.value} {'ON' if checked else 'OFF'}")
        self.selection_changed.emit()

    def _on_meta_toggled(self, key: str, checked: bool) -> None:
        self._ui_state.meta_flags[key] = checked
        self._save_state()
        self._refresh_status()
        self.progress.emit(f"layers flag: {key} {'ON' if checked else 'OFF'}")
        self.selection_changed.emit()

    # ------------------------------------------------------------------
    # Background mode
    # ------------------------------------------------------------------

    def _on_bkg_mode_selected(self, mode: str) -> None:
        if mode == self._ui_state.bkg_mode:
            # Keep the button checked (user clicked active one) — re-sync
            self._sync_bkg_buttons()
            return
        self._ui_state.bkg_mode = mode
        self._sync_bkg_buttons()
        self._apply_bkg_mode_to_pages()
        self._save_state()
        self.progress.emit(f"layers bg: {mode}")
        # Re-render so _transform_pixmap can knock out white for non-white bg.
        self.selection_changed.emit()

    def _sync_bkg_buttons(self) -> None:
        for key, btn in self._bg_mode_buttons.items():
            active = key == self._ui_state.bkg_mode
            if btn.isChecked() != active:
                btn.blockSignals(True)
                btn.setChecked(active)
                btn.blockSignals(False)

    def _apply_bkg_mode_to_pages(self) -> None:
        """Push the current bg mode onto every PageWidget so paintEvent draws
        the right colour / pattern under the (transparent) pixmap."""
        mode = self._ui_state.bkg_mode
        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                pw._layers_bg_mode = mode
                pw.update()

    # Override PreviewView hook: rewrite the rendered pixmap according to
    # the chosen background mode. For "black" we directly paint white
    # pixels as black (opaque, reliable). For "transparent"/"checker" we
    # use alpha-knockout so the PageWidget's paintEvent background shows
    # through the transparent areas.
    def _transform_pixmap(self, qpix: QPixmap) -> QPixmap:
        mode = self._ui_state.bkg_mode
        if mode == "white":
            return qpix
        if qpix.isNull():
            return qpix
        try:
            import numpy as np
        except ImportError:
            return qpix
        img = qpix.toImage().convertToFormat(QImage.Format.Format_RGBA8888)
        w, h = img.width(), img.height()
        if w <= 0 or h <= 0:
            return qpix
        try:
            ptr = img.bits()
            ptr.setsize(img.sizeInBytes())
            arr = np.frombuffer(ptr, dtype=np.uint8).reshape((h, w, 4)).copy()
        except Exception:
            return qpix
        # Near-white mask (threshold ≥245 covers anti-aliasing fringes).
        r, g, b = arr[:, :, 0], arr[:, :, 1], arr[:, :, 2]
        near_white = (r >= 245) & (g >= 245) & (b >= 245)

        if mode == "black":
            # Direct pixel replacement — no alpha juggling. Always works.
            arr[near_white, 0] = 0
            arr[near_white, 1] = 0
            arr[near_white, 2] = 0
            arr[:, :, 3] = 255
        else:
            # transparent / checker: alpha-knockout so bg shows through
            arr[:, :, 3] = np.where(near_white, 0, 255).astype(np.uint8)

        buf = arr.tobytes()
        out = QImage(buf, w, h, w * 4, QImage.Format.Format_RGBA8888)
        return QPixmap.fromImage(out.copy())

    def _on_method_selected(self, method: RenderMethod) -> None:
        if method == self._ui_state.method:
            return
        self._ui_state.method = method
        # Update radio-like checkmarks across the menu actions.
        for m, act in self._method_action_group:
            act.setChecked(m == method)
        self._apply_method_capabilities()
        self._save_state()
        self._refresh_buttons_text()
        self._refresh_status()
        self.selection_changed.emit()
        logger.info("Layers: method set to %s", method.value)

    def _apply_method_capabilities(self) -> None:
        """Grey-out type checkboxes the active method can't filter (option A)."""
        method = self._ui_state.method
        for layer_type, cb in self._type_checkboxes.items():
            supported = method_supports_type(method, layer_type)
            cb.setEnabled(supported)
            # When disabled, also clear tooltip noise — keep the type label
            # readable so user can see why a row is greyed.
            if supported:
                cb.setToolTip("")
            else:
                cb.setToolTip(
                    f"{DISPLAY_LABELS[layer_type]} — not supported by "
                    f"{method_info(method).label}"
                )

    def _bulk_set_types(self, value: bool) -> None:
        changed = False
        for layer_type, cb in self._type_checkboxes.items():
            if not cb.isEnabled():
                continue
            if cb.isChecked() != value:
                cb.blockSignals(True)
                cb.setChecked(value)
                cb.blockSignals(False)
                if value:
                    self._ui_state.enabled_types.add(layer_type)
                else:
                    self._ui_state.enabled_types.discard(layer_type)
                changed = True
        if changed:
            self._save_state()
            self._refresh_buttons_text()
            self._refresh_status()
            self.selection_changed.emit()

    def _invert_types(self) -> None:
        changed = False
        for layer_type, cb in self._type_checkboxes.items():
            if not cb.isEnabled():
                continue
            new = not cb.isChecked()
            cb.blockSignals(True)
            cb.setChecked(new)
            cb.blockSignals(False)
            if new:
                self._ui_state.enabled_types.add(layer_type)
            else:
                self._ui_state.enabled_types.discard(layer_type)
            changed = True
        if changed:
            self._save_state()
            self._refresh_buttons_text()
            self._refresh_status()
            self.selection_changed.emit()

    # ------------------------------------------------------------------
    # Status / button labels
    # ------------------------------------------------------------------

    def _refresh_buttons_text(self) -> None:
        if self._method_btn is not None:
            self._method_btn.setText(f"Method ▾ · {method_info(self._ui_state.method).label}")

    def _refresh_status(self) -> None:
        """Status line on the right of the header.

        Shows: method · types(N): a,b,c · render: Xms · flags: …
        `render:` is "running…" while a re-render is in flight, the last
        measured time otherwise, or "—" before the first render.
        """
        if self._status_label is None:
            return
        active_type_names = [
            t.value for t, cb in self._type_checkboxes.items()
            if cb.isEnabled() and cb.isChecked()
        ]
        count = len(active_type_names)
        MAX_SHOW = 4
        shown = active_type_names[:MAX_SHOW]
        suffix = f" +{count - MAX_SHOW}" if count > MAX_SHOW else ""
        types_str = (", ".join(shown) + suffix) if shown else "none"

        if self._render_start_wall is not None:
            render_str = "running…"
        elif self._last_render_ms is not None:
            render_str = f"{self._last_render_ms}ms"
        else:
            render_str = "—"

        flags_on = [key for key, val in self._ui_state.meta_flags.items() if val]
        flags_str = f" · flags: {', '.join(flags_on)}" if flags_on else ""
        self._status_label.setText(
            f"method: {self._ui_state.method.value} · types({count}): {types_str} "
            f"· render: {render_str}{flags_str}"
        )
        self._status_label.setToolTip(
            f"Active method: {method_info(self._ui_state.method).label}\n"
            f"Enabled types ({count}):\n  " + ("\n  ".join(active_type_names) or "(none)")
        )

    # ------------------------------------------------------------------
    # Per-type object counts — shown next to each checkbox in the sidebar
    # ------------------------------------------------------------------

    def _current_spread_pages(self) -> list[int]:
        """Return 0-based page indices for the spread containing page_spin.

        Uses `_build_spread_pairs` (inherited) so it matches whatever the
        PreviewView is currently showing — one page, 2-page spread, or a
        cover-isolated page.
        """
        if not self._doc:
            return []
        current = self.page_spin.value() - 1
        if current < 0:
            return []
        for left, right in self._build_spread_pairs():
            if current in (left, right):
                return [p for p in (left, right) if p >= 0]
        return [current] if 0 <= current < self._page_count else []

    SUBITEM_MAX = 60  # cap per-type sub-list to keep the sidebar navigable

    def _refresh_type_counts(self, *_args) -> None:
        """Recompute per-type object lists for the current spread and rebuild
        both the numeric count and the subitem rows under each type."""
        pages = self._current_spread_pages()
        objects = self._collect_type_objects(pages)
        self._current_objects = objects
        # Invalidate preview cache — spread changed, stale previews may point
        # to now-missing objects on the current spread.
        self._preview_tooltip_cache.clear()
        # Rebuild fast obj.id → ObjectRef lookup for tooltip events.
        self._obj_by_id = {o.id: o for objs in objects.values() for o in objs}

        for layer_type, lbl in self._type_count_labels.items():
            objs = objects.get(layer_type, [])
            n = len(objs)
            lbl.setText(str(n) if n > 0 else "—")
            lbl.setStyleSheet(
                "color: #444; font-size: 10px; font-family: Consolas, monospace;"
                if n > 0
                else "color: #BBB; font-size: 10px; font-family: Consolas, monospace;"
            )

            # Expand toggle is disabled when there's nothing to expand.
            btn = self._type_toggle_btns.get(layer_type)
            if btn is not None:
                btn.setEnabled(n > 0)
                if n == 0 and layer_type in self._expanded_types:
                    # Auto-collapse if previously expanded but now empty.
                    self._expanded_types.discard(layer_type)
                    container = self._type_child_containers.get(layer_type)
                    if container is not None:
                        container.setVisible(False)
                    btn.setText("▸")

            # Rebuild subitems
            child_lay = self._type_child_layouts.get(layer_type)
            if child_lay is not None:
                self._rebuild_subitems(child_lay, objs)

    def _rebuild_subitems(self, layout: QVBoxLayout, objs: list[ObjectRef]) -> None:
        """Drop + repopulate the subitem widgets under one type row.

        Each subitem: [checkbox] id  label — click the checkbox to hide that
        specific object on the current render.
        """
        while layout.count():
            item = layout.takeAt(0)
            w = item.widget()
            if w is not None:
                w.deleteLater()

        shown = objs[: self.SUBITEM_MAX]
        for obj in shown:
            row = QWidget()
            h = QHBoxLayout(row)
            h.setContentsMargins(0, 0, 0, 0)
            h.setSpacing(4)

            cb = QCheckBox()
            cb.setChecked(obj.id not in self._hidden_object_ids)
            cb.setFixedWidth(18)
            cb.setToolTip("Show / hide this specific object on the render")
            cb.toggled.connect(
                lambda checked, oid=obj.id: self._on_subitem_toggled(oid, checked)
            )
            h.addWidget(cb)

            lbl = QLabel(f"{obj.id}  {obj.label}")
            lbl.setStyleSheet(
                "color: #666; font-size: 10px; padding: 1px 0;"
                " font-family: Consolas, 'Courier New', monospace;"
            )
            # Placeholder tooltip so the ToolTip event fires on hover; the
            # event filter hijacks it and shows the rendered preview lazily.
            lbl.setToolTip(" ")
            lbl.setProperty("layer_obj_id", obj.id)
            lbl.installEventFilter(self)
            h.addWidget(lbl)
            h.addStretch()

            layout.addWidget(row)

        if len(objs) > self.SUBITEM_MAX:
            more = QLabel(f"… +{len(objs) - self.SUBITEM_MAX} more")
            more.setStyleSheet(
                "color: #999; font-size: 10px; font-style: italic; padding: 1px 0 1px 24px;"
            )
            layout.addWidget(more)

    def _format_bbox_tooltip(self, obj: ObjectRef) -> str:
        lines = [obj.id, obj.label, f"page: {obj.page_idx + 1}"]
        if obj.bbox is not None:
            x0, y0, x1, y1 = obj.bbox
            lines.append(f"bbox: ({x0:.1f}, {y0:.1f}) → ({x1:.1f}, {y1:.1f})")
            lines.append(f"size: {(x1 - x0):.1f} × {(y1 - y0):.1f} pt")
        return "\n".join(lines)

    def _collect_type_objects(
        self, page_indices: list[int]
    ) -> dict[LayerType, list[ObjectRef]]:
        out: dict[LayerType, list[ObjectRef]] = {t: [] for t in LayerType}
        if not self._doc or not page_indices:
            return out
        for pi in page_indices:
            if pi < 0 or pi >= len(self._doc):
                continue
            try:
                page = self._doc[pi]
            except Exception:
                continue
            self._collect_page_objects(page, pi, out)
        return out

    def _collect_page_objects(
        self, page, page_idx: int, out: dict[LayerType, list[ObjectRef]]
    ) -> None:
        """Gather ObjectRef entries for one page into `out`.

        Positional op_index semantics (0-based):
          TEXT                     — Nth text show op across the stream
          PATH_STROKE / PATH_FILL /
          PATH_STROKE_FILL         — Nth paint op of that class
          ANNOTATION_*             — global index in page.annots()
          IMAGE_XOBJECT /
          FORM_XOBJECT             — unused (filter matches on xobject_name)
        """
        # ----- Text spans — use texttrace for STREAM ORDER + readable text.
        # get_texttrace() walks the content stream (including form-xobject
        # expansions) in drawing order and resolves font CMaps to Unicode.
        # Index in the returned list matches the filter's global text counter.
        try:
            trace = page.get_texttrace()
            for i, entry in enumerate(trace or []):
                chars = entry.get("chars") or []
                # chars entries: (ord, origin, bbox) — extract code points
                text = "".join(
                    chr(c[0]) if isinstance(c, (tuple, list)) and c and isinstance(c[0], int)
                    else ""
                    for c in chars
                )
                display = text if len(text) <= 28 else text[:25] + "…"
                bbox = _maybe_bbox(entry.get("bbox"))
                out[LayerType.TEXT].append(ObjectRef(
                    id=f"text#{page_idx + 1}:{i + 1}",
                    label=f'"{display}"' if display else "(empty)",
                    bbox=bbox,
                    page_idx=page_idx,
                    op_index=i,
                ))
        except Exception:
            pass

        # ----- IMAGE_XOBJECT (unique image refs — matches header 'Images') -----
        try:
            images = page.get_images(full=True)
            for i, img in enumerate(images):
                xref = img[0] if len(img) > 0 else 0
                w = img[2] if len(img) > 2 else 0
                h = img[3] if len(img) > 3 else 0
                name = img[7] if len(img) > 7 else f"Im{i + 1}"
                out[LayerType.IMAGE_XOBJECT].append(ObjectRef(
                    id=f"img#{xref}",
                    label=f"/{name} {w}×{h}",
                    bbox=None,
                    page_idx=page_idx,
                    xobject_name=str(name),
                ))
        except Exception:
            pass

        # ----- FORM_XOBJECT (page.get_xobjects returns Form XObjects only).
        # NOTE: bbox intentionally omitted — get_xobjects gives the form's
        # internal /BBox, not the placed-on-page rect. Computing placed bbox
        # requires CTM tracking through content streams (heavy).
        # The same /FmN name can refer to DIFFERENT form xobjects in
        # different /Resources dicts (page vs nested forms), so we annotate
        # each row with its parent context to disambiguate.
        try:
            doc = page.parent
            # Build reverse map: child_xref → [(parent_label, local_name)]
            # so each form xobject can be shown with where it's referenced.
            parents: dict[int, list[tuple[str, str]]] = {}
            for local_name, target in _parse_xobject_resources(
                doc, getattr(page, "xref", 0)
            ).items():
                parents.setdefault(target, []).append(("page", local_name))
            for xobj_tuple in page.get_xobjects():
                parent_xref = xobj_tuple[0] if len(xobj_tuple) > 0 else 0
                if not parent_xref:
                    continue
                for local_name, target in _parse_xobject_resources(
                    doc, parent_xref
                ).items():
                    parents.setdefault(target, []).append(
                        (f"form#{parent_xref}", local_name)
                    )

            for i, xobj in enumerate(page.get_xobjects()):
                xref = xobj[0] if len(xobj) > 0 else 0
                name = xobj[1] if len(xobj) > 1 else f"Fm{i + 1}"
                # Prefer the first parent context that matches this xref
                parent_hint = ""
                for parent_label, parent_local_name in parents.get(xref, []):
                    # Use the parent that used this exact local name
                    if parent_local_name == name:
                        parent_hint = f" via {parent_label}"
                        break
                out[LayerType.FORM_XOBJECT].append(ObjectRef(
                    id=f"form#{xref}",
                    label=f"/{name}{parent_hint}",
                    bbox=None,
                    page_idx=page_idx,
                    xobject_name=str(name),
                ))
        except Exception:
            pass

        # ----- Drawings: per-class counters so op_index matches filter.
        # IMPORTANT: op_index must increment for EVERY drawing in the stream
        # (even invisible ones) because the filter sees every paint op in
        # sequence. We still SKIP adding invisible strokes/fills to the
        # sidebar so users don't see phantom entries that correspond to
        # nothing on the render.
        try:
            stroke_i = fill_i = sf_i = clip_i = 0
            for drw in page.get_drawings():
                t = drw.get("type", "")
                bbox = _maybe_bbox(drw.get("rect"))
                label = _size_label(bbox)
                visible = True
                if t == "s":
                    layer = LayerType.PATH_STROKE
                    i = stroke_i
                    stroke_i += 1
                    visible = _is_stroke_visible(drw)
                elif t == "f":
                    layer = LayerType.PATH_FILL
                    i = fill_i
                    fill_i += 1
                    visible = _is_fill_visible(drw)
                elif t in ("fs", "sf"):
                    layer = LayerType.PATH_STROKE_FILL
                    i = sf_i
                    sf_i += 1
                    # Combined fill+stroke with no real stroke width is
                    # effectively a fill — but we still classify it fs
                    # because the content-stream operator is B/b/etc.
                    visible = _is_fill_visible(drw)
                elif t == "c":
                    layer = LayerType.CLIP_PATH
                    i = clip_i
                    clip_i += 1
                else:
                    continue
                if not visible:
                    continue  # phantom — counter already advanced
                out[layer].append(ObjectRef(
                    id=f"{layer.value}#{page_idx + 1}:{i + 1}",
                    label=label,
                    bbox=bbox,
                    page_idx=page_idx,
                    op_index=i,
                ))
        except Exception:
            pass

        # ----- Tables -----
        try:
            found = page.find_tables()
            try:
                tabs = list(found.tables)
            except AttributeError:
                tabs = list(found)
            for i, tbl in enumerate(tabs):
                bbox = _maybe_bbox(getattr(tbl, "bbox", None))
                rows = cols = 0
                cells = getattr(tbl, "cells", None)
                if cells:
                    try:
                        rows = len(cells)
                        cols = len(cells[0]) if cells[0] is not None else 0
                    except Exception:
                        pass
                label = f"{rows}×{cols}" if rows or cols else _size_label(bbox)
                out[LayerType.TABLE].append(ObjectRef(
                    id=f"table#{page_idx + 1}:{i + 1}",
                    label=label,
                    bbox=bbox,
                    page_idx=page_idx,
                    op_index=i,
                ))
        except Exception:
            pass

        # ----- Shading: scan page + form-xobject content streams for `sh` -----
        # PyMuPDF doesn't surface shading operators via a high-level API, so
        # we parse raw content streams. Catalog PDFs typically wrap content
        # in a form XObject — our filter also scans that stream, so sh ops
        # inside it are the ones we want to count and optionally hide.
        try:
            from app.ui.preview_view import (
                _parse_content_commands,
                _SHADING_OPERATOR,
            )
            doc = page.parent
            sh_running = 0

            def _scan_for_sh(raw_stream: bytes, stream_tag: str):
                nonlocal sh_running
                local_i = 0
                for raw, op in _parse_content_commands(raw_stream):
                    if op == _SHADING_OPERATOR:
                        name = raw.decode("latin-1", errors="replace")
                        name = name.strip().rstrip(" sh").strip()
                        if name.startswith("/"):
                            name = name[1:]
                        out[LayerType.SHADING].append(ObjectRef(
                            id=f"shading#{page_idx + 1}:{sh_running + 1}",
                            label=f"/{name or 'sh'} ({stream_tag})",
                            bbox=None,
                            page_idx=page_idx,
                            op_index=local_i,
                        ))
                        local_i += 1
                        sh_running += 1

            # Page-level content streams
            try:
                for xref in page.get_contents() or []:
                    stream = doc.xref_stream(xref) or b""
                    if stream:
                        _scan_for_sh(stream, "page")
            except Exception:
                pass

            # Form XObject streams referenced by this page.
            # IMPORTANT: iterate in ascending xref order — that matches the
            # order get_pixmap_filtered walks through tmp_doc's xrefs, so the
            # global shading counter stays in sync between collection and
            # filter (critical for per-object hide to target the right sh op).
            try:
                form_xrefs = sorted({
                    xobj[0] for xobj in page.get_xobjects()
                    if xobj and xobj[0]
                })
                for form_xref in form_xrefs:
                    try:
                        stream = doc.xref_stream(form_xref) or b""
                    except Exception:
                        continue
                    if stream:
                        _scan_for_sh(stream, f"form#{form_xref}")
            except Exception:
                pass
        except Exception:
            pass

        # ----- Annotations (+ Form widgets) — op_index = global annot idx -----
        try:
            annots_iter = page.annots()
            if annots_iter is not None:
                for i, annot in enumerate(annots_iter):
                    if annot is None:
                        continue
                    try:
                        atype = annot.type
                        subtype = (
                            str(atype[1])
                            if isinstance(atype, (tuple, list)) and len(atype) >= 2
                            else str(atype)
                        )
                    except Exception:
                        continue
                    bbox = _maybe_bbox(getattr(annot, "rect", None))
                    if subtype in ("Text", "FreeText"):
                        layer = LayerType.ANNOTATION_TEXT
                    elif subtype == "Link":
                        layer = LayerType.ANNOTATION_LINK
                    elif subtype in ("Highlight", "Underline", "Squiggly", "StrikeOut"):
                        layer = LayerType.ANNOTATION_HIGHLIGHT
                    elif subtype in ("Line", "Square", "Circle", "Polygon", "PolyLine", "Ink"):
                        layer = LayerType.ANNOTATION_SHAPE
                    elif subtype in ("Stamp", "Watermark"):
                        layer = LayerType.ANNOTATION_STAMP
                    elif subtype in ("FileAttachment", "Sound", "Movie"):
                        layer = LayerType.ANNOTATION_FILE
                    elif subtype == "Widget":
                        layer = LayerType.FORM_WIDGET
                    else:
                        continue
                    out[layer].append(ObjectRef(
                        id=f"{layer.value}#{page_idx + 1}:{i + 1}",
                        label=f"[{subtype}] {_size_label(bbox)}",
                        bbox=bbox,
                        page_idx=page_idx,
                        op_index=i,
                    ))
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Rendering hook-up — commit 2 (MUPDF_FILTER_STREAM only)
    # ------------------------------------------------------------------

    # Mapping: LayerType → coarse bucket that get_pixmap_filtered understands.
    # Text and images are still bucketed because MuPDF's filter can't split
    # inline vs XObject images at operator level meaningfully. Paths are now
    # granular — each of stroke / fill / stroke+fill is its own layer_key
    # entry driving the new show_path_* flags in _filter_content_stream.
    _TEXT_BUCKET: frozenset[LayerType] = frozenset({LayerType.TEXT})
    _IMAGES_BUCKET: frozenset[LayerType] = frozenset({
        LayerType.IMAGE_INLINE,
        LayerType.IMAGE_XOBJECT,
    })
    # Annotations collapse into one rail because MuPDF's annots= flag is
    # all-or-nothing. Form widgets are a kind of annotation too.
    _ANNOTATIONS_BUCKET: frozenset[LayerType] = frozenset({
        LayerType.ANNOTATION_TEXT,
        LayerType.ANNOTATION_LINK,
        LayerType.ANNOTATION_HIGHLIGHT,
        LayerType.ANNOTATION_SHAPE,
        LayerType.ANNOTATION_STAMP,
        LayerType.ANNOTATION_FILE,
        LayerType.FORM_WIDGET,
    })

    def _effective_layer_key(self) -> frozenset:
        """Override of PreviewView: single source of truth is LayersUIState.

        Returns the frozenset of hidden-bucket keys consumed by
        `_render_page_sync`. An empty frozenset means "render everything".

        Only MUPDF_FILTER_STREAM actually uses this filter path in commit 2.
        Other methods fall back to an empty key (= full render) until their
        implementations land in commits 3-5.

        Layer key vocabulary for this method:
            "text" "images"                       — coarse content classes
            "path_stroke" "path_fill"
            "path_stroke_fill"                    — granular paint classes
            "shading"                             — sh operator
            "annotations"                         — MuPDF's annots=False
        """
        if self._ui_state.method != RenderMethod.MUPDF_FILTER_STREAM:
            return frozenset()

        enabled = self._ui_state.enabled_types
        hidden: set[str] = set()
        if not (self._TEXT_BUCKET & enabled):
            hidden.add("text")
        if not (self._IMAGES_BUCKET & enabled):
            hidden.add("images")
        # Granular path filtering — per paint class. For MUPDF_FILTER_STREAM
        # the combined ops (B/B*/b/b*) are decomposed at the content-stream
        # level into stroke+fill halves based on these two toggles, so
        # PATH_STROKE_FILL is not a separate control here (and is NOT in
        # _MUPDF_FILTER_SUPPORTED — its checkbox is greyed out).
        if LayerType.PATH_STROKE not in enabled:
            hidden.add("path_stroke")
        if LayerType.PATH_FILL not in enabled:
            hidden.add("path_fill")
        # Shading (sh operator).
        if LayerType.SHADING not in enabled:
            hidden.add("shading")
        # Annotations collapse — any one enabled shows them all.
        if not (self._ANNOTATIONS_BUCKET & enabled):
            hidden.add("annotations")
        return frozenset(hidden)

    def _trigger_rerender(self) -> None:
        """Invalidate pixmap cache and re-render visible pages.

        Wired to `selection_changed`, so any Type / Method / flag change
        kicks off a fresh render at the active layer key.
        """
        import time
        if not self._doc or not self._file_path:
            return
        self._render_start_wall = time.perf_counter()
        self._last_render_ms = None
        self._refresh_status()  # flip to "running…" immediately
        # Translate per-object hide selections into a HiddenOpSet consumed
        # by _render_page_sync / PageRenderWorker via this instance attr.
        self._hidden_ops_for_page = self._build_hidden_ops_spec()
        self._pixmap_cache.clear()
        # Reuse the inherited refresh path — it schedules the background
        # worker and updates visible pages at the current zoom.
        self._refresh_visible_pages()

    def _observe_progress_for_timing(self, msg: str) -> None:
        """Freeze render time on the first 'done' message after a trigger."""
        if self._render_start_wall is None:
            return
        if not msg:
            return
        low = msg.lower()
        if "done" not in low:
            return
        import time
        elapsed = time.perf_counter() - self._render_start_wall
        self._last_render_ms = int(round(elapsed * 1000))
        self._render_start_wall = None
        self._refresh_status()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _qs(self) -> QSettings:
        return QSettings("CatalogAPIStudio", "CatalogAPIStudio")

    def _save_state(self) -> None:
        s = self._qs()
        s.setValue(f"{_SETTINGS_PREFIX}/method", self._ui_state.method.value)
        enabled = sorted(t.value for t in self._ui_state.enabled_types)
        s.setValue(f"{_SETTINGS_PREFIX}/types", ",".join(enabled))
        for key, val in self._ui_state.meta_flags.items():
            s.setValue(f"{_SETTINGS_PREFIX}/flag/{key}", bool(val))
        s.setValue(f"{_SETTINGS_PREFIX}/bkg_mode", self._ui_state.bkg_mode)
        s.sync()

    def _restore_state(self) -> None:
        s = self._qs()

        # Method
        saved_method = s.value(f"{_SETTINGS_PREFIX}/method")
        if isinstance(saved_method, str):
            try:
                self._ui_state.method = RenderMethod(saved_method)
                for m, act in self._method_action_group:
                    act.setChecked(m == self._ui_state.method)
            except ValueError:
                pass  # stale key; keep default

        # Types
        saved_types = s.value(f"{_SETTINGS_PREFIX}/types")
        if isinstance(saved_types, str) and saved_types:
            restored: set[LayerType] = set()
            for token in saved_types.split(","):
                token = token.strip()
                if not token:
                    continue
                try:
                    restored.add(LayerType(token))
                except ValueError:
                    continue
            self._ui_state.enabled_types = restored
            for t, cb in self._type_checkboxes.items():
                cb.blockSignals(True)
                cb.setChecked(t in restored)
                cb.blockSignals(False)

        # Meta flags
        for key, _ in META_FLAGS:
            saved = s.value(f"{_SETTINGS_PREFIX}/flag/{key}")
            if saved is None:
                continue
            val = saved is True or saved == "true" or saved == 1 or saved == "1"
            self._ui_state.meta_flags[key] = val
            cb = self._meta_checkboxes.get(key)
            if cb is not None:
                cb.blockSignals(True)
                cb.setChecked(val)
                cb.blockSignals(False)

        # Background mode
        saved_bg = s.value(f"{_SETTINGS_PREFIX}/bkg_mode")
        if isinstance(saved_bg, str) and saved_bg in {k for k, _ in BG_MODES}:
            self._ui_state.bkg_mode = saved_bg
            self._sync_bkg_buttons()
