"""Preview tab — 2-page PDF viewer with zoom, continuous scroll, and bounding box overlay."""

import logging
import re
import shutil
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
from PySide6.QtGui import QAction, QBrush, QColor, QCursor, QFont, QFontMetricsF, QGuiApplication, QImage, QPainter, QPen, QPixmap, QWheelEvent
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
# Layered PDF rendering — content stream filtering
# ---------------------------------------------------------------------------

# PDF content stream operator classification
_TEXT_OPERATORS = {
    b"BT", b"ET", b"Tf", b"Td", b"TD", b"Tm", b"T*",
    b"Tj", b"TJ", b"'", b'"',
    b"Tc", b"Tw", b"Tz", b"TL", b"Tr", b"Ts",
}
_CHECKER_BRUSH: "QBrush | None" = None


def _checker_brush() -> "QBrush":
    """Photoshop-style 20x20 gray/white checker pattern for transparency bg."""
    global _CHECKER_BRUSH
    if _CHECKER_BRUSH is not None:
        return _CHECKER_BRUSH
    from PySide6.QtGui import QBrush
    tile = QPixmap(20, 20)
    tile.fill(QColor(235, 235, 235))   # light cell
    p = QPainter(tile)
    p.fillRect(0, 0, 10, 10, QColor(200, 200, 200))  # dark cell top-left
    p.fillRect(10, 10, 10, 10, QColor(200, 200, 200))  # dark cell bottom-right
    p.end()
    _CHECKER_BRUSH = QBrush(tile)
    return _CHECKER_BRUSH


_PATH_PAINT_OPERATORS = {
    b"S", b"s", b"f", b"F", b"f*", b"B", b"B*", b"b", b"b*", b"n",
}
_PATH_CONSTRUCTION_OPERATORS = {
    b"m", b"l", b"c", b"v", b"y", b"re", b"h",
}
_CLIP_OPERATORS = {b"W", b"W*"}
_SHADING_OPERATOR = b"sh"  # paints a shading pattern (ISO 32000 §8.7.4)
_TEXT_SHOW_OPERATORS = {b"Tj", b"TJ", b"'", b'"'}  # ops that emit glyphs


from dataclasses import dataclass, field


@dataclass(frozen=True)
class HiddenOpSet:
    """Per-object visibility spec passed through the render chain.

    Consumers:
        _filter_content_stream  — uses xobject_names + positional indices
                                  for text / paint / shading classes
        get_pixmap_filtered     — uses annotation_indices to delete specific
                                  annots from the temp page before rendering

    Positional indices are 0-based, counted per class across the entire
    content stream of the page (form-XObject streams included — their
    operators are filtered independently so indices restart per stream,
    which matches get_drawings()/get_text() flat enumerations closely
    enough for the UX).
    """

    xobject_names: frozenset[str] = field(default_factory=frozenset)
    text_indices: frozenset[int] = field(default_factory=frozenset)
    stroke_indices: frozenset[int] = field(default_factory=frozenset)
    fill_indices: frozenset[int] = field(default_factory=frozenset)
    stroke_fill_indices: frozenset[int] = field(default_factory=frozenset)
    shading_indices: frozenset[int] = field(default_factory=frozenset)
    annotation_indices: frozenset[int] = field(default_factory=frozenset)

    def is_empty(self) -> bool:
        return not (
            self.xobject_names
            or self.text_indices
            or self.stroke_indices
            or self.fill_indices
            or self.stroke_fill_indices
            or self.shading_indices
            or self.annotation_indices
        )


_EMPTY_HIDDEN = HiddenOpSet()


def _filter_content_stream(
    data: bytes,
    show_text: bool = True,
    show_images: bool = True,
    show_drawings: bool = True,
    show_path_stroke: bool | None = None,
    show_path_fill: bool | None = None,
    show_path_stroke_fill: bool | None = None,
    show_shading: bool = True,
    hidden_ops: HiddenOpSet = _EMPTY_HIDDEN,
    counters: dict[str, int] | None = None,
) -> bytes:
    """Filter PDF content stream by removing operators of unwanted types.

    Coarse flags:
        show_text       — BT..ET blocks
        show_images     — Do / BI..EI
        show_drawings   — all path painting (overrides granular when False)

    Granular path flags (each maps to PDF paint operators per ISO 32000 §8.5):
        show_path_stroke      — S, s   (stroke only)
        show_path_fill        — f, F, f*  (fill only)
        show_path_stroke_fill — B, B*, b, b*  (stroke + fill combined)

    Granular defaults (None) inherit from show_drawings for backwards
    compatibility: old callers passing only (show_text, show_images,
    show_drawings) get identical behavior. When any granular flag is an
    explicit bool, the path accumulator buffers construction ops (m/l/c/re/…)
    and decides per paint op whether to emit or drop.

    Graphics state operators (q, Q, cm, gs, color, etc.) are always kept.
    Clipping (W, W*) is always kept — needed for correct layout.
    """
    # Resolve effective per-paint-class flags. show_drawings=False forces
    # all three off (backwards compat with the old coarse API).
    if not show_drawings:
        eff_stroke = False
        eff_fill = False
        eff_stroke_fill = False
    else:
        eff_stroke = True if show_path_stroke is None else bool(show_path_stroke)
        eff_fill = True if show_path_fill is None else bool(show_path_fill)
        eff_stroke_fill = True if show_path_stroke_fill is None else bool(show_path_stroke_fill)

    commands = _parse_content_commands(data)
    out_parts: list[bytes] = []
    skip_until_et = not show_text

    # Path buffer: construction ops accumulate here until a paint op decides.
    # On paint, buffer is either emitted (keep) or dropped (hide).
    path_buffer: list[bytes] = []

    # Diagnostic counters — paint-op census to confirm what's actually in the
    # PDF. Logged once at end of filter.
    _paint_counts = {"stroke": 0, "fill": 0, "stroke_fill": 0, "n": 0}

    # Per-class positional counters for per-object hide. If `counters` is
    # passed by the caller (get_pixmap_filtered does this when filtering
    # multiple streams for one page), the counters are GLOBAL across all
    # streams — matching what _collect_page_objects in LayersView sees when
    # it walks the page's content. Without `counters`, counters are local
    # to this call (legacy single-stream callers).
    if counters is None:
        counters = {"text": 0, "stroke": 0, "fill": 0, "stroke_fill": 0, "shading": 0}

    _hidden_xobject_names = hidden_ops.xobject_names
    _hidden_text = hidden_ops.text_indices
    _hidden_stroke = hidden_ops.stroke_indices
    _hidden_fill = hidden_ops.fill_indices
    _hidden_stroke_fill = hidden_ops.stroke_fill_indices
    _hidden_shading = hidden_ops.shading_indices

    def _extract_xobject_name(raw_bytes: bytes) -> str | None:
        """Pull the /Name out of a `... /Name Do` command. Returns None if absent."""
        # Find the last `/` before `Do`
        i = raw_bytes.rfind(b"/")
        if i < 0:
            return None
        # Name chars: alphanumeric + common PDF name chars until whitespace
        j = i + 1
        n = len(raw_bytes)
        while j < n and raw_bytes[j:j + 1] not in (b" ", b"\t", b"\r", b"\n", b"\x00", b"\x0c"):
            j += 1
        try:
            return raw_bytes[i + 1:j].decode("ascii", errors="replace")
        except Exception:
            return None

    for raw, op in commands:
        # --- Text blocks: BT ... ET ---
        if op == b"BT":
            if not show_text:
                skip_until_et = True
                continue
        if op == b"ET":
            if skip_until_et:
                skip_until_et = False
                continue
        if skip_until_et and op in _TEXT_OPERATORS:
            continue
        # Per-span hide: skip Nth show op if its index is in hidden set.
        # Must still emit other text-state ops inside the same BT..ET.
        if op in _TEXT_SHOW_OPERATORS:
            idx = counters["text"]
            counters["text"] += 1
            if idx in _hidden_text:
                continue

        # --- Images / Form XObjects: /Name Do ---
        if op == b"Do":
            if not show_images:
                continue
            if _hidden_xobject_names:
                name = _extract_xobject_name(raw)
                if name is not None and name in _hidden_xobject_names:
                    continue

        # --- Inline images: BI ... ID <data> EI ---
        if op == b"BI":
            if not show_images:
                # raw contains everything from BI through EI
                continue

        # --- Shading: sh /ShadingName (gradient fills, outside path paint) ---
        if op == _SHADING_OPERATOR:
            if not show_shading:
                continue
            idx = counters["shading"]
            counters["shading"] += 1
            if idx in _hidden_shading:
                continue

        # --- Path construction: buffer until paint op arrives ---
        if op in _PATH_CONSTRUCTION_OPERATORS:
            path_buffer.append(raw)
            continue

        # --- Clipping: always kept, flush any pending path buffer ---
        if op in _CLIP_OPERATORS:
            if path_buffer:
                out_parts.extend(path_buffer)
                path_buffer.clear()
            out_parts.append(raw)
            continue

        # --- Path painting: classify + emit-or-drop the buffered path ---
        if op in _PATH_PAINT_OPERATORS:
            if op in (b"S", b"s"):
                _paint_counts["stroke"] += 1
                idx = counters["stroke"]
                counters["stroke"] += 1
                per_object_hidden = idx in _hidden_stroke
                if eff_stroke and not per_object_hidden:
                    out_parts.extend(path_buffer)
                    out_parts.append(raw)
            elif op in (b"f", b"F", b"f*"):
                _paint_counts["fill"] += 1
                idx = counters["fill"]
                counters["fill"] += 1
                per_object_hidden = idx in _hidden_fill
                if eff_fill and not per_object_hidden:
                    out_parts.extend(path_buffer)
                    out_parts.append(raw)
            elif op in (b"B", b"B*", b"b", b"b*"):
                # Combined fill+stroke: decompose into the active subset.
                # Both enabled  → emit original op (efficient, single pass).
                # Only stroke   → S / s (the 'close' bit preserved for b/b*).
                # Only fill     → f / f* (h prepended for b/b* to close path).
                # Both disabled → drop entirely.
                _paint_counts["stroke_fill"] += 1
                idx = counters["stroke_fill"]
                counters["stroke_fill"] += 1
                per_object_hidden = idx in _hidden_stroke_fill
                if per_object_hidden:
                    pass  # drop entirely
                elif eff_stroke and eff_fill:
                    out_parts.extend(path_buffer)
                    out_parts.append(raw)
                elif eff_fill and not eff_stroke:
                    out_parts.extend(path_buffer)
                    if op in (b"b", b"b*"):
                        out_parts.append(b"h")  # close path first
                    out_parts.append(b"f*" if op in (b"B*", b"b*") else b"f")
                elif eff_stroke and not eff_fill:
                    out_parts.extend(path_buffer)
                    out_parts.append(b"s" if op in (b"b", b"b*") else b"S")
                # else: both off → drop
            else:  # n — end path without painting; harmless, always keep
                _paint_counts["n"] += 1
                out_parts.extend(path_buffer)
                out_parts.append(raw)
            path_buffer.clear()
            continue

        # Any other operator (state, text, …). If a path is still buffered,
        # emit it inline — unpainted construction is a no-op on the canvas.
        if path_buffer:
            out_parts.extend(path_buffer)
            path_buffer.clear()
        out_parts.append(raw)

    # Trailing construction with no paint op — flush (harmless).
    if path_buffer:
        out_parts.extend(path_buffer)

    # Diagnostic: emit paint-op census so we can tell whether a PDF even has
    # combined stroke+fill operators (B/B*/b/b*). A zero count here means
    # that toggling path_stroke_fill has no observable effect — the paths
    # are split into separate fill and stroke sequences instead.
    if any(_paint_counts.values()):
        logger.info(
            "paint census: stroke=%d fill=%d stroke_fill=%d n=%d (eff S=%s F=%s SF=%s)",
            _paint_counts["stroke"], _paint_counts["fill"],
            _paint_counts["stroke_fill"], _paint_counts["n"],
            eff_stroke, eff_fill, eff_stroke_fill,
        )

    return b"\n".join(out_parts)


def _parse_content_commands(data: bytes) -> list[tuple[bytes, bytes]]:
    """Parse PDF content stream into list of (raw_bytes, operator) pairs.

    Each entry contains the full raw bytes of the command (operands + operator)
    and the operator keyword separately for classification.

    Handles literal strings (...), hex strings <...>, inline images (BI..EI),
    and comments (%).
    """
    commands: list[tuple[bytes, bytes]] = []
    pos = 0
    n = len(data)
    cmd_start = 0
    operand_stack: list[bytes] = []

    while pos < n:
        # Skip whitespace
        while pos < n and data[pos:pos+1] in (b" ", b"\t", b"\r", b"\n", b"\x00", b"\x0c"):
            pos += 1
        if pos >= n:
            break

        ch = data[pos:pos+1]

        # Comment — skip to end of line
        if ch == b"%":
            while pos < n and data[pos:pos+1] not in (b"\r", b"\n"):
                pos += 1
            continue

        # Literal string (...)
        if ch == b"(":
            start = pos
            pos += 1
            depth = 1
            while pos < n and depth > 0:
                c = data[pos:pos+1]
                if c == b"\\":
                    pos += 2
                    continue
                if c == b"(":
                    depth += 1
                elif c == b")":
                    depth -= 1
                pos += 1
            operand_stack.append(data[start:pos])
            continue

        # Hex string <...> (but not dict <<...>>)
        if ch == b"<" and data[pos+1:pos+2] != b"<":
            start = pos
            pos += 1
            while pos < n and data[pos:pos+1] != b">":
                pos += 1
            pos += 1  # skip >
            operand_stack.append(data[start:pos])
            continue

        # Dict <<...>>
        if ch == b"<" and data[pos+1:pos+2] == b"<":
            start = pos
            pos += 2
            depth = 1
            while pos < n - 1 and depth > 0:
                if data[pos:pos+2] == b"<<":
                    depth += 1
                    pos += 2
                elif data[pos:pos+2] == b">>":
                    depth -= 1
                    pos += 2
                else:
                    pos += 1
            operand_stack.append(data[start:pos])
            continue

        # Array [...]
        if ch == b"[":
            start = pos
            pos += 1
            depth = 1
            while pos < n and depth > 0:
                c = data[pos:pos+1]
                if c == b"[":
                    depth += 1
                elif c == b"]":
                    depth -= 1
                elif c == b"(":
                    # skip string inside array
                    pos += 1
                    sd = 1
                    while pos < n and sd > 0:
                        sc = data[pos:pos+1]
                        if sc == b"\\":
                            pos += 2
                            continue
                        if sc == b"(":
                            sd += 1
                        elif sc == b")":
                            sd -= 1
                        pos += 1
                    continue
                pos += 1
            operand_stack.append(data[start:pos])
            continue

        # Name /...
        if ch == b"/":
            start = pos
            pos += 1
            while pos < n and data[pos:pos+1] not in (
                b" ", b"\t", b"\r", b"\n", b"\x00", b"\x0c",
                b"/", b"(", b")", b"<", b">", b"[", b"]", b"{", b"}",
                b"%",
            ):
                pos += 1
            operand_stack.append(data[start:pos])
            continue

        # Number or keyword
        start = pos
        while pos < n and data[pos:pos+1] not in (
            b" ", b"\t", b"\r", b"\n", b"\x00", b"\x0c",
            b"/", b"(", b")", b"<", b">", b"[", b"]", b"{", b"}",
            b"%",
        ):
            pos += 1
        token = data[start:pos]
        if not token:
            pos += 1  # skip unknown byte
            continue

        # Is it a number?
        is_number = True
        try:
            float(token)
        except (ValueError, OverflowError):
            is_number = False

        if is_number or token in (b"true", b"false", b"null"):
            operand_stack.append(token)
            continue

        # It's an operator keyword
        operator = token

        # Special: inline image BI ... ID <binary> EI
        if operator == b"BI":
            # Collect everything through EI
            # Find ID marker (end of inline image dict, start of data)
            id_pos = data.find(b"ID", pos)
            if id_pos < 0:
                # Malformed — just emit what we have
                raw = b" ".join(operand_stack) + b" " + operator if operand_stack else operator
                commands.append((raw, operator))
                operand_stack.clear()
                continue
            # After ID there's one whitespace byte, then binary data until EI
            data_start = id_pos + 3  # "ID" + 1 whitespace byte
            # Search for EI preceded by whitespace
            ei_pos = data_start
            while ei_pos < n - 2:
                if (data[ei_pos:ei_pos+1] in (b" ", b"\r", b"\n", b"\t")
                        and data[ei_pos+1:ei_pos+3] == b"EI"
                        and (ei_pos + 3 >= n or data[ei_pos+3:ei_pos+4] in (
                            b" ", b"\r", b"\n", b"\t", b"\x00", b"\x0c"))):
                    break
                ei_pos += 1
            end_pos = ei_pos + 3  # past "EI"
            raw = data[start:end_pos]
            commands.append((raw, b"BI"))
            operand_stack.clear()
            pos = end_pos
            continue

        # Regular operator — build the raw command
        if operand_stack:
            raw = b" ".join(operand_stack) + b" " + operator
        else:
            raw = operator
        commands.append((raw, operator))
        operand_stack.clear()

    return commands


def get_pixmap_redacted(
    doc: "fitz.Document",
    page_idx: int,
    dpi: float,
    show_text: bool = True,
    show_images: bool = True,
    show_drawings: bool = True,
) -> tuple:
    """Render a PDF page with hidden content TYPES genuinely removed via
    PyMuPDF's :meth:`Page.apply_redactions`. This operates at the PDF object
    level (not pixel level), so images/drawings/text really disappear from
    the rendered pixmap — unlike content-stream operator filtering which can
    leave "orphan" references or dependent clipping paths visible.

    Returns (samples_bytes, width, height, n, stride).
    Falls back to :func:`get_pixmap_filtered` if redactions aren't supported
    or fail for this document.
    """
    mat = fitz.Matrix(dpi / 72.0, dpi / 72.0)

    # Fast path — no filtering needed
    if show_text and show_images and show_drawings:
        pix = doc[page_idx].get_pixmap(matrix=mat)
        return bytes(pix.samples), pix.width, pix.height, pix.n, pix.stride

    try:
        tmp_doc = fitz.open()
        tmp_doc.insert_pdf(doc, from_page=page_idx, to_page=page_idx)
        tmp_page = tmp_doc[0]

        # A single redaction annotation covering the whole page. Combined with
        # the per-type flags in apply_redactions, this nukes everything of the
        # requested kinds from the page's actual content/resource tree.
        tmp_page.add_redact_annot(tmp_page.rect)

        # Resolve enum constants (names stabilized across fitz versions).
        img_flag = getattr(fitz, "PDF_REDACT_IMAGE_REMOVE", 1) if not show_images \
            else getattr(fitz, "PDF_REDACT_IMAGE_NONE", 0)
        gfx_flag = getattr(fitz, "PDF_REDACT_LINE_ART_REMOVE_IF_COVERED", 1) if not show_drawings \
            else getattr(fitz, "PDF_REDACT_LINE_ART_NONE", 0)

        tmp_page.apply_redactions(
            images=img_flag,
            graphics=gfx_flag,
            text=not show_text,
        )

        pix = tmp_page.get_pixmap(matrix=mat)
        result = bytes(pix.samples), pix.width, pix.height, pix.n, pix.stride
        tmp_doc.close()
        return result
    except Exception as e:
        logger.warning(
            "Redaction-based rendering failed (%s); falling back to content-stream filter",
            e,
        )
        try:
            tmp_doc.close()
        except Exception:
            pass
        return get_pixmap_filtered(doc, page_idx, dpi, show_text, show_images, show_drawings)


def get_pixmap_filtered(
    doc: "fitz.Document",
    page_idx: int,
    dpi: float,
    show_text: bool = True,
    show_images: bool = True,
    show_drawings: bool = True,
    show_path_stroke: bool | None = None,
    show_path_fill: bool | None = None,
    show_path_stroke_fill: bool | None = None,
    show_shading: bool = True,
    render_annotations: bool = True,
    hidden_ops: HiddenOpSet = _EMPTY_HIDDEN,
) -> tuple:
    """Render a PDF page showing only selected content types.

    Coarse + granular filtering; see _filter_content_stream for the exact
    semantics of the three show_path_* params. Callers that don't pass them
    get the legacy coarse behavior (all paint ops treated as one bucket).

    Creates a temporary copy of the page, filters the content streams —
    both the page-level one AND any referenced Form XObject streams
    (catalogs often wrap entire page content inside a single form
    XObject) — and renders the result. The original document is never
    modified.

    Returns (samples_bytes, width, height, n, stride) tuple —
    raw pixel data that is safe to use after this function returns.
    """
    mat = fitz.Matrix(dpi / 72.0, dpi / 72.0)

    # Granular resolution for the fast-path check: any explicit False means
    # we must filter.
    granular_all_on = (
        (show_path_stroke is None or show_path_stroke)
        and (show_path_fill is None or show_path_fill)
        and (show_path_stroke_fill is None or show_path_stroke_fill)
    )

    # Fast path — nothing to strip, render as-is
    if (show_text and show_images and show_drawings and granular_all_on
            and show_shading and render_annotations and hidden_ops.is_empty()):
        pix = doc[page_idx].get_pixmap(matrix=mat)
        return bytes(pix.samples), pix.width, pix.height, pix.n, pix.stride

    # Copy page to temp document
    tmp_doc = fitz.open()
    tmp_doc.insert_pdf(doc, from_page=page_idx, to_page=page_idx)
    tmp_page = tmp_doc[0]

    # Per-annotation hide: delete selected annots from the temp page before
    # rendering. Works even when render_annotations=True (we keep visible
    # annots, drop the specific ones the user hid). Collect refs first then
    # delete to avoid iterator invalidation.
    if hidden_ops.annotation_indices and render_annotations:
        try:
            annots_list = list(tmp_page.annots()) if tmp_page.annots() is not None else []
            to_delete = [
                annots_list[i] for i in sorted(hidden_ops.annotation_indices)
                if 0 <= i < len(annots_list)
            ]
            for annot in to_delete:
                try:
                    tmp_page.delete_annot(annot)
                except Exception:
                    pass
        except Exception:
            pass

    # Normalize content stream into a single object
    tmp_page.clean_contents()

    # Filter every stream that contributes to the page render:
    #   1. The page-level content streams
    #   2. Every Form-XObject stream in the temp document
    # Form XObjects need their own filtering because page-level filtering
    # can't "see into" the form's ops — for PDFs that wrap all content
    # inside a single /Form XObject (catalog pages), filtering only the
    # outer stream is a no-op.
    streams_to_filter: list[int] = []
    contents = tmp_page.get_contents()
    if contents:
        streams_to_filter.extend(contents)
    try:
        for xref in range(1, tmp_doc.xref_length()):
            try:
                obj_def = tmp_doc.xref_object(xref)
            except Exception:
                continue
            if not obj_def:
                continue
            # Match /Subtype /Form (with or without whitespace) — that's a
            # Form XObject; Image XObjects are /Subtype /Image and don't
            # contain operator streams we can filter.
            if "/Subtype /Form" in obj_def or "/Subtype/Form" in obj_def:
                streams_to_filter.append(xref)
    except Exception:
        pass

    total_before = total_after = 0
    # Global counters shared across every stream this page is filtered from
    # (page content streams + any Form XObject streams). This makes
    # per-class positional indices unique across streams, matching how
    # LayersView enumerates objects into its sidebar.
    shared_counters: dict[str, int] = {
        "text": 0, "stroke": 0, "fill": 0, "stroke_fill": 0, "shading": 0,
    }
    for xref in streams_to_filter:
        try:
            stream = tmp_doc.xref_stream(xref)
        except Exception:
            continue
        if not stream:
            continue
        total_before += len(stream)
        filtered = _filter_content_stream(
            stream,
            show_text=show_text,
            show_images=show_images,
            show_drawings=show_drawings,
            show_path_stroke=show_path_stroke,
            show_path_fill=show_path_fill,
            show_path_stroke_fill=show_path_stroke_fill,
            show_shading=show_shading,
            hidden_ops=hidden_ops,
            counters=shared_counters,
        )
        total_after += len(filtered)
        try:
            tmp_doc.update_stream(xref, filtered)
        except Exception:
            continue

    logger.info(
        "Content streams filtered: %d streams, %d → %d bytes (text=%s img=%s draw=%s)",
        len(streams_to_filter), total_before, total_after,
        show_text, show_images, show_drawings,
    )

    # Nothing to render? Just return blank.
    if not streams_to_filter:
        pix = tmp_page.get_pixmap(matrix=mat, annots=render_annotations)
        result = bytes(pix.samples), pix.width, pix.height, pix.n, pix.stride
        tmp_doc.close()
        return result

    # Render filtered page. annots=False suppresses all page annotations
    # (arrows, stamps, highlights, form widgets, ink drawings) which live
    # OUTSIDE the content stream and are unaffected by operator filtering.
    pix = tmp_page.get_pixmap(matrix=mat, annots=render_annotations)
    # Copy raw pixel data before closing tmp_doc
    result = bytes(pix.samples), pix.width, pix.height, pix.n, pix.stride
    tmp_doc.close()
    return result


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

    Emits *page_ready* with (page_index, QImage, dpi, layer_key) for each
    completed page.  The caller must convert QImage → QPixmap on the main thread.
    """

    page_ready = Signal(int, QImage, float, object)  # page_idx, image, dpi, layer_key
    all_done = Signal()

    def __init__(
        self,
        doc_path: str,
        requests: list[tuple[int, float]],
        layer_key: frozenset = frozenset(),
        hidden_ops: "HiddenOpSet | None" = None,
        parent=None,
    ):
        super().__init__(parent)
        self._doc_path = doc_path
        self._requests = requests  # [(page_idx, dpi), ...]
        self._layer_key = layer_key
        self._hidden_ops = hidden_ops if hidden_ops is not None else _EMPTY_HIDDEN
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        try:
            doc = fitz.open(self._doc_path)
        except Exception:
            return
        try:
            force_filter = not self._hidden_ops.is_empty()
            if self._layer_key or force_filter:
                show_text = "pdf_text" not in self._layer_key and "text" not in self._layer_key
                show_images = "pdf_image" not in self._layer_key and "images" not in self._layer_key
                show_drawings = (
                    "pdf_table" not in self._layer_key
                    and "pdf_drawing" not in self._layer_key
                    and "drawings" not in self._layer_key
                )
                # Granular path paint flags (None = use coarse show_drawings)
                show_path_stroke = "path_stroke" not in self._layer_key
                show_path_fill = "path_fill" not in self._layer_key
                show_path_stroke_fill = "path_stroke_fill" not in self._layer_key
                # Shading + annotations (non-content-stream render toggles)
                show_shading = "shading" not in self._layer_key
                render_annotations = "annotations" not in self._layer_key
            else:
                show_text = show_images = show_drawings = True
                show_path_stroke = show_path_fill = show_path_stroke_fill = True
                show_shading = True
                render_annotations = True

            for page_idx, dpi in self._requests:
                if self._cancelled:
                    break
                try:
                    if self._layer_key or force_filter:
                        samples, w, h, n, stride = get_pixmap_filtered(
                            doc, page_idx, dpi,
                            show_text=show_text,
                            show_images=show_images,
                            show_drawings=show_drawings,
                            show_path_stroke=show_path_stroke,
                            show_path_fill=show_path_fill,
                            show_path_stroke_fill=show_path_stroke_fill,
                            show_shading=show_shading,
                            render_annotations=render_annotations,
                            hidden_ops=self._hidden_ops,
                        )
                        img = QImage(
                            samples, w, h, stride,
                            QImage.Format.Format_RGB888,
                        ).copy()
                    else:
                        page = doc[page_idx]
                        mat = fitz.Matrix(dpi / 72.0, dpi / 72.0)
                        pix = page.get_pixmap(matrix=mat)
                        img = QImage(
                            pix.samples, pix.width, pix.height,
                            pix.stride, QImage.Format.Format_RGB888,
                        ).copy()  # .copy() — prevent dangling pointer after pix freed
                    if not self._cancelled:
                        self.page_ready.emit(page_idx, img, dpi, self._layer_key)
                except Exception as exc:
                    logger.error("Background render page %d failed: %s", page_idx, exc)
        finally:
            doc.close()
        if not self._cancelled:
            self.all_done.emit()


# Bump when _extract_pdf_objects output format or semantics change so that
# previously-persisted pdf_objects_cache entries get discarded and re-detected.
PDF_OBJECTS_DETECTION_VERSION = 2


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

def _format_page_stats(page_num: int, bboxes: list[dict]) -> str:
    """Header-bar stats line: per-page totals + per-type breakdown."""
    n_text = sum(1 for b in bboxes if b.get("type") in ("text", "pdf_text"))
    n_img = sum(1 for b in bboxes if b.get("type") in ("image", "photo", "picture", "pdf_image"))
    n_tbl = sum(1 for b in bboxes if b.get("type") in ("table", "pdf_table"))
    n_gfx = sum(1 for b in bboxes if b.get("type") in ("drawing", "gfx", "pdf_drawing"))
    return (
        f"P{page_num + 1}  |  Total: {len(bboxes)}  |  "
        f"Text: {n_text}  |  Images: {n_img}  |  "
        f"Tables: {n_tbl}  |  Gfx: {n_gfx}"
    )


BBOX_BORDER_COLORS = {
    "table": QColor(0, 120, 215, 200),
    "text": QColor(76, 175, 80, 160),
    "photo": QColor(255, 87, 34, 160),
    "picture": QColor(255, 152, 0, 160),
    "drawing": QColor(156, 39, 176, 160),
    "template": QColor(200, 0, 200, 160),
    "unknown": QColor(158, 158, 158, 160),
}


def _install_menu_wheel_passthrough(menu: "QMenu", scroll_area: "QScrollArea") -> None:
    """Keep mouse-wheel (zoom / scroll) working over the pages while *menu* is
    open. By default QMenu intercepts wheel events globally while shown — this
    installs an app-level filter on menu show that forwards wheel events
    landing outside the menu's geometry straight to *scroll_area.viewport()*.
    """
    from PySide6.QtCore import QEvent, QObject, QPointF
    from PySide6.QtGui import QWheelEvent

    class _Forwarder(QObject):
        def __init__(self):
            super().__init__(menu)
            self._app = None
            menu.aboutToShow.connect(self._on_show)
            menu.aboutToHide.connect(self._on_hide)

        def _on_show(self) -> None:
            from PySide6.QtWidgets import QApplication
            self._app = QApplication.instance()
            if self._app is not None:
                self._app.installEventFilter(self)

        def _on_hide(self) -> None:
            if self._app is not None:
                self._app.removeEventFilter(self)
                self._app = None

        def eventFilter(self, obj, event) -> bool:
            if event.type() != QEvent.Type.Wheel:
                return False
            try:
                gp = event.globalPosition().toPoint()
            except Exception:
                try:
                    gp = event.globalPos()
                except Exception:
                    return False
            menu_rect = menu.rect().translated(menu.pos())
            if menu_rect.contains(gp):
                return False  # wheel over the menu itself — normal behavior
            vp = scroll_area.viewport()
            vp_pos = vp.mapFromGlobal(gp)
            if not vp.rect().contains(vp_pos):
                return False  # wheel outside scroll area too — ignore
            new_event = QWheelEvent(
                QPointF(vp_pos), QPointF(gp),
                event.pixelDelta(), event.angleDelta(),
                event.buttons(), event.modifiers(),
                event.phase(), event.inverted(),
            )
            from PySide6.QtWidgets import QApplication
            QApplication.sendEvent(vp, new_event)
            return True  # consume so menu doesn't also swallow it

    _Forwarder()


class _Ruler(QWidget):
    """Horizontal or vertical ruler. Units: mm (PDF native). Ticks every 1/5/10 mm.

    When ``_segments`` is set (list of (start_px_in_content, width_px)), each
    segment draws its own 0-origin scale — useful for per-page horizontal
    rulers where every spread restarts at 0 mm at its left edge.
    """

    H = 18   # horizontal ruler thickness (px)
    V = 24   # vertical ruler thickness (px)

    def __init__(self, orientation: Qt.Orientation, parent=None) -> None:
        super().__init__(parent)
        self._orient = orientation
        self._zoom = 1.0
        self._base_dpi = 225
        self._content_offset_px = 0   # scroll offset in px
        self._content_origin_px = 0   # where the pages_container starts (centered offset)
        # per-page segments: (start_px_in_content, width_px, mirror)
        # mirror=True → 0 mm at right edge, increasing to the left (used for
        # the left page of a 2-page spread so both pages measure outward from
        # the spread centerline).
        self._segments: list[tuple[int, int, bool]] = []
        if orientation == Qt.Orientation.Horizontal:
            self.setFixedHeight(self.H)
        else:
            self.setFixedWidth(self.V)
        self.setAutoFillBackground(True)
        pal = self.palette()
        pal.setColor(self.backgroundRole(), QColor(246, 246, 246))
        self.setPalette(pal)

    def set_segments(self, segments: list[tuple[int, int, bool]]) -> None:
        self._segments = list(segments)
        self.update()

    def set_scale(self, zoom: float, base_dpi: float) -> None:
        self._zoom = max(0.01, zoom)
        self._base_dpi = max(1.0, base_dpi)
        self.update()

    def set_offset(self, px: int, origin_px: int = 0) -> None:
        self._content_offset_px = px
        self._content_origin_px = origin_px
        self.update()

    def _px_per_mm(self) -> float:
        # 1 mm = 72/25.4 pt; px = pt * zoom * base_dpi / 72
        return (72.0 / 25.4) * self._zoom * self._base_dpi / 72.0

    def paintEvent(self, event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, False)
        painter.setPen(QPen(QColor(80, 80, 80), 1))
        painter.setFont(QFont("Consolas", 7))
        fm = painter.fontMetrics()
        mm_px = self._px_per_mm()
        if mm_px <= 0:
            painter.end()
            return

        h, w = self.height(), self.width()

        # Ruler edge line
        if self._orient == Qt.Orientation.Horizontal:
            painter.drawLine(0, h - 1, w, h - 1)
        else:
            painter.drawLine(w - 1, 0, w - 1, h)

        if self._segments:
            # Per-page rulers: each segment restarts at 0 mm.
            # mirror=True flips the origin to the right edge so mm grows leftward
            # (left page of a 2-page spread measures outward from the spine).
            for seg_start, seg_w, mirror in self._segments:
                seg_left_screen = seg_start - self._content_offset_px
                seg_right_screen = seg_left_screen + seg_w
                # Skip segments fully outside the ruler's visible area
                if seg_right_screen < -2 or seg_left_screen > w + 2:
                    continue
                mm_end = int(seg_w / mm_px) + 1
                if mirror:
                    for mm in range(0, mm_end + 1):
                        pos = int(seg_right_screen - mm * mm_px)
                        # Clip: only draw inside the segment's extent
                        if pos < seg_left_screen or pos > seg_right_screen:
                            continue
                        self._draw_tick(painter, mm, pos, h, w, fm,
                                        mirror=True,
                                        bounds=(seg_left_screen, seg_right_screen))
                else:
                    for mm in range(0, mm_end + 1):
                        pos = int(seg_left_screen + mm * mm_px)
                        if pos < seg_left_screen or pos > seg_right_screen:
                            continue
                        self._draw_tick(painter, mm, pos, h, w, fm,
                                        mirror=False,
                                        bounds=(seg_left_screen, seg_right_screen))
        else:
            # Single continuous ruler (legacy / fallback)
            view_px = w if self._orient == Qt.Orientation.Horizontal else h
            start_px = self._content_offset_px - self._content_origin_px
            mm_start = int((start_px / mm_px) - 2)
            mm_end = int(((start_px + view_px) / mm_px) + 2)
            for mm in range(mm_start, mm_end + 1):
                pos = int(mm * mm_px - start_px)
                self._draw_tick(painter, mm, pos, h, w, fm)
        painter.end()

    def _draw_tick(self, painter: QPainter, mm: int, pos: int,
                    h: int, w: int, fm, mirror: bool = False,
                    bounds: tuple[int, int] | None = None) -> None:
        if self._orient == Qt.Orientation.Horizontal:
            if pos < -10 or pos > w + 10:
                return
            if mm % 10 == 0:
                painter.drawLine(pos, h - 10, pos, h - 1)
                if mm != 0:
                    label = str(mm)
                    tw = fm.horizontalAdvance(label)
                    # For mirrored rulers, place label to the LEFT of its tick
                    # so the label stays over the segment (values grow leftward).
                    lx = pos - tw - 2 if mirror else pos + 2
                    # Keep label strictly within segment bounds if provided,
                    # otherwise within ruler bounds. This prevents labels from
                    # the left (mirrored) ruler spilling onto the right page
                    # and vice-versa — which otherwise looks like "doubled"
                    # numbers running in both directions.
                    if bounds is not None:
                        bl, br = bounds
                        if lx < bl or lx + tw > br:
                            return
                    elif not (2 <= lx <= w - tw - 2):
                        return
                    painter.drawText(lx, h - 10, label)
            elif mm % 5 == 0:
                painter.drawLine(pos, h - 6, pos, h - 1)
            else:
                painter.drawLine(pos, h - 3, pos, h - 1)
        else:
            if pos < -10 or pos > h + 10:
                return
            if mm % 10 == 0:
                painter.drawLine(w - 10, pos, w - 1, pos)
                if mm != 0 and pos > 12 and pos < h - 12:
                    painter.save()
                    painter.translate(2, pos - 2)
                    painter.rotate(-90)
                    painter.drawText(0, fm.ascent(), str(mm))
                    painter.restore()
            elif mm % 5 == 0:
                painter.drawLine(w - 6, pos, w - 1, pos)
            else:
                painter.drawLine(w - 3, pos, w - 1, pos)


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
    page_rerender_requested = Signal(int)    # (page_index,) — request to re-render page pixmap
    template_excluded = Signal(str)           # (obj_id,) — exclude object from template group

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
            "pdf_text": False, "pdf_image": False, "pdf_drawing": False, "pdf_table": False,
        }
        self._pdf_objects: list[dict] = []  # native PDF object rects
        self._content_mask: dict[str, bool] = {}  # types to mask (hide content)
        self._detect_method_idx: int = 8  # detection method index
        self._show_object_labels: bool = True  # draw labels on PDF object bboxes
        self._edit_bboxes: bool = True  # allow drag/resize handles; False = select-only
        # Probe overlay: Recognition tab's PyMuPDF-probe results — list of
        # (pts, label) tuples drawn as bright dashed rectangles on top.
        self._probe_rects: list[tuple[tuple[float, float, float, float], str]] = []

        # Layers-tab hover highlight: (x0, y0, x1, y1) in PDF points, drawn
        # as a pulsing yellow outline on top of everything else. Cleared on
        # mouse leave from the corresponding sidebar subitem.
        self._layers_highlight_bbox: tuple[float, float, float, float] | None = None
        self._layers_highlight_phase: float = 0.0
        self._layers_highlight_timer = QTimer(self)
        self._layers_highlight_timer.setInterval(60)  # ~16 fps — smooth pulse
        self._layers_highlight_timer.timeout.connect(self._layers_highlight_tick)

        # Layers-tab background mode: one of "white", "black", "checker",
        # "transparent". Other views leave this at "white" (default) so
        # their paintEvent path is unchanged.
        self._layers_bg_mode: str = "white"

        # Cropping overlays (fractions 0..1)
        self._crop_h_lines: list[float] = []
        self._crop_v_lines: list[float] = []
        self._crop_boxes: list[list[float]] = []

        # Drag state
        self._drag_mode: str = ""       # "move", "resize"
        self._drag_handle: int = -1     # which handle (0-7) for resize
        self._drag_start_px: QPointF | None = None  # mouse pos at drag start (px)
        self._drag_orig_pts: tuple | None = None     # original bbox pts at drag start
        # Pure PDF drag offset
        self._pure_pdf_drag_offset: tuple[float, float] = (0.0, 0.0)  # (dx_pt, dy_pt) offset during drag
        self.setMouseTracking(True)

    def set_pixmap(self, pixmap: QPixmap) -> None:
        self._pixmap = pixmap
        self.setFixedSize(pixmap.width(), pixmap.height() + self.HEADER_HEIGHT)
        self.update()

    def set_placeholder(self, zoom_factor: float) -> None:
        """Size the widget from the PDF point dimensions without allocating
        a full-size blank pixmap. Used during bulk spread creation to avoid
        ~2 MB per page memory allocation. paintEvent handles the empty
        pixmap by just leaving the widget background visible."""
        w_pt, h_pt = self._page_size_pt
        w = max(1, int(w_pt * zoom_factor))
        h = max(1, int(h_pt * zoom_factor))
        self._pixmap = QPixmap()  # null, paintEvent returns early
        self._zoom_factor = zoom_factor
        self.setFixedSize(w, h + self.HEADER_HEIGHT)
        # Flat background colour while awaiting a real render.
        self.setAutoFillBackground(True)
        pal = self.palette()
        pal.setColor(self.backgroundRole(), QColor(245, 245, 245))
        self.setPalette(pal)
        self.update()

    def set_bboxes(self, bboxes_pts: list[dict], zoom_factor: float, method_idx: int = 8) -> None:
        """Set bounding boxes in PDF point coordinates."""
        self._bboxes_pts = bboxes_pts
        self._zoom_factor = zoom_factor
        self._detect_method_idx = method_idx
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

    def set_show_object_labels(self, show: bool) -> None:
        """Toggle label drawing on PDF object bboxes."""
        self._show_object_labels = show
        self.update()

    def set_layers_highlight(
        self, bbox: tuple[float, float, float, float] | None
    ) -> None:
        """LayersView entry point: set or clear the pulsing hover highlight
        on this page. Starting the timer drives paintEvent updates at ~16 fps
        so the border pulses between dim and bright yellow."""
        self._layers_highlight_bbox = bbox
        if bbox is None:
            self._layers_highlight_timer.stop()
            self._layers_highlight_phase = 0.0
        else:
            if not self._layers_highlight_timer.isActive():
                self._layers_highlight_phase = 0.0
                self._layers_highlight_timer.start()
        self.update()

    def _layers_highlight_tick(self) -> None:
        """Timer tick — advance phase for the pulsing highlight and repaint."""
        # Pulse cycle ~1.2s: 0.05 per tick × 20 ticks × 60 ms = 1.2 s
        self._layers_highlight_phase = (self._layers_highlight_phase + 0.05) % 1.0
        self.update()

    def set_probe_overlay(self, rects: list, zoom_factor: float | None = None) -> None:
        """Set the Recognition-tab PyMuPDF-probe overlay rects. Each entry is
        either a ``(x0, y0, x1, y1)`` tuple or ``((x0, y0, x1, y1), label)``."""
        out: list[tuple[tuple[float, float, float, float], str]] = []
        for r in rects or []:
            if isinstance(r, tuple) and len(r) == 2 and not isinstance(r[0], (int, float)):
                pts, lbl = r[0], r[1]
            else:
                pts, lbl = r, ""
            if pts and len(pts) >= 4:
                out.append(((float(pts[0]), float(pts[1]),
                             float(pts[2]), float(pts[3])), str(lbl)))
        self._probe_rects = out
        if zoom_factor is not None:
            self._zoom_factor = zoom_factor
        self.update()

    def set_edit_bboxes(self, allow: bool) -> None:
        """Enable (True) or disable (False) bbox editing — drag / resize / handles.
        Selection (highlight) still works in either mode."""
        self._edit_bboxes = allow
        if not allow:
            self._drag_mode = ""
            self._drag_handle = -1
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
                if any(bbox.get(k) for k in ["is_template_exact", "is_template_medium", "is_template_loose"]) and self._content_mask.get("template", False):
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

        # Exclude from template (if it's a template)
        is_template = any(bbox.get(k) for k in ["is_template_exact", "is_template_medium", "is_template_loose"])
        if is_template:
            menu.addSeparator()
            exclude_action = menu.addAction("Exclude from template")
            exclude_action.triggered.connect(lambda: self.template_excluded.emit(obj_id))

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
        # Middle button always belongs to the scroll-area pan filter —
        # propagate before any bbox logic can swallow it.
        if event.button() == Qt.MouseButton.MiddleButton:
            event.ignore()
            return super().mousePressEvent(event)
        if event.button() == Qt.MouseButton.RightButton:
            return super().mousePressEvent(event)
        if not self._show_bboxes or not self._bboxes_pts:
            return super().mousePressEvent(event)

        click = event.position()

        # Read-only mode: select-only, skip handle/move drag logic entirely.
        if not self._edit_bboxes:
            pass  # fall through to selection logic below
        else:
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
                    # Pure PDF drag: move object visually, not bbox
                    if self._detect_method_idx == 10:
                        self._drag_mode = "pure_pdf_move"
                        self._pure_pdf_drag_offset = (0.0, 0.0)
                        self._drag_start_px = click
                        return
                    # Normal drag: move/resize bbox
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
            if any(bbox.get(k) for k in ["is_template_exact", "is_template_medium", "is_template_loose"]) and self._content_mask.get("template", False):
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

        # Pure PDF drag: just track offset, re-render with offset
        if self._drag_mode == "pure_pdf_move" and self._drag_start_px:
            zf = self._zoom_factor
            dx_pts = (pos.x() - self._drag_start_px.x()) / zf
            dy_pts = (pos.y() - self._drag_start_px.y()) / zf
            self._pure_pdf_drag_offset = (dx_pts, dy_pts)
            self.update()  # re-render with offset
            return

        # Active drag (normal mode)
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

        # Hover cursor changes — only when bbox editing is allowed
        if self._edit_bboxes:
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
        # Propagate so the scroll-area viewport event filter can handle pan
        # (middle-button drag grabs the mouse on this widget, so without this
        # the pan handler never sees MouseMove events while the cursor is
        # over a PageWidget).
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event) -> None:
        if self._drag_mode and self._selected_idx >= 0:
            if self._drag_mode == "pure_pdf_move":
                # Pure PDF: apply drag offset to bbox and save new position
                bbox = self._bboxes_pts[self._selected_idx]
                dx_pt, dy_pt = self._pure_pdf_drag_offset
                # Update bbox coordinates [x0, y0, x1, y1]
                pts = bbox["pts"]
                new_pts = [pts[0] + dx_pt, pts[1] + dy_pt, pts[2] + dx_pt, pts[3] + dy_pt]
                bbox["pts"] = new_pts
                logger.info(f"Pure PDF drag completed: offset=({dx_pt}, {dy_pt}), new_pts={new_pts}")
                # Emit modified signal
                obj_id = bbox.get("id", "")
                if obj_id:
                    self.bbox_modified.emit(obj_id, tuple(new_pts))
                    # Request page re-render to update pixmap
                    if self._page_index >= 0:
                        self.page_rerender_requested.emit(self._page_index)
                # Reset offset after emitting signals
                self._pure_pdf_drag_offset = (0.0, 0.0)
            else:  # Normal drag: emit modified bbox
                bbox = self._bboxes_pts[self._selected_idx]
                obj_id = bbox.get("id", "")
                if obj_id:
                    self.bbox_modified.emit(obj_id, tuple(bbox["pts"]))
        self._drag_mode = ""
        self._drag_handle = -1
        self._drag_start_px = None
        self._drag_orig_pts = None
        # Use timer to update after signals are processed
        QTimer.singleShot(10, self.update)
        # Propagate so pan handler (scroll-area viewport filter) sees release
        super().mouseReleaseEvent(event)

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
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)

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

        # Layers-tab background (under the pixmap). Paint ONLY over the
        # page area — the header and any non-page regions of the widget are
        # untouched. Skipped for default ("white") mode which matches the
        # normal page-background colour.
        bg_mode = getattr(self, "_layers_bg_mode", "white")
        if bg_mode != "white":
            page_rect = QRectF(0, h, self._pixmap.width(), self._pixmap.height())
            if bg_mode == "black":
                painter.fillRect(page_rect, QColor(0, 0, 0))
            elif bg_mode == "checker":
                painter.fillRect(page_rect, _checker_brush())
            # "transparent" → leave bg alone (app's default colour shows).

        # Draw page image below header
        painter.drawPixmap(0, h, self._pixmap)

        # Content hiding — fill hidden object areas with white
        if self._content_mask and self._bboxes_pts:
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(QColor(255, 255, 255))
            for bbox in self._bboxes_pts:
                bbox_type = bbox.get("type", "unknown")
                is_tpl = any(bbox.get(k, False) for k in ["is_template_exact", "is_template_medium", "is_template_loose"])
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
            "pdf_drawing": QColor(156, 39, 176, 180),  # purple (vectors)
            "pdf_table": QColor(0, 120, 215, 180),     # blue
        }
        if (self._show_bboxes
                and self._visible_layers.get("pdf_objects", False)
                and self._pdf_objects):
            font = QFont("Consolas", 7)
            painter.setFont(font)
            fm = QFontMetricsF(font)
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
                lbl = obj.get("label", "") if self._show_object_labels else ""
                if lbl:
                    tw = fm.horizontalAdvance(lbl) + 4
                    th = fm.height() + 2
                    lx = r.x()
                    ly = r.y()
                    bg = QColor(color)
                    bg.setAlpha(200)
                    painter.setPen(Qt.PenStyle.NoPen)
                    painter.setBrush(bg)
                    painter.drawRect(QRectF(lx, ly, tw, th))
                    painter.setPen(QColor(255, 255, 255))
                    painter.setBrush(Qt.BrushStyle.NoBrush)
                    painter.drawText(int(lx + 2), int(ly + th - 3), lbl)

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
                # Selected accent border + handles (handles only when editing allowed)
                if i == self._selected_idx:
                    accent = QPen(QColor(255, 255, 0, 240), 3)
                    painter.setBrush(Qt.BrushStyle.NoBrush)
                    painter.setPen(accent)
                    painter.drawRect(r.adjusted(-3, -3, 3, 3))
                    if self._edit_bboxes:
                        painter.setPen(QPen(QColor(80, 80, 80), 1))
                        painter.setBrush(QColor(255, 255, 255))
                        for hr in self._handle_rects(r):
                            painter.drawRect(hr)

        # Independent labels pass — draws object labels regardless of whether
        # bbox outlines are visible. Respects type filters / content mask so
        # labels only appear for object types the user is currently viewing.
        if self._show_object_labels and self._bboxes_pts:
            label_font = QFont("Consolas", 8)
            painter.setFont(label_font)
            fm = painter.fontMetrics()
            label_bg = QColor(20, 30, 60, 200)
            label_fg = QColor(255, 255, 255)
            for bbox in self._bboxes_pts:
                if bbox.get("hidden", False):
                    continue
                bbox_type = bbox.get("type", "unknown")
                # Hide label for types turned off via View / Show-objects
                if bbox_type in self._visible_layers and not self._visible_layers[bbox_type]:
                    continue
                if self._content_mask.get(bbox_type, False):
                    continue
                label = bbox.get("label", "")
                if not label:
                    continue
                r = self._bbox_rect_px(bbox)
                if not r:
                    continue
                tw = fm.horizontalAdvance(label) + 6
                th = fm.height() + 2
                lx = int(r.x())
                ly = int(r.y() - th)
                if ly < 0:
                    ly = int(r.y())  # overflow → paint inside top of bbox
                painter.setPen(Qt.PenStyle.NoPen)
                painter.setBrush(label_bg)
                painter.drawRect(lx, ly, tw, th)
                painter.setPen(label_fg)
                painter.drawText(lx + 3, ly + fm.ascent() + 1, label)

        # ── Probe overlay (Recognition tab: PyMuPDF-method test results) ──
        # Visualize detected objects as translucent magenta fills — no borders,
        # no labels, just "this is where the probe found content".
        if self._probe_rects:
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(QColor(230, 30, 200, 90))  # 35% opacity magenta
            for (x0, y0, x1, y1), _lbl in self._probe_rects:
                rect = QRectF(x0 * zf, y0 * zf + h,
                              max(1.0, (x1 - x0) * zf),
                              max(1.0, (y1 - y0) * zf))
                painter.drawRect(rect)

        if self._show_bboxes and self._bboxes_pts:
            layers = self._visible_layers

            # Pure PDF rendering: simple outline, no fill, no labels
            if self._detect_method_idx == 10:
                # Pure PDF: just outlines, clickable objects, draggable when selected
                outline_pen = QPen(QColor(100, 100, 100, 200), 1)
                selected_pen = QPen(QColor(0, 0, 0, 255), 2)  # black dashed
                selected_pen.setStyle(Qt.PenStyle.DashLine)
                dragging_pen = QPen(QColor(0, 0, 0, 255), 2)  # black dashed
                dragging_pen.setStyle(Qt.PenStyle.DashLine)

                show_template = layers.get("template", False)
                zf = self._zoom_factor
                dx_px, dy_px = self._pure_pdf_drag_offset[0] * zf, self._pure_pdf_drag_offset[1] * zf
                for i, bbox in enumerate(self._bboxes_pts):
                    if bbox.get("hidden", False):
                        continue
                    is_tpl = any(bbox.get(k) for k in [
                        "is_template_exact", "is_template_medium", "is_template_loose"])
                    if is_tpl and not show_template:
                        continue
                    # Type-based visibility filter (Layout's Text/Vectors/Images/Tables)
                    bbox_type = bbox.get("type", "unknown")
                    if bbox_type in layers and not layers[bbox_type]:
                        continue
                    # Respect content-mask (Layout's "Show objects" dropdown):
                    # if the content for this type is hidden, don't draw a bbox
                    # outline either — otherwise the code-generated rectangle
                    # looks like real PDF content.
                    if self._content_mask.get(bbox_type, False):
                        continue
                    r = self._bbox_rect_px(bbox)
                    if not r:
                        continue

                    # Skip drawing original bbox if currently dragging (will draw at new position)
                    if i == self._selected_idx and self._drag_mode == "pure_pdf_move":
                        dragged_r = r.translated(dx_px, dy_px)
                        # Draw pixmap content from original position at new dragged position
                        if self._pixmap and not self._pixmap.isNull():
                            # Extract bbox region from pixmap (r has header offset already applied)
                            src_x = int(r.x())
                            src_y = int(r.y())
                            src_w = int(r.width())
                            src_h = int(r.height())
                            # Clamp to pixmap bounds
                            if (src_x >= 0 and src_y >= 0 and
                                src_x + src_w <= self._pixmap.width() and
                                src_y + src_h <= self._pixmap.height()):
                                # Copy content from original position
                                content_pix = self._pixmap.copy(src_x, src_y, src_w, src_h)
                                # Draw at dragged position
                                painter.drawPixmap(int(dragged_r.x()), int(dragged_r.y()), content_pix)
                        # Draw black dashed border
                        painter.setBrush(Qt.BrushStyle.NoBrush)
                        painter.setPen(dragging_pen)
                        painter.drawRect(dragged_r)
                    elif i == self._selected_idx:
                        painter.setBrush(Qt.BrushStyle.NoBrush)
                        painter.setPen(selected_pen)
                        painter.drawRect(r)
                    else:
                        # Table group: 80% transparent cyan fill
                        if bbox.get("_table_group"):
                            painter.setPen(QPen(QColor(0, 180, 200, 120), 1))
                            painter.setBrush(QColor(0, 200, 220, 51))  # ~80% transparent
                            painter.drawRect(r)
                        # Template hatch fill (Qt brush patterns)
                        elif bbox.get("is_template_exact"):
                            painter.setPen(QPen(QColor(180, 0, 180, 120), 1))
                            brush = QBrush(QColor(180, 0, 180, 80), Qt.BrushStyle.BDiagPattern)
                            painter.setBrush(brush)
                            painter.drawRect(r)
                        elif bbox.get("is_template_medium"):
                            painter.setPen(QPen(QColor(0, 180, 180, 120), 1))
                            brush = QBrush(QColor(0, 180, 180, 60), Qt.BrushStyle.FDiagPattern)
                            painter.setBrush(brush)
                            painter.drawRect(r)
                        elif bbox.get("is_template_loose"):
                            painter.setPen(QPen(QColor(255, 165, 0, 120), 1))
                            brush = QBrush(QColor(255, 165, 0, 60), Qt.BrushStyle.HorPattern)
                            painter.setBrush(brush)
                            painter.drawRect(r)
                        else:
                            painter.setBrush(Qt.BrushStyle.NoBrush)
                            painter.setPen(outline_pen)
                            painter.drawRect(r)
                painter.end()
                return

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
                    if any(bbox.get(k) for k in ["is_template_exact", "is_template_medium", "is_template_loose"]) and not show_template:
                        continue
                    r = self._bbox_rect_px(bbox)
                    if not r:
                        continue
                    # Template exact: diagonal hatch ↘ (magenta, thick)
                    if bbox.get("is_template_exact"):
                        hatch_color = QColor(180, 0, 180, 80)
                        painter.setPen(QPen(QColor(180, 0, 180, 120), 1))
                        painter.setBrush(Qt.BrushStyle.NoBrush)
                        painter.drawRect(r)
                        painter.setPen(QPen(hatch_color, 2))  # Thick lines
                        step = 8
                        rx, ry = int(r.x()), int(r.y())
                        rw, rh = int(r.width()), int(r.height())
                        painter.setClipRect(r)
                        for d in range(-rh, rw, step):
                            painter.drawLine(rx + d, ry, rx + d + rh, ry + rh)
                        painter.setClipping(False)
                    # Template medium: diagonal hatch ↙ (cyan, thin)
                    elif bbox.get("is_template_medium"):
                        hatch_color = QColor(0, 180, 180, 60)
                        painter.setPen(QPen(QColor(0, 180, 180, 120), 1))
                        painter.setBrush(Qt.BrushStyle.NoBrush)
                        painter.drawRect(r)
                        painter.setPen(QPen(hatch_color, 1))  # Thin lines
                        step = 8
                        rx, ry = int(r.x()), int(r.y())
                        rw, rh = int(r.width()), int(r.height())
                        painter.setClipRect(r)
                        for d in range(-rh, rw, step):
                            painter.drawLine(rx + rw - d, ry, rx + rw - d - rh, ry + rh)
                        painter.setClipping(False)
                    # Template loose: horizontal hatch (yellow/orange, thin)
                    elif bbox.get("is_template_loose"):
                        hatch_color = QColor(255, 165, 0, 60)
                        painter.setPen(QPen(QColor(255, 165, 0, 120), 1))
                        painter.setBrush(Qt.BrushStyle.NoBrush)
                        painter.drawRect(r)
                        painter.setPen(QPen(hatch_color, 1))
                        step = 8
                        rx, ry = int(r.x()), int(r.y())
                        rw, rh = int(r.width()), int(r.height())
                        painter.setClipRect(r)
                        for d in range(0, rh, step):
                            painter.drawLine(rx, ry + d, rx + rw, ry + d)
                        painter.setClipping(False)
                    else:
                        painter.setBrush(bbox_fill)
                    painter.setPen(bbox_pen)
                    painter.drawRect(r)
                    # Label on outer top side: dark background, white text
                    label = bbox.get("label", bbox.get("type", "")) if self._show_object_labels else ""
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

                    # Resize handles — only when editing is allowed
                    if self._edit_bboxes:
                        painter.setPen(QPen(QColor(80, 80, 80), 1))
                        painter.setBrush(QColor(255, 255, 255))
                        for hr in self._handle_rects(r):
                            painter.drawRect(hr)

        # Cropping is handled by PDF cropbox — no visual overlay needed

        # Layers-tab hover highlight — drawn on top of everything else so
        # it's always visible regardless of the underlying filter overlays.
        if self._layers_highlight_bbox is not None:
            # Derive the actual display zoom from the rendered pixmap's
            # width ÷ page width in points. This avoids relying on
            # self._zoom_factor, which stays stale in LayersView because
            # that view never calls set_bboxes (the only set_* method
            # besides set_placeholder that refreshes _zoom_factor).
            page_w_pt = self._page_size_pt[0] if self._page_size_pt else 0.0
            pix_w = self._pixmap.width() if self._pixmap else 0
            actual_zf = (pix_w / page_w_pt) if (page_w_pt > 0 and pix_w > 0) else zf

            x0, y0, x1, y1 = self._layers_highlight_bbox
            rect = QRectF(
                x0 * actual_zf, y0 * actual_zf + h,
                max(1.0, (x1 - x0) * actual_zf),
                max(1.0, (y1 - y0) * actual_zf),
            )

            # Pulsing effect: phase 0..1 → sin wave → 0.3..1.0 intensity.
            import math
            pulse = 0.5 + 0.5 * math.sin(self._layers_highlight_phase * 2 * math.pi)
            fill_alpha = int(40 + 80 * pulse)    # 40..120
            border_alpha = int(180 + 75 * pulse)  # 180..255
            border_w = 2.0 + 2.0 * pulse         # 2..4 px

            # Dark outline so the halo reads on both white and black pages.
            painter.setPen(QPen(QColor(20, 20, 20, 200), border_w + 2.5))
            painter.setBrush(Qt.BrushStyle.NoBrush)
            painter.drawRect(rect)
            # Yellow halo — pulsing border and translucent fill.
            painter.setPen(QPen(QColor(255, 220, 0, border_alpha), border_w))
            painter.setBrush(QColor(255, 220, 0, fill_alpha))
            painter.drawRect(rect)

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
    steps_out: list | None = None,  # Optional: track intermediate steps
    col_first: bool = False,  # Scan columns first (primary), rows second (if True)
) -> tuple[list[tuple[int, int, int, int]], list[tuple[int, int, int, int]]]:
    """Shared scanline v1: grow-from-seed on binary image.

    Returns (bboxes, artifacts). Skips skip_rects areas.
    Full-page frames (>80% area) are erased and skipped.

    If steps_out is provided (list), appends (bboxes, artifacts, scan_coord) tuples
    for each object detected, allowing animation of the detection process.
    scan_coord is positive for row scans (y), negative for column scans (-x).

    If col_first=True, scans columns first (outer loop x), rows second (inner loop y).
    """
    page_area = h * w
    all_scan = list(skip_rects or [])
    bboxes: list[tuple[int, int, int, int]] = []
    artifacts: list[tuple[int, int, int, int]] = []

    if col_first:
        # Column-first scanning: outer loop over columns (x), inner loop over rows (y)
        x = 0
        while x < w:
            y = 0
            while y < h:
                skipped = False
                for (bx0, by0, bx1, by1) in all_scan:
                    if bx0 - 2 <= x <= bx1 + 2 and by0 - 2 <= y <= by1 + 2:
                        y = int(by1) + 3; skipped = True; break
                if skipped:
                    continue
                if binary[y, x]:
                    gx0, gy0 = x, y
                    gx1, gy1 = min(w, x + 1), min(h, y + 1)
                    # Track seed step (include previously found bboxes) - use negative x for column scan
                    if steps_out is not None:
                        steps_out.append((bboxes + [(gx0, gy0, gx1, gy1)], [], -x))
                    for _ in range(4000):
                        grown = False
                        t = max(0, gy0 - margin)
                        if t < gy0 and binary[t:gy0, gx0:gx1].any():
                            gy0 = max(0, gy0 - 1); grown = True
                            if steps_out is not None:
                                steps_out.append((bboxes + [(gx0, gy0, gx1, gy1)], [], -x))
                        b = min(h, gy1 + margin)
                        if b > gy1 and binary[gy1:b, gx0:gx1].any():
                            gy1 = min(h, gy1 + 1); grown = True
                            if steps_out is not None:
                                steps_out.append((bboxes + [(gx0, gy0, gx1, gy1)], [], -x))
                        l = max(0, gx0 - margin)
                        if l < gx0 and binary[gy0:gy1, l:gx0].any():
                            gx0 = max(0, gx0 - 1); grown = True
                            if steps_out is not None:
                                steps_out.append((bboxes + [(gx0, gy0, gx1, gy1)], [], -x))
                        r = min(w, gx1 + margin)
                        if r > gx1 and binary[gy0:gy1, gx1:r].any():
                            gx1 = min(w, gx1 + 1); grown = True
                            if steps_out is not None:
                                steps_out.append((bboxes + [(gx0, gy0, gx1, gy1)], [], -x))
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
                        y += 1; continue
                    all_scan.append(bt)
                    if bw < min_obj and bh < min_obj:
                        artifacts.append(bt)
                    else:
                        bboxes.append(bt)
                    # Track final step with accumulated bboxes
                    if steps_out is not None:
                        steps_out.append((bboxes.copy(), artifacts.copy(), -x))
                    y = gy1 + 3
                else:
                    y += 1
            x += 1
    else:
        # Row-first scanning (original): outer loop over rows (y), inner loop over columns (x)
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
                    # Track seed step (include previously found bboxes)
                    if steps_out is not None:
                        steps_out.append((bboxes + [(gx0, gy0, gx1, gy1)], [], y))
                    for _ in range(4000):
                        grown = False
                        t = max(0, gy0 - margin)
                        if t < gy0 and binary[t:gy0, gx0:gx1].any():
                            gy0 = max(0, gy0 - 1); grown = True
                            if steps_out is not None:
                                steps_out.append((bboxes + [(gx0, gy0, gx1, gy1)], [], y))
                        b = min(h, gy1 + margin)
                        if b > gy1 and binary[gy1:b, gx0:gx1].any():
                            gy1 = min(h, gy1 + 1); grown = True
                            if steps_out is not None:
                                steps_out.append((bboxes + [(gx0, gy0, gx1, gy1)], [], y))
                        l = max(0, gx0 - margin)
                        if l < gx0 and binary[gy0:gy1, l:gx0].any():
                            gx0 = max(0, gx0 - 1); grown = True
                            if steps_out is not None:
                                steps_out.append((bboxes + [(gx0, gy0, gx1, gy1)], [], y))
                        r = min(w, gx1 + margin)
                        if r > gx1 and binary[gy0:gy1, gx1:r].any():
                            gx1 = min(w, gx1 + 1); grown = True
                            if steps_out is not None:
                                steps_out.append((bboxes + [(gx0, gy0, gx1, gy1)], [], y))
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
                    # Track final step with accumulated bboxes
                    if steps_out is not None:
                        steps_out.append((bboxes.copy(), artifacts.copy(), y))
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


class PassMarkerSlider(QSlider):
    """QSlider with visual markers indicating Pass boundaries (Pre-Pass, Pass 1, Pass 2, Final)."""

    # Signal emitted on each effective step (after snap logic)
    stepped = Signal(int)

    def __init__(self, orientation=Qt.Orientation.Horizontal, parent=None):
        super().__init__(orientation, parent)
        self._markers: list[tuple[int, str, str]] = []
        self._marker_steps: set[int] = set()
        self._prev_step: int = 0
        self._frozen: bool = False
        self._freeze_timer = QTimer(self)
        self._freeze_timer.setSingleShot(True)
        self._freeze_timer.timeout.connect(self._unfreeze)

    def set_markers(self, markers: list[tuple[int, str, str]]) -> None:
        self._markers = sorted(markers, key=lambda m: m[0])
        self._marker_steps = {m[0] for m in self._markers} - {0}
        self._frozen = False
        self._freeze_timer.stop()
        self.update()

    def sliderChange(self, change):
        """Intercept all value changes (mouse drag, keyboard, programmatic)."""
        super().sliderChange(change)
        if change != self.SliderChange.SliderValueChange:
            return

        value = self.value()
        if self._frozen:
            # During freeze: force slider back to snapped marker
            super().setValue(self._prev_step)
            return

        prev = self._prev_step
        if value == prev:
            return

        # Check if crossing a marker
        for ms in self._marker_steps:
            crossed = (prev < ms <= value) or (value <= ms < prev)
            if crossed:
                self._prev_step = ms
                super().setValue(ms)
                self.stepped.emit(ms)
                self._frozen = True
                self._freeze_timer.start(400)
                return

        self._prev_step = value
        self.stepped.emit(value)

    def _unfreeze(self) -> None:
        self._frozen = False

    def set_value_direct(self, value: int) -> None:
        """Set value bypassing snap logic (for programmatic/play use)."""
        self._prev_step = value
        self._frozen = False
        self._freeze_timer.stop()
        super().setValue(value)

    def paintEvent(self, event):
        """Draw slider with pass boundary markers and labels."""
        super().paintEvent(event)

        if not self._markers or self.maximum() <= 0:
            return

        # Get slider rect
        slider_rect = self.rect()
        h = slider_rect.height()
        y_center = h // 2

        # Draw markers
        from PySide6.QtGui import QColor, QPainter, QFont
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        # Set font for labels
        font = QFont()
        font.setPointSize(8)
        painter.setFont(font)

        for step_idx, label, color_hex in self._markers:
            # Map step index to pixel position on slider
            x_pos = self._step_to_pixel(step_idx)
            if x_pos is None:
                continue

            # Draw vertical marker line
            color = QColor(color_hex)
            painter.setPen(QPen(color, 2))
            marker_height = h // 3
            painter.drawLine(x_pos, y_center - marker_height, x_pos, y_center + marker_height)

            # Draw small circle at center
            painter.setBrush(color)
            painter.drawEllipse(x_pos - 4, y_center - 4, 8, 8)

            # Draw label text above marker
            text_rect = painter.fontMetrics().boundingRect(label)
            text_x = x_pos - text_rect.width() // 2
            text_y = y_center - marker_height - 15
            painter.setPen(QPen(QColor("#333333")))
            painter.drawText(text_x, text_y, label)

        painter.end()

    def _step_to_pixel(self, step: int) -> int | None:
        """Convert step index to pixel position on slider."""
        max_steps = self.maximum()
        if max_steps <= 0:
            return None

        slider_width = self.width() - 20  # Leave margin for thumb
        x_base = 10
        x_pos = x_base + int((step / max_steps) * slider_width)
        return x_pos



class BboxDetectionDialog(QWidget):
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
        "Pure PDF",
        "Hybrid 2-pass V.2",
        "Scanline PDF Filter",
    ]

    def __init__(self, doc, page_idx: int, parent=None):
        super().__init__(parent)
        import numpy as np
        self.setWindowTitle(f"Bbox Testbench — Page {page_idx+1}")
        self.setWindowFlags(Qt.WindowType.Window)
        self.resize(900, 750)

        self._doc = doc
        self._page_idx = page_idx
        page = doc[page_idx]
        self._page = page
        self._dpi = 72           # full page display
        self._filter_dpi = 72    # filtered renders for detection (same as display)
        pix = page.get_pixmap(dpi=self._dpi)
        self._page_img_full = np.frombuffer(pix.samples, dtype=np.uint8).reshape(
            pix.height, pix.width, pix.n
        ).copy()
        self._page_img = self._page_img_full.copy()
        self._h, self._w = pix.height, pix.width
        self._scale = self._dpi / 72  # px per pt

        # Pre-render text-only layer for pre-pass visualization (at display DPI)
        try:
            samples, tw, th, tn, tstride = get_pixmap_filtered(
                doc, page_idx, self._dpi,
                show_text=True, show_images=False, show_drawings=False,
            )
            self._page_img_text_only = np.frombuffer(samples, dtype=np.uint8).reshape(
                th, tw, tn
            ).copy()
        except Exception:
            self._page_img_text_only = None
        self._last_pixmap: QPixmap | None = None

        gray = np.mean(self._page_img[:, :, :3], axis=2)
        self._binary = (gray < 240).astype(np.uint8)

        # Save unmasked binary for hybrid 2-pass
        self._binary_raw = self._binary.copy()

        # Extract native PDF text blocks and mask them from binary
        # Use dict extraction + line splitting to avoid PyMuPDF merging
        # horizontally separated text into one block.
        self._pdf_text_blocks: list[tuple[int, int, int, int]] = []
        td = page.get_text("dict")
        for b in td["blocks"]:
            if b["type"] == 0:  # text block
                lines = b.get("lines", [])
                sub_groups = PreviewView._split_text_block_lines(lines)
                for sg in sub_groups:
                    bx0 = min(ln["bbox"][0] for ln in sg)
                    by0 = min(ln["bbox"][1] for ln in sg)
                    bx1 = max(ln["bbox"][2] for ln in sg)
                    by1 = max(ln["bbox"][3] for ln in sg)
                    x0 = int(bx0 * self._scale)
                    y0 = int(by0 * self._scale)
                    x1 = int(bx1 * self._scale)
                    y1 = int(by1 * self._scale)
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
        self._show_mask_cb = QCheckBox("Mask")
        self._show_mask_cb.setStyleSheet("font-size: 10px;")
        self._show_mask_cb.toggled.connect(self._render)
        top_bar.addWidget(self._show_mask_cb)

        # PDF layer filter dropdown
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.VLine)
        sep.setStyleSheet("color: #888;")
        top_bar.addWidget(sep)
        self._layer_btn = QToolButton()
        self._layer_btn.setText("Layers ▾")
        self._layer_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        self._layer_btn.setStyleSheet("font-size: 10px;")
        layer_menu = QMenu(self._layer_btn)
        self._layer_cbs: dict[str, QCheckBox] = {}
        for key, label in [("text", "Text"), ("images", "Images"), ("drawings", "Drawings")]:
            w = QWidget()
            row = QHBoxLayout(w)
            row.setContentsMargins(4, 1, 4, 1)
            cb = QCheckBox(label)
            cb.setChecked(True)
            cb.toggled.connect(self._on_layer_filter_changed)
            row.addWidget(cb)
            row.addStretch()
            wa = QWidgetAction(layer_menu)
            wa.setDefaultWidget(w)
            layer_menu.addAction(wa)
            self._layer_cbs[key] = cb
        self._layer_btn.setMenu(layer_menu)
        top_bar.addWidget(self._layer_btn)

        top_bar.addStretch()
        layout.addLayout(top_bar)

        # Info + progress bar row
        info_bar = QHBoxLayout()
        self._info = QLabel()
        self._info.setStyleSheet("font: 11px Consolas;")
        info_bar.addWidget(self._info)
        info_bar.addStretch()
        from PySide6.QtWidgets import QProgressBar
        self._progress_bar = QProgressBar()
        self._progress_bar.setFixedHeight(14)
        self._progress_bar.setFixedWidth(200)
        self._progress_bar.setTextVisible(True)
        self._progress_bar.setStyleSheet("font-size: 9px;")
        self._progress_bar.hide()
        info_bar.addWidget(self._progress_bar)
        layout.addLayout(info_bar)

        self._image_label = QLabel()
        self._image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._scroll = QScrollArea()
        self._scroll.setWidget(self._image_label)
        self._scroll.setWidgetResizable(True)
        layout.addWidget(self._scroll)

        # Animation slider + play button + stage nav
        anim_bar = QHBoxLayout()
        self._prev_stage_btn = QPushButton("◀◀")
        self._prev_stage_btn.setFixedSize(32, 24)
        self._prev_stage_btn.setToolTip("Previous stage")
        self._prev_stage_btn.clicked.connect(self._on_prev_stage)
        anim_bar.addWidget(self._prev_stage_btn)
        self._next_stage_btn = QPushButton("▶▶")
        self._next_stage_btn.setFixedSize(32, 24)
        self._next_stage_btn.setToolTip("Next stage")
        self._next_stage_btn.clicked.connect(self._on_next_stage)
        anim_bar.addWidget(self._next_stage_btn)
        self._play_btn = QPushButton("▶ Play steps")
        self._play_btn.setFixedWidth(100)
        self._play_btn.setFixedHeight(24)
        self._play_btn.clicked.connect(self._on_play_methods)
        anim_bar.addWidget(self._play_btn)

        self._anim_slider = PassMarkerSlider(Qt.Orientation.Horizontal)
        self._anim_slider.setMinimum(0)
        self._anim_slider.setMaximum(0)
        self._anim_slider.setMinimumHeight(50)  # Space for marker labels
        self._anim_slider.stepped.connect(self._on_anim_step)
        anim_bar.addWidget(self._anim_slider)
        self._anim_label = QLabel("—")
        self._anim_label.setFixedWidth(120)
        self._anim_label.setStyleSheet("font: 10px Consolas;")
        anim_bar.addWidget(self._anim_label)
        layout.addLayout(anim_bar)

        # Animation timer for cycling through detection steps
        self._anim_timer = QTimer(self)
        self._anim_timer.timeout.connect(self._on_anim_tick)
        self._anim_step_idx = 0
        self._anim_playing = False

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
        # Pass boundary markers for slider: (step_index, label, color_hex)
        self._pass_markers: list[tuple[int, str, str]] = []
        self._pending_algo_idx: int = 0
        self._algo_ran: bool = False
        self._on_algo_changed(0)
        self.show()

    # ── Algorithm dispatcher ─────────────────────────────────────

    def _on_algo_changed(self, idx: int) -> None:
        # Skip non-testbench methods
        if idx in (9, 10):
            return

        # Don't run detection immediately — just reset and show initial state
        self._pending_algo_idx = idx
        self._algo_ran = False
        self._bboxes = []
        self._artifacts = []
        self._detection_steps = []
        self._pass_markers = []
        self._text_bboxes = []
        self._nontext_bboxes = []
        self._anim_slider.setMaximum(0)
        self._anim_slider.set_markers([])
        self._progress_bar.hide()
        self._info.setText(
            f"{self._ALGOS[idx]} — scroll slider or press Play to run"
        )
        # Show clean page
        self._render_frame([], [], -1)

    def _ensure_algo_ran(self) -> None:
        """Run detection if not yet executed for current method."""
        if self._algo_ran:
            return
        self._algo_ran = True

        import time
        from PySide6.QtWidgets import QApplication

        idx = self._pending_algo_idx
        self._info.setText(f"Running: {self._ALGOS[idx]}...")
        QApplication.processEvents()

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
            lambda: ([], []),
            lambda: ([], []),
            self._run_hybrid_2pass_v2,
            self._run_scanline_pdf_filter,
        ]
        self._bboxes, self._artifacts = runners[idx]()
        self._progress_bar.hide()
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
        # Update animation slider and pass markers
        n_steps = len(self._detection_steps)
        if n_steps > 0:
            self._anim_slider.setMaximum(n_steps - 1)
            self._anim_slider.set_value_direct(0)
        else:
            self._anim_slider.setMaximum(0)
        if self._pass_markers:
            self._anim_slider.set_markers(self._pass_markers)
        self._render()

    # ── v1: Original scanline (margin=3, 1px steps, O(n²) skip) ──

    def _run_v1_original(self):
        import numpy as np
        self._detection_steps = []
        min_obj = int(10 * self._scale)
        # Use a copy so intermediate steps see the correct binary state
        binary_copy = self._binary.copy()
        bboxes, artifacts = _scanline_v1_core(
            binary_copy, self._h, self._w, margin=3, min_obj=min_obj,
            steps_out=self._detection_steps,  # Track every object detection
        )
        self._last_margin = 3
        # Add final result if no intermediate steps were tracked
        if not self._detection_steps:
            self._detection_steps.append((bboxes, artifacts, -1))
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
        self._detection_steps = []
        min_obj = int(10 * self._scale)
        visited = np.zeros((self._h, self._w), dtype=bool)
        bboxes: list[tuple[int, int, int, int]] = []
        artifacts: list[tuple[int, int, int, int]] = []
        binary_copy = self._binary.copy()  # Use copy to preserve original state

        for y in range(self._h):
            x = 0
            while x < self._w:
                if visited[y, x] or not binary_copy[y, x]:
                    x += 1
                    continue
                bbox = self._grow_fast(x, y, margin=3, binary=binary_copy)
                x0, y0, x1, y1 = bbox
                visited[y0:y1, x0:x1] = True
                if (x1 - x0) < min_obj and (y1 - y0) < min_obj:
                    artifacts.append(bbox)
                else:
                    bboxes.append(bbox)
                # Track intermediate step for each object detected
                self._detection_steps.append((bboxes.copy(), artifacts.copy(), y))
                x = x1 + 1
        self._last_margin = 3
        if not self._detection_steps:
            self._detection_steps.append((bboxes, artifacts, -1))
        return bboxes, artifacts

    def _grow_fast(self, cx: int, cy: int, margin: int = 3, binary=None):
        """Fast grow: jump directly to the nearest/farthest content pixel."""
        import numpy as np
        x0, y0 = cx, cy
        x1, y1 = min(self._w, cx + 1), min(self._h, cy + 1)
        if binary is None:
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
        self._detection_steps = []
        min_obj = int(10 * self._scale)
        margin = self._compute_adaptive_margin()
        self._last_margin = margin
        visited = np.zeros((self._h, self._w), dtype=bool)
        bboxes: list[tuple[int, int, int, int]] = []
        artifacts: list[tuple[int, int, int, int]] = []
        binary_copy = self._binary.copy()  # Use copy to preserve original state

        for y in range(self._h):
            x = 0
            while x < self._w:
                if visited[y, x] or not binary_copy[y, x]:
                    x += 1
                    continue
                bbox = self._grow_fast(x, y, margin=margin, binary=binary_copy)
                x0, y0, x1, y1 = bbox
                visited[y0:y1, x0:x1] = True
                if (x1 - x0) < min_obj and (y1 - y0) < min_obj:
                    artifacts.append(bbox)
                else:
                    bboxes.append(bbox)
                # Track intermediate step for each object detected
                self._detection_steps.append((bboxes.copy(), artifacts.copy(), y))
                x = x1 + 1
        if not self._detection_steps:
            self._detection_steps.append((bboxes, artifacts, -1))
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
        self._detection_steps = []
        try:
            import cv2
        except ImportError:
            logging.warning("OpenCV not installed — CCA unavailable")
            self._last_margin = 0
            self._detection_steps.append(([], [], -1))
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
        # Add final result as animation step
        self._detection_steps.append((bboxes, artifacts, -1))
        return bboxes, artifacts

    # ── MSER text zone detection ───────────────────────────────

    def _run_mser_text(self):
        """Detect text regions via MSER + geometric filtering + row grouping.

        MSER finds stable extremal regions (character candidates).
        Filter by size/aspect → group into text lines → merge into zones.
        Non-text objects detected via CCA on the remaining pixels.
        """
        import numpy as np
        self._detection_steps = []
        try:
            import cv2
        except ImportError:
            logging.warning("OpenCV not installed — MSER unavailable")
            self._last_margin = 0
            self._detection_steps.append(([], [], -1))
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
            self._detection_steps.append(([], [], -1))
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
        # Add final result as animation step
        self._detection_steps.append((bboxes, artifacts, -1))
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
        # Add final result as animation step
        self._detection_steps.append((text_bboxes + nontext, scan_artifacts, -1))
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
        self._detection_steps = []

        # Pass 1: scanline on masked binary (text removed) - use copy
        min_obj = int(10 * self._scale)
        pass1_steps: list = []
        pass1_bboxes, pass1_artifacts = _scanline_v1_core(
            self._binary.copy(), self._h, self._w, margin=3, min_obj=min_obj,
            steps_out=pass1_steps,
        )
        self._detection_steps.extend(pass1_steps)

        # Pass 2: scanline on raw binary (with text) - use fresh copy
        min_obj = int(10 * self._scale)
        pass2_steps: list = []
        pass2_bboxes, _ = _scanline_v1_core(
            self._binary_raw.copy(), self._h, self._w, margin=3, min_obj=min_obj,
            steps_out=pass2_steps,
        )
        self._detection_steps.extend(pass2_steps)

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
        self._detection_steps.append((all_bboxes, pass1_artifacts, -1))
        return all_bboxes, pass1_artifacts

    def _run_hybrid_2pass_v2(self):
        """Hybrid 2-pass V.2: Pre-Pass detects inside PDF objects, then Pass 1+2.

        Pre-Pass: For each PDF text/table object:
          - Run scanline+grow ONLY inside that object's bounds
          - Detect internal bboxes with animation tracking
          - Invert mask of detected areas (mark as processed)
        Pass 1: scanline on raw binary with refined text masks
        Pass 2: merge fragmented objects on full binary
        """
        import numpy as np
        self._detection_steps = []
        self._pass_markers = []

        gray = np.mean(self._page_img[:, :, :3], axis=2)
        min_obj = int(10 * self._scale)

        # Pre-Pass: Show initial state with PDF text blocks
        self._detection_steps.append((list(self._pdf_text_blocks), [], -1))

        # Pre-Pass: single top-to-bottom scanline, only inside text block areas
        # Create binary where only pixels inside pdf_text_blocks are active
        prepass_binary = np.zeros_like(self._binary_raw)
        for (x0, y0, x1, y1) in self._pdf_text_blocks:
            prepass_binary[y0:y1, x0:x1] = self._binary_raw[y0:y1, x0:x1]

        prepass_steps: list = []
        prepass_bboxes_raw, prepass_artifacts = _scanline_v1_core(
            prepass_binary, self._h, self._w, margin=3, min_obj=min_obj,
            steps_out=prepass_steps,
            col_first=False,
        )

        # Add animation steps with growing boxes
        prepass_bboxes: list[tuple[int, int, int, int]] = []
        for step_bboxes, step_artifacts, scan_y in prepass_steps:
            self._detection_steps.append(
                (list(self._pdf_text_blocks) + step_bboxes, [], scan_y)
            )
        prepass_bboxes = list(prepass_bboxes_raw)

        # Pre-Pass final step
        self._detection_steps.append(
            (list(self._pdf_text_blocks) + prepass_bboxes, [], -1)
        )

        # Refine text skip rectangles for Pass 1
        text_skip_refined = []
        for tb in self._pdf_text_blocks:
            x0, y0, x1, y1 = tb
            text_region = gray[y0:y1, x0:x1]
            dark_mask = text_region < 220
            row_has_text = np.any(dark_mask, axis=1)
            row_indices = np.where(row_has_text)[0]
            if len(row_indices) == 0:
                continue
            first_row = row_indices[0]
            last_row = row_indices[-1] + 1
            col_has_text = np.any(dark_mask[first_row:last_row, :], axis=0)
            col_indices = np.where(col_has_text)[0]
            if len(col_indices) == 0:
                continue
            first_col = col_indices[0]
            last_col = col_indices[-1] + 1
            refined_x0 = x0 + first_col
            refined_y0 = y0 + first_row
            refined_x1 = x0 + last_col
            refined_y1 = y0 + last_row
            text_skip_refined.append((refined_x0, refined_y0, refined_x1, refined_y1))

        # Mark Pass 1 start
        pass1_start_idx = len(self._detection_steps)

        # Pass 1: scanline with refined text masks (keep prepass mask, don't clear)
        pass1_steps: list = []
        pass1_bboxes, pass1_artifacts = _scanline_v1_core(
            self._binary_raw.copy(), self._h, self._w, margin=3, min_obj=min_obj,
            skip_rects=text_skip_refined,
            steps_out=pass1_steps,
        )
        # Preserve prepass results by including them in each Pass 1 step
        for step_bboxes, step_artifacts, scan_y in pass1_steps:
            combined_bboxes = list(self._pdf_text_blocks) + prepass_bboxes + step_bboxes
            self._detection_steps.append((combined_bboxes, step_artifacts, scan_y))

        # Filter: remove bboxes >=80% covered by text
        pass1_filtered = []
        for bt in pass1_bboxes:
            bbox_area = max((bt[2]-bt[0]) * (bt[3]-bt[1]), 1)
            text_cover = 0.0
            for tb in self._pdf_text_blocks:
                ix0 = max(bt[0], tb[0]); iy0 = max(bt[1], tb[1])
                ix1 = min(bt[2], tb[2]); iy1 = min(bt[3], tb[3])
                if ix0 < ix1 and iy0 < iy1:
                    text_cover += (ix1-ix0) * (iy1-iy0)
            if text_cover / bbox_area < 0.8:
                pass1_filtered.append(bt)

        # Mark Pass 2 start
        pass2_start_idx = len(self._detection_steps)

        # Pass 2: merge fragmented objects on full binary (keep all previous results)
        pass2_steps: list = []
        pass2_bboxes, _ = _scanline_v1_core(
            (gray < 240).astype(np.uint8), self._h, self._w, margin=3,
            min_obj=min_obj,
            steps_out=pass2_steps,
        )
        # Preserve previous results: PDF blocks + prepass + pass1 + current pass2
        for step_bboxes, step_artifacts, scan_y in pass2_steps:
            combined_bboxes = list(self._pdf_text_blocks) + prepass_bboxes + pass1_filtered + step_bboxes
            self._detection_steps.append((combined_bboxes, step_artifacts, scan_y))

        # Merge logic: Pass2 bbox containing 2+ Pass1 bboxes → table
        merged: list[tuple[int, int, int, int]] = []
        used_p1: set[int] = set()
        for p2 in pass2_bboxes:
            contained: list[int] = []
            for i, p1 in enumerate(pass1_filtered):
                if i in used_p1:
                    continue
                p1_area = max((p1[2]-p1[0]) * (p1[3]-p1[1]), 1)
                ix0 = max(p1[0], p2[0]); iy0 = max(p1[1], p2[1])
                ix1 = min(p1[2], p2[2]); iy1 = min(p1[3], p2[3])
                if ix0 < ix1 and iy0 < iy1:
                    inter = (ix1-ix0) * (iy1-iy0)
                    if inter / p1_area >= 0.7:
                        contained.append(i)
            if len(contained) >= 2:
                merged.append(p2)
                used_p1.update(contained)

        remaining_p1 = [b for i, b in enumerate(pass1_filtered) if i not in used_p1]

        # Combine all results
        self._text_bboxes = list(self._pdf_text_blocks)
        self._nontext_bboxes = remaining_p1 + merged + prepass_bboxes
        self._last_margin = 3
        all_bboxes = self._text_bboxes + self._nontext_bboxes

        # Mark Final result
        final_idx = len(self._detection_steps)
        self._detection_steps.append((all_bboxes, pass1_artifacts, -1))

        # Build pass markers for slider
        self._pass_markers = [
            (0, "Pass 1", "#888888"),
            (pass1_start_idx, "Pass 2", "#4488ff"),
            (pass2_start_idx, "Pass 3", "#ff8844"),
            (final_idx, "Final", "#44ff44"),
        ]

        return all_bboxes, pass1_artifacts

    # ── Scanline PDF Filter: 3-pass filtered rendering ─────────

    def _progress(self, msg: str, step: int = 0, total: int = 0) -> None:
        """Update info label and progress bar, process events to keep UI alive."""
        from PySide6.QtWidgets import QApplication
        self._info.setText(msg)
        if total > 0:
            self._progress_bar.setMaximum(total)
            self._progress_bar.setValue(step)
            self._progress_bar.setFormat(f"{step}/{total}")
            self._progress_bar.show()
        QApplication.processEvents()

    def _run_scanline_pdf_filter(self):
        """3-pass detection on separately filtered PDF renders.

        Pass 1 (Text):     scanline+grow on text-only filtered render
        Pass 2 (Images):   render images only → scanline+grow → orange bboxes
        Pass 3 (Drawings): render drawings only → scanline+grow → blue bboxes

        Each pass shows its filtered page render during animation.
        """
        import numpy as np

        self._detection_steps = []
        self._pass_markers = []
        min_obj = int(10 * self._scale)

        all_text_bboxes: list[tuple[int, int, int, int]] = []
        all_nontext_bboxes: list[tuple[int, int, int, int]] = []
        all_artifacts: list[tuple[int, int, int, int]] = []

        # Store per-pass results for colored overlay
        self._pdf_filter_passes: list[tuple[str, list, list[int]]] = []
        # Store per-pass filtered images and step ranges for animation
        self._pdf_filter_pass_ranges: list[tuple[int, int]] = []  # (start, end) step indices
        self._pdf_filter_images: list = []  # filtered numpy images per pass
        # Per-pass native PDF object rects for dashed borders
        self._pdf_filter_obj_rects: list[list[tuple[int, int, int, int]]] = []

        # Extract native PDF objects scaled to display coords
        scale = self._scale
        page = self._page
        _text_rects: list[tuple[int, int, int, int]] = list(self._pdf_text_blocks)
        _image_rects: list[tuple[int, int, int, int]] = []
        _drawing_rects: list[tuple[int, int, int, int]] = []
        try:
            for img_info in page.get_images():
                try:
                    r = page.get_image_bbox(img_info)
                    if not r.is_empty:
                        _image_rects.append((
                            int(r.x0 * scale), int(r.y0 * scale),
                            int(r.x1 * scale), int(r.y1 * scale),
                        ))
                except Exception:
                    pass
        except Exception:
            pass
        try:
            for drw in page.get_drawings():
                r = drw.get("rect")
                if r and r[2] > r[0] and r[3] > r[1]:
                    _drawing_rects.append((
                        int(r[0] * scale), int(r[1] * scale),
                        int(r[2] * scale), int(r[3] * scale),
                    ))
        except Exception:
            pass

        # ── Pass 1: Text — scanline+grow on text-only filtered render ──
        self._progress("Pass 1: rendering text-only...", 0, 6)
        pass1_start = len(self._detection_steps)
        text_color = [0, 200, 0]

        # Render text-only image and create binary
        if self._page_img_text_only is not None:
            text_img = self._page_img_text_only.copy()
        else:
            text_img = self._page_img_full.copy()

        text_gray = np.mean(text_img[:, :, :3], axis=2)
        text_binary = (text_gray < 240).astype(np.uint8)

        self._progress("Pass 1: collecting text blocks...", 1, 6)
        # Use native PDF text block containers directly (fast, keeps words together)
        text_bboxes_raw = list(self._pdf_text_blocks)

        # Generate animation: show blocks appearing one by one
        for i in range(len(text_bboxes_raw)):
            self._detection_steps.append(
                (text_bboxes_raw[:i + 1], [], -1)
            )
        all_text_bboxes = list(text_bboxes_raw)
        self._pdf_filter_passes.append(("Pass 1", list(text_bboxes_raw), text_color))
        self._pdf_filter_images.append(text_img)
        self._pdf_filter_obj_rects.append(_text_rects)
        pass1_end = len(self._detection_steps)
        self._pdf_filter_pass_ranges.append((pass1_start, pass1_end))
        self._pass_markers.append((pass1_start, "Pass 1", "#00c800"))

        # ── Pass 2 & 3: Images and Drawings — scanline+grow ──
        scanline_passes = [
            ("Pass 2", False, True, False, [255, 120, 0]),    # orange — Images
            ("Pass 3", False, False, True, [0, 120, 255]),    # blue — Drawings
        ]

        dpi_ratio = self._filter_dpi / self._dpi  # scale factor hi-res → display
        filter_scale = self._filter_dpi / 72
        min_obj_hires = int(10 * filter_scale)

        for pass_name, show_text, show_images, show_drawings, color in scanline_passes:
            pass_num = 3 if pass_name == "Pass 2" else 5
            self._progress(f"{pass_name}: rendering {self._filter_dpi}dpi...", pass_num, 6)
            pass_start_idx = len(self._detection_steps)

            # Render at high DPI for detection
            try:
                samples, fw, fh, fn, fstride = get_pixmap_filtered(
                    self._doc, self._page_idx, self._filter_dpi,
                    show_text=show_text,
                    show_images=show_images,
                    show_drawings=show_drawings,
                )
                hires_img = np.frombuffer(samples, dtype=np.uint8).reshape(fh, fw, fn).copy()
                gray = np.mean(hires_img[:, :, :3], axis=2)
                binary = (gray < 240).astype(np.uint8)
            except Exception as e:
                logger.error("PDF filter pass '%s' failed: %s", pass_name, e)
                hires_img = None
                binary = np.zeros((int(self._h * dpi_ratio), int(self._w * dpi_ratio)), dtype=np.uint8)

            # Downscale hi-res render to display size for animation background
            if hires_img is not None:
                from PySide6.QtGui import QImage, QPixmap
                qimg = QImage(hires_img.data, fw, fh, fw * fn,
                              QImage.Format.Format_RGB888 if fn == 3
                              else QImage.Format.Format_RGBA8888)
                scaled = qimg.scaled(self._w, self._h,
                                     Qt.AspectRatioMode.IgnoreAspectRatio,
                                     Qt.TransformationMode.SmoothTransformation)
                ptr = scaled.bits()
                display_img = np.frombuffer(ptr, dtype=np.uint8).reshape(
                    self._h, self._w, fn
                ).copy()
            else:
                display_img = self._page_img_full.copy()

            self._pdf_filter_images.append(display_img)

            self._progress(f"{pass_name}: scanline+grow...", pass_num + 1, 6)
            # Run scanline+grow at high DPI (no steps_out for speed)
            bh, bw = binary.shape[:2]
            pass_bboxes_hires, pass_artifacts = _scanline_v1_core(
                binary, bh, bw, margin=3, min_obj=min_obj_hires,
                col_first=False,
            )

            # Scale bboxes back to display DPI coordinates
            inv = 1.0 / dpi_ratio
            pass_bboxes = [
                (int(x0 * inv), int(y0 * inv), int(x1 * inv), int(y1 * inv))
                for x0, y0, x1, y1 in pass_bboxes_hires
            ]

            # Generate animation: show bboxes appearing one by one
            prev_bboxes = all_text_bboxes + all_nontext_bboxes
            for i in range(len(pass_bboxes)):
                self._detection_steps.append(
                    (prev_bboxes + list(pass_bboxes[:i + 1]), [], -1)
                )

            self._pdf_filter_passes.append((pass_name, list(pass_bboxes), color))
            obj_rects = _image_rects if show_images else _drawing_rects
            self._pdf_filter_obj_rects.append(obj_rects)
            all_nontext_bboxes.extend(pass_bboxes)
            all_artifacts.extend(pass_artifacts)

            pass_end_idx = len(self._detection_steps)
            self._pdf_filter_pass_ranges.append((pass_start_idx, pass_end_idx))

            # Pass marker
            self._pass_markers.append(
                (pass_start_idx, pass_name, f"#{color[0]:02x}{color[1]:02x}{color[2]:02x}")
            )

        # Final step with all results
        final_idx = len(self._detection_steps)
        all_bboxes = all_text_bboxes + all_nontext_bboxes
        self._detection_steps.append((all_bboxes, all_artifacts, -1))
        self._pass_markers.append((final_idx, "Final", "#44ff44"))

        self._text_bboxes = all_text_bboxes
        self._nontext_bboxes = all_nontext_bboxes

        return all_bboxes, all_artifacts

    # ── PDF text blocks + Scanline for non-text ────────────────

    def _run_pdf_text_scanline(self):
        """Extract text blocks from PDF native layer, scanline for the rest.

        1. page.get_text("dict") → native text bboxes (green), split by X-overlap
        2. Mask text areas on binary image (white out)
        3. Scanline v1 on masked image → non-text objects (red)
        """
        import numpy as np
        self._detection_steps = []

        # Step 1: Extract native PDF text blocks (with line splitting)
        td = self._page.get_text("dict")
        scale = self._dpi / 72  # pt → px
        text_bboxes: list[tuple[int, int, int, int]] = []
        for b in td["blocks"]:
            if b.get("type") != 0:
                continue
            lines = b.get("lines", [])
            sub_groups = PreviewView._split_text_block_lines(lines)
            for sg in sub_groups:
                bx0 = min(ln["bbox"][0] for ln in sg)
                by0 = min(ln["bbox"][1] for ln in sg)
                bx1 = max(ln["bbox"][2] for ln in sg)
                by1 = max(ln["bbox"][3] for ln in sg)
                x0 = int(bx0 * scale)
                y0 = int(by0 * scale)
                x1 = int(bx1 * scale)
                y1 = int(by1 * scale)
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
        min_obj = int(10 * self._scale)
        nontext_steps: list = []
        nontext_bboxes, artifacts = _scanline_v1_core(
            binary_masked, self._h, self._w, margin=3, min_obj=min_obj,
            steps_out=nontext_steps,
        )
        self._detection_steps.extend(nontext_steps)

        self._text_bboxes = text_bboxes
        self._nontext_bboxes = nontext_bboxes
        self._last_margin = 3
        all_bboxes = text_bboxes + nontext_bboxes
        if not self._detection_steps:
            self._detection_steps.append((all_bboxes, artifacts, -1))
        return all_bboxes, artifacts

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
        self._ensure_algo_ran()
        if not self._detection_steps:
            return
        step = min(step, len(self._detection_steps) - 1)
        bboxes, artifacts, scan_y = self._detection_steps[step]

        # Swap page image for PDF Filter passes
        if (self._algo_combo.currentIndex() == 12
                and hasattr(self, '_pdf_filter_pass_ranges')
                and self._pdf_filter_images):
            for i, (start, end) in enumerate(self._pdf_filter_pass_ranges):
                if start <= step < end:
                    self._page_img = self._pdf_filter_images[i].copy()
                    break
            else:
                # Final step or beyond — show full page
                self._page_img = self._page_img_full.copy()

        self._anim_label.setText(
            f"Step {step+1}/{len(self._detection_steps)}"
        )
        self._render_frame(bboxes, artifacts, scan_y)

    def _on_prev_stage(self) -> None:
        """Jump slider to previous stage marker."""
        self._ensure_algo_ran()
        if not self._pass_markers:
            return
        current = self._anim_slider.value()
        # Find the largest marker step that is strictly less than current
        target = None
        for step_idx, _, _ in reversed(self._pass_markers):
            if step_idx < current:
                target = step_idx
                break
        if target is not None:
            self._anim_slider.set_value_direct(target)
            self._on_anim_step(target)

    def _on_next_stage(self) -> None:
        """Jump slider to next stage marker."""
        self._ensure_algo_ran()
        if not self._pass_markers:
            return
        current = self._anim_slider.value()
        # Find the smallest marker step that is strictly greater than current
        target = None
        for step_idx, _, _ in self._pass_markers:
            if step_idx > current:
                target = step_idx
                break
        if target is not None:
            self._anim_slider.set_value_direct(target)
            self._on_anim_step(target)

    def _on_play_methods(self) -> None:
        """Play animation cycling through detection steps (slider frames)."""
        if self._anim_playing:
            self._anim_timer.stop()
            self._anim_playing = False
            self._play_btn.setText("▶ Play steps")
            return

        self._ensure_algo_ran()
        if not self._detection_steps:
            return

        self._anim_playing = True
        self._anim_step_idx = 0
        self._play_btn.setText("⏸ Stop")
        self._anim_timer.start(100)  # 100ms per step
        self._on_anim_tick()

    def _on_anim_tick(self) -> None:
        """Advance to next detection step, pause at stage boundaries."""
        if self._anim_step_idx < len(self._detection_steps):
            # Check if we hit a stage boundary marker — pause playback
            if self._pass_markers and self._anim_step_idx > 0:
                marker_steps = {m[0] for m in self._pass_markers}
                if self._anim_step_idx in marker_steps:
                    # Show this frame, then pause
                    self._anim_slider.set_value_direct(self._anim_step_idx)
                    self._on_anim_step(self._anim_step_idx)
                    self._anim_step_idx += 1
                    self._anim_timer.stop()
                    self._anim_playing = False
                    self._play_btn.setText("▶ Play steps")
                    return

            self._anim_slider.set_value_direct(self._anim_step_idx)
            self._on_anim_step(self._anim_step_idx)
            self._anim_step_idx += 1
        else:
            # Loop finished
            self._anim_timer.stop()
            self._anim_playing = False
            self._play_btn.setText("▶ Play steps")
            self._anim_step_idx = 0

    def _on_layer_filter_changed(self) -> None:
        """Re-render page image with filtered PDF content."""
        import numpy as np

        show_text = self._layer_cbs["text"].isChecked()
        show_images = self._layer_cbs["images"].isChecked()
        show_drawings = self._layer_cbs["drawings"].isChecked()

        if show_text and show_images and show_drawings:
            self._page_img = self._page_img_full.copy()
        else:
            try:
                samples, w, h, n, stride = get_pixmap_filtered(
                    self._doc, self._page_idx, self._dpi,
                    show_text=show_text,
                    show_images=show_images,
                    show_drawings=show_drawings,
                )
                self._page_img = np.frombuffer(samples, dtype=np.uint8).reshape(
                    h, w, n
                ).copy()
            except Exception as e:
                logger.error("Layer filter failed: %s", e)
                self._page_img = self._page_img_full.copy()

        self._render()

    def _render(self, _=None) -> None:
        """Render final result (all bboxes)."""
        algo_idx = self._algo_combo.currentIndex()
        is_split = algo_idx in (6, 7, 8, 12)
        if is_split and hasattr(self, '_text_bboxes'):
            self._render_frame(
                self._text_bboxes, self._artifacts, -1,
                nontext=self._nontext_bboxes,
            )
            if algo_idx == 12 and hasattr(self, '_pdf_filter_passes'):
                parts = "  ".join(
                    f"{name}: {len(bb)}" for name, bb, _ in self._pdf_filter_passes
                )
                self._step_label.setText(
                    f"{parts}  Total: {len(self._text_bboxes) + len(self._nontext_bboxes)}"
                )
            else:
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
        import numpy as np

        # Show mask overlaid on page with 70% opacity
        if self._show_mask_cb.isChecked():
            algo_idx = self._algo_combo.currentIndex()
            # Detect Pre-Pass stage (not for PDF Filter — it manages images itself)
            pdf_set = set(self._pdf_text_blocks)
            is_prepass = (
                algo_idx != 12 and
                pdf_set and pdf_set.issubset(set(bboxes)) and
                (nontext is None or len(nontext) == 0) and
                len(artifacts) == 0
            )

            # Select base page image: text-only for pre-pass, current for others
            if is_prepass and self._page_img_text_only is not None:
                page_layer = self._page_img_text_only
            else:
                page_layer = self._page_img

            ch = page_layer.shape[2]

            # Create mask layer: white background, black detected areas
            mask_layer = np.ones((self._h, self._w, 3), dtype=np.uint8) * 255

            for (x0, y0, x1, y1) in bboxes:
                mask_layer[max(0, y0):min(self._h, y1), max(0, x0):min(self._w, x1)] = [0, 0, 0]
            if nontext:
                for (x0, y0, x1, y1) in nontext:
                    mask_layer[max(0, y0):min(self._h, y1), max(0, x0):min(self._w, x1)] = [0, 0, 0]
            for (x0, y0, x1, y1) in artifacts:
                mask_layer[max(0, y0):min(self._h, y1), max(0, x0):min(self._w, x1)] = [0, 0, 0]

            # Blend: mask as base (70%), rendered page overlaid on top (30%)
            vis = (mask_layer * 0.7 + page_layer[:, :, :3] * 0.3).astype(np.uint8)

            # Overlay bbox borders to see grow operations
            show = self._show_bboxes_cb.isChecked()
            if show:
                pdf_block_set = set(self._pdf_text_blocks)
                for (x0, y0, x1, y1) in bboxes:
                    if (x0, y0, x1, y1) in pdf_block_set:
                        # PDF text block border
                        self._draw_rect(vis, x0, y0, x1, y1, [255, 255, 255], 1)
                    elif is_prepass:
                        # Pre-Pass growing boxes: white border
                        self._draw_rect(vis, x0, y0, x1, y1, [255, 255, 255], 2)
                    else:
                        # Check if mostly inside masked area
                        inside_mask = 0
                        for (px0, py0, px1, py1) in self._pdf_text_blocks:
                            ix0 = max(x0, px0)
                            iy0 = max(y0, py0)
                            ix1 = min(x1, px1)
                            iy1 = min(y1, py1)
                            if ix0 < ix1 and iy0 < iy1:
                                inside_mask += (ix1 - ix0) * (iy1 - iy0)
                        bbox_area = max((x1 - x0) * (y1 - y0), 1)
                        if inside_mask / bbox_area > 0.5:
                            self._draw_rect(vis, x0, y0, x1, y1, [255, 0, 255], 2)
                        else:
                            self._draw_rect(vis, x0, y0, x1, y1, [0, 200, 0], 2)
                if nontext:
                    for (x0, y0, x1, y1) in nontext:
                        self._draw_rect(vis, x0, y0, x1, y1, [200, 0, 0], 2)
                for (x0, y0, x1, y1) in artifacts:
                    self._draw_rect(vis, x0, y0, x1, y1, [150, 150, 150], 1)
        else:
            algo_idx = self._algo_combo.currentIndex()

            # Detect Pre-Pass stage in non-mask mode (not for PDF Filter — it manages images itself)
            pdf_set = set(self._pdf_text_blocks)
            is_prepass = (
                algo_idx != 12 and
                pdf_set and pdf_set.issubset(set(bboxes)) and
                (nontext is None or len(nontext) == 0) and
                len(artifacts) == 0
            )

            if is_prepass:
                # Pre-Pass non-mask: render page with text objects only
                if self._page_img_text_only is not None:
                    vis = self._page_img_text_only.copy()
                else:
                    vis = self._page_img.copy()
                ch = vis.shape[2]

                # Draw text block borders in blue, growing boxes in green
                pdf_block_set = set(self._pdf_text_blocks)
                for (x0, y0, x1, y1) in self._pdf_text_blocks:
                    self._draw_rect(vis, x0, y0, x1, y1, [50, 100, 220], 1)
                if self._show_bboxes_cb.isChecked():
                    for (x0, y0, x1, y1) in bboxes:
                        if (x0, y0, x1, y1) not in pdf_block_set:
                            self._draw_rect(vis, x0, y0, x1, y1, [0, 200, 0], 2)
            else:
                # Pass 1+: show full page with all objects
                vis = self._page_img.copy()
                show = self._show_bboxes_cb.isChecked()
                ch = vis.shape[2]

                # Scanline PDF Filter: colored fill + border per pass
                if (hasattr(self, '_pdf_filter_passes')
                        and algo_idx == 12):
                    # Build set of finished bboxes per pass for color lookup
                    bbox_to_color: dict[tuple, list[int]] = {}
                    for pass_name, pass_bboxes, color in self._pdf_filter_passes:
                        for bb in pass_bboxes:
                            bbox_to_color[bb] = color

                    # Determine current pass index and color
                    current_pass_idx = -1
                    current_pass_color = [0, 200, 0]
                    if hasattr(self, '_pdf_filter_pass_ranges'):
                        step = self._anim_slider.value()
                        for i, (start, end) in enumerate(self._pdf_filter_pass_ranges):
                            if start <= step < end:
                                current_pass_idx = i
                                current_pass_color = self._pdf_filter_passes[i][2]
                                break

                    # Draw dashed borders for native PDF objects of current pass
                    if (current_pass_idx >= 0
                            and hasattr(self, '_pdf_filter_obj_rects')
                            and current_pass_idx < len(self._pdf_filter_obj_rects)):
                        obj_color = [c // 2 for c in current_pass_color]  # darker
                        for (ox0, oy0, ox1, oy1) in self._pdf_filter_obj_rects[current_pass_idx]:
                            self._draw_rect_dashed(vis, ox0, oy0, ox1, oy1, obj_color)

                    for (x0, y0, x1, y1) in bboxes:
                        bb = (x0, y0, x1, y1)
                        is_finished = bb in bbox_to_color
                        color = bbox_to_color.get(bb, current_pass_color)
                        fill = [min(255, c + 180) for c in color]
                        # Semi-transparent fill
                        region = vis[max(0,y0):min(self._h,y1),
                                     max(0,x0):min(self._w,x1)]
                        if region.size > 0:
                            tint = np.array(fill[:3], dtype=np.uint8)
                            region[:] = (region[:,:,:3] * 0.6
                                         + tint * 0.4).astype(np.uint8)
                        if show:
                            if is_finished:
                                self._draw_rect(vis, x0, y0, x1, y1, color, 2)
                            else:
                                # Growing bbox: 1px dark border
                                self._draw_rect(vis, x0, y0, x1, y1, [40, 40, 40], 1)
                else:
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

        # Scanline indicator: horizontal line for row scans, vertical line for column scans
        # Positive scan_y = row scan (horizontal line), Negative = column scan (vertical line at -scan_y)
        if scan_y >= 0:
            # Row scan: horizontal line
            if 0 <= scan_y < self._h:
                for x in range(self._w):
                    # Check if this position is inside any PDF text block
                    inside_mask = False
                    for (px0, py0, px1, py1) in self._pdf_text_blocks:
                        if px0 <= x < px1 and py0 <= scan_y < py1:
                            inside_mask = True
                            break

                    # Draw inverted (white) inside mask, red outside
                    color = [255, 255, 255] if inside_mask else [255, 0, 0]
                    vis[scan_y, x] = color + [255] * (ch - 3)
        else:
            # Column scan: vertical line at column -scan_y
            scan_x = -scan_y
            if 0 <= scan_x < self._w:
                for y in range(self._h):
                    # Check if this position is inside any PDF text block
                    inside_mask = False
                    for (px0, py0, px1, py1) in self._pdf_text_blocks:
                        if px0 <= scan_x < px1 and py0 <= y < py1:
                            inside_mask = True
                            break

                    # Draw inverted (white) inside mask, cyan outside (for column scan)
                    color = [255, 255, 255] if inside_mask else [0, 255, 255]
                    vis[y, scan_x] = color + [255] * (ch - 3)

        h, w = vis.shape[:2]
        fmt = QImage.Format.Format_RGB888 if ch == 3 else QImage.Format.Format_RGBA8888
        qimg = QImage(vis.data, w, h, w * ch, fmt)
        self._last_pixmap = QPixmap.fromImage(qimg)
        self._fit_pixmap_to_width()

    def _fit_pixmap_to_width(self) -> None:
        """Scale last rendered pixmap to fit scroll area width."""
        pix = getattr(self, '_last_pixmap', None)
        if pix is None:
            return
        available_w = self._scroll.viewport().width() - 2
        if available_w > 0 and pix.width() > 0:
            scaled = pix.scaledToWidth(
                available_w, Qt.TransformationMode.SmoothTransformation
            )
            self._image_label.setPixmap(scaled)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._fit_pixmap_to_width()

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

    def _draw_rect_dashed(self, vis, x0, y0, x1, y1, color, dash=4, gap=3):
        """Draw 1px dashed rectangle border."""
        ch = vis.shape[2]
        c = color + [255] * (ch - 3)
        period = dash + gap
        # Top edge
        if 0 <= y0 < self._h:
            for x in range(max(0, x0), min(self._w, x1)):
                if (x - x0) % period < dash:
                    vis[y0, x] = c
        # Bottom edge
        by = y1 - 1
        if 0 <= by < self._h:
            for x in range(max(0, x0), min(self._w, x1)):
                if (x - x0) % period < dash:
                    vis[by, x] = c
        # Left edge
        if 0 <= x0 < self._w:
            for y in range(max(0, y0), min(self._h, y1)):
                if (y - y0) % period < dash:
                    vis[y, x0] = c
        # Right edge
        rx = x1 - 1
        if 0 <= rx < self._w:
            for y in range(max(0, y0), min(self._h, y1)):
                if (y - y0) % period < dash:
                    vis[y, rx] = c


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

        if self._bbox.get("excluded_from_template"):
            lines.append("Template:    EXCLUDED")
        elif self._bbox.get("is_template_exact"):
            lines.append("Template:    EXACT (identical content hash)")
        elif self._bbox.get("is_template_medium"):
            lines.append("Template:    MEDIUM (70-99% content hash match)")
        elif self._bbox.get("is_template_loose"):
            lines.append("Template:    LOOSE (40-69% content hash match)")

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

            self._pixmap = base_pixmap

            # Extract nested objects info for statistics (don't draw them)
            self._nested_objects = self._get_nested_pdf_objects(page, clip_rect)

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

    def __init__(self, view_only: bool = True) -> None:
        super().__init__()
        # When True, processing controls are hidden and no auto/lazy detection runs.
        self._view_only = view_only
        self._doc: fitz.Document | None = None
        self._file_path: Path | None = None       # working copy path (opened by fitz)
        self._original_path: Path | None = None   # original file in uploads/ (read-only)
        self._page_count = 0
        self._zoom = 1.0
        self._base_dpi = 225
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
        self._show_object_labels: bool = True
        # Preview is view-only by default → bboxes stay hidden. LayoutView
        # flips this to True in its own __init__ (via the View-menu toggle).
        self._bboxes_visible: bool = not view_only
        # Printed-catalog spread defaults: first page alone on right (front cover),
        # last page alone on left (back cover).
        self._first_is_cover: bool = True
        self._last_is_back_cover: bool = True
        # When True, background detection is paused — the current worker is
        # cancelled and _detect_visible_pages / _start_next_detect bail out.
        self._detection_paused: bool = False

        # --- Pixmap cache & background renderer ---
        # page_idx → {layer_key: (QPixmap, dpi)}
        # layer_key is a frozenset of hidden pdf_type strings
        self._pixmap_cache: dict[int, dict[frozenset, tuple[QPixmap, float]]] = {}
        self._render_worker: PageRenderWorker | None = None
        self._target_dpi: float = 0.0  # DPI that we want visible pages at
        self._target_layer_key: frozenset = frozenset()
        self._last_pdf_layer_key: frozenset = frozenset()

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

        # Toolbar (hosted on a widget so it can be wrapped in a horizontal scroll area)
        toolbar_host = QWidget()
        toolbar = QHBoxLayout(toolbar_host)
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

        # PDF render layers — filter content stream by object type
        self.render_layer_btn = QToolButton()
        self.render_layer_btn.setText("Layers ▾")
        self.render_layer_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        layers_menu = QMenu(self.render_layer_btn)
        self._render_layer_cbs: dict[str, QCheckBox] = {}
        for label, layer_key in [("Text", "text"), ("Images", "images"), ("Drawings", "drawings")]:
            lw = QWidget()
            lr = QHBoxLayout(lw)
            lr.setContentsMargins(4, 1, 4, 1)
            lcb = QCheckBox(label)
            lcb.setChecked(True)
            lcb.toggled.connect(self._on_render_layer_changed)
            lr.addWidget(lcb)
            lr.addStretch()
            lwa = QWidgetAction(layers_menu)
            lwa.setDefaultWidget(lw)
            layers_menu.addAction(lwa)
            self._render_layer_cbs[layer_key] = lcb
        self.render_layer_btn.setMenu(layers_menu)
        toolbar.addWidget(self.render_layer_btn)

        self._detect_method_combo = QComboBox()
        _ALGO_DESCRIPTIONS = [
            "Basic scanline + grow from seed pixel",
            "Visited bitmap + fast jump grow",
            "Adaptive margin based on object size",
            "OpenCV connected component analysis",
            "OpenCV CCA with dilation preprocessing",
            "MSER maximally stable text regions",
            "Scanline seeds + MSER text splitting",
            "PDF native text blocks + scanline for rest",
            "2-pass: scanline then merge fragmented",
            "Native PDF object extraction only",
            "Pure PDF structure analysis",
            "3-pass: text pre-pass + scanline + merge",
            "3-pass: text/images/drawings filtered separately",
        ]
        for i, name in enumerate(BboxDetectionDialog._ALGOS):
            desc = _ALGO_DESCRIPTIONS[i] if i < len(_ALGO_DESCRIPTIONS) else ""
            self._detect_method_combo.addItem(name)
            self._detect_method_combo.setItemData(i, f"{name}\n{desc}", Qt.ItemDataRole.ToolTipRole)
        # Use a 2-column table view for the dropdown
        from PySide6.QtWidgets import QTableView, QHeaderView
        from PySide6.QtGui import QStandardItemModel, QStandardItem
        model = QStandardItemModel(len(BboxDetectionDialog._ALGOS), 2)
        for i, name in enumerate(BboxDetectionDialog._ALGOS):
            desc = _ALGO_DESCRIPTIONS[i] if i < len(_ALGO_DESCRIPTIONS) else ""
            item_name = QStandardItem(name)
            item_desc = QStandardItem(desc)
            item_desc.setForeground(QColor(120, 120, 120))
            font = item_desc.font()
            font.setPointSize(8)
            item_desc.setFont(font)
            model.setItem(i, 0, item_name)
            model.setItem(i, 1, item_desc)
        table = QTableView()
        table.setModel(model)
        table.horizontalHeader().hide()
        table.verticalHeader().hide()
        table.setShowGrid(False)
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self._detect_method_combo.setModel(model)
        self._detect_method_combo.setView(table)
        self._detect_method_combo.setCurrentIndex(10)  # Pure PDF default
        self._detect_method_combo.setFixedHeight(24)
        self._detect_method_combo.setFixedWidth(180)
        toolbar.addWidget(self._detect_method_combo)

        self.clear_bbox_btn = QPushButton("Clear && Re-detect")
        self.clear_bbox_btn.clicked.connect(self._clear_and_redetect)
        toolbar.addWidget(self.clear_bbox_btn)


        self.remove_template_btn = QPushButton("Remove Template")
        self.remove_template_btn.clicked.connect(self._remove_template_and_save)
        self.remove_template_btn.setEnabled(False)
        toolbar.addWidget(self.remove_template_btn)

        self.revert_btn = QPushButton("Revert")
        self.revert_btn.setToolTip("Revert to original file (discard all changes)")
        self.revert_btn.clicked.connect(self._revert_to_original)
        self.revert_btn.setEnabled(False)
        toolbar.addWidget(self.revert_btn)

        self.crop_btn = QPushButton("Cropping")
        self.crop_btn.clicked.connect(self._open_cropping_dialog)
        toolbar.addWidget(self.crop_btn)

        # Detection progress bar
        from PySide6.QtWidgets import QProgressBar
        self._detect_progress = QProgressBar()
        self._detect_progress.setFixedHeight(16)
        self._detect_progress.setFixedWidth(150)
        self._detect_progress.setTextVisible(True)
        self._detect_progress.setFormat("0/0 pages")
        self._detect_progress.setStyleSheet("font-size: 9px;")
        toolbar.addWidget(self._detect_progress)
        toolbar.addStretch()

        # In view-only mode, hide all processing controls. Widgets remain as
        # attributes so existing code paths don't crash.
        if self._view_only:
            for _w in (
                self.bbox_filter_btn,
                self.content_filter_btn,
                self.render_layer_btn,
                self._detect_method_combo,
                self.clear_bbox_btn,
                self.remove_template_btn,
                self.revert_btn,
                self.crop_btn,
                self._detect_progress,
            ):
                _w.setVisible(False)

        # Wrap toolbar in a horizontal scroll area so it doesn't force a minimum width on the window
        toolbar_scroll = QScrollArea()
        toolbar_scroll.setWidget(toolbar_host)
        toolbar_scroll.setWidgetResizable(True)
        toolbar_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        toolbar_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        toolbar_scroll.setFrameShape(QFrame.Shape.NoFrame)
        toolbar_scroll.setFixedHeight(toolbar_host.sizeHint().height() + 14)
        toolbar_scroll.setMinimumWidth(0)
        self._toolbar_scroll = toolbar_scroll  # exposed so subclasses can hide it
        layout.addWidget(toolbar_scroll)

        # Zoom / Fit controls are hosted on the main window header now — hide
        # the in-tab copies to avoid duplication.
        for _zw in (
            self.zoom_out_btn,
            self.zoom_slider,
            self.zoom_in_btn,
            self.zoom_label,
            self.fit_btn,
        ):
            _zw.setVisible(False)

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

        # Horizontal + vertical rulers (children of scroll_area, shown via _show_rulers)
        self._h_ruler = _Ruler(Qt.Orientation.Horizontal, self.scroll_area)
        self._v_ruler = _Ruler(Qt.Orientation.Vertical, self.scroll_area)
        self._h_ruler.hide()
        self._v_ruler.hide()
        self.scroll_area.horizontalScrollBar().valueChanged.connect(self._sync_rulers_offset)
        self.scroll_area.verticalScrollBar().valueChanged.connect(self._sync_rulers_offset)

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
        """Handle pan (Space+drag / middle mouse) and Ctrl+wheel zoom on scroll viewport."""
        if obj is not self.scroll_area.viewport():
            return False

        etype = event.type()

        # Ctrl + wheel → zoom anchored at cursor (intercept before QScrollArea
        # consumes the wheel for scrolling).
        if etype == event.Type.Wheel and (
            event.modifiers() & Qt.KeyboardModifier.ControlModifier
        ):
            delta = event.angleDelta().y()
            if delta == 0:
                return True
            step = 0.1 if delta > 0 else -0.1
            # event.position() is already QPointF in viewport-local coords
            self._set_zoom(self._zoom + step, anchor=event.position())
            return True

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

    def _ensure_working_copy(self, file_path: Path) -> Path:
        """Create working copy of PDF in data/working/ if needed.

        Original stays untouched in uploads/. Returns path to working copy.
        """
        from app.config.settings import settings
        working_path = settings.working_dir / file_path.name
        if not working_path.exists() or working_path == file_path:
            shutil.copy2(str(file_path), str(working_path))
            logger.info("Created working copy: %s", working_path)
        return working_path

    def load_document(self, file_path: Path) -> None:
        """Load a PDF document for preview."""
        self.progress.emit(f"Loading {file_path.name}...")
        if self._doc:
            self._doc.close()

        self._cancel_render_worker()
        self._cancel_detect_worker()
        self._detect_method_idx = self._detect_method_combo.currentIndex()

        # Create working copy — original stays untouched
        from app.config.settings import settings
        if file_path.parent == settings.working_dir:
            # Already a working copy (e.g. after revert)
            self._original_path = settings.uploads_dir / file_path.name
            self._file_path = file_path
        else:
            self._original_path = file_path
            self._file_path = self._ensure_working_copy(file_path)

        self._bboxes_cache.clear()
        self._stats_cache.clear()
        self._pdf_objects_cache.clear()
        self._pixmap_cache.clear()
        self._catalog_meta = load_meta(self._original_path)

        # Load cropping settings — default to printed-catalog layout (covers isolated).
        crop = self._catalog_meta.get("cropping", {})
        self._first_is_cover = crop.get("first_is_cover", True)
        self._last_is_back_cover = crop.get("last_is_back_cover", True)
        self._cropping_data = crop

        self.revert_btn.setEnabled(True)

        try:
            self._doc = fitz.open(str(self._file_path))
            self._page_count = len(self._doc)
            self._detect_progress.setMaximum(self._page_count)
            self._detect_progress.setValue(0)
            self._detect_progress.setFormat(f"0/{self._page_count} pages")
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

        # Attempt to restore detection results from the on-disk meta cache.
        # PDFs don't change, so if (size, mtime) match what we previously
        # processed, re-use all bboxes / pdf_objects / stats without re-detecting.
        cache_hit = self._try_restore_detection_cache()

        self._fit_to_width()   # sets self._zoom, calls _set_zoom → _update_spreads_zoom (no-op: no spreads yet)
        self._rebuild_spreads()  # create spread widgets + kick off background render
        if not self._view_only and not cache_hit:
            # Auto-run detection only when cache is missing / stale.
            self._clear_and_redetect()
        elif cache_hit:
            # Restore the cached detection state to the just-rebuilt spreads.
            self._apply_bboxes_to_spreads()
            if hasattr(self, "_detect_progress"):
                self._detect_progress.setValue(self._page_count)
                self._detect_progress.setFormat(
                    f"{self._page_count}/{self._page_count} pages (cached)"
                )
            self.progress.emit(f"Loaded detection cache ({len(self._bboxes_cache)} pages)  0.0s")
        logger.info("Loaded document: %s (%d pages)%s",
                    file_path.name, self._page_count,
                    " — cache hit" if cache_hit else "")

    def _try_restore_detection_cache(self) -> bool:
        """If the meta cache's file_id matches the current file, populate
        _bboxes_cache / _pdf_objects_cache / _stats_cache from meta. Returns
        True if the cache is valid and at least one page of bboxes was loaded.
        """
        if not self._original_path or not self._catalog_meta:
            return False
        file_id = self._catalog_meta.get("file_id")
        if not file_id:
            return False
        try:
            st = self._original_path.stat()
        except OSError:
            return False
        if file_id.get("size") != st.st_size:
            return False
        if int(file_id.get("mtime", 0)) != int(st.st_mtime):
            return False
        # Invalidate caches produced by older extractor versions.
        if int(self._catalog_meta.get("detection_version", 0)) != PDF_OBJECTS_DETECTION_VERSION:
            return False

        objects = self._catalog_meta.get("objects", [])
        if not objects:
            return False

        by_page: dict[int, list[dict]] = {}
        for o in objects:
            try:
                page = int(o.get("page", 0))
            except (TypeError, ValueError):
                continue
            by_page.setdefault(page, []).append(o)
        self._bboxes_cache.update(by_page)

        # pdf_objects (stored as { "page_idx": [ {...}, ... ] })
        for page_str, objs in self._catalog_meta.get("pdf_objects_cache", {}).items():
            try:
                self._pdf_objects_cache[int(page_str)] = list(objs)
            except (TypeError, ValueError):
                continue

        # Regenerate stats from cached bboxes (old cached stats may predate the
        # per-type breakdown format).
        for page_num, bxs in by_page.items():
            self._stats_cache[page_num] = _format_page_stats(page_num, bxs)

        return True

    def _current_zoom_factor(self) -> float:
        """Current points-to-pixels conversion factor."""
        return self._base_dpi * self._zoom / 72.0

    # ------------------------------------------------------------------
    # Spread layout helpers
    # ------------------------------------------------------------------

    def _build_spread_pairs(self) -> list[tuple[int, int]]:
        """Return [(left_page_idx | -1, right_page_idx | -1), ...].

        For a printed-catalog layout both covers are isolated:
          - first page alone on the right (front cover)
          - last page alone on the left (back cover)
        """
        pairs: list[tuple[int, int]] = []
        if self._page_count == 0:
            return pairs

        # Only isolate the back cover when it keeps the middle pages cleanly paired
        # (even middle count); otherwise fall back to last page on the right.
        last_alone = getattr(self, "_last_is_back_cover", False) and self._page_count > 1
        if last_alone:
            middle_count = self._page_count - 2 if self._first_is_cover else self._page_count - 1
            if middle_count % 2 != 0:
                last_alone = False
        end = self._page_count - 1 if last_alone else self._page_count

        if self._first_is_cover:
            pairs.append((-1, 0))
            pi = 1
        else:
            pi = 0

        while pi < end:
            right = pi + 1 if pi + 1 < end else -1
            pairs.append((pi, right))
            pi += 2

        if last_alone:
            pairs.append((self._page_count - 1, -1))

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
        # Throttle per-spread progress emits — each emit appends to the debug
        # console, which is expensive for large catalogs (hundreds of spreads).
        _progress_step = max(1, total_spreads // 20)
        for si, (left_pi, right_pi) in enumerate(spread_pairs):
            if si == 0 or si == total_spreads - 1 or (si % _progress_step == 0):
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
                # Use a lightweight placeholder for uncached pages — avoids
                # allocating N × page-size blank pixmaps (can be >1 GB for
                # large catalogs). The real pixmap replaces it once the
                # background hi-res renderer completes.
                page_cache = self._pixmap_cache.get(pi, {})
                if page_cache:
                    pw.set_pixmap(self._pixmap_for_page(pi, zf))
                else:
                    pw.set_placeholder(zf)
                pw.set_show_bboxes(self._bboxes_visible)
                pw.set_show_object_labels(self._show_object_labels)
                pw.set_edit_bboxes(getattr(self, "_bbox_edit_allowed", True))
                pw.set_show_hidden(self._show_hidden)
                pw.set_file_path(self._file_path)
                pw.hide_requested.connect(self._on_hide_object)
                pw.selection_changed.connect(self._on_page_selection)
                pw.type_changed.connect(self._on_type_changed)
                pw.bbox_modified.connect(self._on_bbox_modified)
                pw.bbox_testbench.connect(self._on_bbox_testbench)
                pw.object_stats_requested.connect(self._on_object_stats_requested)
                pw.page_rerender_requested.connect(self._on_page_rerender_requested)
                pw.template_excluded.connect(self._on_template_excluded)
                pw.set_visible_layers(cur_layers)
                pw.set_content_mask(cur_content_mask)
                # Apply persistent Layout type-filter state AFTER set_visible_layers
                # (which replaces the entire dict) so our keys are final.
                for k, v in getattr(self, "_layout_layer_state", {}).items():
                    pw._visible_layers[k] = v
                if pi in self._bboxes_cache:
                    pw.set_bboxes(self._bboxes_cache[pi], zf, self._detect_method_idx)
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

        # Defer ruler segment recomputation until Qt finishes its layout pass,
        # so pw.mapTo(...).x() reports actual positions (not 0).
        if getattr(self, "_show_rulers", False):
            from PySide6.QtCore import QTimer
            QTimer.singleShot(0, self._sync_rulers_offset)

    # ------------------------------------------------------------------
    # Pixmap helpers
    # ------------------------------------------------------------------

    def _pdf_layer_key(self) -> frozenset:
        """Return frozenset of pdf_type strings that are currently hidden.

        Empty frozenset means render everything (no filtering).
        """
        pdf_parent = self._filter_checkboxes.get("pdf_objects")
        if not pdf_parent or not pdf_parent.isChecked():
            return frozenset()
        hidden: set[str] = set()
        for sub_key in ("pdf_text", "pdf_image", "pdf_table"):
            cb = self._filter_checkboxes.get(sub_key)
            if cb and not cb.isChecked():
                hidden.add(sub_key)
        # All checked = nothing hidden = no filtering
        if len(hidden) == 3:
            return frozenset()
        return frozenset(hidden)

    @staticmethod
    def _layer_key_to_flags(layer_key: frozenset) -> tuple[bool, bool, bool]:
        """Convert layer_key to (show_text, show_images, show_drawings)."""
        return (
            "pdf_text" not in layer_key,
            "pdf_image" not in layer_key,
            "pdf_table" not in layer_key,  # tables are drawings/lines
        )

    def _pixmap_for_page(self, page_idx: int, zf: float) -> QPixmap:
        """Return the best available pixmap for *page_idx* at zoom-factor *zf*.

        If we already have a cached render (possibly at a different DPI),
        scale it to the expected size so the layout is immediate.
        """
        target_w = int(self._doc[page_idx].rect.width * zf)
        target_h = int(self._doc[page_idx].rect.height * zf)

        layer_key = self._effective_layer_key()
        page_cache = self._pixmap_cache.get(page_idx, {})

        # Try exact layer key first
        cached = page_cache.get(layer_key)
        if cached:
            pix, _cached_dpi = cached
            if pix.width() == target_w and pix.height() == target_h:
                return pix
            return pix.scaled(
                target_w, target_h,
                Qt.AspectRatioMode.IgnoreAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )

        # Fall back to any cached key (for instant feedback before re-render)
        for _key, (pix, _dpi) in page_cache.items():
            return pix.scaled(
                target_w, target_h,
                Qt.AspectRatioMode.IgnoreAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )

        # No cache at all — grey placeholder
        blank = QPixmap(target_w, target_h)
        blank.fill(QColor(245, 245, 245))
        return blank

    def _effective_layer_key(self) -> frozenset:
        """Combine render layer and pdf filter layer keys."""
        render_key = self._render_layer_key()
        pdf_key = self._pdf_layer_key()
        return render_key | pdf_key

    def _render_page_sync(self, page_num: int, dpi: float) -> QPixmap:
        """Render a single page synchronously (used for initial load)."""
        layer_key = self._effective_layer_key()
        _force_filter = not (
            getattr(self, "_hidden_ops_for_page", None) or _EMPTY_HIDDEN
        ).is_empty()

        if layer_key or _force_filter:
            show_text = "text" not in layer_key and "pdf_text" not in layer_key
            show_images = "images" not in layer_key and "pdf_image" not in layer_key
            show_drawings = (
                "drawings" not in layer_key
                and "pdf_drawing" not in layer_key
                and "pdf_table" not in layer_key
            )
            # Granular path paint flags — present key = hide that paint class.
            show_path_stroke = "path_stroke" not in layer_key
            show_path_fill = "path_fill" not in layer_key
            show_path_stroke_fill = "path_stroke_fill" not in layer_key
            # Shading + annotations (render outside content-stream operators)
            show_shading = "shading" not in layer_key
            render_annotations = "annotations" not in layer_key
            # Per-object hide spec: LayersView sets `_hidden_ops_for_page` on
            # self before calling into render; other views leave it absent.
            hidden_ops = getattr(self, "_hidden_ops_for_page", None) or _EMPTY_HIDDEN
            # Selectable rendering via content-stream operator filtering on a
            # temp copy of the page. See `_filter_content_stream` for details.
            samples, w, h, n, stride = get_pixmap_filtered(
                self._doc, page_num, dpi,
                show_text=show_text,
                show_images=show_images,
                show_drawings=show_drawings,
                show_path_stroke=show_path_stroke,
                show_path_fill=show_path_fill,
                show_path_stroke_fill=show_path_stroke_fill,
                show_shading=show_shading,
                render_annotations=render_annotations,
                hidden_ops=hidden_ops,
            )
            img = QImage(samples, w, h, stride, QImage.Format.Format_RGB888)
        else:
            page = self._doc[page_num]
            mat = fitz.Matrix(dpi / 72.0, dpi / 72.0)
            pix = page.get_pixmap(matrix=mat)
            img = QImage(pix.samples, pix.width, pix.height, pix.stride, QImage.Format.Format_RGB888)

        qpix = QPixmap.fromImage(img)
        qpix = self._transform_pixmap(qpix)
        if page_num not in self._pixmap_cache:
            self._pixmap_cache[page_num] = {}
        self._pixmap_cache[page_num][layer_key] = (qpix, dpi)
        return qpix

    def _transform_pixmap(self, qpix: QPixmap) -> QPixmap:
        """Hook to post-process every rendered page pixmap.
        Default: identity. LayoutView overrides it to force b/w + transparent."""
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

    def set_detection_paused(self, paused: bool) -> None:
        """Public API: pause/resume background object-detection.

        Paused: cancels the running worker and gates future scheduling.
        Resumed: clears the flag and restarts detection for visible pages.
        """
        paused = bool(paused)
        if self._detection_paused == paused:
            return
        self._detection_paused = paused
        if paused:
            self._cancel_detect_worker()
        else:
            self._detect_visible_pages()

    def _schedule_hires_render(self) -> None:
        """Queue background rendering for visible pages at the correct DPI."""
        if not self._doc or not self._file_path:
            return
        self._cancel_render_worker()

        layer_key = self._effective_layer_key()
        target_dpi = self._current_zoom_factor() * 72.0
        self._target_dpi = target_dpi
        self._target_layer_key = layer_key

        visible = self._visible_page_indices()
        # Only request pages whose cache doesn't already match target DPI + layer
        requests: list[tuple[int, float]] = []
        for pi in visible:
            page_cache = self._pixmap_cache.get(pi, {})
            cached = page_cache.get(layer_key)
            if cached and abs(cached[1] - target_dpi) < 1.0:
                continue  # already sharp at this layer config
            requests.append((pi, target_dpi))

        if not requests:
            return

        worker = PageRenderWorker(
            str(self._file_path), requests,
            layer_key=layer_key,
            hidden_ops=getattr(self, "_hidden_ops_for_page", None),
            parent=self,
        )
        worker.page_ready.connect(self._on_page_rendered)
        worker.all_done.connect(self._on_render_done)
        self._render_worker = worker
        worker.start()

    def _on_page_rendered(self, page_idx: int, image: QImage, dpi: float, layer_key: object = None) -> None:
        """Slot: background worker delivered a rendered page."""
        if layer_key is None:
            layer_key = frozenset()
        else:
            layer_key = frozenset(layer_key)

        # Stale result? (user changed zoom or layer config while rendering)
        if abs(dpi - self._target_dpi) > 1.0:
            return
        if layer_key != self._target_layer_key:
            return

        self.progress.emit(f"HiRes p.{page_idx+1} @ {int(dpi)} DPI")
        qpix = QPixmap.fromImage(image)
        qpix = self._transform_pixmap(qpix)
        if page_idx not in self._pixmap_cache:
            self._pixmap_cache[page_idx] = {}
        self._pixmap_cache[page_idx][layer_key] = (qpix, dpi)

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
        self._update_template_btn_state()

    # ------------------------------------------------------------------
    # Zoom — instant scale + deferred hi-res
    # ------------------------------------------------------------------

    def _update_spreads_zoom(self) -> None:
        """Instantly rescale cached pixmaps to new zoom & update bboxes."""
        if not self._doc:
            return
        self.progress.emit(f"Zoom {int(self._zoom * 100)}%")
        zf = self._current_zoom_factor()
        # Keep rulers in sync with the new zoom scale
        if hasattr(self, "_h_ruler"):
            self._sync_rulers_scale()
            self._sync_rulers_offset()
        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                pi = getattr(pw, "_page_index", -1)
                if pi >= 0:
                    pw.set_pixmap(self._pixmap_for_page(pi, zf))
                    if pi in self._bboxes_cache:
                        pw.set_bboxes(self._bboxes_cache[pi], zf, self._detect_method_idx)
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

        # Caller-provided anchor => zoom-to-cursor. Otherwise default to viewport
        # centre (and also allow _scroll_to_page safety-net to run).
        anchored = anchor is not None
        if anchor is None:
            anchor = QPointF(vp.width() / 2.0, vp.height() / 2.0)

        # Content-space position of the anchor before zoom
        cx_before = hbar.value() + anchor.x()
        cy_before = vbar.value() + anchor.y()

        ratio = zoom / old_zoom

        # Remember the page that should stay in view (from the page spinner)
        current_page = self._get_current_page()

        # 1) Instant: scale cached pixmaps to new size
        self._update_spreads_zoom()

        # 2) Sync-render the current spread at correct DPI — sharp immediately
        self._sync_render_current_spread(current_page)

        # 3) Let Qt recalculate layout/scrollbar ranges before adjusting scroll
        self.pages_container.adjustSize()

        # 4) Adjust scroll so the anchor stays under the cursor.
        hbar.setValue(int(cx_before * ratio - anchor.x()))
        vbar.setValue(int(cy_before * ratio - anchor.y()))

        # 5) Safety net: only snap to the current page when no explicit anchor
        #    was passed. With a cursor anchor (Ctrl+wheel, pinch) we must NOT
        #    override the anchor-aware scroll — otherwise the point under the
        #    cursor jumps to the page centre.
        if not anchored:
            self._scroll_to_page(current_page + 1)

        # 6) Deferred: render remaining visible pages in background
        self._schedule_hires_render()

    def _sync_render_current_spread(self, current_page: int | None = None) -> None:
        """Render the pages of the spread containing *current_page* synchronously
        at the target DPI, so the viewer sees a sharp image immediately before
        the lazy background pass kicks in."""
        if not self._doc or not self._spreads:
            return

        if current_page is None:
            current_page = self._get_current_page()

        # Find the spread containing current_page using the layout pairs (robust:
        # does not depend on current scroll position, which may be stale).
        pairs = self._build_spread_pairs()
        current_spread = None
        for i, (lp, rp) in enumerate(pairs):
            if current_page == lp or current_page == rp:
                if i < len(self._spreads):
                    current_spread = self._spreads[i]
                break
        if current_spread is None:
            current_spread = self._spreads[0]

        zf = self._current_zoom_factor()
        target_dpi = zf * 72.0
        layer_key = self._effective_layer_key()

        for pw in (current_spread.left_page, current_spread.right_page):
            pi = getattr(pw, "_page_index", -1)
            if pi < 0:
                continue
            cached = self._pixmap_cache.get(pi, {}).get(layer_key)
            if cached and abs(cached[1] - target_dpi) < 1.0:
                continue  # already sharp at this layer config
            try:
                qpix = self._render_page_sync(pi, target_dpi)
            except Exception as e:
                logger.warning("Sync render failed for page %d: %s", pi, e)
                continue
            pw.set_pixmap(qpix)
            if pi in self._bboxes_cache:
                pw.set_bboxes(self._bboxes_cache[pi], zf, self._detect_method_idx)

    # Keep old name as alias for callers that still reference it
    def _render_all_spreads(self) -> None:
        self._pixmap_cache.clear()
        self._rebuild_spreads()

    def _fit_to_width(self) -> None:
        """Calculate zoom so the current spread fits the viewport width.

        Note: cover-only spreads (one real page + one blank placeholder) still
        occupy the full two-page visual width because the blank side mirrors
        the visible page's size. Use the pair tuple as the source of truth.
        """
        if not self._doc or self._page_count == 0:
            return

        current = self._get_current_page()

        pairs = self._build_spread_pairs()
        lp, rp = -1, -1
        for p_lp, p_rp in pairs:
            if current == p_lp or current == p_rp:
                lp, rp = p_lp, p_rp
                break
        if lp < 0 and rp < 0:
            return  # no spread found for this page

        # A spread always occupies two slots side-by-side: a blank side mirrors
        # the sibling's width via _blank_like(). Sum both slot widths.
        left_w = self._doc[lp].rect.width if lp >= 0 else self._doc[rp].rect.width
        right_w = self._doc[rp].rect.width if rp >= 0 else self._doc[lp].rect.width
        total_width_pt = left_w + right_w

        vbar = self.scroll_area.verticalScrollBar()
        scrollbar_w = vbar.width() if vbar.isVisible() else 18
        spread_margin = 10 + 10  # PageSpreadWidget HBoxLayout left+right margins
        spread_spacing = 8        # always 2 slots → always spacing
        safety = 4
        overhead = spread_margin + spread_spacing + scrollbar_w + safety

        available_width = self.scroll_area.viewport().width() - overhead
        base_px = total_width_pt * self._base_dpi / 72.0
        if base_px > 0 and available_width > 0:
            new_zoom = available_width / base_px
            self._set_zoom(new_zoom)
            self._scroll_to_page(current + 1)

    def _fit_to_height(self) -> None:
        """Calculate zoom so the current spread (tallest page) fits the viewport height."""
        if not self._doc or self._page_count == 0:
            return

        current = self._get_current_page()

        pairs = self._build_spread_pairs()
        lp, rp = -1, -1
        for p_lp, p_rp in pairs:
            if current == p_lp or current == p_rp:
                lp, rp = p_lp, p_rp
                break
        if lp < 0 and rp < 0:
            return

        heights_pt = [self._doc[p].rect.height for p in (lp, rp) if p >= 0]
        if not heights_pt:
            return
        max_height_pt = max(heights_pt)

        hbar = self.scroll_area.horizontalScrollBar()
        scrollbar_h = hbar.height() if hbar.isVisible() else 18
        # PageSpreadWidget HBoxLayout top+bottom margins + PageWidget header
        spread_margin_v = 10 + 10
        header = PageWidget.HEADER_HEIGHT
        safety = 4
        overhead = spread_margin_v + header + scrollbar_h + safety

        available_height = self.scroll_area.viewport().height() - overhead
        base_px = max_height_pt * self._base_dpi / 72.0
        if base_px > 0 and available_height > 0:
            new_zoom = available_height / base_px
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

        if not self._view_only:
            # Lazy bbox detection on scroll for processing-capable views
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
                pw.set_bboxes([], zf, self._detect_method_idx)
                pw.set_pdf_objects([], zf)
                pw.update()
        # Kick off lazy background detection for visible pages
        self._detect_visible_pages()

    def _update_template_btn_state(self) -> None:
        """Enable Remove Template button only when no background work is running."""
        busy = (
            (self._render_worker is not None)
            or (hasattr(self, "_detect_worker") and self._detect_worker is not None
                and self._detect_worker.isRunning())
        )
        has_templates = any(
            any(b.get(k, False) for k in ["is_template_exact", "is_template_medium", "is_template_loose"])
            for bboxes in self._bboxes_cache.values()
            for b in bboxes
        )
        self.remove_template_btn.setEnabled(not busy and has_templates)

    def _remove_template_and_save(self) -> None:
        """Remove template objects from PDF via redaction (in-memory, no disk save)."""
        if not self._doc or not self._file_path:
            return

        # Collect template bboxes per page
        template_rects: dict[int, list[tuple]] = {}  # page_idx → [rects in PDF pts]
        for page_num, page_bboxes in self._bboxes_cache.items():
            for bbox in page_bboxes:
                is_tpl = any(bbox.get(k, False) for k in [
                    "is_template_exact", "is_template_medium", "is_template_loose"
                ])
                if is_tpl:
                    pts = bbox.get("pts")
                    if pts:
                        template_rects.setdefault(page_num, []).append(pts)

        if not template_rects:
            from PySide6.QtWidgets import QMessageBox
            QMessageBox.information(self, "Remove Template",
                                    "No template objects detected. Run detection first.")
            return

        total_rects = sum(len(v) for v in template_rects.values())
        logger.info("Removing %d template objects from %d pages",
                     total_rects, len(template_rects))

        try:
            # Apply redactions directly to the in-memory document
            for page_idx, rects in template_rects.items():
                if page_idx >= len(self._doc):
                    continue
                page = self._doc[page_idx]
                for pts in rects:
                    x0, y0, x1, y1 = pts
                    page.add_redact_annot(
                        fitz.Rect(x0, y0, x1, y1),
                        fill=False,
                    )
                page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_REMOVE)

            # Save to a new buffer, close old doc, write buffer to working copy, reopen.
            pdf_bytes = self._doc.tobytes()
            self._doc.close()
            self._doc = None
            self._file_path.write_bytes(pdf_bytes)

            logger.info("Template removed and saved to working copy (%d rects on %d pages)",
                         total_rects, len(template_rects))

            # Reload the modified working copy (applies cropbox, rebuilds, re-detects)
            self.load_document(self._file_path)
        except Exception as e:
            logger.error("Failed to remove template: %s", e)
            from PySide6.QtWidgets import QMessageBox
            QMessageBox.critical(self, "Error", f"Failed to remove template:\n{e}")

    def _revert_to_original(self) -> None:
        """Revert in-memory document by re-opening the working copy from disk."""
        if not self._file_path:
            return

        from PySide6.QtWidgets import QMessageBox
        reply = QMessageBox.question(
            self, "Revert",
            "Revert all unsaved changes?",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        logger.info("Reverting to disk state: %s", self._file_path.name)
        self.load_document(self._file_path)

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

    def _render_layer_key(self) -> frozenset:
        """Return frozenset of hidden render layer keys."""
        hidden: set[str] = set()
        for key, cb in self._render_layer_cbs.items():
            if not cb.isChecked():
                hidden.add(key)
        return frozenset(hidden)

    def _on_render_layer_changed(self, _checked: bool = False) -> None:
        """Re-render visible pages with filtered content stream layers."""
        if not self._doc or not self._file_path:
            return
        self._pixmap_cache.clear()
        self._refresh_visible_pages()

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

        # Check if PDF layer filter changed → re-render with filtered content
        new_layer_key = self._pdf_layer_key()
        if new_layer_key != self._last_pdf_layer_key:
            self._last_pdf_layer_key = new_layer_key
            self._refresh_visible_pages()

    def _refresh_visible_pages(self) -> None:
        """Re-render visible pages with the current layer filter."""
        if not self._doc or not self._file_path:
            return
        zf = self._current_zoom_factor()
        target_dpi = zf * 72.0
        visible = set(self._visible_page_indices())

        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                pi = getattr(pw, "_page_index", -1)
                if pi < 0 or pi not in visible:
                    continue
                new_pixmap = self._render_page_sync(pi, target_dpi)
                pw.set_pixmap(new_pixmap)
        self._schedule_hires_render()

    def _persist_detection_page(self, page_num: int, pdf_objects: list, stats: str) -> None:
        """Save per-page pdf_objects + stats + file_id into the catalog meta
        so a future open can skip detection and restore instantly."""
        if not self._original_path:
            return
        try:
            meta = load_meta(self._original_path)
            meta.setdefault("pdf_objects_cache", {})[str(page_num)] = pdf_objects
            meta.setdefault("stats_cache", {})[str(page_num)] = stats
            meta["detection_version"] = PDF_OBJECTS_DETECTION_VERSION
            if "file_id" not in meta:
                try:
                    st = self._original_path.stat()
                    meta["file_id"] = {"size": st.st_size, "mtime": int(st.st_mtime)}
                except OSError:
                    pass
            save_meta(self._original_path, meta)
            self._catalog_meta = meta
        except Exception as e:
            logger.warning("Failed to persist detection cache for page %d: %s",
                           page_num + 1, e)

    def _apply_bboxes_to_spreads(self) -> None:
        """Apply cached bboxes and pdf objects to all spread widgets with current zoom."""
        zf = self._current_zoom_factor()
        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                pi = pw._page_index
                if pi < 0:
                    continue
                if pi in self._bboxes_cache:
                    pw.set_bboxes(self._bboxes_cache[pi], zf, self._detect_method_idx)
                if pi in self._pdf_objects_cache:
                    pw.set_pdf_objects(self._pdf_objects_cache[pi], zf)
                if pi in self._stats_cache:
                    pw.set_page_stats(self._stats_cache[pi])

    def _get_current_page(self) -> int:
        """Return the 0-based page index currently visible."""
        return max(0, self.page_spin.value() - 1)

    def _detect_visible_pages(self) -> None:
        """Schedule background detection for pages near the viewport."""
        if not self._doc or not self._file_path:
            return
        if getattr(self, "_detection_paused", False):
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
        if getattr(self, "_detection_paused", False):
            return
        # Don't start a new worker if one is already running
        if self._detect_worker and self._detect_worker.isRunning():
            return

        # Priority: pages near current view first, then all remaining
        current = self._get_current_page()
        start = max(0, current - 2)
        end = min(self._page_count, current + 4)

        page_num = None
        # First: nearby pages
        for p in range(start, end):
            if p not in self._bboxes_cache:
                page_num = p
                break
        # Then: any remaining page in document
        if page_num is None:
            for p in range(self._page_count):
                if p not in self._bboxes_cache:
                    page_num = p
                    break
        if page_num is None:
            self.progress.emit("")
            self._detect_progress.setValue(self._page_count)
            self._detect_progress.setFormat(f"{self._page_count}/{self._page_count} pages")
            self._update_template_btn_state()
            return

        import time
        self._op_start = time.perf_counter()
        remaining = self._page_count - len(self._bboxes_cache)
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
        # Update progress bar
        detected = len(self._bboxes_cache) + 1  # +1 for current page
        self._detect_progress.setValue(detected)
        self._detect_progress.setFormat(f"{detected}/{self._page_count} pages")

        for b in bboxes:
            b["page"] = page_num
        if self._file_path:
            bboxes = merge_detected(self._file_path, page_num, bboxes)
            self._catalog_meta = load_meta(self._file_path)

        self._bboxes_cache[page_num] = bboxes
        self._stats_cache[page_num] = stats
        self._pdf_objects_cache[page_num] = pdf_objects
        # Persist pdf_objects / stats / file_id so the whole detection can
        # be restored on next open (without re-running).
        self._persist_detection_page(page_num, pdf_objects, stats)
        self._apply_bboxes_to_spreads()

        # Run template detection at increasing intervals to avoid O(n²) on every page
        n_cached = len(self._bboxes_cache)
        all_done = n_cached >= self._page_count
        # milestones: 3, 10, 20, 50, 100, ... or when all pages done
        run_template = (
            all_done
            or n_cached == 3
            or (n_cached <= 20 and n_cached % 10 == 0)
            or (n_cached > 20 and n_cached % 20 == 0)
        )
        if run_template:
            self._detect_template_objects()
            self._save_template_marks_to_meta()
            self._apply_bboxes_to_spreads()
            self._update_template_btn_state()

        self._emit_done(f"Detect p.{pn}")
        logger.info("Detect page %d: %s", pn, stats)

    @staticmethod
    def _hash_similarity(hash1: bytes, hash2: bytes) -> float:
        """Calculate similarity between two hashes (0.0 to 1.0).

        Uses Hamming distance on binary representation.
        """
        if not hash1 or not hash2 or len(hash1) != len(hash2):
            return 0.0

        # Count bit differences
        differences = 0
        for b1, b2 in zip(hash1, hash2):
            differences += bin(b1 ^ b2).count('1')

        total_bits = len(hash1) * 8
        similarity = 1.0 - (differences / total_bits)
        return similarity

    def _detect_template_objects(self) -> None:
        """Compare bboxes across cached pages to find template objects.

        3-level template matching by position + content hash similarity:
        - Exact: position match (±5pt) + hash identical
        - Medium: position match + hash similarity 70-99%
        - Loose: position match + hash similarity 40-69%
        All must appear on 3+ pages.

        Uses spatial bucketing (grid cell = pos_tol) so only nearby bboxes
        are compared — O(n) average instead of O(n²).
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
            bbox["is_template_exact"] = False
            bbox["is_template_medium"] = False
            bbox["is_template_loose"] = False

        pos_tol = 5  # pt tolerance for position
        cell = pos_tol  # bucket grid cell size

        # Build spatial index: bucket key → list of (index, page, bbox)
        from collections import defaultdict
        buckets: dict[tuple[int, int, int, int], list[int]] = defaultdict(list)
        for i, (pi, bi) in enumerate(all_bboxes):
            pts = bi.get("pts")
            if not pts:
                continue
            key = (int(pts[0] // cell), int(pts[1] // cell),
                   int(pts[2] // cell), int(pts[3] // cell))
            buckets[key].append(i)

        def _neighbor_keys(key):
            """Yield the key itself and all adjacent cells (±1 on each coord)."""
            k0, k1, k2, k3 = key
            for d0 in (-1, 0, 1):
                for d1 in (-1, 0, 1):
                    for d2 in (-1, 0, 1):
                        for d3 in (-1, 0, 1):
                            yield (k0 + d0, k1 + d1, k2 + d2, k3 + d3)

        # Find template candidates
        exact_found = 0
        medium_found = 0
        loose_found = 0
        processed: set[int] = set()  # skip already-marked bboxes

        for i, (pi, bi) in enumerate(all_bboxes):
            if i in processed:
                continue
            if bi.get("type") == "table":
                continue
            if bi.get("excluded_from_template", False):
                continue

            pts_i = bi["pts"]
            wi = pts_i[2] - pts_i[0]
            hi = pts_i[3] - pts_i[1]
            if wi < 5 or hi < 5:
                continue

            hash_i = bi.get("_chash", b"")
            if not hash_i:
                continue

            # Only check bboxes in neighboring spatial buckets
            key_i = (int(pts_i[0] // cell), int(pts_i[1] // cell),
                     int(pts_i[2] // cell), int(pts_i[3] // cell))

            candidates: set[int] = set()
            for nk in _neighbor_keys(key_i):
                if nk in buckets:
                    candidates.update(buckets[nk])
            candidates.discard(i)

            exact_pages: set[int] = {pi}
            exact_indices: list[int] = [i]
            medium_pages: set[int] = {pi}
            medium_indices: list[int] = [i]
            loose_pages: set[int] = {pi}
            loose_indices: list[int] = [i]

            for j in candidates:
                pj, bj = all_bboxes[j]
                if pj == pi:
                    continue
                if bj.get("excluded_from_template", False):
                    continue

                pts_j = bj["pts"]
                if not (abs(pts_i[0] - pts_j[0]) <= pos_tol
                        and abs(pts_i[1] - pts_j[1]) <= pos_tol
                        and abs(pts_i[2] - pts_j[2]) <= pos_tol
                        and abs(pts_i[3] - pts_j[3]) <= pos_tol):
                    continue

                hash_j = bj.get("_chash", b"")
                if not hash_j:
                    continue

                similarity = self._hash_similarity(hash_i, hash_j)

                if similarity == 1.0:
                    exact_pages.add(pj)
                    exact_indices.append(j)
                elif similarity >= 0.7:
                    medium_pages.add(pj)
                    medium_indices.append(j)
                elif similarity >= 0.4:
                    loose_pages.add(pj)
                    loose_indices.append(j)

            # Mark templates if found on 3+ pages
            if len(exact_pages) >= 3:
                for idx in exact_indices:
                    if all_bboxes[idx][1].get("type") != "table":
                        all_bboxes[idx][1]["is_template_exact"] = True
                        processed.add(idx)
                exact_found += 1
            elif len(medium_pages) >= 3:
                for idx in medium_indices:
                    if all_bboxes[idx][1].get("type") != "table":
                        all_bboxes[idx][1]["is_template_medium"] = True
                        processed.add(idx)
                medium_found += 1
            elif len(loose_pages) >= 3:
                for idx in loose_indices:
                    if all_bboxes[idx][1].get("type") != "table":
                        all_bboxes[idx][1]["is_template_loose"] = True
                        processed.add(idx)
                loose_found += 1

        if exact_found or medium_found or loose_found:
            logger.info(
                "Template detection: %d exact, %d medium, %d loose groups",
                exact_found, medium_found, loose_found
            )

    def _save_template_marks_to_meta(self) -> None:
        """Persist template flags from bboxes_cache into catalog metadata."""
        if not self._file_path:
            return
        meta = self._catalog_meta
        objects = meta.get("objects", [])
        # Build lookup: (page, id) → saved object
        obj_map: dict[tuple[int, str], dict] = {}
        for obj in objects:
            key = (obj.get("page"), obj.get("id"))
            obj_map[key] = obj

        changed = False
        for page_num, page_bboxes in self._bboxes_cache.items():
            for bbox in page_bboxes:
                obj_id = bbox.get("id")
                if not obj_id:
                    continue
                saved = obj_map.get((page_num, obj_id))
                if not saved:
                    continue
                for flag in ("is_template_exact", "is_template_medium", "is_template_loose"):
                    new_val = bbox.get(flag, False)
                    if saved.get(flag, False) != new_val:
                        saved[flag] = new_val
                        changed = True

        if changed:
            save_meta(self._file_path, meta)

    def _on_detect_worker_done(self) -> None:
        """Worker finished — schedule next page if needed."""
        self._detect_worker = None
        # Check if more pages need detection
        self._detect_timer.start(10)

    @staticmethod
    def _split_text_block_lines(lines: list[dict]) -> list[list[dict]]:
        """Split block lines into groups that overlap horizontally.

        PyMuPDF sometimes merges lines at the same Y but different X ranges
        into one block (e.g. left title + right title).  This splits them
        into separate groups when their X-ranges don't overlap.

        To avoid splitting table/TOC blocks (where columns don't overlap in X
        but belong together), we check how many Y-rows have lines from
        multiple groups.  If more than 2 such rows exist the block is
        columnar/tabular and we keep it as one.
        """
        if len(lines) <= 1:
            return [lines] if lines else []
        # Build groups by merging lines whose X-ranges overlap
        groups: list[list[dict]] = []
        for ln in lines:
            lx0, lx1 = ln["bbox"][0], ln["bbox"][2]
            merged = False
            for g in groups:
                gx0 = min(l["bbox"][0] for l in g)
                gx1 = max(l["bbox"][2] for l in g)
                if lx0 < gx1 and lx1 > gx0:  # overlap
                    g.append(ln)
                    merged = True
                    break
            if not merged:
                groups.append([ln])
        if len(groups) <= 1:
            return groups
        # Check for columnar/tabular structure: count Y-rows where lines
        # from 2+ different groups co-occur (Y-tolerance 2pt).
        y_tol = 2.0
        # Assign each line a group index
        line_group: dict[int, int] = {}
        for gi, g in enumerate(groups):
            for ln in g:
                line_group[id(ln)] = gi
        # Bucket lines by Y-midpoint
        y_rows: list[list[dict]] = []
        for ln in lines:
            ym = (ln["bbox"][1] + ln["bbox"][3]) / 2
            placed = False
            for row in y_rows:
                ref_ym = (row[0]["bbox"][1] + row[0]["bbox"][3]) / 2
                if abs(ym - ref_ym) <= y_tol:
                    row.append(ln)
                    placed = True
                    break
            if not placed:
                y_rows.append([ln])
        shared_rows = 0
        for row in y_rows:
            group_ids = {line_group[id(ln)] for ln in row}
            if len(group_ids) >= 2:
                shared_rows += 1
        if shared_rows > 2:
            return [lines]  # columnar — keep as one block
        return groups

    def _extract_pdf_objects(self, page) -> list[dict]:
        """Extract native PDF object bounding boxes from a page.

        Includes text, images, tables, and graphics (shapes, lines, curves).
        Excludes objects that cover the entire mediabox.
        """
        objects: list[dict] = []
        obj_num = 0

        # Get mediabox for filtering full-page objects
        mediabox = page.mediabox
        mb_width = mediabox.width
        mb_height = mediabox.height
        mb_area = mb_width * mb_height
        try:
            # get_text("dict") returns text and images
            td = page.get_text("dict")
            blocks = td["blocks"]
            for b in blocks:
                bbox = b["bbox"]
                obj_num += 1
                if b["type"] == 0:
                    pdf_type = "pdf_text"
                    lines = b.get("lines", [])
                    # Split block into sub-groups when lines don't
                    # overlap horizontally (PyMuPDF merges them).
                    sub_groups = self._split_text_block_lines(lines)
                    for sg_lines in sub_groups:
                        # Compute tight bbox for this sub-group
                        sg_x0 = min(ln["bbox"][0] for ln in sg_lines)
                        sg_y0 = min(ln["bbox"][1] for ln in sg_lines)
                        sg_x1 = max(ln["bbox"][2] for ln in sg_lines)
                        sg_y1 = max(ln["bbox"][3] for ln in sg_lines)
                        preview = ""
                        for ln in sg_lines[:2]:
                            for sp in ln.get("spans", []):
                                preview += sp.get("text", "") + " "
                        preview = preview.strip()[:30]
                        label = f"#{obj_num} text: {preview}" if preview else f"#{obj_num} text"
                        # Content fingerprint: text + font + size
                        fp_parts = []
                        for ln in sg_lines:
                            for sp in ln.get("spans", []):
                                fp_parts.append(f"{sp.get('text','')}\t{sp.get('font','')}\t{sp.get('size',0):.1f}")
                        objects.append({
                            "pdf_type": pdf_type,
                            "label": label,
                            "pts": (sg_x0, sg_y0, sg_x1, sg_y1),
                            "_content_fp": "T|" + "\n".join(fp_parts),
                        })
                        if len(sub_groups) > 1:
                            obj_num += 1
                elif b["type"] == 1:
                    pdf_type = "pdf_image"
                    w = int(bbox[2] - bbox[0])
                    h = int(bbox[3] - bbox[1])
                    label = f"#{obj_num} image {w}x{h}pt"
                    # Content fingerprint: hash of image data
                    import hashlib as _hl
                    img_data = b.get("image", b"")
                    img_hash = _hl.md5(img_data).hexdigest() if img_data else "none"
                    objects.append({
                        "pdf_type": pdf_type,
                        "label": label,
                        "pts": (bbox[0], bbox[1], bbox[2], bbox[3]),
                        "_content_fp": f"I|{w}x{h}|{img_hash}",
                    })
                else:
                    pdf_type = "pdf_text"
                    label = f"#{obj_num} block-{b['type']}"
                    objects.append({
                        "pdf_type": pdf_type,
                        "label": label,
                        "pts": (bbox[0], bbox[1], bbox[2], bbox[3]),
                        "_content_fp": f"B|type={b['type']}",
                    })
        except Exception:
            pass
        try:
            tables = page.find_tables()
            if tables:
                for t in tables.tables:
                    obj_num += 1
                    objects.append({
                        "pdf_type": "pdf_table",
                        "label": f"#{obj_num} table {t.row_count}x{t.col_count}",
                        "pts": tuple(t.bbox),
                        "_content_fp": f"TBL|{t.row_count}x{t.col_count}",
                    })
        except Exception:
            pass
        try:
            # Extract graphics (shapes, lines, curves)
            drawings = page.get_drawings()
            for drw in drawings:
                # Compute bbox from ALL points in items, not just drw["rect"]
                # (PyMuPDF rect may exclude line endpoints beyond curve bounds)
                all_pts: list[tuple[float, float]] = []
                for item in drw.get("items", []):
                    kind = item[0]
                    if kind == "l":  # line: (kind, p1, p2)
                        all_pts.append((item[1].x, item[1].y))
                        all_pts.append((item[2].x, item[2].y))
                    elif kind == "c":  # cubic bezier: (kind, p1, p2, p3, p4)
                        for p in item[1:5]:
                            all_pts.append((p.x, p.y))
                    elif kind == "re":  # rect: (kind, rect)
                        r = item[1]
                        all_pts.append((r.x0, r.y0))
                        all_pts.append((r.x1, r.y1))
                    elif kind == "qu":  # quad: (kind, quad)
                        q = item[1]
                        for p in [q.ul, q.ur, q.ll, q.lr]:
                            all_pts.append((p.x, p.y))
                if not all_pts:
                    rect = drw.get("rect")
                    if not rect:
                        continue
                    x0, y0, x1, y1 = rect
                else:
                    x0 = min(p[0] for p in all_pts)
                    y0 = min(p[1] for p in all_pts)
                    x1 = max(p[0] for p in all_pts)
                    y1 = max(p[1] for p in all_pts)
                if x1 <= x0 or y1 <= y0:
                    continue
                w = int(x1 - x0)
                h = int(y1 - y0)
                # Content fingerprint: serialized path commands
                fp_items = []
                for item in drw.get("items", []):
                    kind = item[0]
                    if kind == "l":
                        fp_items.append(f"l({item[1].x:.1f},{item[1].y:.1f},{item[2].x:.1f},{item[2].y:.1f})")
                    elif kind == "c":
                        fp_items.append(f"c({','.join(f'{p.x:.1f},{p.y:.1f}' for p in item[1:5])})")
                    elif kind == "re":
                        r = item[1]
                        fp_items.append(f"re({r.x0:.1f},{r.y0:.1f},{r.x1:.1f},{r.y1:.1f})")
                    elif kind == "qu":
                        q = item[1]
                        fp_items.append(f"qu({q.ul.x:.1f},{q.ul.y:.1f},{q.lr.x:.1f},{q.lr.y:.1f})")
                fill = drw.get("fill")
                color = drw.get("color")
                fp_header = f"G|n={len(fp_items)}|f={fill}|c={color}"
                # Count h/v lines vs other items for table detection
                n_hv = 0
                n_items = len(drw.get("items", []))
                for item in drw.get("items", []):
                    if item[0] == "l":
                        dx = abs(item[2].x - item[1].x)
                        dy = abs(item[2].y - item[1].y)
                        if dx < 1.0 or dy < 1.0:  # h or v line
                            n_hv += 1
                obj_num += 1
                objects.append({
                    "pdf_type": "pdf_drawing",  # vectors / line-art, NOT raster images
                    "label": f"#{obj_num} gfx {w}x{h}",
                    "pts": (x0, y0, x1, y1),
                    "_content_fp": fp_header + "|" + ";".join(fp_items),
                    "_n_hv_lines": n_hv,
                    "_n_items": n_items,
                })
        except Exception:
            pass
        try:
            # Raster image placements. full=True is required so that
            # get_image_bbox() accepts each entry from this list.
            image_list = page.get_images(full=True)
            for img_idx in image_list:
                xref = img_idx[0]
                try:
                    rect = page.get_image_bbox(img_idx)
                    if not rect.is_empty:
                        obj_num += 1
                        objects.append({
                            "pdf_type": "pdf_image",
                            "label": f"#{obj_num} image xref={xref}",
                            "pts": (rect.x0, rect.y0, rect.x1, rect.y1),
                            "_content_fp": f"IMG|xref={xref}",
                        })
                except Exception:
                    pass
        except Exception:
            pass
        try:
            # Form XObjects: only top-level ones placed directly on the page
            # (stream_xref == 0). Nested xobjects are already drawn as part
            # of their parent form, and entries whose bbox lies entirely
            # outside the mediabox are not actually visible.
            xobjects = page.get_xobjects()
            mb = page.rect
            for xobj in xobjects:
                try:
                    if not (isinstance(xobj, (list, tuple)) and len(xobj) >= 4):
                        continue
                    xref, name, stream_xref, bbox = xobj[0], xobj[1], xobj[2], xobj[3]
                    if stream_xref != 0:
                        continue
                    if not (isinstance(bbox, (list, tuple)) and len(bbox) == 4):
                        continue
                    x0, y0, x1, y1 = bbox
                    if x1 <= mb.x0 or x0 >= mb.x1 or y1 <= mb.y0 or y0 >= mb.y1:
                        continue
                    obj_num += 1
                    objects.append({
                        "pdf_type": "pdf_image",
                        "label": f"#{obj_num} xobj {name}",
                        "pts": tuple(bbox),
                        "_content_fp": f"XOBJ|xref={xref}|{name}",
                    })
                except Exception:
                    pass
        except Exception:
            pass

        # Deduplicate pdf_image entries whose bboxes substantially overlap
        # another already-kept one. Handles the common case where the same
        # xref is reported by text-dict type=1 blocks *and* by get_images(),
        # or duplicated across nested Form XObject references.
        def _img_bbox_similar(a, b, min_overlap: float = 0.95) -> bool:
            ax0, ay0, ax1, ay1 = a
            bx0, by0, bx1, by1 = b
            ix0, iy0 = max(ax0, bx0), max(ay0, by0)
            ix1, iy1 = min(ax1, bx1), min(ay1, by1)
            if ix1 <= ix0 or iy1 <= iy0:
                return False
            inter = (ix1 - ix0) * (iy1 - iy0)
            area_a = max(0.0, (ax1 - ax0) * (ay1 - ay0))
            area_b = max(0.0, (bx1 - bx0) * (by1 - by0))
            min_area = min(area_a, area_b)
            return min_area > 0 and inter / min_area >= min_overlap

        kept_image_bboxes: list[tuple[float, float, float, float]] = []
        deduped: list[dict] = []
        for obj in objects:
            if obj.get("pdf_type") == "pdf_image":
                pts = obj.get("pts")
                if pts and any(_img_bbox_similar(pts, k) for k in kept_image_bboxes):
                    continue
                if pts:
                    kept_image_bboxes.append(pts)
            deduped.append(obj)
        objects = deduped

        # Filter out objects that cover the entire mediabox (or nearly all of it)
        # An object is considered "full mediabox" if it covers >95% of the page area
        filtered_objects = []
        for obj in objects:
            pts = obj["pts"]
            x0, y0, x1, y1 = pts
            obj_width = x1 - x0
            obj_height = y1 - y0
            obj_area = obj_width * obj_height
            # Skip if object covers >95% of page area (likely a background or page frame)
            if obj_area > 0.95 * mb_area:
                logger.debug(f"Filtered out mediabox-covering object: {obj['label']}")
                continue
            filtered_objects.append(obj)

        return filtered_objects

    def _detect_native_page(self, page) -> tuple[list[dict], str]:
        """Detect object bounding boxes via rasterization.

        Renders the page to bitmap, finds connected content regions
        within the cropping area.
        Returns bboxes with 'pts' = (x0, y0, x1, y1) in PDF points.
        Type recognition is a separate stage.
        """
        method_idx = getattr(self, '_detect_method_idx', 8)  # default: Hybrid 2-pass

        # ── Fast path: PDF-only methods (no rasterization needed) ──
        if method_idx == 9:
            pdf_objs = self._extract_pdf_objects(page)
            type_map = {
                "pdf_text": "text",
                "pdf_image": "photo",
                "pdf_drawing": "drawing",
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

        # Pure PDF method: preserve native object types
        if method_idx == 10:
            import hashlib
            _pure_type_map = {
                "pdf_text": "text",
                "pdf_image": "photo",
                "pdf_drawing": "drawing",
                "pdf_table": "table",
            }
            pdf_objs = self._extract_pdf_objects(page)

            # ── Vector table detection ───────────────────────────
            # Drawings mostly composed of h/v lines → table grids.
            # Merge overlapping grid rects, then absorb text inside.
            mb_area = page.rect.width * page.rect.height
            grid_rects: list[list[float]] = []  # mutable [x0,y0,x1,y1]
            for obj in pdf_objs:
                if obj.get("pdf_type") != "pdf_drawing":
                    continue
                n_hv = obj.get("_n_hv_lines", 0)
                n_items = obj.get("_n_items", 0)
                if n_items < 5 or n_hv / n_items < 0.8:
                    continue
                pts = obj["pts"]
                w, h = pts[2] - pts[0], pts[3] - pts[1]
                if w < 50 or h < 30:
                    continue
                # Skip full-page drawings (borders, backgrounds)
                if w * h > 0.8 * mb_area:
                    continue
                # Merge with existing grid rects if overlapping
                merged = False
                for gr in grid_rects:
                    if pts[0] <= gr[2] and pts[2] >= gr[0] and pts[1] <= gr[3] and pts[3] >= gr[1]:
                        gr[0] = min(gr[0], pts[0])
                        gr[1] = min(gr[1], pts[1])
                        gr[2] = max(gr[2], pts[2])
                        gr[3] = max(gr[3], pts[3])
                        merged = True
                        break
                if not merged:
                    grid_rects.append([pts[0], pts[1], pts[2], pts[3]])

            # Collect all table rects: find_tables + vector grids
            table_rects: list[tuple[float, ...]] = []
            for obj in pdf_objs:
                if obj.get("pdf_type") == "pdf_table":
                    table_rects.append(obj["pts"])
            for gr in grid_rects:
                table_rects.append(tuple(gr))

            # ── Build bboxes ─────────────────────────────────────
            bboxes: list[dict] = []
            for i, obj in enumerate(pdf_objs):
                pts = obj.get("pts")
                obj_type = _pure_type_map.get(
                    obj.get("pdf_type", ""), "unknown")
                # Mark objects inside table regions
                in_table = obj_type == "table"
                if not in_table and pts:
                    for tr in table_rects:
                        if (pts[0] >= tr[0] - 2 and pts[1] >= tr[1] - 2
                                and pts[2] <= tr[2] + 2 and pts[3] <= tr[3] + 2):
                            in_table = True
                            break
                # No content hash for table-region objects
                fp = obj.get("_content_fp", "")
                if fp and not in_table:
                    digest = hashlib.sha256(fp.encode("utf-8")).digest()
                    chash = digest[:32]
                else:
                    chash = b""
                bboxes.append({
                    "type": obj_type,
                    "label": obj.get("label", f"#{i}"),
                    "pts": pts,
                    "_chash": chash,
                    "_in_table": in_table,
                })

            # Add table group overlays (for cyan fill rendering)
            for gi, gr in enumerate(grid_rects):
                # Count text objects inside this grid
                n_text = sum(
                    1 for b in bboxes
                    if b["type"] == "text" and b.get("_in_table")
                    and b["pts"][0] >= gr[0] - 2 and b["pts"][1] >= gr[1] - 2
                    and b["pts"][2] <= gr[2] + 2 and b["pts"][3] <= gr[3] + 2
                )
                if n_text >= 3:
                    bboxes.append({
                        "type": "table",
                        "label": f"table-region ({n_text} texts)",
                        "pts": tuple(gr),
                        "_chash": b"",
                        "_table_group": True,
                    })

            stats = _format_page_stats(page.number, bboxes)
            return bboxes, stats

        # ── Rasterization path (for scanline-based methods) ──────
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
        td_blocks = page.get_text("dict")["blocks"]
        for tb in td_blocks:
            if tb.get("type") != 0:
                continue
            lines = tb.get("lines", [])
            sub_groups = PreviewView._split_text_block_lines(lines)
            for sg in sub_groups:
                tx0 = max(0, int(min(ln["bbox"][0] for ln in sg)))
                ty0 = max(0, int(min(ln["bbox"][1] for ln in sg)))
                tx1 = min(w_img, int(max(ln["bbox"][2] for ln in sg)))
                ty1 = min(h_img, int(max(ln["bbox"][3] for ln in sg)))
                if tx1 > tx0 and ty1 > ty0:
                    binary[ty0:ty1, tx0:tx1] = 0
                    pdf_text_bboxes.append({
                        "type": "text",
                        "label": f"text {tx1-tx0}x{ty1-ty0}pt",
                        "pts": (float(tx0), float(ty0),
                                float(tx1), float(ty1)),
                    })

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

        # Hybrid 2-pass V.2 with Pre-Pass (refine text masks before pass 1)
        elif method_idx == 11:  # Hybrid 2-pass V.2
            # Pre-Pass: scan masked text areas, exclude whitespace
            binary_with_prepass = binary.copy()
            text_skip_refined = self._refine_text_masks(
                gray, binary_with_prepass, pdf_text_bboxes, h_img, w_img
            )
            # Pass 1 on raw binary with refined text skipping
            pass1_raw_v2, _ = _scanline_v1_core(
                binary_raw.copy(), h_img, w_img, margin=3,
                min_obj=min_obj_size, skip_rects=text_skip_refined,
            )
            # Filter pass 1
            pass1_filtered_v2: list[tuple[int, int, int, int]] = []
            for bt in pass1_raw_v2:
                bbox_area = max((bt[2]-bt[0]) * (bt[3]-bt[1]), 1)
                text_cover = 0.0
                for tb in pdf_text_bboxes:
                    tp = tb["pts"]
                    ix0 = max(bt[0], tp[0]); iy0 = max(bt[1], tp[1])
                    ix1 = min(bt[2], tp[2]); iy1 = min(bt[3], tp[3])
                    if ix0 < ix1 and iy0 < iy1:
                        text_cover += (ix1-ix0) * (iy1-iy0)
                if text_cover / bbox_area < 0.8:
                    pass1_filtered_v2.append(bt)
            # Pass 2: merge fragmented
            binary_full = (gray < 240).astype(np.uint8)
            pass2_raw, _ = _scanline_v1_core(
                binary_full, h_img, w_img, margin=3, min_obj=min_obj_size,
            )
            used_p1: set[int] = set()
            merged_pts: list[tuple[int, int, int, int]] = []
            for p2 in pass2_raw:
                contained: list[int] = []
                for i, p1 in enumerate(pass1_filtered_v2):
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
            final_pts = [p for i, p in enumerate(pass1_filtered_v2) if i not in used_p1]
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
    def _refine_text_masks(gray, binary: "np.ndarray", pdf_text_bboxes: list[dict],
                           h_img: int, w_img: int) -> list[tuple]:
        """Pre-Pass: refine text masks by excluding whitespace areas.

        For each PDF text block, scan the region and exclude areas with
        brightness > 220 (whitespace between text). This allows drawings
        that cross text areas to be detected more completely.

        Returns: refined skip_rects with whitespace excluded.
        """
        import numpy as np

        refined_rects: list[tuple] = []

        for tb in pdf_text_bboxes:
            pts = tb["pts"]
            x0, y0, x1, y1 = int(pts[0]), int(pts[1]), int(pts[2]), int(pts[3])

            # Clamp to image bounds
            x0 = max(0, x0); y0 = max(0, y0)
            x1 = min(w_img, x1); y1 = min(h_img, y1)

            if x1 <= x0 or y1 <= y0:
                continue

            # Extract text block region from grayscale
            text_region = gray[y0:y1, x0:x1]

            # Find actual text pixels (dark, <220) vs whitespace (bright, >=220)
            dark_mask = text_region < 220

            # Scan vertically to find text rows
            row_has_text = np.any(dark_mask, axis=1)
            row_indices = np.where(row_has_text)[0]

            if len(row_indices) == 0:
                # No text pixels found, skip this region
                continue

            # Find first and last row with text
            first_row = row_indices[0]
            last_row = row_indices[-1] + 1

            # Scan horizontally within text rows
            col_has_text = np.any(dark_mask[first_row:last_row, :], axis=0)
            col_indices = np.where(col_has_text)[0]

            if len(col_indices) == 0:
                continue

            # Find first and last column with text
            first_col = col_indices[0]
            last_col = col_indices[-1] + 1

            # Create refined rect (text content only, no whitespace margins)
            refined_x0 = x0 + first_col
            refined_y0 = y0 + first_row
            refined_x1 = x0 + last_col
            refined_y1 = y0 + last_row

            refined_rects.append((refined_x0, refined_y0, refined_x1, refined_y1))

        return refined_rects

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
        from app.pdf.tablelines import extract_table_lines

        return extract_table_lines(page, table_rect)

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
        """Launch standalone testbench process for the given page."""
        import subprocess
        import sys

        if not self._file_path or not self._doc or page_idx < 0:
            logger.warning("Testbench: no file loaded (file_path=%s, page_idx=%d)", self._file_path, page_idx)
            return

        project_root = Path(__file__).resolve().parent.parent.parent
        testbench_script = project_root / "testbench.py"
        venv_python = project_root / ".venv" / "Scripts" / "python.exe"

        # Use venv python if available, otherwise sys.executable
        python_exe = str(venv_python) if venv_python.exists() else sys.executable

        if not testbench_script.exists():
            logger.error("Testbench script not found: %s", testbench_script)
            return

        cmd = [python_exe, str(testbench_script), str(self._file_path), str(page_idx + 1)]
        logger.info("Launching testbench: %s", " ".join(cmd))

        subprocess.Popen(cmd, cwd=str(project_root))

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

    def _on_template_excluded(self, obj_id: str) -> None:
        """Exclude object from template matching by ID.

        Marks all objects at the same position (±5pt) across all pages
        with excluded_from_template=True, then re-runs template detection.
        """
        if not self._file_path:
            return

        # Find the excluded object to get its position
        excluded_pts = None
        for bboxes in self._bboxes_cache.values():
            for bbox in bboxes:
                if bbox.get("id") == obj_id:
                    excluded_pts = bbox.get("pts")
                    break
            if excluded_pts:
                break

        if not excluded_pts:
            return

        # Mark all objects at same position as excluded
        pos_tol = 5
        x0, y0, x1, y1 = excluded_pts
        for bboxes in self._bboxes_cache.values():
            for bbox in bboxes:
                pts = bbox.get("pts")
                if not pts:
                    continue
                # Check if position matches
                pos_match = (abs(x0 - pts[0]) <= pos_tol and
                             abs(y0 - pts[1]) <= pos_tol and
                             abs(x1 - pts[2]) <= pos_tol and
                             abs(y1 - pts[3]) <= pos_tol)
                if pos_match:
                    bbox["excluded_from_template"] = True

        # Update metadata
        update_object(self._file_path, obj_id, excluded_from_template=True)
        self._catalog_meta = load_meta(self._file_path)

        # Clear template marks and re-detect
        for bboxes in self._bboxes_cache.values():
            for bbox in bboxes:
                bbox["is_template_exact"] = False
                bbox["is_template_medium"] = False
                bbox["is_template_loose"] = False

        self._detect_template_objects()
        self._apply_bboxes_to_spreads()
        logger.info(f"Template excluded for object {obj_id}")

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

    def _on_page_rerender_requested(self, page_idx: int) -> None:
        """Re-render page pixmap after bbox modification (e.g., Pure PDF drag)."""
        logger.info(f"Page rerender requested: page {page_idx}")
        if page_idx < 0 or page_idx >= len(self._doc):
            return
        # Clear pixmap cache for this page to force re-render
        self._pixmap_cache.pop(page_idx, None)
        # Re-render and update spreads
        zf = self._current_zoom_factor()
        new_pixmap = self._render_page_sync(page_idx, zf * 72.0)
        logger.info(f"Page {page_idx} re-rendered at zoom {zf}, size: {new_pixmap.width()}x{new_pixmap.height()}")
        # Update pixmap in the PageWidget, preserve selection
        saved_selected_idx = -1
        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                if pw._page_index == page_idx:
                    logger.info(f"Updating pixmap for page widget {page_idx}, selection={pw._selected_idx}")
                    saved_selected_idx = pw._selected_idx
                    pw.set_pixmap(new_pixmap)
                    # Reset offset explicitly
                    pw._pure_pdf_drag_offset = (0.0, 0.0)
                    pw.update()
        # Reapply all bboxes to force redraw (will reset _selected_idx)
        self._apply_bboxes_to_spreads()
        # Restore selection after bboxes are reapplied
        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                if pw._page_index == page_idx:
                    logger.info(f"Restoring selection {saved_selected_idx} after reapply")
                    pw._selected_idx = saved_selected_idx
                    pw.update()

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

    def _apply_rulers_visibility(self) -> None:
        """Show/hide rulers on the scroll area, reserving viewport margins."""
        show = bool(getattr(self, "_show_rulers", False))
        if show:
            self.scroll_area.setViewportMargins(_Ruler.V, _Ruler.H, 0, 0)
            self._position_rulers()
            self._h_ruler.show()
            self._v_ruler.show()
            self._sync_rulers_scale()
            self._sync_rulers_offset()
            self._h_ruler.raise_()
            self._v_ruler.raise_()
        else:
            self.scroll_area.setViewportMargins(0, 0, 0, 0)
            self._h_ruler.hide()
            self._v_ruler.hide()

    def _position_rulers(self) -> None:
        """Lay out rulers along the scroll area's margins."""
        r = self.scroll_area.rect()
        self._v_ruler.setGeometry(0, _Ruler.H, _Ruler.V, r.height() - _Ruler.H)
        self._h_ruler.setGeometry(_Ruler.V, 0, r.width() - _Ruler.V, _Ruler.H)

    def _sync_rulers_scale(self) -> None:
        self._h_ruler.set_scale(self._zoom, self._base_dpi)
        self._v_ruler.set_scale(self._zoom, self._base_dpi)

    def _sync_rulers_offset(self) -> None:
        if not getattr(self, "_show_rulers", False):
            return
        hbar = self.scroll_area.horizontalScrollBar()
        vbar = self.scroll_area.verticalScrollBar()
        origin_x = self._spreads[0].x() if self._spreads else 0
        origin_y = self._spreads[0].y() if self._spreads else 0
        self._h_ruler.set_offset(hbar.value(), origin_x)
        self._v_ruler.set_offset(vbar.value(), origin_y)

        # Per-page horizontal segments — each page starts its own 0 mm origin
        # at the spine side. That means ANY left-positioned page (including
        # back-cover-alone) has 0 mm at its right edge (spine side) with values
        # growing leftward — i.e. mirror=True. Right-positioned pages always
        # have 0 at the left edge growing rightward — mirror=False.
        from PySide6.QtCore import QPoint
        h_segs: list[tuple[int, int, bool]] = []
        for spread in self._spreads:
            for pw, is_left in ((spread.left_page, True), (spread.right_page, False)):
                pi = getattr(pw, "_page_index", -1)
                if pi < 0:
                    continue  # blank side — skip
                try:
                    x_in_content = pw.mapTo(self.pages_container, QPoint(0, 0)).x()
                except Exception:
                    continue
                h_segs.append((x_in_content, pw.width(), is_left))
        self._h_ruler.set_segments(h_segs)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        if getattr(self, "_h_ruler", None) is not None:
            self._position_rulers()

    def rotate_right(self) -> None:
        """Rotate every page 90° clockwise and re-render."""
        if not self._doc:
            return
        self._cancel_render_worker()
        for i in range(len(self._doc)):
            page = self._doc[i]
            page.set_rotation((page.rotation + 90) % 360)
        # Rotated pages have swapped width/height — invalidate caches + rebuild
        self._pixmap_cache.clear()
        self._bboxes_cache.clear()
        self._stats_cache.clear()
        self._pdf_objects_cache.clear()
        self._rebuild_spreads()

    def refresh(self) -> None:
        pass


class LayoutView(PreviewView):
    """PDF view with native PDF-object detection enabled and bounding boxes shown.

    Shares rendering infrastructure with PreviewView but runs the "PDF Objects"
    detection method (index 10) on load and on scroll, and makes the PDF Objects
    layer visible so users see detected text / image / table bboxes.
    """

    def __init__(self) -> None:
        super().__init__(view_only=False)
        # Layout view is for seeing detected-object bboxes — favour speed over
        # render sharpness. Lower DPI cuts render time ~6-8x vs Preview's 225.
        self._base_dpi = 90
        # Layout is read-only: selection highlights, but no drag / resize / handles.
        self._bbox_edit_allowed = False
        # Default to "Native PDF object extraction only" (index 10 in _detect_method_combo)
        self._detect_method_combo.setCurrentIndex(10)
        self._detect_method_idx = 10

        # Turn on the PDF Objects layer + its sub-types so bboxes are visible
        for key in ("pdf_objects", "pdf_text", "pdf_image", "pdf_table"):
            cb = self._filter_checkboxes.get(key)
            if cb is not None:
                cb.setChecked(True)
            act = self.bbox_filter_actions.get(key)
            if act is not None and hasattr(act, "setChecked"):
                act.setChecked(True)
        self._on_bbox_filter_changed()

        # Layout tab hides the full Preview toolbar (zoom / fit / page nav live
        # on the app header) but gets its own compact header with a "View ▾"
        # dropdown for layer-visibility checkboxes.
        self._toolbar_scroll.setVisible(False)
        self._build_layout_header()

    def _transform_pixmap(self, qpix: QPixmap) -> QPixmap:
        """B/W with white background → transparent.

        Pipeline: rendered RGB → compute luminance → pixels near white get
        alpha=0 (transparent background), all other pixels become solid black
        with full alpha. Result: content (text, vectors, images) drawn as
        black silhouettes on transparent canvas; PageWidget background fills
        where white was."""
        if qpix.isNull():
            return qpix
        import numpy as np
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

        # Luminance (simple average; fast enough at Layout's low DPI)
        lum = (arr[:, :, 0].astype(np.int16)
               + arr[:, :, 1].astype(np.int16)
               + arr[:, :, 2].astype(np.int16)) // 3
        white_mask = lum > 235  # treat near-white as background

        # Transparent where white, opaque otherwise
        arr[:, :, 3] = np.where(white_mask, 0, 255).astype(np.uint8)
        # Collapse all non-transparent pixels to pure black for high contrast
        arr[:, :, 0] = np.where(white_mask, 255, 0).astype(np.uint8)
        arr[:, :, 1] = np.where(white_mask, 255, 0).astype(np.uint8)
        arr[:, :, 2] = np.where(white_mask, 255, 0).astype(np.uint8)

        out = QImage(arr.tobytes(), w, h, w * 4, QImage.Format.Format_RGBA8888)
        return QPixmap.fromImage(out.copy())

    def _build_layout_header(self) -> None:
        """Compact tab header with a dropdown for layer visibility."""
        from PySide6.QtWidgets import QToolButton, QMenu, QWidgetAction

        header = QWidget()
        h = QHBoxLayout(header)
        h.setContentsMargins(8, 4, 8, 4)
        h.setSpacing(8)

        view_btn = QToolButton()
        view_btn.setText("View ▾")
        view_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        menu = QMenu(view_btn)

        # "PDF Objects" checkbox — toggles bbox filter + sub-types
        pdf_cb = QCheckBox("PDF Objects")
        pdf_cb.setChecked(bool(getattr(self, "_bboxes_visible", True)))

        # Nested "Show labels" checkbox — enabled only when PDF Objects is on
        labels_cb = QCheckBox("Show labels")
        labels_cb.setChecked(bool(getattr(self, "_show_object_labels", True)))
        labels_cb.setEnabled(pdf_cb.isChecked())

        def _apply_labels_to_pages() -> None:
            show = labels_cb.isChecked() and pdf_cb.isChecked()
            self._show_object_labels = show  # propagate to future PageWidgets
            for spread in self._spreads:
                for pw in (spread.left_page, spread.right_page):
                    pw.set_show_object_labels(show)

        def _on_pdf_objects_toggled(checked: bool) -> None:
            # Sync the legacy pdf_* sub-layer filters (hidden, kept for
            # compatibility with the bbox-filter-changed dispatcher).
            for k in ("pdf_objects", "pdf_text", "pdf_image", "pdf_table"):
                cb = self._filter_checkboxes.get(k)
                if cb is not None:
                    cb.blockSignals(True)
                    cb.setChecked(checked)
                    cb.blockSignals(False)
            self._on_bbox_filter_changed()
            # Master gate: toggle bbox rendering on every PageWidget.
            self._bboxes_visible = checked
            # Per-type child state (Text / Vectors / Images / Tables) must
            # survive a parent OFF→ON cycle. _on_bbox_filter_changed above
            # rebuilds _visible_layers from _filter_checkboxes and clobbers
            # child selections — re-apply _layout_layer_state AFTER it.
            for spread in self._spreads:
                for pw in (spread.left_page, spread.right_page):
                    pw.set_show_bboxes(checked)
                    for k, v in getattr(self, "_layout_layer_state", {}).items():
                        pw._visible_layers[k] = v
                    pw.update()
            # Link child items to parent: disable when parent is off.
            labels_cb.setEnabled(checked)
            for _child_cb in getattr(self, "_layout_type_cbs", {}).values():
                _child_cb.setEnabled(checked)
            _apply_labels_to_pages()

        pdf_cb.toggled.connect(_on_pdf_objects_toggled)
        pdf_wrap = QWidget()
        pdf_row = QHBoxLayout(pdf_wrap)
        pdf_row.setContentsMargins(6, 2, 6, 2)
        pdf_row.addWidget(pdf_cb)
        pdf_act = QWidgetAction(menu)
        pdf_act.setDefaultWidget(pdf_wrap)
        menu.addAction(pdf_act)

        # Nested labels row (indented under PDF Objects)
        labels_cb.toggled.connect(lambda _c: _apply_labels_to_pages())
        labels_wrap = QWidget()
        labels_row = QHBoxLayout(labels_wrap)
        labels_row.setContentsMargins(24, 2, 6, 2)  # left-indent for hierarchy
        labels_row.addWidget(labels_cb)
        labels_act = QWidgetAction(menu)
        labels_act.setDefaultWidget(labels_wrap)
        menu.addAction(labels_act)

        self._layout_show_labels_cb = labels_cb  # keep a reference; reapply on spread rebuild
        self._layout_pdf_cb = pdf_cb  # expose for state restore / external save hooks

        # Per-type filter rows nested under "Show labels" (controls which
        # detected object types are displayed at all).
        self._layout_type_cbs: dict[str, QCheckBox] = {}
        _TYPE_FILTERS = [
            ("Text",    ("text", "pdf_text")),
            ("Vectors", ("drawing", "gfx", "pdf_drawing")),
            ("Images",  ("image", "photo", "picture", "pdf_image")),
            ("Tables",  ("table", "pdf_table")),
        ]
        # Per-view persistent layer state so rebuilt spreads keep user choices.
        # Force-disable the generic "bbox" umbrella layer — it renders as the
        # dark-blue rectangles around every detected object (Layer 1 in
        # PageWidget.paintEvent) which clashes with the per-type filters that
        # Layout exposes via the View-dropdown.
        if not hasattr(self, "_layout_layer_state"):
            self._layout_layer_state: dict[str, bool] = {}
        self._layout_layer_state["bbox"] = False
        # Push to any spreads that already exist (rebuild on next load otherwise).
        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                pw._visible_layers["bbox"] = False
                pw.update()

        def _apply_type_filter(keys: tuple[str, ...], checked: bool) -> None:
            # Apply directly to every PageWidget's _visible_layers — bypassing
            # the legacy `_on_bbox_filter_changed` auto-re-enable that fires
            # when all pdf_* subs are unchecked.
            for k in keys:
                self._layout_layer_state[k] = checked
            for spread in self._spreads:
                for pw in (spread.left_page, spread.right_page):
                    for k in keys:
                        pw._visible_layers[k] = checked
                    pw.update()

        for name, keys in _TYPE_FILTERS:
            # Initial check state: True if ANY of the underlying filters is on
            initial = any(
                (self._filter_checkboxes.get(k).isChecked()
                 if self._filter_checkboxes.get(k) else False)
                for k in keys
            ) or True  # default to on for a fresh view
            cb = QCheckBox(name)
            cb.setChecked(bool(initial))
            cb.setEnabled(pdf_cb.isChecked())  # child of PDF Objects, same level as "Show labels"
            cb.toggled.connect(lambda checked, ks=keys: _apply_type_filter(ks, checked))
            wrap = QWidget()
            row = QHBoxLayout(wrap)
            row.setContentsMargins(24, 2, 6, 2)  # same indent as "Show labels"
            row.addWidget(cb)
            act = QWidgetAction(menu)
            act.setDefaultWidget(wrap)
            menu.addAction(act)
            self._layout_type_cbs[name] = cb

        # Apply initial filter state once — ensures bbox_filter_actions match the checkboxes
        for name, keys in _TYPE_FILTERS:
            _apply_type_filter(keys, self._layout_type_cbs[name].isChecked())

        # "Rulers" checkbox — stored for future ruler overlays
        if not hasattr(self, "_show_rulers"):
            self._show_rulers = False
        rulers_cb = QCheckBox("Rulers")
        rulers_cb.setChecked(bool(self._show_rulers))
        self._layout_rulers_cb = rulers_cb

        def _on_rulers_toggled(checked: bool) -> None:
            self._show_rulers = checked
            self._apply_rulers_visibility()

        rulers_cb.toggled.connect(_on_rulers_toggled)
        rul_wrap = QWidget()
        rul_row = QHBoxLayout(rul_wrap)
        rul_row.setContentsMargins(6, 2, 6, 2)
        rul_row.addWidget(rulers_cb)
        rul_act = QWidgetAction(menu)
        rul_act.setDefaultWidget(rul_wrap)
        menu.addAction(rul_act)

        view_btn.setMenu(menu)
        # Keep wheel (zoom / scroll) over the pages working while menu is open
        _install_menu_wheel_passthrough(menu, self.scroll_area)
        h.addWidget(view_btn)

        # --- Second dropdown: "Show objects" — hides PDF content per type ---
        # Uses the existing _content_checkboxes mechanism (white-mask overlay).
        # Semantics: cb CHECKED here = SHOW content, UNCHECKED = HIDE content.
        self._layout_show_btn = QToolButton()
        self._layout_show_btn.setText("Show objects ▾")
        self._layout_show_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        show_menu = QMenu(self._layout_show_btn)

        self._layout_show_cbs: dict[str, QCheckBox] = {}

        # "PDF Objects" parent — master toggle for all content hiding.
        show_parent_cb = QCheckBox("PDF Objects")
        show_parent_cb.setChecked(True)
        show_parent_wrap = QWidget()
        show_parent_row = QHBoxLayout(show_parent_wrap)
        show_parent_row.setContentsMargins(6, 2, 6, 2)
        show_parent_row.addWidget(show_parent_cb)
        show_parent_act = QWidgetAction(show_menu)
        show_parent_act.setDefaultWidget(show_parent_wrap)
        show_menu.addAction(show_parent_act)
        self._layout_show_cbs["__parent__"] = show_parent_cb

        # Per-type Show/Hide content rows.
        # Map each type to the set of internal content_mask keys it controls.
        _SHOW_TYPES = [
            ("Text",    ("text", "pdf_text")),
            ("Vectors", ("drawing", "gfx", "pdf_drawing")),
            ("Images",  ("image", "photo", "picture", "pdf_image")),
            ("Tables",  ("table", "pdf_table")),
        ]

        def _apply_show_filter() -> None:
            """Drive the PDF content-stream filter so hidden types are REMOVED
            at render time (no white-overlay tricks).

            Drawings are a shared bucket for Vectors AND Tables in PDF content
            streams, so drawings are hidden only when BOTH are unchecked.
            """
            parent_on = show_parent_cb.isChecked()
            text_on = self._layout_show_cbs["Text"].isChecked() if "Text" in self._layout_show_cbs else True
            vectors_on = self._layout_show_cbs["Vectors"].isChecked() if "Vectors" in self._layout_show_cbs else True
            images_on = self._layout_show_cbs["Images"].isChecked() if "Images" in self._layout_show_cbs else True
            tables_on = self._layout_show_cbs["Tables"].isChecked() if "Tables" in self._layout_show_cbs else True

            show_text = parent_on and text_on
            show_images = parent_on and images_on
            show_drawings = parent_on and (vectors_on or tables_on)

            logger.info(
                "Layout Show-objects: parent=%s text=%s vec=%s img=%s tbl=%s → render: text=%s img=%s draw=%s",
                parent_on, text_on, vectors_on, images_on, tables_on,
                show_text, show_images, show_drawings,
            )

            # Drive the legacy render-layer checkboxes — _render_layer_key()
            # reads them, _effective_layer_key() feeds get_pixmap_filtered().
            for layer_key, should_show in (
                ("text", show_text),
                ("images", show_images),
                ("drawings", show_drawings),
            ):
                cb = self._render_layer_cbs.get(layer_key)
                if cb is not None and cb.isChecked() != should_show:
                    cb.blockSignals(True)
                    cb.setChecked(should_show)
                    cb.blockSignals(False)

            # Clear any leftover white-overlay mask from prior mode.
            for spread in self._spreads:
                for pw in (spread.left_page, spread.right_page):
                    pw.set_content_mask({})

            # Clear pixmap cache and re-render visible pages with the new filter flags.
            self._on_render_layer_changed()

        def _on_show_parent_toggled(checked: bool) -> None:
            # Enable/disable children visually; preserve their individual states
            for _name, _keys in _SHOW_TYPES:
                cb = self._layout_show_cbs.get(_name)
                if cb is not None:
                    cb.setEnabled(checked)
            _apply_show_filter()

        show_parent_cb.toggled.connect(_on_show_parent_toggled)

        for name, keys in _SHOW_TYPES:
            cb = QCheckBox(name)
            cb.setChecked(True)
            cb.setEnabled(show_parent_cb.isChecked())
            cb.toggled.connect(lambda _c: _apply_show_filter())
            wrap = QWidget()
            row = QHBoxLayout(wrap)
            row.setContentsMargins(24, 2, 6, 2)
            row.addWidget(cb)
            act = QWidgetAction(show_menu)
            act.setDefaultWidget(wrap)
            show_menu.addAction(act)
            self._layout_show_cbs[name] = cb

        self._layout_show_btn.setMenu(show_menu)
        _install_menu_wheel_passthrough(show_menu, self.scroll_area)
        h.addWidget(self._layout_show_btn)

        h.addStretch()

        # Insert at top of the LayoutView's main QVBoxLayout (above the
        # (hidden) preview toolbar and the pages scroll area).
        self.layout().insertWidget(0, header)


class RecognitionView(LayoutView):
    """PDF view specialised for running detection algorithms.

    Exposes the full recognition pipeline — method combobox, re-detect
    button, progress bar — via its own header. Inherits LayoutView's
    bbox-visible + labels + rulers behavior.
    """

    # Available PyMuPDF probe methods — each maps to a callable that takes
    # a `page` and returns a text report (lines of strings).
    _PYMUPDF_PROBES: dict[str, str] = {
        "page.get_text('dict')":      "get_text_dict",
        "page.get_text('blocks')":    "get_text_blocks",
        "page.get_text('words')":     "get_text_words",
        "page.get_text('rawdict')":   "get_text_rawdict",
        "page.get_text('html')":      "get_text_html",
        "page.get_drawings()":        "get_drawings",
        "page.get_drawings(extended=True)": "get_drawings_ext",
        "page.get_images(full=True)": "get_images_full",
        "page.get_image_bbox(each)":  "get_image_bboxes",
        "page.get_xobjects()":        "get_xobjects",
        "page.find_tables()":         "find_tables",
        "page.annots()":              "annots",
        "page.links()":               "links",
        "page.get_fonts()":           "get_fonts",
        "page.mediabox / rect":       "mediabox",
    }

    def __init__(self) -> None:
        super().__init__()
        self._build_recognition_header()
        self._build_probe_header()

    def _build_recognition_header(self) -> None:
        header = QWidget()
        h = QHBoxLayout(header)
        h.setContentsMargins(8, 4, 8, 4)
        h.setSpacing(8)

        h.addWidget(QLabel("Method:"))

        # Re-parent the existing method combo (lives in the hidden preview
        # toolbar) into our own header so the user can see and change it.
        self._detect_method_combo.setParent(header)
        self._detect_method_combo.setVisible(True)
        self._detect_method_combo.setFixedHeight(26)
        self._detect_method_combo.setFixedWidth(220)
        h.addWidget(self._detect_method_combo)

        # Re-detect button (re-parent the existing one)
        self.clear_bbox_btn.setParent(header)
        self.clear_bbox_btn.setText("Re-detect")
        self.clear_bbox_btn.setVisible(True)
        self.clear_bbox_btn.setFixedHeight(26)
        h.addWidget(self.clear_bbox_btn)

        # Progress bar
        self._detect_progress.setParent(header)
        self._detect_progress.setVisible(True)
        self._detect_progress.setFixedHeight(18)
        self._detect_progress.setFixedWidth(220)
        h.addWidget(self._detect_progress)

        h.addStretch()

        # Insert above pages (and below the existing Layout header, if any —
        # _build_layout_header from the parent class inserted at index 0, so
        # inserting at index 1 puts Recognition header right below it).
        self.layout().insertWidget(1, header)

    def _build_probe_header(self) -> None:
        """Second header row: PyMuPDF-method probe dropdown + Test button.

        Applies the selected method to the CURRENT page and dumps a summary
        to the console / debug bar, so you can compare results method-by-method.
        """
        header = QWidget()
        h = QHBoxLayout(header)
        h.setContentsMargins(8, 2, 8, 4)
        h.setSpacing(8)

        h.addWidget(QLabel("PyMuPDF probe:"))

        self._probe_combo = QComboBox()
        for name in self._PYMUPDF_PROBES.keys():
            self._probe_combo.addItem(name)
        self._probe_combo.setFixedHeight(26)
        self._probe_combo.setFixedWidth(300)
        h.addWidget(self._probe_combo)

        from PySide6.QtWidgets import QPushButton
        self._probe_btn = QPushButton("Run on current page")
        self._probe_btn.setFixedHeight(26)
        self._probe_btn.clicked.connect(self._run_probe_on_current_page)
        h.addWidget(self._probe_btn)

        self._probe_summary = QLabel("")
        self._probe_summary.setStyleSheet("color: #444; font-size: 11px;")
        h.addWidget(self._probe_summary, 1)

        h.addStretch()
        self.layout().insertWidget(2, header)

    def _run_probe_on_current_page(self) -> None:
        """Apply the selected PyMuPDF method to the current page, visualize
        results as a magenta overlay on the page, and log details via
        ``self.progress`` (debug-console) + a short summary in the header."""
        if not self._doc:
            self.progress.emit("Probe: no document loaded  0.0s")
            return
        page_idx = self._get_current_page()
        if page_idx < 0 or page_idx >= len(self._doc):
            self.progress.emit("Probe: invalid page  0.0s")
            return
        page = self._doc[page_idx]
        probe_name = self._probe_combo.currentText()
        key = self._PYMUPDF_PROBES.get(probe_name, "")
        handler = getattr(self, f"_probe_{key}", None)
        import time
        t0 = time.perf_counter()
        rects: list = []
        try:
            result = handler(page) if handler else ("no handler", [], [])
            if len(result) == 3:
                summary, details, rects = result
            else:
                summary, details = result
                rects = []
        except Exception as e:
            summary = f"ERROR: {e}"
            details = [f"Exception: {e!r}"]
        dur = time.perf_counter() - t0

        # Short summary on header label
        self._probe_summary.setText(f"P{page_idx + 1} → {summary}  ({dur:.2f}s, {len(rects)} rects)")

        # Push overlay rects to the current page's PageWidget(s).
        # Clear overlays on OTHER pages so we only show probe output for
        # the current one.
        zf = self._current_zoom_factor()
        for spread in self._spreads:
            for pw in (spread.left_page, spread.right_page):
                pi = getattr(pw, "_page_index", -1)
                if pi == page_idx:
                    pw.set_probe_overlay(rects, zf)
                else:
                    pw.set_probe_overlay([], zf)

        # Full dump to debug console
        self.progress.emit(f"[probe] {probe_name} p.{page_idx + 1}: {summary}  {dur:.2f}s")
        for line in details[:30]:  # cap to avoid flooding the console
            self.progress.emit(f"[probe]   {line}  0.0s")
        if len(details) > 30:
            self.progress.emit(f"[probe]   ... (+{len(details) - 30} more)  0.0s")

    # -- individual probes --------------------------------------------

    def _probe_get_text_dict(self, page):
        td = page.get_text("dict")
        blocks = td.get("blocks", [])
        n_text = sum(1 for b in blocks if b.get("type") == 0)
        n_img = sum(1 for b in blocks if b.get("type") == 1)
        details = []
        rects = []
        for i, b in enumerate(blocks):
            t = "text" if b.get("type") == 0 else "image"
            bbox = b.get("bbox")
            if bbox:
                rects.append((bbox, f"#{i} {t}"))
            if i < 20:
                details.append(f"{t} bbox={bbox}")
        return f"{len(blocks)} blocks ({n_text} text, {n_img} image)", details, rects

    def _probe_get_text_blocks(self, page):
        blocks = page.get_text("blocks")
        details = []
        rects = []
        for i, b in enumerate(blocks):
            # b = (x0, y0, x1, y1, text, block_no, block_type)
            rects.append(((b[0], b[1], b[2], b[3]), f"#{b[5] if len(b) > 5 else i}"))
            if i < 20:
                details.append(repr(b)[:120])
        return f"{len(blocks)} block-tuples", details, rects

    def _probe_get_text_words(self, page):
        words = page.get_text("words")
        details = []
        rects = []
        for i, w in enumerate(words):
            rects.append(((w[0], w[1], w[2], w[3]), w[4][:12]))
            if i < 20:
                details.append(f"{w[4]!r} at ({w[0]:.1f},{w[1]:.1f},{w[2]:.1f},{w[3]:.1f}) blk={w[5]} line={w[6]}")
        return f"{len(words)} words", details, rects

    def _probe_get_text_rawdict(self, page):
        rd = page.get_text("rawdict")
        blocks = rd.get("blocks", [])
        total_chars = 0
        rects = []
        for i, b in enumerate(blocks):
            bbox = b.get("bbox")
            if bbox:
                rects.append((bbox, f"blk#{i}"))
            for ln in b.get("lines", []):
                for sp in ln.get("spans", []):
                    total_chars += len(sp.get("chars", []))
        return (
            f"{len(blocks)} blocks, {total_chars} characters",
            [f"block {i}: bbox={b.get('bbox')} type={b.get('type')}" for i, b in enumerate(blocks[:10])],
            rects,
        )

    def _probe_get_text_html(self, page):
        html = page.get_text("html")
        size = len(html)
        preview = html[:200].replace("\n", " ")
        return f"{size} bytes of HTML", [preview + ("..." if size > 200 else "")], []

    def _probe_get_drawings(self, page):
        drws = page.get_drawings()
        n_items = sum(len(d.get("items", [])) for d in drws)
        ops = {}
        for d in drws:
            for it in d.get("items", []):
                ops[it[0]] = ops.get(it[0], 0) + 1
        details = [
            f"#{i} rect={d.get('rect')} items={len(d.get('items', []))} fill={d.get('fill')} stroke={d.get('color')}"
            for i, d in enumerate(drws[:10])
        ]
        rects = []
        for i, d in enumerate(drws):
            rect = d.get("rect")
            if rect is not None:
                rects.append(((rect.x0, rect.y0, rect.x1, rect.y1), f"d#{i}"))
        return f"{len(drws)} drawings, {n_items} path items, ops={ops}", details, rects

    def _probe_get_drawings_ext(self, page):
        try:
            drws = page.get_drawings(extended=True)
        except TypeError:
            return "extended=True not supported in this PyMuPDF", [], []
        details = [
            f"#{i} type={d.get('type')} rect={d.get('rect')} layer={d.get('layer')}"
            for i, d in enumerate(drws[:10])
        ]
        rects = []
        for i, d in enumerate(drws):
            rect = d.get("rect")
            if rect is not None:
                rects.append(((rect.x0, rect.y0, rect.x1, rect.y1), f"d#{i}"))
        return f"{len(drws)} drawings (extended)", details, rects

    def _probe_get_images_full(self, page):
        imgs = page.get_images(full=True)
        details = []
        rects = []
        for i, ref in enumerate(imgs):
            if i < 15:
                details.append(f"xref={ref[0]} smask={ref[1]} w={ref[2]} h={ref[3]} bpc={ref[4]} cs={ref[5]} filter={ref[8] if len(ref) > 8 else '?'}")
            try:
                bbox = page.get_image_bbox(ref)
                rects.append(((bbox.x0, bbox.y0, bbox.x1, bbox.y1), f"img#{i} xref={ref[0]}"))
            except Exception:
                pass
        return f"{len(imgs)} image refs", details, rects

    def _probe_get_image_bboxes(self, page):
        imgs = page.get_images()
        details = []
        rects = []
        for i, ref in enumerate(imgs):
            try:
                rect = page.get_image_bbox(ref)
                details.append(f"xref={ref[0]} bbox={rect}")
                rects.append(((rect.x0, rect.y0, rect.x1, rect.y1), f"img#{i}"))
            except Exception as e:
                details.append(f"xref={ref[0]} ERROR: {e}")
        return f"{len(imgs)} images", details, rects

    def _probe_get_xobjects(self, page):
        try:
            xos = page.get_xobjects()
        except Exception as e:
            return f"ERROR: {e}", [], []
        details = [repr(x)[:120] for x in xos[:15]]
        rects = []
        for i, x in enumerate(xos):
            # xobj tuple: (xref, name, index, bbox)
            if isinstance(x, (list, tuple)) and len(x) >= 4:
                bbox = x[3]
                if isinstance(bbox, (list, tuple)) and len(bbox) == 4:
                    rects.append((tuple(bbox), f"xo#{i} {x[1] if len(x) > 1 else ''}"))
        return f"{len(xos)} XObjects", details, rects

    def _probe_find_tables(self, page):
        try:
            tabs = page.find_tables()
        except Exception as e:
            return f"ERROR: {e}", [], []
        table_list = getattr(tabs, "tables", []) or []
        count = len(table_list)
        details = []
        rects = []
        for i, t in enumerate(table_list):
            rc = getattr(t, "row_count", "?")
            cc = getattr(t, "col_count", "?")
            bbox = t.bbox
            details.append(f"table#{i} rows={rc} cols={cc} bbox={bbox}")
            if isinstance(bbox, (list, tuple)) and len(bbox) >= 4:
                rects.append((tuple(bbox[:4]), f"t#{i} {rc}x{cc}"))
        return f"{count} tables", details, rects

    def _probe_annots(self, page):
        annots = list(page.annots() or [])
        details = []
        rects = []
        for i, a in enumerate(annots):
            r = a.rect
            details.append(f"type={a.type} rect={r}")
            rects.append(((r.x0, r.y0, r.x1, r.y1), f"a#{i}"))
        return f"{len(annots)} annotations", details, rects

    def _probe_links(self, page):
        links = page.links()
        details = []
        rects = []
        for i, lk in enumerate(links):
            r = lk.get("from")
            details.append(f"kind={lk.get('kind')} uri={lk.get('uri')} rect={r}")
            if r is not None:
                rects.append(((r.x0, r.y0, r.x1, r.y1), f"l#{i}"))
        return f"{len(links)} links", details, rects

    def _probe_get_fonts(self, page):
        fonts = page.get_fonts(full=True) if hasattr(page, "get_fonts") else []
        details = [f"xref={f[0]} type={f[1]} name={f[3]}" for f in fonts[:15]]
        return f"{len(fonts)} fonts used", details, []

    def _probe_mediabox(self, page):
        mb = page.mediabox
        cb = page.cropbox
        rot = page.rotation
        rect = page.rect
        # Visualize mediabox + cropbox as overlay rects
        rects = [
            ((rect.x0, rect.y0, rect.x1, rect.y1), "rect"),
            ((cb.x0, cb.y0, cb.x1, cb.y1), "cropbox"),
        ]
        return (
            f"rot={rot} mediabox={tuple(mb)}",
            [f"cropbox={tuple(cb)}", f"rect={tuple(rect)}", f"derotation={page.derotation_matrix}"],
            rects,
        )
