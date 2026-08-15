"""Render methods and their type-support capabilities.

Every method declares which LayerType values it can actually filter. A type
the method doesn't support appears greyed-out in the Types dropdown when
that method is active (option A from the design discussion).

Method 1 (mupdf.filter_stream) only exposes 3 coarse buckets — text, images,
drawings — so most of the ~55 types are unsupported. New pdfium+pikepdf
methods aim to support all native-PDF types fully, plus semantic/domain
types once analyzer.py produces them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from app.services.pdf_layers.types import LayerType


class RenderMethod(str, Enum):
    """Rendering approach. String values are stable IDs for QSettings."""

    MUPDF_FILTER_STREAM = "mupdf.filter_stream"           # baseline: current get_pixmap_filtered
    MUPDF_REDACTIONS = "mupdf.redactions"                 # add_redact_annot + apply_redactions
    MUPDF_CROP = "mupdf.crop_from_full"                   # full render + crop/alpha-mask
    PDFIUM_FULL = "pdfium.full"                           # pypdfium2 baseline (no filter)
    PDFIUM_CROP = "pdfium.crop_from_full"                 # pypdfium2 full render + crop
    PDFIUM_PIKEPDF_STREAM = "pdfium_pikepdf.stream_rebuild"  # main new method
    PDFIUM_PIKEPDF_PER_OBJECT = "pdfium_pikepdf.per_object"  # render one object at a time


@dataclass(frozen=True)
class MethodInfo:
    """Static metadata per method: label, subtitle, supported types, flags."""

    method: RenderMethod
    label: str
    subtitle: str
    supported_types: frozenset[LayerType]
    supports_hide_background: bool
    implemented: bool
    notes: str = ""


# Types that method 1 (MuPDF content-stream filter) can actually toggle.
# Paths are granular (stroke/fill/stroke+fill each its own switch). Shading
# is its own operator (sh) handled in the filter. Annotations collapse to
# one collective switch because MuPDF's get_pixmap only exposes an
# all-or-nothing annots= flag — per-subtype filtering isn't available at
# this layer. Still expose every annotation subtype in the UI so the user
# can flip them together (they all feed the same rail for this method).
_MUPDF_FILTER_SUPPORTED: frozenset[LayerType] = frozenset({
    LayerType.TEXT,
    LayerType.IMAGE_INLINE,
    LayerType.IMAGE_XOBJECT,
    LayerType.PATH_STROKE,
    LayerType.PATH_FILL,
    # PATH_STROKE_FILL intentionally NOT supported here: the filter decomposes
    # combined paint ops (B/B*/b/b*) into stroke and/or fill based on the
    # individual PATH_STROKE / PATH_FILL checkboxes. Keeping a third toggle
    # would either do nothing or conflict. Future pdfium+pikepdf methods may
    # expose genuine per-op-type control and will include this type.
    LayerType.SHADING,
    LayerType.ANNOTATION_TEXT,
    LayerType.ANNOTATION_LINK,
    LayerType.ANNOTATION_HIGHLIGHT,
    LayerType.ANNOTATION_SHAPE,
    LayerType.ANNOTATION_STAMP,
    LayerType.ANNOTATION_FILE,
    LayerType.FORM_WIDGET,
})

# Full native-PDF coverage — what pdfium+pikepdf should handle once
# analyzer.py lands. Covers everything in LAYER_TYPE_GROUPS[0] (Native PDF)
# plus clip paths and shading.
_PDFIUM_PIKEPDF_SUPPORTED: frozenset[LayerType] = frozenset({
    LayerType.TEXT,
    LayerType.PATH_STROKE,
    LayerType.PATH_FILL,
    LayerType.PATH_STROKE_FILL,
    LayerType.CLIP_PATH,
    LayerType.IMAGE_INLINE,
    LayerType.IMAGE_XOBJECT,
    LayerType.FORM_XOBJECT,
    LayerType.SHADING,
    LayerType.SHADING_XOBJECT,
    LayerType.TILING_PATTERN,
})

# Crop-based methods accept ANY type — they don't look at the operator
# stream, they work off the bounding-box of whatever the analyzer reports.
# The caveat is that "crop" is really masking over a full-page render, so
# objects that overlap share pixels. That's acceptable for debug/testing.
_CROP_SUPPORTED: frozenset[LayerType] = frozenset(LayerType)


METHOD_REGISTRY: tuple[MethodInfo, ...] = (
    MethodInfo(
        method=RenderMethod.MUPDF_FILTER_STREAM,
        label="MuPDF: filter content stream",
        subtitle="Baseline. Current pipeline. Coarse text/images/drawings only.",
        supported_types=_MUPDF_FILTER_SUPPORTED,
        supports_hide_background=True,  # empty stream → blank page; not transparent in MuPDF default
        implemented=True,  # wired in commit 2
        notes="Existing get_pixmap_filtered(); 3 coarse buckets. "
              "Text/Images/Paths are per-checkbox; other Native PDF types are greyed out.",
    ),
    MethodInfo(
        method=RenderMethod.MUPDF_REDACTIONS,
        label="MuPDF: redactions",
        subtitle="add_redact_annot on non-selected objects, then apply_redactions.",
        supported_types=_PDFIUM_PIKEPDF_SUPPORTED,
        supports_hide_background=True,
        implemented=False,
        notes="Slow on pages with many objects; one annot per excluded object.",
    ),
    MethodInfo(
        method=RenderMethod.MUPDF_CROP,
        label="MuPDF: crop from full render",
        subtitle="Render whole page, crop/mask to selected object bboxes.",
        supported_types=_CROP_SUPPORTED,
        supports_hide_background=True,
        implemented=False,
        notes="Fast but overlapping objects share pixels.",
    ),
    MethodInfo(
        method=RenderMethod.PDFIUM_FULL,
        label="pypdfium2: full render",
        subtitle="Baseline pypdfium2 render, no filter. Reference for quality.",
        supported_types=frozenset(),  # renders everything — filter is a no-op
        supports_hide_background=False,
        implemented=False,
        notes="Reference: what pdfium looks like without modification.",
    ),
    MethodInfo(
        method=RenderMethod.PDFIUM_CROP,
        label="pypdfium2: crop from full render",
        subtitle="pypdfium2 full render, then crop/mask to selected bboxes.",
        supported_types=_CROP_SUPPORTED,
        supports_hide_background=True,
        implemented=False,
        notes="Faster than stream rebuild; same overlap caveat as MuPDF crop.",
    ),
    MethodInfo(
        method=RenderMethod.PDFIUM_PIKEPDF_STREAM,
        label="pypdfium2 + pikepdf: stream rebuild",
        subtitle="Rebuild content stream with only selected objects, render transparent.",
        supported_types=_PDFIUM_PIKEPDF_SUPPORTED,
        supports_hide_background=True,
        implemented=False,
        notes="Main new pipeline. True per-type filtering at operator level.",
    ),
    MethodInfo(
        method=RenderMethod.PDFIUM_PIKEPDF_PER_OBJECT,
        label="pypdfium2 + pikepdf: per-object",
        subtitle="Render exactly one object at a time; for outline / highlight.",
        supported_types=_PDFIUM_PIKEPDF_SUPPORTED,
        supports_hide_background=True,
        implemented=False,
        notes="Rendering a single object uses the same pipeline, clipped to its bbox.",
    ),
)


_METHOD_BY_ID: dict[RenderMethod, MethodInfo] = {m.method: m for m in METHOD_REGISTRY}


def method_info(method: RenderMethod) -> MethodInfo:
    return _METHOD_BY_ID[method]


def method_supports_type(method: RenderMethod, layer_type: LayerType) -> bool:
    """Option A: a Type checkbox is only clickable if the active method
    supports that type. PDFIUM_FULL returns True for nothing — its supported
    set is empty because it doesn't filter at all (every Type checkbox is
    disabled when PDFIUM_FULL is active)."""
    info = _METHOD_BY_ID[method]
    if not info.supported_types:
        return False
    return layer_type in info.supported_types


def default_method() -> RenderMethod:
    """Default method on fresh Layers tab: baseline MuPDF filter."""
    return RenderMethod.MUPDF_FILTER_STREAM
