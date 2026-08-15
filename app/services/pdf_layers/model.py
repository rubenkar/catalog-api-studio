"""Data model for the Layers subsystem.

Skeleton only — analyzer.py (commit 3) fills these in. Kept here so UI and
method stubs can reference the types without pulling in analyzer's deps.

Frozen where it makes sense: GraphicsState must be hashable (goes into
cache keys); PdfObject is a value object once analyzed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from app.services.pdf_layers.types import LayerType


# PDF transformation matrix: 6-tuple [a b c d e f] per ISO 32000 §8.3.3.
# Identity = (1, 0, 0, 1, 0, 0).
Matrix = tuple[float, float, float, float, float, float]

# Axis-aligned rectangle in PDF user space: (x0, y0, x1, y1), y-up.
Rect = tuple[float, float, float, float]

# PDF color — variable width: 1 (gray), 3 (rgb), 4 (cmyk), or n for DeviceN.
Color = tuple[float, ...]


IDENTITY_MATRIX: Matrix = (1.0, 0.0, 0.0, 1.0, 0.0, 0.0)


@dataclass(frozen=True)
class GraphicsState:
    """Snapshot of PDF graphics state at the moment an object is painted.

    Needed to render the object standalone: the painting operator alone
    doesn't carry its color/CTM/clip — those come from prior state-setting
    operators in the content stream. For standalone render we replay them.
    """

    ctm: Matrix = IDENTITY_MATRIX
    clip: Rect | None = None
    fill_color: Color = (0.0,)
    stroke_color: Color = (0.0,)
    fill_alpha: float = 1.0
    stroke_alpha: float = 1.0
    line_width: float = 1.0
    # Text state (only meaningful for text objects)
    font_name: str | None = None
    font_size: float = 0.0
    text_render_mode: int = 0
    # Blend mode: "Normal", "Multiply", "Screen", etc.
    blend_mode: str = "Normal"


@dataclass
class PdfObject:
    """One addressable object on a PDF page.

    `ops` is the raw operator sequence for this object alone (e.g. a single
    painting op `Tj` or `S`, optionally preceded by path-construction ops).
    `state` is the resolved graphics state needed to render it in isolation.
    """

    id: str                          # stable within (page, pdf_hash): e.g. "p3:op47"
    type: LayerType
    bbox: Rect                       # in PDF user space, after CTM
    state: GraphicsState
    ops: tuple[bytes, ...] = ()     # raw content-stream bytes for this object
    source_page: int = 0            # 0-based page index
    # Optional references for xobject-type operators
    xobject_name: str | None = None  # e.g. "Im5" for `Do /Im5`
    # Detection-time metadata that doesn't fit the bbox/type pair
    meta: dict = field(default_factory=dict)


@dataclass
class Layer:
    """A group of PdfObject instances sharing a type (or a custom predicate).

    Layers are a derived view — built by classifier.py from a flat
    `list[PdfObject]`. The actual storage is `PageLayerModel.objects`.
    """

    id: str                          # e.g. "text", "product", or custom id
    layer_type: LayerType | None     # None for custom predicate layers
    label: str
    objects: list[PdfObject] = field(default_factory=list)
    visible: bool = True
    opacity: float = 1.0
    blend_mode: Literal["normal", "multiply", "screen", "overlay"] = "normal"


@dataclass
class PageLayerModel:
    """Analyzed representation of one PDF page.

    Built by analyzer.py once per (page, pdf_hash). Renderers query it via
    type filters or object-id lookup.
    """

    page_index: int                  # 0-based
    page_bbox: Rect                  # MediaBox in PDF user space
    rotation: int = 0                # 0/90/180/270
    objects: list[PdfObject] = field(default_factory=list)
    # Resources dictionary from the original page (fonts, XObjects, color
    # spaces). Preserved because stream_builder.py must reference them when
    # emitting the minimal content stream.
    resources_ref: object | None = None

    def objects_of(self, types: frozenset[LayerType]) -> list[PdfObject]:
        """Filter objects by type — O(n), fine for a few hundred objects."""
        if not types:
            return []
        return [o for o in self.objects if o.type in types]

    def object_by_id(self, obj_id: str) -> PdfObject | None:
        for o in self.objects:
            if o.id == obj_id:
                return o
        return None
