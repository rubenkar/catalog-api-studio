"""PDF layered rendering subsystem.

Model: a PDF page is decomposed into typed objects, each object belongs to
a layer (by type). Rendering produces per-layer images with transparent
background — or any arbitrary subset of objects, by type filter or by id.

Modules:
- types:    enum of all object types, grouped
- methods:  enum of render methods, capability flags per method
- model:    PdfObject / Layer / GraphicsState / PageLayerModel dataclasses
- analyzer: content-stream parse → list[PdfObject] (not yet implemented)
- renderer: dispatch to method implementation (not yet implemented)
"""

from app.services.pdf_layers.methods import (
    METHOD_REGISTRY,
    RenderMethod,
    method_supports_type,
)
from app.services.pdf_layers.types import (
    LAYER_TYPE_GROUPS,
    META_FLAGS,
    LayerType,
    LayerTypeGroup,
)

__all__ = [
    "LayerType",
    "LayerTypeGroup",
    "LAYER_TYPE_GROUPS",
    "META_FLAGS",
    "RenderMethod",
    "METHOD_REGISTRY",
    "method_supports_type",
]
