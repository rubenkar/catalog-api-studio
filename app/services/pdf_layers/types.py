"""Layer types: the full taxonomy of addressable PDF objects.

Includes types that current detection does NOT yet produce (semantic layout,
catalog-domain). Those stay in the enum as a roadmap — they appear in the
UI, show 0 objects until detection catches up.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class LayerTypeGroup(str, Enum):
    """Sections within the Types dropdown."""

    NATIVE_PDF = "native_pdf"          # base painting operators
    PDF_STRUCTURE = "pdf_structure"    # annotations, forms, OCG, marked content
    SEMANTIC_LAYOUT = "semantic"        # tables, headings, lists (layout analysis)
    CATALOG_DOMAIN = "catalog"          # product, price, sku, barcode... (domain semantic)
    META_FLAGS = "meta"                 # render flags (hide background, etc.)


class LayerType(str, Enum):
    """Every addressable object type the Layers tab exposes.

    Values are stable string keys used for persistence (QSettings) and
    for cache keys. Display labels live in DISPLAY_LABELS below.
    """

    # --- Group 1: Native PDF content (painting operators) ---
    TEXT = "text"
    PATH_STROKE = "path_stroke"
    PATH_FILL = "path_fill"
    PATH_STROKE_FILL = "path_stroke_fill"
    CLIP_PATH = "clip_path"
    IMAGE_INLINE = "image_inline"
    IMAGE_XOBJECT = "image_xobject"
    FORM_XOBJECT = "form_xobject"
    SHADING = "shading"
    SHADING_XOBJECT = "shading_xobject"
    TILING_PATTERN = "tiling_pattern"

    # --- Group 2: PDF structure (non-painting or page-level) ---
    BACKGROUND = "background"                       # everything outside detected objects
    ANNOTATION_TEXT = "annotation_text"
    ANNOTATION_LINK = "annotation_link"
    ANNOTATION_HIGHLIGHT = "annotation_highlight"
    ANNOTATION_SHAPE = "annotation_shape"
    ANNOTATION_STAMP = "annotation_stamp"
    ANNOTATION_FILE = "annotation_file"
    FORM_WIDGET = "form_widget"
    MARKED_CONTENT = "marked_content"
    OPTIONAL_CONTENT_GROUP = "optional_content_group"  # native PDF layers (OCG)

    # --- Group 3: Semantic layout ---
    TABLE = "table"
    TABLE_ROW = "table_row"
    TABLE_CELL = "table_cell"
    TABLE_HEADER = "table_header"
    PARAGRAPH = "paragraph"
    HEADING = "heading"
    LIST = "list"
    LIST_ITEM = "list_item"
    FIGURE = "figure"
    CAPTION = "caption"
    FOOTNOTE = "footnote"
    PAGE_HEADER = "page_header"
    PAGE_FOOTER = "page_footer"
    PAGE_NUMBER = "page_number"
    COLUMN = "column"
    SECTION = "section"

    # --- Group 4: Catalog domain ---
    PRODUCT = "product"
    SKU = "sku"
    PRICE = "price"
    PRODUCT_NAME = "product_name"
    PRODUCT_DESCRIPTION = "product_description"
    PRODUCT_SPECS = "product_specs"
    PRODUCT_IMAGE = "product_image"
    BARCODE = "barcode"
    QR_CODE = "qr_code"
    LOGO = "logo"
    ICON = "icon"
    BADGE = "badge"
    TECHNICAL_DRAWING = "technical_drawing"
    DIMENSION = "dimension"
    COLOR_SWATCH = "color_swatch"
    GRID_CELL = "grid_cell"


# Meta flags are NOT layer types — they are render-time modifiers.
# Kept as plain strings so the UI can mix them into the same dropdown popup
# without polluting LayerType.
META_FLAGS: tuple[tuple[str, str], ...] = (
    ("hide_background", "Hide background (render only objects)"),
    ("render_outlines_only", "Outlines only (bbox overlay)"),
    ("show_zero_sized", "Show zero-sized objects"),
)


DISPLAY_LABELS: dict[LayerType, str] = {
    # Group 1
    LayerType.TEXT: "Text",
    LayerType.PATH_STROKE: "Path — stroke",
    LayerType.PATH_FILL: "Path — fill",
    LayerType.PATH_STROKE_FILL: "Path — stroke + fill",
    LayerType.CLIP_PATH: "Clip path",
    LayerType.IMAGE_INLINE: "Image — inline",
    LayerType.IMAGE_XOBJECT: "Image — XObject",
    LayerType.FORM_XOBJECT: "Form XObject",
    LayerType.SHADING: "Shading",
    LayerType.SHADING_XOBJECT: "Shading XObject",
    LayerType.TILING_PATTERN: "Tiling pattern",
    # Group 2
    LayerType.BACKGROUND: "Background",
    LayerType.ANNOTATION_TEXT: "Annotation — text",
    LayerType.ANNOTATION_LINK: "Annotation — link",
    LayerType.ANNOTATION_HIGHLIGHT: "Annotation — highlight",
    LayerType.ANNOTATION_SHAPE: "Annotation — shape",
    LayerType.ANNOTATION_STAMP: "Annotation — stamp",
    LayerType.ANNOTATION_FILE: "Annotation — file",
    LayerType.FORM_WIDGET: "Form widget",
    LayerType.MARKED_CONTENT: "Marked content",
    LayerType.OPTIONAL_CONTENT_GROUP: "Optional content group (OCG)",
    # Group 3
    LayerType.TABLE: "Table",
    LayerType.TABLE_ROW: "Table row",
    LayerType.TABLE_CELL: "Table cell",
    LayerType.TABLE_HEADER: "Table header",
    LayerType.PARAGRAPH: "Paragraph",
    LayerType.HEADING: "Heading",
    LayerType.LIST: "List",
    LayerType.LIST_ITEM: "List item",
    LayerType.FIGURE: "Figure",
    LayerType.CAPTION: "Caption",
    LayerType.FOOTNOTE: "Footnote",
    LayerType.PAGE_HEADER: "Page header",
    LayerType.PAGE_FOOTER: "Page footer",
    LayerType.PAGE_NUMBER: "Page number",
    LayerType.COLUMN: "Column",
    LayerType.SECTION: "Section",
    # Group 4
    LayerType.PRODUCT: "Product",
    LayerType.SKU: "SKU",
    LayerType.PRICE: "Price",
    LayerType.PRODUCT_NAME: "Product name",
    LayerType.PRODUCT_DESCRIPTION: "Product description",
    LayerType.PRODUCT_SPECS: "Product specs",
    LayerType.PRODUCT_IMAGE: "Product image",
    LayerType.BARCODE: "Barcode",
    LayerType.QR_CODE: "QR code",
    LayerType.LOGO: "Logo",
    LayerType.ICON: "Icon",
    LayerType.BADGE: "Badge",
    LayerType.TECHNICAL_DRAWING: "Technical drawing",
    LayerType.DIMENSION: "Dimension",
    LayerType.COLOR_SWATCH: "Color swatch",
    LayerType.GRID_CELL: "Grid cell",
}


@dataclass(frozen=True)
class LayerTypeSection:
    """A group of LayerType values shown under one dropdown section header."""

    group: LayerTypeGroup
    title: str
    types: tuple[LayerType, ...]


LAYER_TYPE_GROUPS: tuple[LayerTypeSection, ...] = (
    LayerTypeSection(
        group=LayerTypeGroup.NATIVE_PDF,
        title="Native PDF content",
        types=(
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
        ),
    ),
    LayerTypeSection(
        group=LayerTypeGroup.PDF_STRUCTURE,
        title="PDF structure",
        types=(
            LayerType.BACKGROUND,
            LayerType.ANNOTATION_TEXT,
            LayerType.ANNOTATION_LINK,
            LayerType.ANNOTATION_HIGHLIGHT,
            LayerType.ANNOTATION_SHAPE,
            LayerType.ANNOTATION_STAMP,
            LayerType.ANNOTATION_FILE,
            LayerType.FORM_WIDGET,
            LayerType.MARKED_CONTENT,
            LayerType.OPTIONAL_CONTENT_GROUP,
        ),
    ),
    LayerTypeSection(
        group=LayerTypeGroup.SEMANTIC_LAYOUT,
        title="Semantic layout",
        types=(
            LayerType.TABLE,
            LayerType.TABLE_ROW,
            LayerType.TABLE_CELL,
            LayerType.TABLE_HEADER,
            LayerType.PARAGRAPH,
            LayerType.HEADING,
            LayerType.LIST,
            LayerType.LIST_ITEM,
            LayerType.FIGURE,
            LayerType.CAPTION,
            LayerType.FOOTNOTE,
            LayerType.PAGE_HEADER,
            LayerType.PAGE_FOOTER,
            LayerType.PAGE_NUMBER,
            LayerType.COLUMN,
            LayerType.SECTION,
        ),
    ),
    LayerTypeSection(
        group=LayerTypeGroup.CATALOG_DOMAIN,
        title="Catalog domain",
        types=(
            LayerType.PRODUCT,
            LayerType.SKU,
            LayerType.PRICE,
            LayerType.PRODUCT_NAME,
            LayerType.PRODUCT_DESCRIPTION,
            LayerType.PRODUCT_SPECS,
            LayerType.PRODUCT_IMAGE,
            LayerType.BARCODE,
            LayerType.QR_CODE,
            LayerType.LOGO,
            LayerType.ICON,
            LayerType.BADGE,
            LayerType.TECHNICAL_DRAWING,
            LayerType.DIMENSION,
            LayerType.COLOR_SWATCH,
            LayerType.GRID_CELL,
        ),
    ),
)


def all_types() -> tuple[LayerType, ...]:
    """Flat tuple of every LayerType across all groups, preserving group order."""
    out: list[LayerType] = []
    for sec in LAYER_TYPE_GROUPS:
        out.extend(sec.types)
    return tuple(out)


def default_enabled_types() -> frozenset[LayerType]:
    """Types checked on a fresh Layers tab. Conservative: Group 1 (native PDF)
    on, everything else off — user opts in as detection catches up."""
    return frozenset(LAYER_TYPE_GROUPS[0].types)
