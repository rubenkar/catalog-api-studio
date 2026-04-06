"""Extraction pipeline — raw rows to structured Product records."""

import logging
import re

from catalog_api_studio.importers.base import BaseImporter
from catalog_api_studio.models.schemas import ProductCreate

logger = logging.getLogger(__name__)

# Heuristic column name candidates
SKU_HINTS = ["sku", "артикул", "article", "код", "code", "part", "номер", "number", "ref"]
NAME_HINTS = ["name", "наименование", "название", "title", "description", "товар", "product"]
BRAND_HINTS = ["brand", "бренд", "марка", "manufacturer", "производитель", "mfg"]
PRICE_HINTS = ["price", "цена", "cost", "стоимость"]
UNIT_HINTS = ["unit", "единица", "ед", "uom"]
CATEGORY_HINTS = ["category", "категория", "group", "группа", "type", "тип"]


class ExtractionPipeline:
    """Convert raw extracted rows (dicts) into ProductCreate records."""

    def __init__(self, source: str = "") -> None:
        self.source = source

    def process(self, raw_rows: list[dict]) -> list[ProductCreate]:
        """Process raw rows into product records."""
        if not raw_rows:
            return []

        # Separate structured rows from raw text
        structured = [r for r in raw_rows if "_raw_text" not in r or len(r) > 3]
        text_only = [r for r in raw_rows if "_raw_text" in r and len(r) <= 3]

        products: list[ProductCreate] = []

        if structured:
            col_mapping = self._detect_fields(structured[0])
            for row in structured:
                if "_raw_text" in row and len(row) <= 3:
                    continue
                product = self._map_row(row, col_mapping)
                if product:
                    products.append(product)

        # Try to extract from raw text blocks
        for row in text_only:
            text = row.get("_raw_text", "")
            product = self._extract_from_text(text, row.get("_page", 0))
            if product:
                products.append(product)

        logger.info("Pipeline produced %d products from %d rows", len(products), len(raw_rows))
        return products

    def _detect_fields(self, sample_row: dict) -> dict[str, str | None]:
        """Auto-detect which columns map to which product fields."""
        keys = [k for k in sample_row.keys() if not k.startswith("_")]
        mapping: dict[str, str | None] = {}

        for field, hints in [
            ("sku", SKU_HINTS),
            ("name", NAME_HINTS),
            ("brand", BRAND_HINTS),
            ("price", PRICE_HINTS),
            ("unit", UNIT_HINTS),
            ("category", CATEGORY_HINTS),
        ]:
            mapping[field] = BaseImporter.detect_column(keys, hints)

        return mapping

    def _map_row(self, row: dict, col_mapping: dict[str, str | None]) -> ProductCreate | None:
        """Map a raw dict row to ProductCreate using detected column mapping."""

        def get(field: str) -> str | None:
            col = col_mapping.get(field)
            if col and col in row:
                val = row[col]
                return str(val).strip() if val else None
            # Fallback: try all values if no mapping
            return None

        sku = get("sku")
        name = get("name")
        brand = get("brand")

        # If no mapping worked, try positional (first few columns)
        if not sku and not name:
            values = [v for k, v in row.items() if not k.startswith("_") and v]
            if len(values) >= 2:
                sku = str(values[0]).strip()
                name = str(values[1]).strip()
            elif values:
                name = str(values[0]).strip()

        if not sku and not name:
            return None

        price = self._parse_price(get("price"))

        filled = sum(1 for v in [sku, name, brand, price] if v)
        confidence = filled / 4.0

        return ProductCreate(
            source=self.source,
            sku=sku,
            brand=brand,
            name=name,
            category=get("category"),
            unit=get("unit"),
            price=price,
            confidence_score=confidence,
        )

    def _extract_from_text(self, text: str, page: int) -> ProductCreate | None:
        """Try to extract a product from raw text block."""
        if len(text) < 10:
            return None

        lines = text.strip().split("\n")
        name = lines[0][:200] if lines else None

        # Try to find SKU-like pattern
        sku_match = re.search(r"\b([A-Z0-9]{3,}[-/]?[A-Z0-9]+)\b", text)
        sku = sku_match.group(1) if sku_match else None

        # Try to find price
        price = self._parse_price(text)

        if not name:
            return None

        filled = sum(1 for v in [sku, name, price] if v)
        confidence = filled / 4.0 * 0.5  # Lower confidence for text extraction

        return ProductCreate(
            source=self.source,
            sku=sku,
            name=name,
            price=price,
            confidence_score=confidence,
        )

    @staticmethod
    def _parse_price(text: str | None) -> float | None:
        if not text:
            return None
        # Find price-like patterns
        match = re.search(r"(\d[\d\s]*[.,]?\d*)", str(text))
        if match:
            try:
                price_str = match.group(1).replace(" ", "").replace(",", ".")
                return float(price_str)
            except ValueError:
                pass
        return None
