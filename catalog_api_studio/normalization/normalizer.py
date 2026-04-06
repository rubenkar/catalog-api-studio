"""Product data normalization — SKU, units, brand, text cleaning."""

import logging
import re

logger = logging.getLogger(__name__)

UNIT_MAP = {
    "шт": "pcs",
    "шт.": "pcs",
    "штук": "pcs",
    "штука": "pcs",
    "кг": "kg",
    "килограмм": "kg",
    "г": "g",
    "грамм": "g",
    "м": "m",
    "метр": "m",
    "мм": "mm",
    "миллиметр": "mm",
    "см": "cm",
    "сантиметр": "cm",
    "л": "l",
    "литр": "l",
    "мл": "ml",
    "миллилитр": "ml",
    "уп": "pack",
    "упаковка": "pack",
    "компл": "set",
    "комплект": "set",
    "pcs": "pcs",
    "pc": "pcs",
    "ea": "pcs",
    "each": "pcs",
    "kg": "kg",
    "mm": "mm",
    "m": "m",
}


class Normalizer:
    """Normalize product data fields."""

    def __init__(self, known_brands: list[str] | None = None) -> None:
        self.known_brands = {b.upper(): b for b in (known_brands or [])}

    def normalize_sku(self, sku: str | None) -> str | None:
        """Normalize SKU: uppercase, strip whitespace, remove special chars."""
        if not sku:
            return None
        sku = sku.strip().upper()
        sku = re.sub(r"[^\w\-/.]", "", sku)
        sku = re.sub(r"\s+", "", sku)
        return sku or None

    def normalize_unit(self, unit: str | None) -> str | None:
        """Normalize measurement unit to standard form."""
        if not unit:
            return None
        unit_lower = unit.strip().lower().rstrip(".")
        return UNIT_MAP.get(unit_lower, unit.strip())

    def normalize_brand(self, brand: str | None) -> str | None:
        """Normalize brand name against known brands list."""
        if not brand:
            return None
        brand = brand.strip()
        brand_upper = brand.upper()

        if brand_upper in self.known_brands:
            return self.known_brands[brand_upper]

        # Clean up common patterns
        brand = re.sub(r"\s+", " ", brand)
        return brand

    def normalize_price(self, price: float | None) -> float | None:
        """Clean up price value."""
        if price is None:
            return None
        if price < 0:
            return abs(price)
        return round(price, 2)

    def clean_text(self, text: str | None) -> str | None:
        """Clean text: strip HTML, normalize whitespace."""
        if not text:
            return None
        # Remove HTML tags
        text = re.sub(r"<[^>]+>", "", text)
        # Normalize whitespace
        text = re.sub(r"\s+", " ", text).strip()
        # Remove control characters
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        return text or None

    def normalize_product(self, data: dict) -> dict:
        """Normalize all fields of a product dict."""
        return {
            **data,
            "sku": self.normalize_sku(data.get("sku")),
            "brand": self.normalize_brand(data.get("brand")),
            "name": self.clean_text(data.get("name")),
            "description": self.clean_text(data.get("description")),
            "category": self.clean_text(data.get("category")),
            "unit": self.normalize_unit(data.get("unit")),
            "price": self.normalize_price(data.get("price")),
        }
