"""Export service — JSON and CSV export."""

import csv
import json
import logging
from pathlib import Path

from sqlalchemy.orm import Session

from catalog_api_studio.db.models import Product

logger = logging.getLogger(__name__)


class ExportService:
    """Export products to JSON or CSV files."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def export_json(self, output_path: Path, product_ids: list[int] | None = None) -> int:
        """Export products to JSON file. Returns count exported."""
        products = self._get_products(product_ids)
        data = [self._product_to_dict(p) for p in products]

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)

        logger.info("Exported %d products to JSON: %s", len(data), output_path)
        return len(data)

    def export_csv(self, output_path: Path, product_ids: list[int] | None = None) -> int:
        """Export products to CSV file. Returns count exported."""
        products = self._get_products(product_ids)
        if not products:
            return 0

        fieldnames = [
            "id", "source", "sku", "brand", "name", "category",
            "description", "unit", "price", "stock", "confidence_score", "reviewed",
        ]

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for product in products:
                writer.writerow(self._product_to_dict(product))

        logger.info("Exported %d products to CSV: %s", len(products), output_path)
        return len(products)

    def _get_products(self, product_ids: list[int] | None) -> list[Product]:
        q = self.session.query(Product)
        if product_ids:
            q = q.filter(Product.id.in_(product_ids))
        return q.order_by(Product.id).all()

    @staticmethod
    def _product_to_dict(product: Product) -> dict:
        return {
            "id": product.id,
            "source": product.source,
            "sku": product.sku,
            "brand": product.brand,
            "name": product.name,
            "category": product.category,
            "description": product.description,
            "attributes": product.attributes,
            "unit": product.unit,
            "price": float(product.price) if product.price else None,
            "stock": product.stock,
            "documents": product.documents,
            "images": product.images,
            "confidence_score": product.confidence_score,
            "reviewed": product.reviewed,
        }
