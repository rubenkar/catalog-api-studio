"""Product service — CRUD operations and search delegation."""

import logging

from sqlalchemy.orm import Session

from catalog_api_studio.db.models import Product
from catalog_api_studio.models.schemas import ProductUpdate

logger = logging.getLogger(__name__)


class ProductService:
    """Product CRUD and query operations."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def get_all(
        self, page: int = 1, per_page: int = 20, brand: str | None = None, category: str | None = None
    ) -> tuple[list[Product], int]:
        q = self.session.query(Product)
        if brand:
            q = q.filter(Product.brand == brand)
        if category:
            q = q.filter(Product.category == category)

        total = q.count()
        products = q.order_by(Product.id).offset((page - 1) * per_page).limit(per_page).all()
        return products, total

    def get_by_id(self, product_id: int) -> Product | None:
        return self.session.query(Product).filter(Product.id == product_id).first()

    def update(self, product_id: int, data: ProductUpdate) -> Product | None:
        product = self.get_by_id(product_id)
        if not product:
            return None

        update_data = data.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            setattr(product, key, value)

        # Recalculate confidence if fields were updated
        filled = sum(
            1 for v in [product.sku, product.name, product.brand, product.price] if v
        )
        product.confidence_score = filled / 4.0

        self.session.commit()
        return product

    def get_brands(self) -> list[str]:
        rows = (
            self.session.query(Product.brand)
            .filter(Product.brand.isnot(None), Product.brand != "")
            .distinct()
            .order_by(Product.brand)
            .all()
        )
        return [r[0] for r in rows]

    def get_categories(self) -> list[str]:
        rows = (
            self.session.query(Product.category)
            .filter(Product.category.isnot(None), Product.category != "")
            .distinct()
            .order_by(Product.category)
            .all()
        )
        return [r[0] for r in rows]

    def get_units(self) -> list[str]:
        rows = (
            self.session.query(Product.unit)
            .filter(Product.unit.isnot(None), Product.unit != "")
            .distinct()
            .order_by(Product.unit)
            .all()
        )
        return [r[0] for r in rows]

    def get_review_products(self, threshold: float = 0.8) -> list[Product]:
        return (
            self.session.query(Product)
            .filter((Product.confidence_score < threshold) | (Product.reviewed == False))
            .order_by(Product.confidence_score)
            .all()
        )

    def delete(self, product_id: int) -> bool:
        product = self.get_by_id(product_id)
        if not product:
            return False
        self.session.delete(product)
        self.session.commit()
        return True
