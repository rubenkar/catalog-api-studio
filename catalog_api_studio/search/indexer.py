"""Search indexer — Typesense with SQLite LIKE fallback."""

import logging

from sqlalchemy import or_
from sqlalchemy.orm import Session

from catalog_api_studio.config.settings import settings
from catalog_api_studio.db.models import Product

logger = logging.getLogger(__name__)


class SearchIndexer:
    """Typesense search with graceful fallback to SQLite LIKE queries."""

    def __init__(self) -> None:
        self._client = None
        self._available = False
        self._init_typesense()

    def _init_typesense(self) -> None:
        try:
            import typesense

            self._client = typesense.Client({
                "nodes": [{
                    "host": settings.typesense_host,
                    "port": str(settings.typesense_port),
                    "protocol": "http",
                }],
                "api_key": settings.typesense_api_key,
                "connection_timeout_seconds": 2,
            })
            self._ensure_collection()
            self._available = True
            logger.info("Typesense connected")
        except Exception as e:
            logger.warning("Typesense not available, using SQLite fallback: %s", e)
            self._available = False

    def _ensure_collection(self) -> None:
        schema = {
            "name": settings.typesense_collection,
            "fields": [
                {"name": "sku", "type": "string", "optional": True},
                {"name": "brand", "type": "string", "optional": True, "facet": True},
                {"name": "name", "type": "string", "optional": True},
                {"name": "category", "type": "string", "optional": True, "facet": True},
                {"name": "description", "type": "string", "optional": True},
                {"name": "unit", "type": "string", "optional": True, "facet": True},
                {"name": "price", "type": "float", "optional": True},
            ],
        }
        try:
            self._client.collections[settings.typesense_collection].retrieve()
        except Exception:
            self._client.collections.create(schema)
            logger.info("Created Typesense collection: %s", settings.typesense_collection)

    @property
    def available(self) -> bool:
        return self._available

    def index_product(self, product: Product) -> None:
        if not self._available:
            return
        doc = {
            "id": str(product.id),
            "sku": product.sku or "",
            "brand": product.brand or "",
            "name": product.name or "",
            "category": product.category or "",
            "description": product.description or "",
            "unit": product.unit or "",
            "price": float(product.price) if product.price else 0.0,
        }
        try:
            self._client.collections[settings.typesense_collection].documents.upsert(doc)
        except Exception as e:
            logger.error("Failed to index product %s: %s", product.id, e)

    def index_all(self, session: Session) -> int:
        products = session.query(Product).all()
        count = 0
        for product in products:
            self.index_product(product)
            count += 1
        logger.info("Indexed %d products", count)
        return count

    def search(
        self,
        query: str,
        session: Session,
        brand: str | None = None,
        category: str | None = None,
        page: int = 1,
        per_page: int = 20,
    ) -> tuple[list[Product], int]:
        """Search products. Returns (products, total_count)."""
        if self._available and query:
            return self._typesense_search(query, session, brand, category, page, per_page)
        return self._sqlite_search(query, session, brand, category, page, per_page)

    def _typesense_search(
        self,
        query: str,
        session: Session,
        brand: str | None,
        category: str | None,
        page: int,
        per_page: int,
    ) -> tuple[list[Product], int]:
        filter_parts = []
        if brand:
            filter_parts.append(f"brand:={brand}")
        if category:
            filter_parts.append(f"category:={category}")

        params = {
            "q": query,
            "query_by": "sku,name,brand,description,category",
            "per_page": per_page,
            "page": page,
        }
        if filter_parts:
            params["filter_by"] = " && ".join(filter_parts)

        try:
            result = self._client.collections[settings.typesense_collection].documents.search(
                params
            )
            ids = [int(hit["document"]["id"]) for hit in result["hits"]]
            total = result["found"]

            if ids:
                products = session.query(Product).filter(Product.id.in_(ids)).all()
                id_order = {pid: i for i, pid in enumerate(ids)}
                products.sort(key=lambda p: id_order.get(p.id, 999))
            else:
                products = []

            return products, total
        except Exception as e:
            logger.error("Typesense search failed, falling back to SQLite: %s", e)
            return self._sqlite_search(query, session, brand, category, page, per_page)

    @staticmethod
    def _sqlite_search(
        query: str,
        session: Session,
        brand: str | None,
        category: str | None,
        page: int,
        per_page: int,
    ) -> tuple[list[Product], int]:
        q = session.query(Product)

        if query:
            like = f"%{query}%"
            q = q.filter(
                or_(
                    Product.sku.ilike(like),
                    Product.name.ilike(like),
                    Product.brand.ilike(like),
                    Product.description.ilike(like),
                    Product.category.ilike(like),
                )
            )

        if brand:
            q = q.filter(Product.brand == brand)
        if category:
            q = q.filter(Product.category == category)

        total = q.count()
        products = q.offset((page - 1) * per_page).limit(per_page).all()

        return products, total

    def delete_product(self, product_id: int) -> None:
        if not self._available:
            return
        try:
            self._client.collections[settings.typesense_collection].documents[
                str(product_id)
            ].delete()
        except Exception:
            pass
