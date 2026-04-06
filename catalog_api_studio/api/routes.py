"""FastAPI route definitions — all MVP endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from catalog_api_studio.db.engine import get_session
from catalog_api_studio.models.schemas import (
    FilterResponse,
    ProductList,
    ProductResponse,
    SearchResult,
)
from catalog_api_studio.search.indexer import SearchIndexer
from catalog_api_studio.services.product_service import ProductService

router = APIRouter()

_search_indexer: SearchIndexer | None = None


def get_db() -> Session:
    session = get_session()
    try:
        yield session
    finally:
        session.close()


def get_search() -> SearchIndexer:
    global _search_indexer
    if _search_indexer is None:
        _search_indexer = SearchIndexer()
    return _search_indexer


@router.get("/health")
def health(session: Session = Depends(get_db)):
    from catalog_api_studio.db.models import Product

    count = session.query(Product).count()
    search = get_search()
    return {
        "status": "ok",
        "products_count": count,
        "search_available": search.available,
    }


@router.get("/products", response_model=ProductList)
def list_products(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    brand: str | None = None,
    category: str | None = None,
    session: Session = Depends(get_db),
):
    svc = ProductService(session)
    products, total = svc.get_all(page=page, per_page=per_page, brand=brand, category=category)
    return ProductList(
        items=[ProductResponse.model_validate(p) for p in products],
        total=total,
        page=page,
        per_page=per_page,
    )


@router.get("/products/{product_id}", response_model=ProductResponse)
def get_product(product_id: int, session: Session = Depends(get_db)):
    svc = ProductService(session)
    product = svc.get_by_id(product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return ProductResponse.model_validate(product)


@router.get("/search", response_model=SearchResult)
def search_products(
    q: str = "",
    brand: str | None = None,
    category: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    session: Session = Depends(get_db),
):
    search = get_search()
    products, total = search.search(
        query=q, session=session, brand=brand, category=category, page=page, per_page=per_page
    )
    return SearchResult(
        items=[ProductResponse.model_validate(p) for p in products],
        total=total,
        query=q,
    )


@router.get("/brands", response_model=list[str])
def list_brands(session: Session = Depends(get_db)):
    svc = ProductService(session)
    return svc.get_brands()


@router.get("/categories", response_model=list[str])
def list_categories(session: Session = Depends(get_db)):
    svc = ProductService(session)
    return svc.get_categories()


@router.get("/filters", response_model=FilterResponse)
def get_filters(session: Session = Depends(get_db)):
    svc = ProductService(session)
    return FilterResponse(
        brands=svc.get_brands(),
        categories=svc.get_categories(),
        units=svc.get_units(),
    )
