"""Tests for FastAPI endpoints."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from catalog_api_studio.api.app import create_app
from catalog_api_studio.api.routes import get_db
from catalog_api_studio.db.models import Base, Product


@pytest.fixture
def test_app():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    TestSession = sessionmaker(bind=engine)

    # Seed data
    session = TestSession()
    for i in range(5):
        session.add(Product(
            sku=f"SKU-{i:03d}",
            brand="TestBrand",
            name=f"Product {i}",
            category="TestCat",
            price=10.0 + i,
            confidence_score=0.5 + i * 0.1,
        ))
    session.commit()
    session.close()

    app = create_app()

    def override_get_db():
        session = TestSession()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app)


def test_health(test_app):
    resp = test_app.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["products_count"] == 5


def test_list_products(test_app):
    resp = test_app.get("/products")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 5
    assert len(data["items"]) == 5


def test_get_product(test_app):
    resp = test_app.get("/products/1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["sku"] == "SKU-000"


def test_get_product_not_found(test_app):
    resp = test_app.get("/products/999")
    assert resp.status_code == 404


def test_brands(test_app):
    resp = test_app.get("/brands")
    assert resp.status_code == 200
    assert "TestBrand" in resp.json()


def test_categories(test_app):
    resp = test_app.get("/categories")
    assert resp.status_code == 200
    assert "TestCat" in resp.json()


def test_filters(test_app):
    resp = test_app.get("/filters")
    assert resp.status_code == 200
    data = resp.json()
    assert "TestBrand" in data["brands"]
    assert "TestCat" in data["categories"]


def test_search(test_app):
    resp = test_app.get("/search?q=Product")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] > 0
    assert data["query"] == "Product"
