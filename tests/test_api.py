"""Tests for FastAPI endpoints."""

import pytest
from fastapi.testclient import TestClient

from catalog_api_studio.db.engine import get_session, init_db
from catalog_api_studio.db.models import Product


@pytest.fixture(scope="module", autouse=True)
def setup_db():
    """Initialize the real DB and seed test data."""
    init_db()
    session = get_session()
    # Clean existing
    session.query(Product).delete()
    session.commit()

    # Seed
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

    yield

    # Cleanup
    session = get_session()
    session.query(Product).delete()
    session.commit()
    session.close()


@pytest.fixture
def client():
    from catalog_api_studio.api.app import create_app

    app = create_app()
    return TestClient(app)


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["products_count"] == 5


def test_list_products(client):
    resp = client.get("/products")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 5
    assert len(data["items"]) == 5


def test_get_product(client):
    resp = client.get("/products/1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["sku"] == "SKU-000"


def test_get_product_not_found(client):
    resp = client.get("/products/999")
    assert resp.status_code == 404


def test_brands(client):
    resp = client.get("/brands")
    assert resp.status_code == 200
    assert "TestBrand" in resp.json()


def test_categories(client):
    resp = client.get("/categories")
    assert resp.status_code == 200
    assert "TestCat" in resp.json()


def test_filters(client):
    resp = client.get("/filters")
    assert resp.status_code == 200
    data = resp.json()
    assert "TestBrand" in data["brands"]
    assert "TestCat" in data["categories"]


def test_search(client):
    resp = client.get("/search?q=Product")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] > 0
    assert data["query"] == "Product"
