"""Tests for database models and CRUD."""

from catalog_api_studio.db.models import ImportJob, Product


def test_create_import_job(db_session):
    job = ImportJob(
        filename="test.csv",
        file_type="csv",
        file_path="/tmp/test.csv",
        status="pending",
    )
    db_session.add(job)
    db_session.commit()

    assert job.id is not None
    assert job.status == "pending"
    assert job.products_count == 0


def test_create_product(db_session):
    job = ImportJob(
        filename="test.csv",
        file_type="csv",
        file_path="/tmp/test.csv",
        status="done",
    )
    db_session.add(job)
    db_session.commit()

    product = Product(
        import_job_id=job.id,
        sku="SKF-6205",
        brand="SKF",
        name="Ball Bearing 6205",
        category="Bearings",
        price=15.50,
        confidence_score=0.75,
    )
    db_session.add(product)
    db_session.commit()

    assert product.id is not None
    assert product.sku == "SKF-6205"
    assert product.confidence_score == 0.75
    assert product.reviewed is False


def test_product_service_get_brands(db_session):
    from catalog_api_studio.services.product_service import ProductService

    for brand in ["SKF", "FAG", "NSK", "SKF"]:
        db_session.add(Product(brand=brand, name=f"{brand} bearing", confidence_score=0.5))
    db_session.commit()

    svc = ProductService(db_session)
    brands = svc.get_brands()
    assert "SKF" in brands
    assert "FAG" in brands
    assert len(brands) == 3  # distinct


def test_product_update(db_session):
    from catalog_api_studio.models.schemas import ProductUpdate
    from catalog_api_studio.services.product_service import ProductService

    product = Product(sku="TEST-001", name="Test", confidence_score=0.25)
    db_session.add(product)
    db_session.commit()

    svc = ProductService(db_session)
    updated = svc.update(product.id, ProductUpdate(brand="TestBrand", reviewed=True))

    assert updated is not None
    assert updated.brand == "TestBrand"
    assert updated.reviewed is True
    assert updated.confidence_score > 0.25  # recalculated
