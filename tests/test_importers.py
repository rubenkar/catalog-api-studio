"""Tests for file importers."""

import tempfile
from pathlib import Path

from catalog_api_studio.importers.csv_importer import CSVImporter


def test_csv_import():
    csv_content = """sku,name,brand,price,unit
SKF-6205,Ball Bearing 6205,SKF,15.50,pcs
FAG-6206,Ball Bearing 6206,FAG,18.00,pcs
NSK-6207,Ball Bearing 6207,NSK,,pcs
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as f:
        f.write(csv_content)
        f.flush()

        importer = CSVImporter(Path(f.name))
        products = importer.extract()

    assert len(products) == 3
    assert products[0].sku == "SKF-6205"
    assert products[0].brand == "SKF"
    assert products[0].price == 15.50
    assert products[0].confidence_score == 1.0  # all 4 key fields present

    # Third product has no price
    assert products[2].price is None
    assert products[2].confidence_score == 0.75


def test_csv_import_russian_headers():
    csv_content = """Артикул,Наименование,Бренд,Цена
SKF-6205,Подшипник 6205,SKF,1500
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as f:
        f.write(csv_content)
        f.flush()

        importer = CSVImporter(Path(f.name))
        products = importer.extract()

    assert len(products) == 1
    assert products[0].sku == "SKF-6205"
    assert products[0].brand == "SKF"


def test_csv_import_empty():
    csv_content = """sku,name
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as f:
        f.write(csv_content)
        f.flush()

        importer = CSVImporter(Path(f.name))
        products = importer.extract()

    assert len(products) == 0
