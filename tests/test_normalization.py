"""Tests for normalization functions."""

from catalog_api_studio.normalization.normalizer import Normalizer


def test_normalize_sku():
    n = Normalizer()
    assert n.normalize_sku("  skf-6205  ") == "SKF-6205"
    assert n.normalize_sku("abc 123") == "ABC123"
    assert n.normalize_sku(None) is None
    assert n.normalize_sku("") is None


def test_normalize_unit():
    n = Normalizer()
    assert n.normalize_unit("шт") == "pcs"
    assert n.normalize_unit("шт.") == "pcs"
    assert n.normalize_unit("кг") == "kg"
    assert n.normalize_unit("mm") == "mm"
    assert n.normalize_unit(None) is None


def test_normalize_brand():
    n = Normalizer(known_brands=["SKF", "FAG", "NSK"])
    assert n.normalize_brand("skf") == "SKF"
    assert n.normalize_brand("fag") == "FAG"
    assert n.normalize_brand("Unknown Brand") == "Unknown Brand"
    assert n.normalize_brand(None) is None


def test_clean_text():
    n = Normalizer()
    assert n.clean_text("  hello   world  ") == "hello world"
    assert n.clean_text("<b>bold</b> text") == "bold text"
    assert n.clean_text(None) is None
    assert n.clean_text("") is None


def test_normalize_price():
    n = Normalizer()
    assert n.normalize_price(15.555) == 15.56
    assert n.normalize_price(-10.0) == 10.0
    assert n.normalize_price(None) is None


def test_normalize_product():
    n = Normalizer(known_brands=["SKF"])
    result = n.normalize_product({
        "sku": " skf-6205 ",
        "brand": "skf",
        "name": "  Ball  Bearing  ",
        "unit": "шт.",
        "price": 15.555,
    })
    assert result["sku"] == "SKF-6205"
    assert result["brand"] == "SKF"
    assert result["name"] == "Ball Bearing"
    assert result["unit"] == "pcs"
    assert result["price"] == 15.56
