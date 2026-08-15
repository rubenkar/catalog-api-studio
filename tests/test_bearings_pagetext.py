import fitz
import pytest

from app.extraction.bearings.pagetext import (
    is_data_page, layout_text, numeric_density, page_words,
)


@pytest.fixture()
def table_page(tmp_path):
    """Synthetic one-page PDF resembling a bearing table."""
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    rows = [
        ("Bearing", "d", "D", "B", "Cr"),
        ("6204", "20", "47", "14", "12.8"),
        ("6205", "25", "52", "15", "14.0"),
        ("6206", "30", "62", "16", "19.5"),
    ]
    xs = [50, 150, 220, 290, 360]
    y = 100
    for row in rows:
        for x, text in zip(xs, row):
            page.insert_text((x, y), text, fontsize=10)
        y += 20
    yield page
    doc.close()


def test_page_words_sorted(table_page):
    words = page_words(table_page)
    assert len(words) == 20
    ys = [round(w[1], 1) for w in words]
    assert ys == sorted(ys)


def test_numeric_density(table_page):
    words = page_words(table_page)
    # 15 numeric out of 20 (заголовок + колонка Bearing частично)
    assert numeric_density(words) == pytest.approx(15 / 20)


def test_is_data_page(table_page):
    words = page_words(table_page)
    assert is_data_page(words, min_words=10, min_density=0.3)
    assert not is_data_page(words[:5], min_words=10, min_density=0.3)


def test_layout_text_contains_coords(table_page):
    words = page_words(table_page)
    text = layout_text(words, page_width=595)
    assert "6205[50]" in text
    lines = text.splitlines()
    assert lines[0].startswith("y=")
