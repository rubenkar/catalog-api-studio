import fitz
import pytest

from app.pdf.tablelines import column_bounds, extract_table_lines


@pytest.fixture()
def grid_page():
    """Page with a 3x2 ruled table drawn as lines."""
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    xs = [50, 200, 350, 500]   # 4 vertical borders → 3 columns
    ys = [100, 130, 160]       # 3 horizontal borders → 2 rows
    for x in xs:
        page.draw_line((x, ys[0]), (x, ys[-1]))
    for y in ys:
        page.draw_line((xs[0], y), (xs[-1], y))
    yield page
    doc.close()


def test_extract_table_lines(grid_page):
    rect = fitz.Rect(40, 90, 510, 170)
    h_segs, v_segs = extract_table_lines(grid_page, rect)
    h_ys = sorted({round(s[2]) for s in h_segs})
    v_xs = sorted({round(s[0]) for s in v_segs})
    assert h_ys == [100, 130, 160]
    assert v_xs == [50, 200, 350, 500]


def test_column_bounds_merges_close():
    v_segs = [(50.0, 0, 100), (51.0, 0, 100), (200.0, 0, 100)]
    assert column_bounds(v_segs) == [50.5, 200.0]
