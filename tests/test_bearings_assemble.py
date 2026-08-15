import json

from app.extraction.bearings.assemble import assemble, dedupe_items, write_result
from app.extraction.bearings.models import Issue


def test_dedupe_merges_nulls():
    items = [
        {"designation": "6205", "d": 25.0, "Cr": None, "page": 10},
        {"designation": "6205", "d": None, "Cr": 14.0, "page": 43},
        {"designation": "6206", "d": 30.0, "Cr": None, "page": 11},
    ]
    result = dedupe_items(items)
    assert len(result) == 2
    merged = next(i for i in result if i["designation"] == "6205")
    assert merged == {"designation": "6205", "d": 25.0, "Cr": 14.0, "page": 10}


def test_assemble_and_write(tmp_path):
    r = assemble(
        source="a.pdf", brand="X",
        items=[{"designation": "6205", "page": 1}],
        issues=[Issue(page=2, problem="x")],
        pages_total=3, pages_with_data=1,
        extracted_at="2026-08-15T00:00:00Z",
    )
    assert r.stats == {"pages_total": 3, "pages_with_data": 1, "items_count": 1}
    out = tmp_path / "x.json"
    write_result(r, out)
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["brand"] == "X" and data["items"][0]["designation"] == "6205"
