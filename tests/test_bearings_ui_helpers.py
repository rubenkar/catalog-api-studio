"""Tests for the pure result-table helper used by ExtractResultDialog."""

from app.ui.result_table import result_table_rows


RESULT = {
    "items": [
        {"designation": "6205", "d": 25.0, "D": 52.0, "page": 14},
        {"designation": "6206", "d": None, "D": 62.0, "page": 15},
    ],
}

MANIFEST = {
    "fields": [
        {"key": "designation", "label": "Bearing designation", "unit": None, "core": True},
        {"key": "d", "label": "Bore diameter", "unit": "mm", "core": True},
        {"key": "D", "label": "Outer diameter", "unit": "mm", "core": True},
    ]
}


def test_headers_follow_manifest_order_with_page_last():
    headers, rows = result_table_rows(RESULT, MANIFEST)
    assert headers == ["designation", "d", "D", "page"]
    assert rows == [
        ["6205", "25.0", "52.0", "14"],
        ["6206", "", "62.0", "15"],
    ]


def test_headers_without_manifest_derive_from_items():
    headers, rows = result_table_rows(RESULT, None)
    assert headers[0] == "designation"
    assert headers[-1] == "page"
    assert set(headers) == {"designation", "d", "D", "page"}
    assert len(rows) == 2


def test_empty_result():
    headers, rows = result_table_rows({"items": []}, None)
    assert headers == []
    assert rows == []
