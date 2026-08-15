import json

import fitz
import pytest

from app.extraction.bearings.cli import brand_from_filename, main
from app.extraction.bearings.llm import DeepSeekClient
from app.extraction.bearings.models import FieldSpec, Manifest
from app.extraction.bearings.profiler import build_manifest
from app.extraction.bearings.rules import build_rule, extract_page_direct


def make_client(tmp_path, content: dict):
    def fake_post(url, headers, payload, timeout):
        return {"choices": [{"message": {"content": json.dumps(content)}}]}
    return DeepSeekClient("sk", tmp_path, post_fn=fake_post)


def test_build_manifest(tmp_path):
    client = make_client(tmp_path, {
        "fields": [
            {"key": "designation", "label": "Bearing number", "unit": None, "core": True},
            {"key": "d", "label": "Bore diameter", "unit": "mm", "core": True},
        ]
    })
    m = build_manifest(client, "KOYO.pdf", "KOYO", ["y=100 | 6205[50] 25[150]"])
    assert m.brand == "KOYO"
    assert m.field_keys() == ["designation", "d"]


def test_build_rule(tmp_path):
    client = make_client(tmp_path, {
        "header_skip_lines": 1,
        "columns": [{"x_min": 40, "x_max": 120, "field": "designation"}],
        "row_gap_pt": 5.0,
        "inherit_fields": [],
        "type_default": "deep_groove_ball",
    })
    manifest = Manifest(source="a.pdf", brand="X",
                        fields=[FieldSpec(key="designation", label="No", core=True)])
    rule = build_rule(client, manifest, "AAA", ["y=100 | Bearing[50]"])
    assert rule.header_skip_lines == 1
    assert rule.columns[0].field == "designation"


def test_extract_page_direct(tmp_path):
    client = make_client(tmp_path, {"items": [{"designation": "6205", "d": "25"}]})
    manifest = Manifest(source="a.pdf", brand="X",
                        fields=[FieldSpec(key="designation", label="No", core=True),
                                FieldSpec(key="d", label="Bore", unit="mm", core=True)])
    rows = extract_page_direct(client, manifest, 7, "y=100 | 6205[50] 25[150]")
    assert rows == [{"designation": "6205", "d": "25"}]


@pytest.fixture()
def tiny_catalog(tmp_path):
    """Two data pages with identical layout + a cover page."""
    doc = fitz.open()
    cover = doc.new_page(width=595, height=842)
    cover.insert_text((100, 100), "KOYO GENERAL CATALOGUE", fontsize=20)
    for page_no in range(2):
        page = doc.new_page(width=595, height=842)
        rows = [("Bearing", "d", "D", "B")] + [
            (f"62{page_no}{i}", str(20 + i), str(47 + i), "14") for i in range(10)
        ]
        y = 100
        for row in rows:
            for x, text in zip([50, 150, 220, 290], row):
                page.insert_text((x, y), text, fontsize=10)
            y += 20
    pdf = tmp_path / "KOYO - Test.pdf"
    doc.save(str(pdf))
    doc.close()
    return pdf


def test_brand_from_filename(tmp_path):
    assert brand_from_filename(tmp_path / "KOYO - Ball Bearings.pdf") == "KOYO"
    assert brand_from_filename(tmp_path / "Dinroll.pdf") == "Dinroll"


def test_extract_end_to_end(tiny_catalog, tmp_path, monkeypatch):
    responses = {
        "manifest": {"fields": [
            {"key": "designation", "label": "Bearing number", "unit": None, "core": True},
            {"key": "d", "label": "Bore", "unit": "mm", "core": True},
            {"key": "D", "label": "Outer", "unit": "mm", "core": True},
            {"key": "B", "label": "Width", "unit": "mm", "core": True},
        ]},
        "rule": {"header_skip_lines": 1,
                 "columns": [
                     {"x_min": 40, "x_max": 130, "field": "designation"},
                     {"x_min": 140, "x_max": 200, "field": "d"},
                     {"x_min": 210, "x_max": 270, "field": "D"},
                     {"x_min": 280, "x_max": 340, "field": "B"},
                 ],
                 "row_gap_pt": 5.0, "inherit_fields": [], "type_default": None},
    }

    def fake_post(url, headers, payload, timeout):
        key = "manifest" if "какие характеристики" in payload["messages"][0]["content"] \
            else "rule"
        return {"choices": [{"message": {"content": json.dumps(responses[key])}}]}

    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test")
    monkeypatch.setattr("app.extraction.bearings.llm._default_post", fake_post)

    out = tmp_path / "out"
    code = main(["extract", str(tiny_catalog), "--out", str(out)])
    assert code == 0

    data = json.loads((out / "koyo.json").read_text(encoding="utf-8"))
    assert data["brand"] == "KOYO"
    assert data["stats"]["items_count"] == 20
    assert data["stats"]["pages_total"] == 3
    assert all(isinstance(i["d"], float) for i in data["items"])
    pages = {i["page"] for i in data["items"]}
    assert pages == {2, 3}


def test_extract_end_to_end_fallback_direct(tiny_catalog, tmp_path, monkeypatch):
    """apply_rule yields 0 rows (columns outside the page) -> extract_page_direct fallback."""
    import re

    responses = {
        "manifest": {"fields": [
            {"key": "designation", "label": "Bearing number", "unit": None, "core": True},
            {"key": "d", "label": "Bore", "unit": "mm", "core": True},
            {"key": "D", "label": "Outer", "unit": "mm", "core": True},
            {"key": "B", "label": "Width", "unit": "mm", "core": True},
        ]},
        "rule": {"header_skip_lines": 1,
                 "columns": [
                     {"x_min": 1000, "x_max": 1010, "field": "designation"},
                 ],
                 "row_gap_pt": 5.0, "inherit_fields": [], "type_default": None},
    }

    def fake_post(url, headers, payload, timeout):
        system = payload["messages"][0]["content"]
        if "какие характеристики" in system:
            return {"choices": [{"message": {"content": json.dumps(responses["manifest"])}}]}
        if "правило разбора" in system:
            return {"choices": [{"message": {"content": json.dumps(responses["rule"])}}]}
        assert "Извлеки ВСЕ записи" in system
        user = payload["messages"][1]["content"]
        match = re.search(r"(62\d+)", user)
        designation = match.group(1) if match else "FALLBACK"
        content = {"items": [{"designation": designation, "d": "20", "D": "47", "B": "14"}]}
        return {"choices": [{"message": {"content": json.dumps(content)}}]}

    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test")
    monkeypatch.setattr("app.extraction.bearings.llm._default_post", fake_post)
    monkeypatch.setattr("app.extraction.bearings.llm.time.sleep", lambda s: None)

    out = tmp_path / "out"
    code = main(["extract", str(tiny_catalog), "--out", str(out)])
    assert code == 0

    data = json.loads((out / "koyo.json").read_text(encoding="utf-8"))
    designations = {i["designation"] for i in data["items"]}
    assert designations == {"6200", "6210"}
    pages = {i["page"] for i in data["items"]}
    assert pages == {2, 3}
    assert data["issues"] == []


def test_extract_end_to_end_fallback_fails_records_issue(tiny_catalog, tmp_path, monkeypatch):
    """apply_rule yields 0 rows and extract_page_direct never returns valid JSON.

    The page must end up in issues with "rule and fallback failed"; the run
    must not crash and the result file must still be written.
    """
    responses = {
        "manifest": {"fields": [
            {"key": "designation", "label": "Bearing number", "unit": None, "core": True},
            {"key": "d", "label": "Bore", "unit": "mm", "core": True},
            {"key": "D", "label": "Outer", "unit": "mm", "core": True},
            {"key": "B", "label": "Width", "unit": "mm", "core": True},
        ]},
        "rule": {"header_skip_lines": 1,
                 "columns": [
                     {"x_min": 1000, "x_max": 1010, "field": "designation"},
                 ],
                 "row_gap_pt": 5.0, "inherit_fields": [], "type_default": None},
    }

    def fake_post(url, headers, payload, timeout):
        system = payload["messages"][0]["content"]
        if "какие характеристики" in system:
            return {"choices": [{"message": {"content": json.dumps(responses["manifest"])}}]}
        if "правило разбора" in system:
            return {"choices": [{"message": {"content": json.dumps(responses["rule"])}}]}
        assert "Извлеки ВСЕ записи" in system
        return {"choices": [{"message": {"content": "not-json"}}]}

    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test")
    monkeypatch.setattr("app.extraction.bearings.llm._default_post", fake_post)
    monkeypatch.setattr("app.extraction.bearings.llm.time.sleep", lambda s: None)

    out = tmp_path / "out"
    code = main(["extract", str(tiny_catalog), "--out", str(out)])
    assert code == 0

    data = json.loads((out / "koyo.json").read_text(encoding="utf-8"))
    assert data["items"] == []
    assert data["stats"]["items_count"] == 0
    problems = {(issue["page"], issue["problem"]) for issue in data["issues"]}
    assert problems == {(2, "rule and fallback failed"), (3, "rule and fallback failed")}
