import json

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
