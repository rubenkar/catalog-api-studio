from app.extraction.bearings.models import (
    CatalogResult, ColumnSpec, FieldSpec, Issue, Manifest, ParseRule,
)
from app.extraction.bearings.validate import parse_number, validate_items


def test_manifest_field_keys():
    m = Manifest(
        source="KOYO.pdf", brand="KOYO",
        fields=[
            FieldSpec(key="designation", label="Bearing number", core=True),
            FieldSpec(key="d", label="Bore diameter", unit="mm", core=True),
        ],
    )
    assert m.field_keys() == ["designation", "d"]


def test_parse_rule_defaults():
    rule = ParseRule(columns=[ColumnSpec(x_min=40, x_max=90, field="designation")])
    assert rule.header_skip_lines == 0
    assert rule.row_gap_pt == 4.0
    assert rule.inherit_fields == []


def test_catalog_result_round_trip():
    r = CatalogResult(
        source="a.pdf", brand="X", extracted_at="2026-08-15T00:00:00Z",
        items=[{"designation": "6205", "page": 3}],
        stats={"pages_total": 5, "pages_with_data": 1, "items_count": 1},
        issues=[Issue(page=4, problem="rule failed")],
    )
    data = r.model_dump()
    assert data["pipeline_version"] == "1.0"
    assert data["issues"][0]["page"] == 4


def _manifest():
    return Manifest(
        source="x.pdf", brand="X",
        fields=[
            FieldSpec(key="designation", label="No", core=True),
            FieldSpec(key="d", label="Bore", unit="mm", core=True),
            FieldSpec(key="D", label="Outer", unit="mm", core=True),
            FieldSpec(key="Cr", label="Load", unit="kN"),
        ],
    )


def test_parse_number():
    assert parse_number("14,0") == 14.0
    assert parse_number("12 000") == 12000.0
    assert parse_number("—") is None
    assert parse_number("abc") is None


def test_validate_items_ok_and_bad():
    rows = [
        {"designation": "6205", "d": "25", "D": "52", "Cr": "14,0"},
        {"designation": "", "d": "1", "D": "2", "Cr": "3"},        # dropped silently
        {"designation": "BAD", "d": "52", "D": "25", "Cr": "1"},   # d >= D → issue
    ]
    items, issues = validate_items(rows, _manifest(), page=7)
    assert len(items) == 1
    assert items[0] == {"designation": "6205", "d": 25.0, "D": 52.0, "Cr": 14.0, "page": 7}
    assert len(issues) == 1 and issues[0].page == 7
