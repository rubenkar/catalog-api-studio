from app.extraction.bearings.models import (
    CatalogResult, ColumnSpec, FieldSpec, Issue, Manifest, ParseRule,
)


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
