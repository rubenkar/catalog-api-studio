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


def test_parse_number_rejects_glued_multivalue():
    # пробел внутри числа допустим только как разделитель тысяч (группы по 3
    # цифры); иначе это два склеенных числа из соседних строк таблицы
    assert parse_number("10900 3050") is None
    assert parse_number("35 18") is None
    assert parse_number("12 000") == 12000.0
    assert parse_number("1 234 567,5") == 1234567.5


def test_clean_designation_strips_footnotes():
    from app.extraction.bearings.validate import clean_designation

    assert clean_designation("6306 4) 5) 15)") == "6306"
    assert clean_designation("6209 1)4)5)7)11)12)") == "6209"
    assert clean_designation("6006 1)") == "6006"
    assert clean_designation("6205") == "6205"
    assert clean_designation("UCP 205") == "UCP 205"


def test_validate_strips_footnotes_from_designation():
    rows = [{"designation": "6306 4) 5) 15)", "d": "30", "D": "72", "Cr": "2810"}]
    items, issues = validate_items(rows, _manifest(), page=14)
    assert items[0]["designation"] == "6306"
    assert issues == []


def test_validate_drops_merged_rows():
    # две строки таблицы слились: designation из двух обозначений,
    # числовые ячейки содержат по два значения
    rows = [
        {"designation": "6222 6022", "d": "110", "D": "170 200", "Cr": "14400 8200"},
        {"designation": "6205", "d": "25", "D": "52", "Cr": "14,0"},
    ]
    items, issues = validate_items(rows, _manifest(), page=15)
    assert [i["designation"] for i in items] == ["6205"]
    assert len(issues) == 1 and issues[0].page == 15


def test_validate_drops_implausible_core_dimension():
    # D=100150 — склейка «100 150» без пробела: размер >= 3000 мм нереален
    rows = [{"designation": "6412", "d": "60", "D": "100150", "Cr": "1090"}]
    items, issues = validate_items(rows, _manifest(), page=15)
    assert items == []
    assert len(issues) == 1


def test_validate_nulls_implausible_noncore_value():
    # склеенная нагрузка: значение обнуляется, запись остаётся, issue фиксируется
    rows = [{"designation": "6412", "d": "60", "D": "150", "Cr": "109003050"}]
    items, issues = validate_items(rows, _manifest(), page=15)
    assert len(items) == 1
    assert items[0]["Cr"] is None
    assert len(issues) == 1


def test_validate_keeps_row_with_text_in_numeric_cells():
    # «para łożysk» (примечание) заняло ДВЕ числовые колонки — это не склейка
    # строк: ячейки без цифр просто обнуляются, запись остаётся
    rows = [{"designation": "30208 AXA", "d": "80", "D": "para", "Cr": "łożysk"}]
    items, issues = validate_items(rows, _manifest(), page=26)
    assert len(items) == 1
    assert items[0]["designation"] == "30208 AXA"
    assert items[0]["D"] is None and items[0]["Cr"] is None
    assert issues == []


def test_fix_units_from_samples():
    from app.extraction.bearings.profiler import fix_units

    m = _manifest()  # Cr имеет unit="kN"
    fixed = fix_units(m, ["y=324.4 | mm[103] daN[433] obr.[496] kg[552]"])
    assert {f.key: f.unit for f in fixed.fields}["Cr"] == "daN"
    # а если в образцах реально kN — ничего не меняем
    same = fix_units(_manifest(), ["y=100 | mm[103] kN[433] rpm[496]"])
    assert {f.key: f.unit for f in same.fields}["Cr"] == "kN"


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
