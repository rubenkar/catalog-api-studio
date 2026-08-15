from app.extraction.bearings.mechanics import apply_rule
from app.extraction.bearings.models import ColumnSpec, ParseRule


def W(x, y, text):
    return (x, y, x + 20, y + 10, text, 0, 0, 0)


RULE = ParseRule(
    header_skip_lines=1,
    columns=[
        ColumnSpec(x_min=40, x_max=120, field="designation"),
        ColumnSpec(x_min=140, x_max=200, field="d"),
        ColumnSpec(x_min=210, x_max=270, field="D"),
    ],
    row_gap_pt=5.0,
    inherit_fields=["d"],
    type_default="deep_groove_ball",
)


def test_apply_rule_basic():
    words = [
        W(50, 100, "Bearing"), W(150, 100, "d"), W(220, 100, "D"),   # header
        W(50, 120, "6205"), W(150, 120, "25"), W(220, 120, "52"),
        W(50, 140, "6205"), W(50, 141, "ZZ"), W(220, 140, "52"),     # d empty → inherit
    ]
    rows = apply_rule(words, RULE)
    assert rows == [
        {"designation": "6205", "d": "25", "D": "52", "type": "deep_groove_ball"},
        {"designation": "6205 ZZ", "d": "25", "D": "52", "type": "deep_groove_ball"},
    ]


def test_apply_rule_ignores_outside_words():
    words = [
        W(50, 100, "hdr"),
        W(50, 120, "6206"), W(300, 120, "noise"), W(150, 120, "30"),
    ]
    rows = apply_rule(words, RULE)
    assert rows == [{"designation": "6206", "d": "30", "type": "deep_groove_ball"}]
