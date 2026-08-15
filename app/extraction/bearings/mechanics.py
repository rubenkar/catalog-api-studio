"""Deterministic word-to-row extraction driven by an LLM-produced ParseRule."""

import logging

from .models import ParseRule

logger = logging.getLogger(__name__)


def _group_lines(words: list[tuple], gap: float) -> list[list[tuple]]:
    lines: list[list[tuple]] = []
    for w in sorted(words, key=lambda w: ((w[1] + w[3]) / 2, w[0])):
        yc = (w[1] + w[3]) / 2
        if lines and abs(yc - (lines[-1][0][1] + lines[-1][0][3]) / 2) <= gap:
            lines[-1].append(w)
        else:
            lines.append([w])
    return lines


def apply_rule(words: list[tuple], rule: ParseRule) -> list[dict]:
    lines = _group_lines(words, rule.row_gap_pt)[rule.header_skip_lines:]
    rows: list[dict] = []
    last_values: dict[str, str] = {}
    for line in lines:
        cells: dict[str, list[tuple]] = {}
        for w in sorted(line, key=lambda w: w[0]):
            xc = (w[0] + w[2]) / 2
            for col in rule.columns:
                if col.x_min <= xc <= col.x_max:
                    cells.setdefault(col.field, []).append(w)
                    break
        row = {f: " ".join(w[4] for w in ws) for f, ws in cells.items()}
        for field in rule.inherit_fields:
            if not row.get(field) and field in last_values:
                row[field] = last_values[field]
        for field, value in row.items():
            if value:
                last_values[field] = value
        if rule.type_default and not row.get("type"):
            row["type"] = rule.type_default
        if row.get("designation"):
            rows.append(row)
    return rows
