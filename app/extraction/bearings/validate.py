"""Row cleanup and sanity validation against a catalog manifest."""

import logging
import re

from .models import Issue, Manifest

logger = logging.getLogger(__name__)

_EMPTY = {"", "-", "—", "–", "n/a", "N/A"}

# пробел внутри числа допустим только как разделитель тысяч (группы по 3 цифры)
_THOUSANDS = re.compile(r"^\d{1,3}( \d{3})+(\.\d+)?$")
# маркеры сносок каталога в конце обозначения: «6306 4) 5) 15)», «6209 1)4)5)»
_FOOTNOTES = re.compile(r"(?:\s*\d{1,2}\))+\s*$")
_NUMERIC_TOKEN = re.compile(r"^[\d.,/x-]{3,}$")

# правдоподобные верхние границы значений по единицам измерения
_MAX_BY_UNIT = {"mm": 3000.0, "kN": 1e6, "daN": 1e6, "N": 1e7, "rpm": 1e6, "kg": 1e5}
_DEFAULT_MAX = 1e7


def parse_number(text: str) -> float | None:
    if text is None:
        return None
    t = str(text).strip().replace(" ", " ").replace(" ", " ")
    if t in _EMPTY:
        return None
    t = t.replace(",", ".")
    if " " in t:
        if not _THOUSANDS.match(t):
            return None
        t = t.replace(" ", "")
    try:
        return float(t)
    except ValueError:
        return None


def clean_designation(text: str) -> str:
    """Убирает маркеры сносок каталога: «6306 4) 5) 15)» -> «6306»."""
    return _FOOTNOTES.sub("", text).strip()


def _looks_merged(designation: str) -> bool:
    tokens = designation.split()
    return len(tokens) >= 2 and all(_NUMERIC_TOKEN.match(t) for t in tokens)


def validate_items(
    rows: list[dict], manifest: Manifest, page: int
) -> tuple[list[dict], list[Issue]]:
    numeric_units = {f.key: f.unit for f in manifest.fields if f.unit is not None}
    core_keys = {f.key for f in manifest.fields if f.core and f.unit == "mm"}
    items: list[dict] = []
    issues: list[Issue] = []
    for row in rows:
        designation = clean_designation(str(row.get("designation") or ""))
        if not designation:
            continue
        if _looks_merged(designation):
            issues.append(
                Issue(page=page, problem=f"{designation}: merged rows suspected")
            )
            continue
        item: dict = {"designation": designation}
        unparsed = 0
        for key, value in row.items():
            if key == "designation":
                continue
            if key in numeric_units:
                num = parse_number(value)
                # «нечитаемая» ячейка — только с цифрами внутри (склейка строк);
                # чистый текст (примечания вроде «para łożysk») просто обнуляется
                if num is None and value is not None and any(
                    c.isdigit() for c in str(value)
                ):
                    unparsed += 1
                item[key] = num
            else:
                item[key] = value or None
        if unparsed >= 2:
            issues.append(
                Issue(
                    page=page,
                    problem=f"{designation}: {unparsed} unparseable numeric cells",
                )
            )
            continue
        dropped = False
        nulled: list[str] = []
        for key, unit in numeric_units.items():
            v = item.get(key)
            if v is None:
                continue
            if v <= 0 or v >= _MAX_BY_UNIT.get(unit, _DEFAULT_MAX):
                if key in core_keys:
                    issues.append(
                        Issue(page=page, problem=f"{designation}: implausible {key}={v}")
                    )
                    dropped = True
                    break
                item[key] = None
                nulled.append(f"{key}={v}")
        if dropped:
            continue
        if nulled:
            issues.append(
                Issue(
                    page=page,
                    problem=f"{designation}: implausible {', '.join(nulled)} -> null",
                )
            )
        d, big_d = item.get("d"), item.get("D")
        if d is not None and big_d is not None and d >= big_d:
            issues.append(Issue(page=page, problem=f"{designation}: d={d} >= D={big_d}"))
            continue
        item["page"] = page
        items.append(item)
    return items, issues
