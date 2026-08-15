"""Row cleanup and sanity validation against a catalog manifest."""

import logging

from .models import Issue, Manifest

logger = logging.getLogger(__name__)

_EMPTY = {"", "-", "—", "–", "n/a", "N/A"}


def parse_number(text: str) -> float | None:
    if text is None:
        return None
    t = str(text).strip()
    if t in _EMPTY:
        return None
    t = t.replace(",", ".").replace(" ", "").replace(" ", "")
    try:
        return float(t)
    except ValueError:
        return None


def validate_items(
    rows: list[dict], manifest: Manifest, page: int
) -> tuple[list[dict], list[Issue]]:
    numeric_keys = {f.key for f in manifest.fields if f.unit is not None}
    items: list[dict] = []
    issues: list[Issue] = []
    for row in rows:
        designation = str(row.get("designation") or "").strip()
        if not designation:
            continue
        item: dict = {"designation": designation}
        for key, value in row.items():
            if key == "designation":
                continue
            item[key] = parse_number(value) if key in numeric_keys else value or None
        d, big_d = item.get("d"), item.get("D")
        if d is not None and big_d is not None and d >= big_d:
            issues.append(Issue(page=page, problem=f"{designation}: d={d} >= D={big_d}"))
            continue
        item["page"] = page
        items.append(item)
    return items, issues
