"""Stage 6: dedupe, stats and final JSON output."""

import json
import logging
from pathlib import Path

from .models import CatalogResult, Issue

logger = logging.getLogger(__name__)


def dedupe_items(items: list[dict]) -> list[dict]:
    by_designation: dict[str, dict] = {}
    for item in sorted(items, key=lambda i: i.get("page") or 0):
        key = item["designation"]
        if key not in by_designation:
            by_designation[key] = dict(item)
            continue
        merged = by_designation[key]
        for field, value in item.items():
            if field == "page":
                merged["page"] = min(merged.get("page") or value, value)
            elif merged.get(field) is None:
                merged[field] = value
            elif value is not None and merged[field] != value:
                logger.debug("Conflict for %s.%s: %r vs %r", key, field, merged[field], value)
    return list(by_designation.values())


def assemble(
    source: str,
    brand: str,
    items: list[dict],
    issues: list[Issue],
    pages_total: int,
    pages_with_data: int,
    extracted_at: str,
) -> CatalogResult:
    deduped = dedupe_items(items)
    return CatalogResult(
        source=source, brand=brand, extracted_at=extracted_at,
        items=deduped,
        stats={
            "pages_total": pages_total,
            "pages_with_data": pages_with_data,
            "items_count": len(deduped),
        },
        issues=issues,
    )


def write_result(result: CatalogResult, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(result.model_dump(), ensure_ascii=False, indent=1), encoding="utf-8"
    )
    logger.info("Wrote %s (%d items)", out_path, len(result.items))
