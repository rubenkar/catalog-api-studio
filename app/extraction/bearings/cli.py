"""CLI orchestration: profile and extract commands."""

import argparse
import glob as globmod
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import fitz

from app.pdf.tablelines import column_bounds, extract_table_lines

from .assemble import assemble, write_result
from .fingerprint import cluster_pages, page_fingerprint
from .llm import DeepSeekClient, LLMError, load_api_key
from .models import CatalogResult, Issue, Manifest
from .mechanics import apply_rule
from .pagetext import is_data_page, layout_text, page_words
from .profiler import build_manifest
from .rules import build_rule, extract_page_direct
from .validate import validate_items

logger = logging.getLogger(__name__)

DEFAULT_OUT = Path("output/bearings")


def brand_from_filename(path: Path) -> str:
    stem = path.stem
    return stem.split(" -")[0].strip() if " -" in stem else stem.strip()


def _client_for(pdf_path: Path, out_dir: Path, force: bool = False) -> DeepSeekClient:
    cache_dir = out_dir / "cache" / pdf_path.stem
    return DeepSeekClient(load_api_key(), cache_dir)


def _page_data(doc: fitz.Document) -> dict[int, dict]:
    """page_no (1-based) -> {words, text, width, col_xs} for data pages only."""
    pages: dict[int, dict] = {}
    for idx in range(len(doc)):
        page = doc[idx]
        words = page_words(page)
        if not is_data_page(words):
            continue
        _, v_segs = extract_table_lines(page, page.rect)
        pages[idx + 1] = {
            "words": words,
            "text": layout_text(words, page.rect.width),
            "width": page.rect.width,
            "col_xs": column_bounds(v_segs),
        }
    return pages


def _manifest_path(out_dir: Path, brand: str) -> Path:
    return out_dir / f"{brand.lower()}.manifest.json"


def run_profile(
    pdf_path: Path, out_dir: Path, client: DeepSeekClient, sample: int = 12
) -> Manifest:
    brand = brand_from_filename(pdf_path)
    with fitz.open(str(pdf_path)) as doc:
        pages = _page_data(doc)
    if not pages:
        raise RuntimeError(f"{pdf_path.name}: no data pages found")
    page_nos = sorted(pages)
    step = max(1, len(page_nos) // sample)
    sample_texts = [pages[n]["text"] for n in page_nos[::step][:sample]]
    manifest = build_manifest(client, pdf_path.name, brand, sample_texts)
    path = _manifest_path(out_dir, brand)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(manifest.model_dump(), ensure_ascii=False, indent=1), encoding="utf-8"
    )
    logger.info("Manifest written: %s", path)
    return manifest


def run_extract(pdf_path: Path, out_dir: Path, client: DeepSeekClient) -> CatalogResult:
    brand = brand_from_filename(pdf_path)
    manifest_file = _manifest_path(out_dir, brand)
    if manifest_file.exists():
        manifest = Manifest(**json.loads(manifest_file.read_text(encoding="utf-8")))
    else:
        manifest = run_profile(pdf_path, out_dir, client)

    with fitz.open(str(pdf_path)) as doc:
        pages_total = len(doc)
        pages = _page_data(doc)

    fps = {
        n: page_fingerprint(p["words"], p["width"], v_xs=p["col_xs"])
        for n, p in pages.items()
    }
    clusters = cluster_pages(fps)

    all_items: list[dict] = []
    issues: list[Issue] = []
    for cluster_id, page_nos in clusters.items():
        try:
            rule = build_rule(
                client, manifest, cluster_id,
                [pages[n]["text"] for n in page_nos[:2]],
                column_hints=pages[page_nos[0]]["col_xs"] or None,
            )
        except (LLMError, ValueError) as exc:
            logger.warning("Rule failed for cluster %s: %s", cluster_id, exc)
            rule = None
        for n in page_nos:
            items: list[dict] = []
            page_issues: list[Issue] = []
            raw_count = 0
            if rule is not None:
                try:
                    raw = apply_rule(pages[n]["words"], rule)
                    raw_count = len(raw)
                    items, page_issues = validate_items(raw, manifest, n)
                except Exception as exc:  # noqa: BLE001 — правило от LLM, не падаем
                    logger.warning("apply_rule failed on page %d: %s", n, exc)
            if rule is None or raw_count == 0 or len(items) < raw_count * 0.5:
                try:
                    raw = extract_page_direct(client, manifest, n, pages[n]["text"])
                    items, page_issues = validate_items(raw, manifest, n)
                except LLMError:
                    issues.append(Issue(page=n, problem="rule and fallback failed"))
                    continue
            all_items.extend(items)
            issues.extend(page_issues)

    result = assemble(
        source=pdf_path.name, brand=brand, items=all_items, issues=issues,
        pages_total=pages_total, pages_with_data=len(pages),
        extracted_at=datetime.now(timezone.utc).isoformat(),
    )
    write_result(result, out_dir / f"{brand.lower()}.json")
    return result


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(prog="python -m app.extraction.bearings")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("profile", "extract"):
        p = sub.add_parser(name)
        p.add_argument("pdf_glob")
        p.add_argument("--out", default=str(DEFAULT_OUT))
        p.add_argument("--force", action="store_true")
        if name == "profile":
            p.add_argument("--sample", type=int, default=12)
    args = parser.parse_args(argv)

    paths = [Path(p) for p in sorted(globmod.glob(args.pdf_glob))]
    if not paths:
        parser.error(f"no PDF matches {args.pdf_glob}")
    out_dir = Path(args.out)
    for pdf_path in paths:
        client = _client_for(pdf_path, out_dir, force=args.force)
        if args.command == "profile":
            run_profile(pdf_path, out_dir, client, sample=args.sample)
        else:
            result = run_extract(pdf_path, out_dir, client)
            print(
                f"{pdf_path.name}: {result.stats['items_count']} items, "
                f"{len(result.issues)} issues"
            )
    return 0
