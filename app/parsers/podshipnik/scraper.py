"""
shop.podshipnik.ru catalog scraper - 3-stage pipeline.

Stage 1: CATEGORIES — scrape full category tree (TYPE/SUBTYPE) + dimension dropdowns
Stage 2: ITEMS     — iterate categories × bore diameters, paginated catalog with all=all
Stage 3: DETAILS   — fetch detail pages for specs, images, analogs

Key fixes vs v1:
- all=all param (was missing → only got 1 subcategory, 904 items)
- Pagination by total count (was broken at page 10 because Bitrix only shows 10 links)
- Category-based iteration (covers all product types, not just bearings)
"""
import argparse
import json
import logging
import math
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlencode

from bs4 import BeautifulSoup

# Add parent dir to path so we can import the shared library
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.base_scraper import BaseScraper
from lib.models import ScrapedItem, ScrapedCategory
from lib.utils import parse_article
from config import (
    BASE_URL, REQUEST_DELAY,
    CATALOG_SELECTORS, DETAIL_SELECTORS,
    PAGE_PARAM,
)

logger = logging.getLogger('podshipnik')

ITEMS_PER_PAGE = 30  # Bitrix shows 30 items per page


class PodshipnikScraper(BaseScraper):
    """3-stage scraper for shop.podshipnik.ru (~60k items)."""

    PARSER_NAME = "podshipnik"
    SOURCE_NAME = "podshipnik.ru"

    def __init__(self, output_dir: str = "./output", resume: bool = True,
                 stage: str = "all", max_items: int = 0,
                 skip_images: bool = False):
        super().__init__(output_dir=output_dir, resume=resume, delay=REQUEST_DELAY)
        self.stage = stage          # "categories", "items", "details", "all"
        self.max_items = max_items  # 0 = unlimited
        self.skip_images = skip_images

        # Stage data files
        self._categories_file = self.output_dir / "categories.json"
        self._items_file = self.output_dir / "items.json"

        # Loaded category data
        self._category_tree = []    # [{type_id, name, subtypes: [{id, name}]}]
        self._diameters = []        # [str] bore diameter values

        # Dedup
        self._seen_articles = set()
        self._items_dirty = False  # Track if items changed since last save

        # Category mapping: subtype_name → TYPE_name
        # Built from categories.json, used to resolve column 0 into [TYPE, SUBTYPE]
        self._subtype_to_type = {}

    # ══════════════════════════════════════════════════════════
    #  MAIN ENTRY
    # ══════════════════════════════════════════════════════════

    def scrape(self):
        """Run the requested stage(s)."""
        if self.stage in ('categories', 'all'):
            self._stage1_categories()

        if self.stage in ('items', 'all'):
            self._stage2_items()

        if self.stage in ('details', 'all'):
            self._stage3_details()

        return self._items

    # ══════════════════════════════════════════════════════════
    #  STAGE 1: CATEGORIES
    # ══════════════════════════════════════════════════════════

    def _stage1_categories(self):
        """Scrape the full category tree + dimension dropdown values from the main page."""
        self.progress.status('stage1', 'Fetching main page for categories and dimensions...')

        html = self.fetcher.get_text(BASE_URL)
        if not html:
            self.progress.error("Failed to fetch main page", fatal=True)
            return

        soup = BeautifulSoup(html, 'lxml')

        # ── Extract category checkboxes ──
        # TYPE[N] checkboxes for main categories, SUBTYPE[N][M] for subcategories
        categories = self._extract_categories(soup)
        logger.info(f"Found {len(categories)} main categories with subtypes")

        # ── Extract dimension dropdowns ──
        diameters = self._extract_select_values(soup, 'INNER_D')
        outer_diameters = self._extract_select_values(soup, 'OUTER_D')
        widths = self._extract_select_values(soup, 'WIDTH')

        # ── Extract vendor checkboxes ──
        vendors = self._extract_vendors(soup)

        # ── Get total count with all=all to verify ──
        total_count = self._get_total_count({'all': 'all'})

        cat_data = {
            'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            'total_products': total_count,
            'categories': categories,
            'diameters': diameters,
            'outer_diameters': outer_diameters,
            'widths': widths,
            'vendors': vendors,
        }

        # Save categories
        with open(self._categories_file, 'w', encoding='utf-8') as f:
            json.dump(cat_data, f, ensure_ascii=False, indent=2)

        self.progress.status('stage1', (
            f'Categories: {len(categories)} types, '
            f'{sum(len(c.get("subtypes", [])) for c in categories)} subtypes, '
            f'{len(diameters)} bore diameters, {len(vendors)} vendors, '
            f'{total_count} total products'
        ))
        logger.info(f"Stage 1 complete — saved to {self._categories_file}")

    def _extract_categories(self, soup: BeautifulSoup) -> list[dict]:
        """Extract TYPE[N] and SUBTYPE[N][M] checkbox hierarchy."""
        categories = []

        # Find all TYPE checkboxes
        for inp in soup.select('input[name^="TYPE["]'):
            type_name_attr = inp.get('name', '')
            m = re.match(r'TYPE\[(\d+)\]', type_name_attr)
            if not m:
                continue
            type_id = m.group(1)

            # Get label text - next sibling or parent label
            label = ''
            label_el = inp.find_next_sibling(string=True)
            if label_el:
                label = label_el.strip()
            if not label:
                parent = inp.find_parent('label')
                if parent:
                    label = parent.get_text(strip=True)
            if not label:
                # Try next span/div
                next_el = inp.find_next(['span', 'div', 'label'])
                if next_el:
                    label = next_el.get_text(strip=True)

            # Find subtypes for this type
            subtypes = []
            for sub_inp in soup.select(f'input[name^="SUBTYPE[{type_id}]["]'):
                sub_name = sub_inp.get('name', '')
                sm = re.match(rf'SUBTYPE\[{type_id}\]\[(\d+)\]', sub_name)
                if not sm:
                    continue
                sub_id = sm.group(1)

                sub_label = ''
                sub_label_el = sub_inp.find_next_sibling(string=True)
                if sub_label_el:
                    sub_label = sub_label_el.strip()
                if not sub_label:
                    sub_parent = sub_inp.find_parent('label')
                    if sub_parent:
                        sub_label = sub_parent.get_text(strip=True)

                if sub_label:
                    subtypes.append({'id': sub_id, 'name': sub_label})

            if label:
                categories.append({
                    'type_id': type_id,
                    'name': label,
                    'subtypes': subtypes,
                })

        return categories

    def _extract_select_values(self, soup: BeautifulSoup, name: str) -> list[str]:
        """Extract option values from a named select dropdown."""
        select = soup.find('select', {'name': name, 'class': 'size-list'})
        if not select:
            select = soup.find('select', {'name': name})
        if not select:
            return []

        values = []
        for option in select.find_all('option'):
            val = (option.get('value') or option.get_text(strip=True)).strip()
            if val and val not in ('', 'Все', 'все'):
                values.append(val)
        return values

    def _extract_vendors(self, soup: BeautifulSoup) -> list[dict]:
        """Extract VENDOR[] checkboxes."""
        vendors = []
        for inp in soup.select('input[name="VENDOR[]"]'):
            vid = inp.get('value', '')
            label = ''
            label_el = inp.find_next_sibling(string=True)
            if label_el:
                label = label_el.strip()
            if not label:
                parent = inp.find_parent('label')
                if parent:
                    label = parent.get_text(strip=True)
            if vid and label:
                vendors.append({'id': vid, 'name': label})
        return vendors

    def _get_total_count(self, params: dict) -> int:
        """Fetch first page of search results and extract total item count."""
        url = BASE_URL + '/?' + urlencode(params)
        html = self.fetcher.get_text(url)
        if not html:
            return 0
        return self._extract_total_from_html(html)

    def _extract_total_from_html(self, html: str) -> int:
        """Extract total from 'Показано N из TOTAL' or result count element."""
        # Pattern: "Показано 15 из 59991" or "Найдено: 59991"
        m = re.search(r'(?:из|of|Найдено:?)\s*([\d\s]+)', html)
        if m:
            num_str = m.group(1).replace(' ', '').replace('\xa0', '')
            try:
                return int(num_str)
            except ValueError:
                pass
        # Fallback: count element
        soup = BeautifulSoup(html, 'lxml')
        count_el = soup.select_one('.catalog-result__count, .catalog-result .count')
        if count_el:
            m2 = re.search(r'(\d+)', count_el.get_text())
            if m2:
                return int(m2.group(1))
        return 0

    # ══════════════════════════════════════════════════════════
    #  STAGE 2: ITEMS (catalog listing)
    # ══════════════════════════════════════════════════════════

    def _stage2_items(self):
        """Iterate subtypes × bore diameters to collect all items from catalog listings."""
        self.progress.status('stage2', 'Loading category data...')

        # Load categories from stage 1
        if not self._categories_file.exists():
            self.progress.error("No categories.json found — run stage 1 first", fatal=True)
            return

        with open(self._categories_file, 'r', encoding='utf-8') as f:
            cat_data = json.load(f)

        self._category_tree = cat_data.get('categories', [])
        self._diameters = cat_data.get('diameters', [])
        total_products = cat_data.get('total_products', 0)

        # Build subtype_name → TYPE_name mapping for multi-level category resolution
        for cat in self._category_tree:
            for sub in cat.get('subtypes', []):
                self._subtype_to_type[sub['name']] = cat['name']

        # Load existing items for dedup on resume
        if self._items_file.exists() and self.resume:
            self._load_items_for_resume()

        self.progress.status('stage2', (
            f'Scraping items: {len(self._category_tree)} types, '
            f'{len(self._diameters)} diameters, '
            f'{len(self._subtype_to_type)} subtypes mapped, '
            f'~{total_products} products expected'
        ))

        # Strategy: for each subtype, iterate all bore diameters
        # This gives us category_path = [type_name, subtype_name] for each item
        tasks = self._build_scrape_tasks()
        total_tasks = len(tasks)

        for i, task in enumerate(tasks):
            if self.is_stopped:
                break
            if self.max_items and self._stats['total_items'] >= self.max_items:
                break

            self.progress.progress(
                'stage2', i + 1, total_tasks,
                f'{task["label"]} - {self._stats["total_items"]} items, {len(self._seen_articles)} unique'
            )

            self._scrape_task(task)

            # Save intermediate output between tasks (only if items changed)
            if self._items_dirty:
                self._save_items_intermediate()

        # Final save
        if self._items_dirty or self._items:
            self._save_items_intermediate()
        self.progress.status('stage2', f'Stage 2 complete: {self._stats["total_items"]} items')

    def _build_scrape_tasks(self) -> list[dict]:
        """Build list of {params, category_path, label} tasks to scrape.

        Two phases:
        1. Each SUBTYPE with all=all — gets all items per subtype
        2. Each bore diameter with all=all — catches items missed by subtype
        Category path is resolved per-row from column 0 using _subtype_to_type map.
        """
        tasks = []

        # Phase 1: each subtype with all=all (Bitrix requires all=all for results)
        for cat in self._category_tree:
            for sub in cat.get('subtypes', []):
                task_key = f"sub_{cat['type_id']}_{sub['id']}"
                if self.is_scraped(task_key):
                    continue
                tasks.append({
                    'key': task_key,
                    'params': {'all': 'all',
                               f"TYPE[{cat['type_id']}]": 'on',
                               f"SUBTYPE[{cat['type_id']}][{sub['id']}]": 'on'},
                    'category_path': [],  # resolved per-row from column 0
                    'label': f"{cat['name']} > {sub['name']}",
                })

        # Phase 2: all=all with each bore diameter (catch remaining items)
        for d_val in self._diameters:
            task_key = f"all_d_{d_val}"
            if self.is_scraped(task_key):
                continue
            tasks.append({
                'key': task_key,
                'params': {'all': 'all', 'INNER_D': d_val},
                'category_path': [],  # resolved per-row from column 0
                'label': f'd={d_val}mm (all)',
            })

        return tasks

    def _scrape_task(self, task: dict):
        """Scrape all paginated results for a single task (params set).

        Pagination: keep fetching pages until we get 0 rows or hit the
        consecutive-no-new-items cap (guards against infinite loops when
        Bitrix keeps returning duplicate items on higher pages).
        """
        base_params = task['params'].copy()
        category_path = task['category_path']
        page = 1
        consecutive_empty = 0      # pages with 0 rows at all
        consecutive_no_new = 0     # pages with rows but 0 NEW items (all dupes)
        MAX_CONSECUTIVE_NO_NEW = 5 # stop after 5 pages of only duplicates
        last_progress_time = time.time()

        while True:
            if self.is_stopped:
                break
            if self.max_items and self._stats['total_items'] >= self.max_items:
                break

            params = dict(base_params)
            if page > 1:
                params[PAGE_PARAM] = str(page)

            # Bitrix requires literal brackets in param names — don't encode them
            qs = urlencode(params, doseq=True).replace('%5B', '[').replace('%5D', ']')
            url = BASE_URL + '/?' + qs

            if self.is_scraped(url):
                page += 1
                if page > 500:  # safety cap
                    break
                continue

            html = self.fetcher.get_text(url)
            if not html:
                self._stats['errors'] += 1
                break

            self.mark_scraped(url)
            soup = BeautifulSoup(html, 'lxml')
            page_items = self._extract_catalog_rows(soup, url, category_path)

            if not page_items:
                consecutive_empty += 1
                # Stop after 2 consecutive empty pages (guards against transient errors)
                if consecutive_empty >= 2:
                    break
                page += 1
                continue

            consecutive_empty = 0
            new_on_page = 0
            for item in page_items:
                dedup_key = f"{item.article}|{item.brand}"
                if dedup_key in self._seen_articles:
                    continue
                self._seen_articles.add(dedup_key)
                self.add_item(item)
                self._items_dirty = True
                new_on_page += 1

                if self.max_items and self._stats['total_items'] >= self.max_items:
                    break

            if new_on_page > 0:
                consecutive_no_new = 0
            else:
                consecutive_no_new += 1
                if consecutive_no_new >= MAX_CONSECUTIVE_NO_NEW:
                    logger.info(
                        f'{task["label"]}: {MAX_CONSECUTIVE_NO_NEW} consecutive pages '
                        f'with 0 new items at p{page} — moving to next task'
                    )
                    break

            # Per-page progress (visible in logs)
            if page % 5 == 0 or new_on_page > 0:
                self.progress.progress(
                    'stage2_pages', page, 0,
                    f'{task["label"]} p{page} +{new_on_page} new, '
                    f'{self._stats["total_items"]} total, {len(self._seen_articles)} unique'
                )
                last_progress_time = time.time()

            # Intermediate save only when items actually changed
            if self._items_dirty and self._stats['total_items'] % 100 < 30:
                self._save_items_intermediate()

            page += 1

        # Mark task complete
        self.mark_scraped(task['key'])

    def _extract_catalog_rows(self, soup: BeautifulSoup, page_url: str,
                               default_category: list) -> list[ScrapedItem]:
        """Extract product items from the catalog results table."""
        items = []
        rows = soup.select(CATALOG_SELECTORS['table_row'])
        if not rows:
            rows = soup.select('tr[data-id]')

        for row in rows:
            item = self._parse_catalog_row(row, page_url, default_category)
            if item and item.article:
                items.append(item)
        return items

    def _parse_catalog_row(self, row, page_url: str,
                            default_category: list) -> ScrapedItem:
        """Parse a single table row into a ScrapedItem."""
        item = ScrapedItem(currency='RUB')

        data_weight = row.get('data-weight', '')
        if data_weight:
            try:
                item.specs['weight_kg'] = float(data_weight)
            except ValueError:
                pass

        cells = row.find_all('td')
        if len(cells) < 9:
            return None

        # Column 0: product type name (matches SUBTYPE names from form)
        # Resolve into multi-level path: [TYPE, SUBTYPE] using static mapping
        type_text = cells[0].get_text(strip=True)
        if type_text:
            parent_type = self._subtype_to_type.get(type_text)
            if parent_type and parent_type != type_text:
                item.category_path = [parent_type, type_text]
            else:
                item.category_path = [type_text]
        elif default_category:
            item.category_path = list(default_category)

        # Column 1: Marking (article link)
        link = cells[1].find('a')
        if link:
            item.article = link.get_text(strip=True)
            item.name = item.article
            href = link.get('href', '')
            if href:
                item.source_url = urljoin(BASE_URL, href)

        # Column 2: Brand
        brand_el = cells[2].find('a') or cells[2]
        item.brand = brand_el.get_text(strip=True)

        # Column 3: Analogs link
        analog_link = cells[3].find('a')
        if analog_link:
            item.specs['analogs_url'] = urljoin(BASE_URL, analog_link.get('href', ''))

        # Columns 4-6: d, D, B
        d_val = self._parse_float(cells[4].get_text(strip=True))
        D_val = self._parse_float(cells[5].get_text(strip=True))
        B_val = self._parse_float(cells[6].get_text(strip=True))
        if d_val is not None or D_val is not None or B_val is not None:
            item.dimensions = {}
            if d_val is not None: item.dimensions['bore_mm'] = d_val
            if D_val is not None: item.dimensions['outer_mm'] = D_val
            if B_val is not None: item.dimensions['width_mm'] = B_val

        # Column 7: Price
        item.price = self._parse_price(cells[7].get_text(strip=True))

        # Column 8: Stock
        item.qty = self._parse_qty(cells[8].get_text(strip=True))

        # Parse article hints
        if item.article:
            parsed = parse_article(item.article)
            item.model_hint = parsed.get('model_hint', '')
            item.series_hint = parsed.get('series_hint', '')
            item.execution_hint = parsed.get('execution_hint', '')
            item.precision_hint = parsed.get('precision_hint', '') or None

        return item

    def _save_items_intermediate(self):
        """Save collected items to intermediate JSON (for resume and monitoring).

        Uses atomic write (temp file + rename) so a failed write doesn't
        corrupt the existing file.  I/O errors are caught and logged as
        warnings instead of crashing the scraper.
        """
        output = {
            'format_version': self.FORMAT_VERSION,
            'source': self.SOURCE_NAME,
            'parser': self.PARSER_NAME,
            'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            'stage': 'items',
            'stats': {
                'total_items': len(self._items),
                'unique_articles': len(self._seen_articles),
                'brands_found': len(self._stats['brands_found']),
                'errors': self._stats['errors'],
            },
            'items': [item.to_dict() for item in self._items],
        }
        tmp_file = self._items_file.with_suffix('.json.tmp')
        try:
            with open(tmp_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, ensure_ascii=False)
            tmp_file.replace(self._items_file)
            self._items_dirty = False
            logger.info(f"Intermediate save: {len(self._items)} items to {self._items_file}")
        except OSError as e:
            logger.warning(f"Could not save items (will retry next cycle): {e}")
            try:
                tmp_file.unlink(missing_ok=True)
            except OSError:
                pass

    def _load_items_for_resume(self):
        """Load previously scraped items for dedup on resume."""
        try:
            with open(self._items_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for item_dict in data.get('items', []):
                article = item_dict.get('article', '')
                brand = item_dict.get('brand', '')
                dedup_key = f"{article}|{brand}"
                if dedup_key not in self._seen_articles:
                    self._seen_articles.add(dedup_key)
                    item = ScrapedItem(
                        article=article,
                        name=item_dict.get('name', ''),
                        brand=brand,
                        qty=item_dict.get('qty'),
                        price=item_dict.get('price'),
                        currency=item_dict.get('currency', 'RUB'),
                        category_path=item_dict.get('category_path', []),
                        source_url=item_dict.get('source_url', ''),
                        dimensions=item_dict.get('dimensions', {}),
                        specs=item_dict.get('specs', {}),
                        description=item_dict.get('description', ''),
                        image_urls=item_dict.get('image_urls', []),
                        analogs=item_dict.get('analogs', []),
                        series_hint=item_dict.get('series_hint', ''),
                        model_hint=item_dict.get('model_hint', ''),
                        execution_hint=item_dict.get('execution_hint', ''),
                        precision_hint=item_dict.get('precision_hint'),
                    )
                    self._items.append(item)
                    self._stats['total_items'] += 1
                    if brand:
                        self._stats['brands_found'].add(brand)
            logger.info(f"Resumed {len(self._items)} items from {self._items_file}")
        except Exception as e:
            logger.warning(f"Could not load items for resume: {e}")

    # ══════════════════════════════════════════════════════════
    #  STAGE 3: DETAILS (enrichment + images)
    # ══════════════════════════════════════════════════════════

    def _stage3_details(self):
        """Fetch detail pages for all items — enrich with specs, images, analogs.

        When run as standalone stage (--stage details), reads items.json as input
        and writes enriched data to details.json — safe to run in parallel with Stage 2.
        """
        self.progress.status('stage3', 'Loading items for detail enrichment...')

        # Load items from stage 2
        if not self._items_file.exists():
            self.progress.error("No items.json found - run stage 2 first", fatal=True)
            return

        if not self._items:
            self._load_items_for_resume()

        # When running standalone, write to separate file to avoid conflicts with Stage 2
        standalone = self.stage == 'details'
        if standalone:
            self._details_output_file = self.output_dir / 'details.json'

        total = len(self._items)
        self.progress.status('stage3', f'Enriching {total} items with details...')

        enriched = 0
        skipped = 0

        for i, item in enumerate(self._items):
            if self.is_stopped:
                break

            if not item.source_url:
                skipped += 1
                continue

            # Skip if already enriched (has specs beyond weight)
            if len(item.specs) > 2 or item.image_urls:
                skipped += 1
                continue

            detail_key = f"detail:{item.source_url}"
            if self.is_scraped(detail_key):
                skipped += 1
                continue

            self._scrape_detail(item)
            self.mark_scraped(detail_key)
            enriched += 1

            if (enriched % 50) == 0:
                self.progress.progress(
                    'stage3', i + 1, total,
                    f'{enriched} enriched, {skipped} skipped, '
                    f'{self._stats["errors"]} errors'
                )
                # Save periodically
                self._save_details(standalone)

        # Final save
        self._save_details(standalone)
        self.progress.status('stage3', f'Stage 3 complete: {enriched} enriched, {skipped} skipped')

    def _save_details(self, standalone: bool = False):
        """Save enriched items — to details.json when standalone, items.json otherwise."""
        if standalone:
            output = {
                'format_version': self.FORMAT_VERSION,
                'source': self.SOURCE_NAME,
                'parser': self.PARSER_NAME,
                'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'stage': 'details',
                'stats': {
                    'total_items': len(self._items),
                    'unique_articles': len(self._seen_articles),
                    'brands_found': len(self._stats['brands_found']),
                    'errors': self._stats['errors'],
                },
                'items': [item.to_dict() for item in self._items],
            }
            target = self.output_dir / 'details.json'
            tmp_file = target.with_suffix('.json.tmp')
            try:
                with open(tmp_file, 'w', encoding='utf-8') as f:
                    json.dump(output, f, ensure_ascii=False)
                tmp_file.replace(target)
                logger.info(f"Details save: {len(self._items)} items to {target}")
            except OSError as e:
                logger.warning(f"Could not save details (will retry next cycle): {e}")
                try:
                    tmp_file.unlink(missing_ok=True)
                except OSError:
                    pass
        else:
            self._save_items_intermediate()

    def _scrape_detail(self, item: ScrapedItem):
        """Fetch product detail page and enrich the item with full data."""
        if not item.source_url:
            return

        html = self.fetcher.get_text(item.source_url)
        if not html:
            self._stats['errors'] += 1
            return

        soup = BeautifulSoup(html, 'lxml')

        # H1 title
        h1 = soup.select_one(DETAIL_SELECTORS['h1'])
        if h1:
            item.name = h1.get_text(strip=True)

        # Article code
        code_el = soup.select_one(DETAIL_SELECTORS['article_code'])
        if code_el:
            code = code_el.get_text(strip=True)
            if code:
                item.article = code

        # Brand name
        brand_el = soup.select_one(DETAIL_SELECTORS['brand_name'])
        if brand_el:
            brand_text = brand_el.get_text(strip=True).replace('Производитель:', '').strip()
            if brand_text and len(brand_text) < 50:
                item.brand = brand_text

        # Price — target .price-bold (visible), not .price-normal (hidden)
        price_el = soup.select_one('.price-bold') or soup.select_one('.product-card__price-block-head-value span')
        if not price_el:
            price_el = soup.select_one(DETAIL_SELECTORS['price'])
        if price_el:
            p = self._parse_price(price_el.get_text(strip=True))
            if p:
                item.price = p

        # Images from slick gallery
        if not self.skip_images:
            seen_imgs = set(item.image_urls)
            for sel in [DETAIL_SELECTORS['images_big'], DETAIL_SELECTORS['images_small']]:
                for img in soup.select(sel):
                    src = img.get('src') or img.get('data-lazy') or img.get('data-src', '')
                    if src and '/upload/' in src and 'noimage' not in src:
                        full = urljoin(BASE_URL, src)
                        if full not in seen_imgs:
                            seen_imgs.add(full)
                            item.image_urls.append(full)

            # Blueprint/drawing
            for img in soup.select(DETAIL_SELECTORS.get('blueprint', '') or '____'):
                src = img.get('src') or img.get('data-src', '')
                if src:
                    full = urljoin(BASE_URL, src)
                    if full not in seen_imgs:
                        seen_imgs.add(full)
                        item.image_urls.append(full)

        # Main characteristics sidebar
        for li in soup.select(DETAIL_SELECTORS['main_chars']):
            text = li.get_text(strip=True)
            if ':' in text:
                key, val = text.split(':', 1)
                key, val = key.strip(), val.strip()
                if key and val:
                    item.specs[key] = val

        # Detailed spec blocks
        for block in soup.select(DETAIL_SELECTORS['spec_blocks']):
            title_el = block.find(['h3', 'div', 'span'])
            block_title = ' '.join(title_el.get_text(strip=True).split()) if title_el else ''

            for li in block.find_all('li'):
                children = li.find_all(['div', 'span'], recursive=False)
                if len(children) >= 2:
                    key = children[0].get_text(strip=True)
                    val = children[1].get_text(strip=True)
                elif ':' in li.get_text():
                    parts = li.get_text(strip=True).split(':', 1)
                    key, val = parts[0].strip(), parts[1].strip()
                else:
                    continue

                if key and val and len(key) < 100:
                    spec_key = key
                    if block_title and block_title not in key:
                        spec_key = f"{block_title} - {key}"
                    item.specs[spec_key] = val

        # Update dimensions from specs
        self._extract_dimensions_from_specs(item)

        # Analogs — stored as structured references {article, brand, source_url}
        analog_table = soup.select_one(DETAIL_SELECTORS['analogs_table'])
        if analog_table:
            for tr in analog_table.select('tr.basket__table-item, tr[data-id]'):
                analog_cells = tr.find_all('td')
                if len(analog_cells) >= 3:
                    marking_el = analog_cells[1].find('a') if len(analog_cells) > 1 else None
                    brand_el = analog_cells[2] if len(analog_cells) > 2 else None
                    if marking_el:
                        analog_article = marking_el.get_text(strip=True)
                        analog_brand = brand_el.get_text(strip=True) if brand_el else ''
                        analog_url = marking_el.get('href', '')
                        if analog_url and not analog_url.startswith('http'):
                            analog_url = urljoin(BASE_URL, analog_url)
                        if analog_article:
                            item.analogs.append({
                                'article': analog_article,
                                'brand': analog_brand,
                                'source_url': analog_url,
                            })

        # Build description
        self._build_description(item)

    def _extract_dimensions_from_specs(self, item: ScrapedItem):
        """Update item.dimensions from parsed specs."""
        dim_map = {
            'Внутренний диаметр': 'bore_mm',
            'Наружный диаметр': 'outer_mm',
            'Ширина': 'width_mm',
        }
        for spec_key, dim_key in dim_map.items():
            for k, v in item.specs.items():
                if spec_key in k:
                    val = self._parse_float(v.replace('мм', '').strip())
                    if val is not None:
                        if not item.dimensions:
                            item.dimensions = {}
                        item.dimensions[dim_key] = val
                    break

        for k, v in item.specs.items():
            if 'Вес' in k or 'вес' in k:
                val = self._parse_float(v.replace('кг', '').strip())
                if val is not None:
                    item.specs['weight_kg'] = val
                break

    def _build_description(self, item: ScrapedItem):
        """Build a structured description from specs."""
        parts = []
        if item.category_path:
            parts.append(item.category_path[0])
        for key in ['Тип открытый / закрытый', 'Радиальный зазор', 'Посадка на вал']:
            for k, v in item.specs.items():
                if key in k:
                    parts.append(f"{key}: {v}")
                    break
        if parts:
            item.description = '. '.join(parts)

    # ══════════════════════════════════════════════════════════
    #  OVERRIDES
    # ══════════════════════════════════════════════════════════

    def save_state(self):
        """Extended state with stage info + category map for resume.
        Also saves items.json to keep counts in sync across files."""
        state = {
            'scraped_urls': list(self._scraped_urls),
            'stats': {
                'total_items': self._stats['total_items'],
                'unique_articles': len(self._seen_articles),
                'brands_found': list(self._stats['brands_found']),
                'categories_found': self._stats['categories_found'],
                'errors': self._stats['errors'],
                'pages_fetched': self._stats['pages_fetched'],
            },
            'stage': self.stage,
            'saved_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        }
        try:
            with open(self._state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Could not save state: {e}")

        # Keep items.json in sync with state file
        if self._items and getattr(self, '_items_dirty', False):
            self._save_items_intermediate()
            self._items_dirty = False

    def load_state(self):
        """Extended load."""
        super().load_state()

    # ══════════════════════════════════════════════════════════
    #  HELPERS
    # ══════════════════════════════════════════════════════════

    @staticmethod
    def _parse_price(text: str) -> float:
        if not text:
            return 0.0
        clean = text.replace('\xa0', '').replace(' ', '').replace('₽', '')
        clean = clean.replace('руб.', '').replace('руб', '').replace('Цена', '').strip()
        clean = clean.replace(',', '.')
        m = re.search(r'(\d+\.?\d*)', clean)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                pass
        return 0.0

    @staticmethod
    def _parse_float(text: str) -> float:
        if not text:
            return None
        clean = text.replace('\xa0', '').replace(' ', '').replace(',', '.').strip()
        m = re.search(r'(\d+\.?\d*)', clean)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                pass
        return None

    @staticmethod
    def _parse_qty(text: str) -> int:
        if not text:
            return None
        text_lower = text.lower().strip()
        if 'нет' in text_lower or 'под заказ' in text_lower:
            return 0
        m = re.search(r'(\d+)', text.replace(' ', '').replace('\xa0', ''))
        if m:
            return int(m.group(1))
        if 'есть' in text_lower or 'в наличии' in text_lower:
            return 1
        return None


# ══════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='shop.podshipnik.ru 3-stage catalog scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Stages:
  categories  Stage 1: scrape category tree, dimensions, vendors
  items       Stage 2: scrape all product listings (catalog pages)
  details     Stage 3: fetch detail pages for specs + images
  all         Run all stages sequentially (default)

Examples:
  python scraper.py --stage categories                    # Just scrape categories
  python scraper.py --stage items --max-items 100         # Test with 100 items
  python scraper.py --stage details --skip-images         # Enrich without images
  python scraper.py --stage all --delay 1.0               # Full run
  python scraper.py --stage items --no-resume             # Fresh start for items
        """)
    parser.add_argument('--stage', default='all',
                        choices=['categories', 'items', 'details', 'all'],
                        help='Which stage to run (default: all)')
    parser.add_argument('--output-dir', default='./output', help='Output directory')
    parser.add_argument('--no-resume', action='store_true', help='Start fresh (clear state)')
    parser.add_argument('--max-items', type=int, default=0, help='Limit total items (0=all)')
    parser.add_argument('--skip-images', action='store_true', help='Skip image URL collection')
    parser.add_argument('--delay', type=float, default=REQUEST_DELAY,
                        help=f'Delay between requests in seconds (default: {REQUEST_DELAY})')
    args = parser.parse_args()

    # Force UTF-8 output (Windows defaults to cp1251)
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    scraper = PodshipnikScraper(
        output_dir=args.output_dir,
        resume=not args.no_resume,
        stage=args.stage,
        max_items=args.max_items,
        skip_images=args.skip_images,
    )
    if args.delay != REQUEST_DELAY:
        scraper.fetcher.delay = args.delay

    try:
        filename = scraper.run()
        print(f"\nDone! Output saved to: {args.output_dir}/{filename}")
    except Exception as e:
        logger.exception(f"Scraper failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
