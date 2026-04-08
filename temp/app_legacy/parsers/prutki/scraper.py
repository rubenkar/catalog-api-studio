#!/usr/bin/env python3
"""
Scraper for prutki.ru — retail metal/plastic blanks and stock materials.

Homepage structure:
  <h2> = Material section (e.g., "Алюминиевые сплавы")
  <table> with <strong> = Shape + Grade group (e.g., "Алюминиевые прутки Д16Т")
  Links in table = Dimension pages (6, 8, 10... → product page URLs)

Product page order table:
  Заготовка D×L | m (кг) | L (мм) | Склад | Цена (руб)

Three-stage pipeline:
  analyze:    Parse homepage structure + fetch 1 sample page → _analyze.json
  categories: Parse homepage → build product page list → _stage1_pages.json
  items:      Visit each product page, extract item rows
"""
import argparse
import json
import logging
import re
import sys
from pathlib import Path

from bs4 import BeautifulSoup, Tag

# Add parent dir to path so we can import shared lib
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.base_scraper import BaseScraper
from lib.models import ScrapedItem, ScrapedCategory
from prutki.config import BASE_URL, CATALOG_URL, REQUEST_DELAY

logger = logging.getLogger(__name__)

# Default column mapping for product page order table
DEFAULT_COLUMN_MAP = {
    "0": "name",
    "1": "weight_kg",
    "2": "length_mm",
    "3": "stock",
    "4": "price",
}


class PrutkiScraper(BaseScraper):
    PARSER_NAME = "prutki"
    SOURCE_NAME = "prutki.ru"
    FORMAT_VERSION = "1.0"

    def __init__(self, output_dir: str = "./output", delay: float = REQUEST_DELAY,
                 stage: str = "all", no_resume: bool = False, max_items: int = 0,
                 max_pages: int = 0):
        super().__init__(output_dir=output_dir, delay=delay, resume=not no_resume)
        self.stage = stage
        self.max_items = max_items
        self.max_pages = max_pages
        self.product_pages = []   # [(url, category_path, group_label)]
        self.materials = []       # structured tree for analyze output
        self.column_map = dict(DEFAULT_COLUMN_MAP)
        self._load_field_map()

    def _load_field_map(self):
        """Load user-customized column mapping if present."""
        fmap_path = Path(self.output_dir) / "_field_map.json"
        if fmap_path.exists():
            try:
                with open(fmap_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if "columns" in data:
                    self.column_map = data["columns"]
                    logger.info(f"Loaded field map: {self.column_map}")
            except Exception as e:
                logger.warning(f"Could not load field map: {e}")

    def run(self) -> str:
        """Override to skip save_output for analyze stage."""
        if self.stage == "analyze":
            self.progress.status('init', f'Starting {self.SOURCE_NAME} analyze')
            self.stage_analyze()
            self.progress.complete(total_items=0, output_file="_analyze.json")
            return "_analyze.json"
        return super().run()

    def scrape(self):
        if self.stage in ("all", "categories"):
            self.stage_categories()
            if self.stage == "all":
                self.stage_items()
        elif self.stage == "items":
            self.stage_items()

    # ── Homepage Parser ───────────────────────────────────────────

    def _parse_homepage(self, soup: BeautifulSoup):
        """Parse homepage using real DOM structure: h2 → table → strong → links."""
        # Find the main content area
        content = soup.find("div", id="content") or soup.find("div", class_="content") or soup.body
        if not content:
            self.progress.error("Could not find content area")
            return

        current_material = None
        self.materials = []
        self.product_pages = []

        # Find all h2 and top-level tables in document order
        # Use find_all to get all h2 and tables, then process in order
        for elem in content.find_all(["h2", "table"]):
            if elem.name == "h2":
                text = elem.get_text(strip=True)
                if text and len(text) < 80:
                    current_material = {
                        "name": text,
                        "groups": [],
                    }
                    self.materials.append(current_material)

            elif elem.name == "table" and current_material is not None:
                # Skip tables nested inside other tables (specs, chemical, etc.)
                if elem.find_parent("table"):
                    continue
                group = self._parse_group_table(elem, current_material["name"])
                if group:
                    current_material["groups"].append(group)

    def _parse_group_table(self, table: Tag, material_name: str) -> dict:
        """Parse a group table: first row has <strong> label, second row has dimension links."""
        rows = table.find_all("tr")
        if not rows:
            return None

        # Find label from <strong> in first row
        strong = rows[0].find("strong")
        if not strong:
            return None
        label = strong.get_text(strip=True)
        if not label or len(label) > 120:
            return None

        # Collect dimension page links from td.third (options container) or remaining rows
        pages = []
        dim_type = None
        third_td = table.find("td", class_="third")
        link_source = third_td.find_all("a", href=True) if third_td else []
        if not link_source:
            for row in rows[1:]:
                link_source.extend(row.find_all("a", href=True))
        for a in link_source:
            href = a["href"]
            if not href.startswith("http"):
                href = BASE_URL + href

            # Detect dimension type from URL
            dt = self._detect_dimension_type(href)
            if dt:
                if dim_type is None:
                    dim_type = dt
                dim_value = a.get_text(strip=True)
                cat_path = [material_name, label]
                pages.append({
                    "url": href,
                    "dimension_value": dim_value,
                    "dimension_type": dt,
                })
                self.product_pages.append((href, cat_path, label))

        if not pages:
            return None

        return {
            "label": label,
            "dimension_type": dim_type or "unknown",
            "page_count": len(pages),
            "pages": pages,
            "sample_url": pages[0]["url"] if pages else None,
        }

    def _detect_dimension_type(self, url: str) -> str:
        """Detect dimension type from URL pattern."""
        for dt in ("diametr", "shirina", "tolschina", "storona", "razmer"):
            if f"-{dt}-" in url:
                return dt
        return None

    # ── Stage: Analyze ────────────────────────────────────────────

    def stage_analyze(self):
        """Quick analysis: parse homepage structure + fetch 1 sample page."""
        self.progress.status("analyze", "Fetching homepage...")
        html = self.fetcher.get_text(CATALOG_URL)
        if not html:
            self.progress.error("Failed to fetch homepage", fatal=True)
            return

        soup = BeautifulSoup(html, "html.parser")
        self._parse_homepage(soup)

        total_pages = sum(len(g["pages"]) for m in self.materials for g in m["groups"])
        self.progress.status("analyze",
                             f"Found {len(self.materials)} materials, "
                             f"{sum(len(m['groups']) for m in self.materials)} groups, "
                             f"{total_pages} pages")

        # Fetch one sample product page to detect column structure
        sample_page = None
        sample_url = None
        for m in self.materials:
            for g in m["groups"]:
                if g.get("sample_url"):
                    sample_url = g["sample_url"]
                    break
            if sample_url:
                break

        if sample_url:
            self.progress.status("analyze", "Fetching sample page...")
            sample_page = self._analyze_sample_page(sample_url)

        # Strip detailed page lists from materials (keep lightweight)
        materials_summary = []
        for m in self.materials:
            groups = []
            for g in m["groups"]:
                groups.append({
                    "label": g["label"],
                    "dimension_type": g["dimension_type"],
                    "page_count": g["page_count"],
                    "sample_url": g.get("sample_url"),
                })
            materials_summary.append({
                "name": m["name"],
                "groups": groups,
            })

        result = {
            "materials": materials_summary,
            "sample_page": sample_page,
            "totals": {
                "materials": len(self.materials),
                "groups": sum(len(m["groups"]) for m in self.materials),
                "pages": total_pages,
            },
        }

        self.save_json("_analyze.json", result)
        self.progress.status("analyze", "Analysis complete")

    def _analyze_sample_page(self, url: str) -> dict:
        """Fetch a sample product page and extract column structure."""
        html = self.fetcher.get_text(url)
        if not html:
            return None

        soup = BeautifulSoup(html, "html.parser")
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else ""

        # Find the order table
        order_table = None
        for table in soup.find_all("table"):
            header_text = table.get_text()
            if "Склад" in header_text and "Цена" in header_text:
                order_table = table
                break

        if not order_table:
            return {"url": url, "title": title, "columns": [], "column_headers": [], "sample_rows": []}

        rows = order_table.find_all("tr")
        if not rows:
            return {"url": url, "title": title, "columns": [], "column_headers": [], "sample_rows": []}

        # Extract headers from first row
        header_row = rows[0]
        headers = []
        for th in header_row.find_all(["th", "td"]):
            headers.append(th.get_text(strip=True))

        # Auto-detect column mapping from headers
        columns = []
        for i, h in enumerate(headers):
            h_lower = h.lower()
            if "заготовка" in h_lower or "d" in h_lower and "l" in h_lower:
                columns.append("name")
            elif h_lower.startswith("m") and "кг" in h_lower:
                columns.append("weight_kg")
            elif h_lower.startswith("l") and "мм" in h_lower:
                columns.append("length_mm")
            elif "склад" in h_lower:
                columns.append("stock")
            elif "цена" in h_lower:
                columns.append("price")
            elif "количество" in h_lower or "кол" in h_lower:
                columns.append("skip")
            else:
                columns.append(f"col_{i}")

        # Extract sample data rows (first 5)
        sample_rows = []
        for row in rows[1:6]:
            cells = row.find_all("td")
            sample_rows.append([c.get_text(strip=True) for c in cells])

        return {
            "url": url,
            "title": title,
            "columns": columns,
            "column_headers": headers,
            "sample_rows": sample_rows,
        }

    # ── Stage: Categories ─────────────────────────────────────────

    def stage_categories(self):
        self.progress.status("categories", "Fetching homepage catalog...")
        html = self.fetcher.get_text(CATALOG_URL)
        if not html:
            self.progress.error("Failed to fetch homepage", fatal=True)
            return

        soup = BeautifulSoup(html, "html.parser")
        self._parse_homepage(soup)

        # Register categories with the base class for combined output
        for m in self.materials:
            cat = ScrapedCategory(name=m["name"], children=[])
            for g in m["groups"]:
                cat.children.append(ScrapedCategory(name=g["label"]))
            self.add_category(cat)

        # Save product page URLs as intermediate state for stage 2 resume
        state_data = {
            "product_pages": [
                {"url": url, "category_path": path, "group_label": group_label}
                for url, path, group_label in self.product_pages
            ],
        }
        self.save_json("_stage1_pages.json", state_data)

        total_pages = len(self.product_pages)
        self.progress.status("categories",
                             f"Found {len(self.materials)} materials, {total_pages} product pages")

    # ── Stage: Items ──────────────────────────────────────────────

    def stage_items(self):
        # Load product pages from stage 1 if not already populated
        if not self.product_pages:
            state_file = Path(self.output_dir) / "_stage1_pages.json"
            if state_file.exists():
                data = self.load_json("_stage1_pages.json")
                self.product_pages = [
                    (p["url"], p["category_path"], p.get("group_label", p.get("section", "")))
                    for p in data.get("product_pages", [])
                ]
            else:
                self.progress.error("No stage 1 data found. Run categories stage first.", fatal=True)
                return

        total = len(self.product_pages)
        effective_total = min(total, self.max_pages) if self.max_pages else total
        self.progress.status("items", f"Scraping {effective_total} of {total} product pages...")

        for i, (url, cat_path, group_label) in enumerate(self.product_pages):
            if self.is_stopped:
                break
            if self.max_pages and i >= self.max_pages:
                break
            if self.max_items and len(self._items) >= self.max_items:
                break
            if self.is_scraped(url):
                continue

            items_before = len(self._items)
            try:
                self._scrape_product_page(url, cat_path, group_label)
            except Exception as e:
                self.progress.error(f"Error on {url}: {e}")
                logger.exception(f"Error scraping {url}")

            items_added = len(self._items) - items_before
            short_url = url.replace(BASE_URL, '')
            self.progress.progress("items", i + 1, effective_total,
                                   f"{i+1}/{effective_total} +{items_added} items {short_url}")

            self.mark_scraped(url)

    def _extract_price_per_unit(self, soup: BeautifulSoup) -> dict:
        """Extract price-per-mm from first/last rows of the borders-dashed table.

        Returns dict with price_per_mm_small (first row) and price_per_mm_bulk (last row).
        First row = small quantity (expensive per unit), last row = bulk (cheaper per unit).
        Uses col[0] for length and col[4] for price.
        """
        table = soup.find("table", class_="borders-dashed")
        if not table:
            return {}

        tbody = table.find("tbody")
        rows = (tbody or table).find_all("tr")
        if not rows:
            return {}

        result = {}
        for label, row in [("small", rows[0]), ("bulk", rows[-1])]:
            cells = row.find_all("td")
            if len(cells) < 5:
                continue
            try:
                length_text = cells[2].get_text(strip=True)
                price_text = cells[4].get_text(strip=True)
                length = float(length_text.replace(",", ".").replace(" ", ""))
                price = float(re.search(r"[\d.,]+", price_text).group().replace(",", ".").replace(" ", ""))
                if length > 0:
                    result[f"price_per_mm_{label}"] = round(price / length, 4)
                    result[f"length_mm_{label}"] = length
                    result[f"price_{label}"] = price
            except (ValueError, AttributeError):
                continue

        return result

    def _scrape_product_page(self, url: str, cat_path: list, group_label: str):
        """Scrape a single product page with item table."""
        html = self.fetcher.get_text(url)
        if not html:
            return

        soup = BeautifulSoup(html, "html.parser")

        # Extract page title
        h1 = soup.find("h1")
        page_title = h1.get_text(strip=True) if h1 else ""

        # Extract price-per-unit from first/last rows of borders-dashed table
        price_per_unit = self._extract_price_per_unit(soup)

        # Extract description
        description = ""
        desc_h3 = soup.find("h3", string=re.compile("Описание", re.I))
        if desc_h3:
            desc_parts = []
            for sib in desc_h3.find_next_siblings():
                if sib.name in ("h3", "h4"):
                    break
                if sib.name == "p":
                    desc_parts.append(sib.get_text(strip=True))
            description = " ".join(desc_parts)

        # Extract specs table
        specs = {}
        specs_h4 = soup.find("h4", string=re.compile("характеристик", re.I))
        if specs_h4:
            specs_table = specs_h4.find_next("table")
            if specs_table:
                for row in specs_table.find_all("tr")[1:]:
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True)
                        val = cells[1].get_text(strip=True)
                        if key and val:
                            specs[key] = val

        # Extract product image
        image_urls = []
        img = soup.find("img", src=re.compile(r"\.(jpg|jpeg|png|gif)", re.I))
        if img and img.get("src"):
            img_url = img["src"]
            if not img_url.startswith("http"):
                img_url = BASE_URL + img_url
            if "logo" not in img_url.lower() and "icon" not in img_url.lower():
                image_urls.append(img_url)

        # Find the order table (borders-dashed with Склад + Цена columns)
        order_table = soup.find("table", class_="borders-dashed")
        if not order_table:
            # Fallback: find by content
            for table in soup.find_all("table"):
                header_text = table.get_text()
                if "Склад" in header_text and "Цена" in header_text:
                    order_table = table
                    break

        if not order_table:
            logger.warning(f"No order table found on {url}")
            return

        rows = order_table.find_all("tr")
        if len(rows) < 2:
            return

        for row in rows[1:]:  # skip header
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            try:
                item = self._parse_item_row(cells, url, cat_path, group_label,
                                            page_title, description, specs, image_urls,
                                            price_per_unit)
                if item:
                    self.add_item(item)
            except Exception as e:
                logger.warning(f"Error parsing row on {url}: {e}")

    def _parse_item_row(self, cells, url, cat_path, group_label,
                        page_title, description, specs, image_urls,
                        price_per_unit=None) -> ScrapedItem:
        """Parse a single table row using column mapping."""
        cmap = self.column_map
        article = ""
        name_text = ""
        weight = None
        length_mm = None
        qty = None
        price = None

        # Support both dict {"0": "name"} and list ["name", "weight_kg", ...]
        if isinstance(cmap, list):
            col_iter = enumerate(cmap)
        else:
            col_iter = ((int(k), v) for k, v in cmap.items())

        for col_idx, field in col_iter:
            if col_idx >= len(cells):
                continue
            text = cells[col_idx].get_text(strip=True)

            if field == "name":
                name_text = text
                # Extract article code (Код: 101097)
                code_match = re.search(r"Код:\s*(\d+)", text)
                if code_match:
                    article = code_match.group(1)
                    name_text = re.sub(r"\s*Код:\s*\d+", "", text).strip()

            elif field == "weight_kg":
                try:
                    weight = float(text.replace(",", "."))
                except ValueError:
                    pass

            elif field == "length_mm":
                try:
                    length_mm = int(float(text.replace(",", ".")))
                except ValueError:
                    pass

            elif field == "stock":
                stock_match = re.search(r"(\d+)", text)
                if stock_match:
                    qty = int(stock_match.group(1))
                if "нет" in text.lower() or "под заказ" in text.lower():
                    qty = 0

            elif field == "price":
                price_match = re.search(r"[\d.,]+", text)
                if price_match:
                    try:
                        price = float(price_match.group().replace(",", ".").replace(" ", ""))
                    except ValueError:
                        pass

        if not name_text:
            return None

        # Parse dimensions from name (e.g., "Д16Т 25 х 50 мм")
        dimensions = {}
        dim_match = re.search(r"(\d+(?:[.,]\d+)?)\s*[хxХX×]\s*(\d+(?:[.,]\d+)?)", name_text)
        if dim_match:
            d = float(dim_match.group(1).replace(",", "."))
            l = float(dim_match.group(2).replace(",", "."))
            dimensions["diameter_mm"] = d
            dimensions["length_mm"] = l

        if weight:
            dimensions["weight_kg"] = weight
        if length_mm:
            dimensions["length_mm"] = length_mm

        # Add price-per-unit data (small qty vs bulk) from page-level extraction
        if price_per_unit:
            dimensions.update(price_per_unit)

        return ScrapedItem(
            article=article,
            name=name_text,
            brand="prutki.ru",
            qty=qty,
            price=price,
            currency="RUB",
            category_path=list(cat_path),
            dimensions=dimensions,
            description=description,
            specs=specs,
            source_url=url,
            image_urls=image_urls,
        )

    # ── Helpers ───────────────────────────────────────────────────

    def save_json(self, filename: str, data):
        path = Path(self.output_dir) / filename
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def load_json(self, filename: str) -> dict:
        path = Path(self.output_dir) / filename
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="prutki.ru scraper")
    parser.add_argument("--output-dir", default="./output", help="Output directory")
    parser.add_argument("--delay", type=float, default=REQUEST_DELAY, help="Delay between requests (seconds)")
    parser.add_argument("--stage", default="all",
                        choices=["all", "analyze", "categories", "items"],
                        help="Which stage to run")
    parser.add_argument("--no-resume", action="store_true", help="Start fresh, ignore saved state")
    parser.add_argument("--max-items", type=int, default=0, help="Max items to scrape (0=unlimited)")
    parser.add_argument("--max-pages", type=int, default=0, help="Max pages to scrape (0=unlimited)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(Path(args.output_dir) / "scraper.log", encoding="utf-8"),
        ]
    )

    scraper = PrutkiScraper(
        output_dir=args.output_dir,
        delay=args.delay,
        stage=args.stage,
        no_resume=args.no_resume,
        max_items=args.max_items,
        max_pages=args.max_pages,
    )
    scraper.run()


if __name__ == "__main__":
    main()
