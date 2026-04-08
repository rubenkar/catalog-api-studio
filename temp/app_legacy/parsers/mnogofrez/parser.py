"""
MnogoFrez.ru full catalog scraper.
Recursively crawls the category tree, scrapes every product detail page,
downloads images, and outputs a well-structured JSON.
"""

import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://mnogofrez.ru"
CATALOG_URL = f"{BASE_URL}/catalog/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.5",
}

REQUEST_DELAY = 0.8  # seconds between requests

ROOT_DIR = Path(__file__).parent
LOG_DIR = ROOT_DIR / "logs"
DATA_DIR = ROOT_DIR / "data"
IMAGES_DIR = ROOT_DIR / "images"
STATE_FILE = DATA_DIR / "scrape_state.json"

logger = logging.getLogger("mnogofrez_parser")


def setup_logging():
    """Configure file + console logging."""
    LOG_DIR.mkdir(exist_ok=True)
    if logger.handlers:
        return
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    fh = logging.FileHandler(LOG_DIR / "parser.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Product:
    """A single product (item) scraped from a detail page."""
    url: str = ""
    name: str = ""
    article: str = ""
    price: str = ""
    price_value: float = 0.0
    in_stock: bool = True
    stock_qty: str = ""
    image_urls: list = field(default_factory=list)   # all images on detail page
    image_local: list = field(default_factory=list)   # local file paths after download
    manufacturer: str = ""
    series: str = ""
    specs: dict = field(default_factory=dict)
    description: str = ""
    breadcrumbs: list = field(default_factory=list)


@dataclass
class CategoryNode:
    """A node in the catalog tree. Leaf nodes contain products."""
    url: str = ""
    name: str = ""
    image_url: str = ""
    image_local: str = ""
    children: list = field(default_factory=list)      # list[CategoryNode]
    products: list = field(default_factory=list)       # list[Product]  (only on leaves)
    is_leaf: bool = False
    depth: int = 0


# ---------------------------------------------------------------------------
# HTTP fetcher with rate limiting and retries
# ---------------------------------------------------------------------------

class Fetcher:
    def __init__(self, delay: float = REQUEST_DELAY):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.delay = delay
        self._last_request = 0.0
        self.request_count = 0

    def _throttle(self):
        elapsed = time.time() - self._last_request
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)

    def get(self, url: str, retries: int = 2) -> requests.Response | None:
        self._throttle()
        for attempt in range(retries + 1):
            logger.debug(f"GET {url} (attempt {attempt+1})")
            try:
                resp = self.session.get(url, timeout=30)
                self._last_request = time.time()
                self.request_count += 1
                if resp.status_code == 404:
                    logger.warning(f"404 Not Found: {url}")
                    return None
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                logger.error(f"Request failed (attempt {attempt+1}): {url} — {e}")
                if attempt < retries:
                    time.sleep(2 * (attempt + 1))
        return None

    def get_soup(self, url: str) -> BeautifulSoup | None:
        resp = self.get(url)
        if resp is None:
            return None
        return BeautifulSoup(resp.text, "lxml")

    def download_file(self, url: str, dest: Path) -> bool:
        """Download a file (image) to dest. Returns True on success."""
        if dest.exists():
            logger.debug(f"Image already exists: {dest}")
            return True
        self._throttle()
        try:
            resp = self.session.get(url, timeout=30, stream=True)
            self._last_request = time.time()
            self.request_count += 1
            resp.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(8192):
                    f.write(chunk)
            logger.debug(f"Downloaded image: {url} -> {dest}")
            return True
        except Exception as e:
            logger.error(f"Image download failed: {url} — {e}")
            return False


# ---------------------------------------------------------------------------
# Catalog scraper
# ---------------------------------------------------------------------------

class CatalogScraper:
    """
    Recursively scrapes the entire mnogofrez.ru catalog.

    Algorithm:
      1. Fetch a category page.
      2. Look for subcategory cards/links → if found, recurse into each.
      3. If no subcategories, look for a product table → extract product rows.
      4. For each product row that has a detail link, fetch the detail page
         for full specs, description, and images.
      5. Download all product images into images/ folder.
      6. Output the full tree as structured JSON.
    """

    def __init__(self, download_images: bool = True, resume: bool = True):
        self.fetcher = Fetcher()
        self.download_images = download_images
        self.resume = resume
        self._visited_urls: set[str] = set()
        self._completed_urls: set[str] = set()  # URLs fully scraped (with children/products)
        self._completed_products: set[str] = set()  # Product URLs already scraped
        self._failed_urls: set[str] = set()     # URLs that failed (network errors)
        self._stop_requested = False
        self._stats = {"categories": 0, "products": 0, "images": 0, "errors": 0}
        self._prev_product_index: dict[str, dict] = {}  # URL -> product dict from previous catalog

        # UI callbacks
        self.on_progress: callable = None       # (msg, current, total)
        self.on_page_fetched: callable = None   # (url, html)
        self.on_product_parsed: callable = None # (product, category_path)
        self.on_category_found: callable = None # (category_node)

    def stop(self):
        self._stop_requested = True

    @property
    def stats(self):
        return dict(self._stats)

    # ------------------------------------------------------------------ #
    # State persistence (resume support)
    # ------------------------------------------------------------------ #

    def _load_state(self):
        """Load completed URLs from previous run."""
        if not self.resume or not STATE_FILE.exists():
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
            self._completed_urls = set(state.get("completed_urls", []))
            self._completed_products = set(state.get("completed_products", []))
            logger.info(f"Loaded state: {len(self._completed_urls)} completed categories, "
                        f"{len(self._completed_products)} completed products")
        except Exception as e:
            logger.warning(f"Could not load state: {e}")

    def _save_state(self):
        """Save completed URLs for resume."""
        DATA_DIR.mkdir(exist_ok=True)
        state = {
            "completed_urls": list(self._completed_urls),
            "completed_products": list(self._completed_products),
            "stats": self._stats,
            "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        try:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Could not save state: {e}")

    def _mark_completed(self, url: str):
        """Mark a URL as fully scraped and save state."""
        self._completed_urls.add(url)
        # Save state every 5 completions
        if len(self._completed_urls) % 5 == 0:
            self._save_state()

    def _load_previous_catalog(self) -> dict | None:
        """Load the previous catalog.json for merging and build product index."""
        catalog_file = DATA_DIR / "catalog.json"
        if not catalog_file.exists():
            return None
        try:
            with open(catalog_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            cat = data.get("catalog")
            if cat:
                # Build product index (URL -> product dict) for skipping already-scraped products
                self._prev_product_index = {}
                self._index_prev_products(cat, self._prev_product_index)
                logger.info(f"Loaded previous catalog.json for merge "
                            f"({len(self._prev_product_index)} products indexed)")
            return cat
        except Exception as e:
            logger.warning(f"Could not load previous catalog: {e}")
            return None

    def _index_prev_products(self, node: dict, index: dict):
        """Recursively index all products from previous catalog by URL."""
        for prod in node.get("products", []):
            url = prod.get("url", "")
            if url:
                index[url] = prod
        for child in node.get("children", []):
            self._index_prev_products(child, index)

    def _merge_previous(self, current: CategoryNode, prev_dict: dict):
        """
        Merge data from the previous catalog into the current tree.
        For nodes that were skipped (already completed), copy children/products
        from the previous tree.
        """
        # Build URL-to-dict index of previous tree
        prev_index = {}
        self._index_prev_tree(prev_dict, prev_index)

        # Walk the current tree and fill in empty nodes from previous data
        self._fill_from_previous(current, prev_index)

    def _index_prev_tree(self, node: dict, index: dict):
        """Recursively index previous tree nodes by URL."""
        url = node.get("url", "")
        if url:
            index[url] = node
        for child in node.get("children", []):
            self._index_prev_tree(child, index)

    def _fill_from_previous(self, node: CategoryNode, prev_index: dict):
        """Fill empty nodes from previous data."""
        # If this node has no children and no products, try to restore from previous
        if not node.children and not node.products and node.url in prev_index:
            prev = prev_index[node.url]
            # Restore children
            for prev_child in prev.get("children", []):
                child = self._dict_to_node(prev_child, node.depth + 1)
                node.children.append(child)
            # Restore products
            for prev_prod in prev.get("products", []):
                prod = self._dict_to_product(prev_prod)
                node.products.append(prod)
            node.is_leaf = prev.get("is_leaf", False)
            if prev.get("image_url"):
                node.image_url = prev["image_url"]
            if prev.get("image_local"):
                node.image_local = prev["image_local"]
            if not node.name or node.name == "(single URL)":
                node.name = prev.get("name", node.name)

        # Recurse into children
        for child in node.children:
            self._fill_from_previous(child, prev_index)

    def _dict_to_node(self, d: dict, depth: int = 0) -> CategoryNode:
        """Convert a dict from previous catalog back to CategoryNode."""
        node = CategoryNode(
            url=d.get("url", ""),
            name=d.get("name", ""),
            image_url=d.get("image_url", ""),
            image_local=d.get("image_local", ""),
            is_leaf=d.get("is_leaf", False),
            depth=depth,
        )
        for child_dict in d.get("children", []):
            node.children.append(self._dict_to_node(child_dict, depth + 1))
        for prod_dict in d.get("products", []):
            node.products.append(self._dict_to_product(prod_dict))
        return node

    def _dict_to_product(self, d: dict) -> Product:
        """Convert a dict from previous catalog back to Product."""
        return Product(
            url=d.get("url", ""),
            name=d.get("name", ""),
            article=d.get("article", ""),
            price=d.get("price", ""),
            price_value=d.get("price_value", 0.0),
            in_stock=d.get("in_stock", True),
            stock_qty=d.get("stock_qty", ""),
            image_urls=d.get("image_urls", []),
            image_local=d.get("image_local", []),
            manufacturer=d.get("manufacturer", ""),
            series=d.get("series", ""),
            specs=d.get("specs", {}),
            description=d.get("description", ""),
            breadcrumbs=d.get("breadcrumbs", []),
        )

    def _emit(self, msg: str, cur: int = 0, tot: int = 0):
        logger.info(msg)
        if self.on_progress:
            self.on_progress(msg, cur, tot)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def scrape_full_catalog(self) -> CategoryNode:
        """Scrape the entire catalog tree starting from /catalog/."""
        self._stop_requested = False
        self._visited_urls.clear()
        self._failed_urls.clear()
        self._stats = {"categories": 0, "products": 0, "images": 0, "errors": 0}

        self._load_state()
        IMAGES_DIR.mkdir(exist_ok=True)

        # Try to load previous catalog for merging on resume
        prev_tree = self._load_previous_catalog() if self.resume else None

        root = CategoryNode(url=CATALOG_URL, name="Каталог товаров", depth=0)
        self._emit("Starting full catalog scrape...")

        self._scrape_category(root)
        self._save_state()

        # Merge previously scraped data for categories we skipped
        if prev_tree:
            self._merge_previous(root, prev_tree)

        self._emit(
            f"Scrape complete: {self._stats['categories']} categories, "
            f"{self._stats['products']} products, {self._stats['images']} images, "
            f"{self._stats['errors']} errors, {len(self._failed_urls)} failed URLs"
        )
        return root

    def scrape_url(self, url: str) -> CategoryNode:
        """Scrape a single category URL (and everything below it)."""
        self._stop_requested = False
        IMAGES_DIR.mkdir(exist_ok=True)

        root = CategoryNode(url=url, name="(single URL)", depth=0)
        self._scrape_category(root)
        return root

    # ------------------------------------------------------------------ #
    # Recursive category scraper
    # ------------------------------------------------------------------ #

    def _scrape_category(self, node: CategoryNode):
        """Recursively scrape a category node."""
        if self._stop_requested:
            return
        if node.url in self._visited_urls:
            return
        self._visited_urls.add(node.url)

        # Skip already completed URLs (resume support)
        if node.url in self._completed_urls:
            self._emit(f"[depth={node.depth}] Skipping completed: {node.name} — {node.url}")
            return

        self._emit(f"[depth={node.depth}] Scraping category: {node.name} — {node.url}")
        soup = self._fetch_page(node.url)
        if not soup:
            self._stats["errors"] += 1
            self._failed_urls.add(node.url)
            # Do NOT mark as visited so next run can retry
            self._visited_urls.discard(node.url)
            return

        # Extract the real page name from h1 if we didn't have it
        h1 = soup.find("h1")
        if h1 and (not node.name or node.name == "(single URL)"):
            node.name = h1.get_text(strip=True)

        # Extract breadcrumbs to determine category path
        breadcrumbs = self._extract_breadcrumbs(soup)

        # Try to find subcategory cards/links
        subcats = self._extract_subcategories(soup, node.url)

        if subcats:
            # This is a BRANCH node — recurse into subcategories
            node.is_leaf = False
            self._stats["categories"] += 1
            if self.on_category_found:
                self.on_category_found(node)

            for i, sub in enumerate(subcats):
                if self._stop_requested:
                    break
                sub.depth = node.depth + 1
                node.children.append(sub)
                self._emit(
                    f"  Subcategory {i+1}/{len(subcats)}: {sub.name}",
                    i + 1, len(subcats)
                )
                self._scrape_category(sub)
        else:
            # This is a LEAF node — extract products
            node.is_leaf = True
            self._stats["categories"] += 1
            if self.on_category_found:
                self.on_category_found(node)

            # Try table extraction first, then card extraction
            products = self._extract_products_from_table(soup, node.url)
            if not products:
                products = self._extract_products_from_cards(soup, node.url)
            self._emit(f"  Leaf category, {len(products)} products found")

            # For each product, fetch detail page (skip already-scraped ones)
            for i, prod in enumerate(products):
                if self._stop_requested:
                    break

                # Check if this product was already scraped in a previous run
                if prod.url and prod.url in self._completed_products:
                    prev = self._prev_product_index.get(prod.url)
                    if prev:
                        restored = self._dict_to_product(prev)
                        node.products.append(restored)
                        self._stats["products"] += 1
                        if self.on_product_parsed:
                            self.on_product_parsed(restored, breadcrumbs)
                        logger.debug(f"    Skipping already-scraped product: {prod.url}")
                        continue

                if prod.url:
                    self._scrape_product_detail(prod)
                # Download images
                if self.download_images:
                    self._download_product_images(prod)
                node.products.append(prod)
                self._stats["products"] += 1
                if prod.url:
                    self._completed_products.add(prod.url)
                if self.on_product_parsed:
                    self.on_product_parsed(prod, breadcrumbs)
                if (i + 1) % 10 == 0:
                    self._emit(f"    Products: {i+1}/{len(products)}", i+1, len(products))

            if products:
                self._emit(f"  Done: {len(products)} products scraped from {node.name}")

        self._mark_completed(node.url)

    # ------------------------------------------------------------------ #
    # Page fetching
    # ------------------------------------------------------------------ #

    def _fetch_page(self, url: str) -> BeautifulSoup | None:
        soup = self.fetcher.get_soup(url)
        if soup and self.on_page_fetched:
            self.on_page_fetched(url, str(soup))
        return soup

    # ------------------------------------------------------------------ #
    # Subcategory extraction
    # ------------------------------------------------------------------ #

    def _extract_subcategories(self, soup: BeautifulSoup, page_url: str) -> list[CategoryNode]:
        """
        Detect subcategory cards on a category page.
        Returns empty list if this is a leaf (product table) page.
        """
        subcats = []
        seen_urls = set()

        # Strategy: find catalog section cards.
        # On mnogofrez.ru branch pages, subcategories appear as card blocks
        # with an image and a link, typically inside the main content area.
        # They link to /catalog/slug/ paths.
        # Product tables have <table> with headers like "Артикул" — if we see
        # that, it's a leaf page.

        # Check if there's a product table or product cards → this is a leaf, not a branch
        if self._has_product_table(soup):
            return []
        if self._has_product_cards(soup):
            return []

        # Collect all /catalog/ links from the main content area
        # Exclude navigation menu duplicates by focusing on content
        main_content = soup.find("main") or soup.find("div", {"id": "content"}) or soup

        for a_tag in main_content.find_all("a", href=True):
            href = a_tag["href"]
            if not href.startswith("/catalog/"):
                continue
            full_url = urljoin(BASE_URL, href)

            # Skip self, root catalog, filters
            if full_url == urljoin(BASE_URL, page_url):
                continue
            if full_url.rstrip("/") == CATALOG_URL.rstrip("/"):
                continue
            if "filter" in href or "?" in href or "#" in href:
                continue
            if full_url in seen_urls:
                continue

            name = a_tag.get_text(strip=True)
            # Skip generic link texts
            if not name or name.lower() in ("подробнее", "далее", "ещё", "все", "показать"):
                continue
            if len(name) > 200 or len(name) < 2:
                continue

            # Try to find associated image
            img_url = ""
            parent = a_tag.parent
            if parent:
                img = parent.find("img", src=True)
                if img:
                    src = img["src"]
                    if "/upload/" in src and "svg" not in src.lower():
                        img_url = urljoin(BASE_URL, src)

            seen_urls.add(full_url)
            cat = CategoryNode(url=full_url, name=name, image_url=img_url)
            subcats.append(cat)
            logger.debug(f"  Subcategory found: {name} -> {full_url}")

        return subcats

    def _has_product_table(self, soup: BeautifulSoup) -> bool:
        """Check if the page has a product specifications table (leaf indicator)."""
        for table in soup.find_all("table"):
            header_row = table.find("tr")
            if not header_row:
                continue
            header_text = header_row.get_text(strip=True).lower()
            # Product tables have "артикул" (article) in header
            if "артикул" in header_text:
                return True
        return False

    def _has_product_cards(self, soup: BeautifulSoup) -> bool:
        """
        Check if the page displays products as cards (not in a table).
        Product card pages have "В корзину" (add to cart) buttons or
        price patterns near product links.
        """
        page_text = soup.get_text()
        # "В корзину" is a strong product-page indicator
        if "В корзину" in page_text:
            return True
        # Price pattern near catalog links (e.g. "24 600 р.")
        if re.search(r'\d[\d\s]*р\.', page_text) and soup.find("a", href=re.compile(r'^/catalog/')):
            # Also need more than just nav links — check for qty/cart elements
            if "Кол-во" in page_text or "кол-во" in page_text:
                return True
        return False

    # ------------------------------------------------------------------ #
    # Product table extraction (leaf category pages)
    # ------------------------------------------------------------------ #

    def _extract_products_from_table(self, soup: BeautifulSoup, category_url: str) -> list[Product]:
        """Extract products from a table on a leaf category page."""
        products = []

        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # Parse header row
            header = rows[0]
            col_headers = [th.get_text(strip=True).lower() for th in header.find_all(["th", "td"])]
            if not any("артикул" in h for h in col_headers):
                continue  # not a product table

            logger.debug(f"  Product table headers: {col_headers}")

            for row in rows[1:]:
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue

                prod = self._parse_product_row(cells, col_headers)
                if prod and prod.article:
                    products.append(prod)

        return products

    def _extract_products_from_cards(self, soup: BeautifulSoup, category_url: str) -> list[Product]:
        """
        Extract products displayed as cards (not in a table).
        Card pages have product links with prices and "В корзину" buttons.
        """
        products = []
        seen_urls = set()

        main_content = soup.find("main") or soup.find("div", {"id": "content"}) or soup

        # Find all product links — they go to /catalog/{category}/{product}/ paths
        # and have associated price text nearby
        for a_tag in main_content.find_all("a", href=True):
            href = a_tag["href"]
            if not href.startswith("/catalog/"):
                continue

            full_url = urljoin(BASE_URL, href)
            if full_url in seen_urls:
                continue
            # Skip self, root catalog, generic links
            if full_url == urljoin(BASE_URL, category_url):
                continue
            if full_url.rstrip("/") == CATALOG_URL.rstrip("/"):
                continue
            if "filter" in href or "?" in href or "#" in href:
                continue

            name = a_tag.get_text(strip=True)
            if not name or len(name) < 3 or len(name) > 300:
                continue
            # Skip generic navigation texts
            if name.lower() in ("подробнее", "далее", "ещё", "все", "показать",
                                "в корзину", "купить"):
                continue

            # Check if there's price text near this link (within parent container)
            parent = a_tag.parent
            # Walk up a few levels to find the product card container
            for _ in range(5):
                if parent is None or parent is main_content:
                    break
                parent_text = parent.get_text()
                if re.search(r'\d[\d\s]*р\.', parent_text):
                    break
                parent = parent.parent

            if parent and parent is not main_content:
                parent_text = parent.get_text()
                # Must have a price indicator
                price_match = re.search(r'([\d\s]+)\s*р\.', parent_text)
                if not price_match:
                    continue
            else:
                continue

            seen_urls.add(full_url)
            prod = Product(url=full_url, name=name)
            # Extract price from parent context
            if price_match:
                self._parse_price(prod, price_match.group(0))

            # Check stock
            if "нет в наличии" in parent_text.lower():
                prod.in_stock = False

            # Try to extract article from URL slug
            slug = href.rstrip("/").split("/")[-1]
            prod.article = slug

            # Try to find image
            if parent:
                img = parent.find("img", src=True)
                if img:
                    src = img["src"]
                    if "/upload/" in src and "svg" not in src.lower():
                        prod.image_urls.append(urljoin(BASE_URL, src))

            products.append(prod)
            logger.debug(f"  Product card found: {name} -> {full_url}")

        return products

    def _parse_product_row(self, cells: list[Tag], headers: list[str]) -> Product | None:
        """Parse a single product table row into a Product."""
        prod = Product()

        for i, cell in enumerate(cells):
            text = cell.get_text(strip=True)
            header = headers[i] if i < len(headers) else ""

            # Article column (first col, or one containing "артикул")
            if "артикул" in header or (i == 0 and not prod.article):
                link = cell.find("a", href=True)
                if link:
                    prod.url = urljoin(BASE_URL, link["href"])
                    prod.article = link.get_text(strip=True)
                else:
                    prod.article = text

            # Dimension columns → specs
            elif "диаметр" in header and ("d" in header or "рабоч" in header):
                prod.specs["diameter_d_mm"] = text
            elif "высота" in header or ("h" in header and "мм" in header):
                prod.specs["working_height_h_mm"] = text
            elif "хвостовик" in header or ("s" in header and "мм" in header):
                prod.specs["shank_diameter_s_mm"] = text
            elif ("длина" in header) or ("l" in header and "мм" in header):
                prod.specs["total_length_l_mm"] = text
            elif "стружколом" in header:
                prod.specs["chipbreaker"] = text

            # Price
            elif "цена" in header or "руб" in header:
                self._parse_price(prod, text)
            elif re.search(r'\d+\s*р\.?$', text):
                self._parse_price(prod, text)

        # Stock check
        row_text = " ".join(c.get_text() for c in cells).lower()
        if "нет в наличии" in row_text:
            prod.in_stock = False

        if not prod.name and prod.article:
            prod.name = prod.article

        return prod if prod.article else None

    def _parse_price(self, prod: Product, text: str):
        """Extract price string and numeric value."""
        clean = text.replace("\xa0", " ").replace(" ", "").replace(",", ".")
        match = re.search(r'([\d.]+)', clean)
        if match:
            prod.price = text.strip()
            try:
                prod.price_value = float(match.group(1))
            except ValueError:
                pass

    # ------------------------------------------------------------------ #
    # Product detail page scraper
    # ------------------------------------------------------------------ #

    def _scrape_product_detail(self, prod: Product):
        """Fetch a product detail page and fill in full data."""
        if not prod.url:
            return
        if prod.url in self._visited_urls:
            return
        self._visited_urls.add(prod.url)

        logger.debug(f"    Fetching product detail: {prod.url}")
        soup = self._fetch_page(prod.url)
        if not soup:
            self._stats["errors"] += 1
            return

        # Name from h1
        h1 = soup.find("h1")
        if h1:
            prod.name = h1.get_text(strip=True)

        # Breadcrumbs
        prod.breadcrumbs = self._extract_breadcrumbs(soup)

        # Images — collect all /upload/iblock/ images
        seen_imgs = set()
        for img in soup.find_all("img", src=True):
            src = img["src"]
            if "/upload/iblock/" in src and "svg" not in src.lower():
                full = urljoin(BASE_URL, src)
                if full not in seen_imgs:
                    seen_imgs.add(full)
                    prod.image_urls.append(full)
        # Also check data-src (lazy loading)
        for img in soup.find_all("img", attrs={"data-src": True}):
            src = img["data-src"]
            if "/upload/iblock/" in src and "svg" not in src.lower():
                full = urljoin(BASE_URL, src)
                if full not in seen_imgs:
                    seen_imgs.add(full)
                    prod.image_urls.append(full)
        # og:image fallback
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            full = urljoin(BASE_URL, og["content"])
            if full not in seen_imgs and "/upload/" in full:
                prod.image_urls.append(full)

        # Specs table (key-value pairs)
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True)
                    val = cells[1].get_text(strip=True)
                    if key and val and len(key) < 100:
                        prod.specs[key] = val
                        kl = key.lower()
                        if "артикул" in kl:
                            prod.article = val
                        elif "производитель" in kl:
                            prod.manufacturer = val
                        elif "серия" in kl:
                            prod.series = val

        # Description
        for sel in [".detail_text", "[itemprop='description']",
                    ".product-item-detail-tab-content", ".product-description"]:
            el = soup.select_one(sel)
            if el:
                prod.description = el.get_text(strip=True)[:3000]
                break
        # Fallback: look for substantial text block after specs
        if not prod.description:
            for div in soup.find_all("div", class_=True):
                classes = " ".join(div.get("class", []))
                if "text" in classes or "descr" in classes:
                    text = div.get_text(strip=True)
                    if len(text) > 50:
                        prod.description = text[:3000]
                        break

        # Price (may be more precise on detail page)
        for sel in [".product-item-detail-price-current", ".price_value",
                    ".price", "[itemprop='price']"]:
            el = soup.select_one(sel)
            if el:
                val = el.get("content") or el.get_text(strip=True)
                self._parse_price(prod, val)
                break

        # Stock
        page_text = soup.get_text().lower()
        if "нет в наличии" in page_text:
            prod.in_stock = False
        else:
            prod.in_stock = True
        # Try to get quantity
        qty_match = re.search(r'в наличии[:\s]*(\d+)', page_text)
        if qty_match:
            prod.stock_qty = qty_match.group(1)

    # ------------------------------------------------------------------ #
    # Image downloader
    # ------------------------------------------------------------------ #

    def _download_product_images(self, prod: Product):
        """Download all images for a product into images/{article}/ folder."""
        if not prod.image_urls:
            return

        # Folder per article
        safe_name = re.sub(r'[<>:"/\\|?*]', '_', prod.article or "unknown")
        folder = IMAGES_DIR / safe_name

        for img_url in prod.image_urls:
            if self._stop_requested:
                break
            # Determine filename from URL
            parsed = urlparse(img_url)
            fname = Path(parsed.path).name
            if not fname or fname == "/":
                # Generate from hash
                fname = hashlib.md5(img_url.encode()).hexdigest()[:12] + ".jpg"

            dest = folder / fname
            ok = self.fetcher.download_file(img_url, dest)
            if ok:
                prod.image_local.append(str(dest.relative_to(ROOT_DIR)))
                self._stats["images"] += 1

    # ------------------------------------------------------------------ #
    # Breadcrumbs
    # ------------------------------------------------------------------ #

    def _extract_breadcrumbs(self, soup: BeautifulSoup) -> list[dict]:
        crumbs = []
        for a in soup.select(".breadcrumb a, .breadcrumbs a, [itemtype*='BreadcrumbList'] a"):
            name = a.get_text(strip=True)
            url = urljoin(BASE_URL, a.get("href", ""))
            if name:
                crumbs.append({"name": name, "url": url})
        return crumbs

    # ------------------------------------------------------------------ #
    # JSON export
    # ------------------------------------------------------------------ #

    def export_json(self, root: CategoryNode, filename: str = "catalog.json") -> Path:
        """Export the full tree as a well-structured JSON file."""
        DATA_DIR.mkdir(exist_ok=True)
        path = DATA_DIR / filename

        output = {
            "source": "https://mnogofrez.ru",
            "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "stats": self._stats,
            "catalog": self._node_to_dict(root),
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        logger.info(f"Exported catalog to {path} ({path.stat().st_size / 1024:.0f} KB)")
        return path

    def _node_to_dict(self, node: CategoryNode) -> dict:
        """Convert a CategoryNode tree to a serializable dict."""
        d = {
            "name": node.name,
            "url": node.url,
            "is_leaf": node.is_leaf,
        }
        if node.image_url:
            d["image_url"] = node.image_url
        if node.image_local:
            d["image_local"] = node.image_local

        if node.children:
            d["children"] = [self._node_to_dict(c) for c in node.children]

        if node.products:
            d["products"] = []
            for p in node.products:
                pd = {
                    "article": p.article,
                    "name": p.name,
                    "url": p.url,
                    "price": p.price,
                    "price_value": p.price_value,
                    "in_stock": p.in_stock,
                }
                if p.stock_qty:
                    pd["stock_qty"] = p.stock_qty
                if p.manufacturer:
                    pd["manufacturer"] = p.manufacturer
                if p.series:
                    pd["series"] = p.series
                if p.specs:
                    pd["specs"] = p.specs
                if p.description:
                    pd["description"] = p.description
                if p.image_urls:
                    pd["image_urls"] = p.image_urls
                if p.image_local:
                    pd["image_local"] = p.image_local
                if p.breadcrumbs:
                    pd["breadcrumbs"] = p.breadcrumbs
                d["products"].append(pd)

        return d


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    import argparse
    ap = argparse.ArgumentParser(description="MnogoFrez.ru catalog scraper")
    ap.add_argument("--no-resume", action="store_true", help="Start fresh, ignore previous state")
    ap.add_argument("--no-images", action="store_true", help="Skip image downloads")
    ap.add_argument("--clear-state", action="store_true", help="Delete state file and exit")
    args = ap.parse_args()

    setup_logging()

    if args.clear_state:
        if STATE_FILE.exists():
            STATE_FILE.unlink()
            print("State file deleted.")
        else:
            print("No state file found.")
        return

    scraper = CatalogScraper(
        download_images=not args.no_images,
        resume=not args.no_resume,
    )
    root = scraper.scrape_full_catalog()
    path = scraper.export_json(root)
    print(f"\nDone! Catalog saved to: {path}")
    print(f"Stats: {scraper.stats}")
    print(f"Images in: {IMAGES_DIR}")
    if scraper._failed_urls:
        print(f"Failed URLs ({len(scraper._failed_urls)}):")
        for url in sorted(scraper._failed_urls):
            print(f"  {url}")


if __name__ == "__main__":
    main()
