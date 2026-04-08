# alekspodshipnik.ru UI Scraper Project

## Overview

This project is a desktop scraper for `alekspodshipnik.ru` with a live user interface.

It is designed to:

- crawl catalog and manufacturer sections
- discover product pages automatically
- parse product data into structured records
- collect linked image URLs
- optionally download images
- show scraping progress live
- preview the page currently being scraped
- highlight relevant elements in the preview
- export results to JSON for later import into a database

---

## Main features

### UI layout

The application window is split into two main areas.

#### Left side

The left side shows live scraping data and progress.

It includes:

- current scraper phase
- listing/product/image counters
- current URL
- current parsed product as a hierarchy tree
- list of scraped products
- log output
- vertical scrolling for long data trees and logs

#### Right side

The right side shows a live preview of the current page being processed.

It includes:

- embedded browser preview
- current URL label
- automatic highlighting of detected elements, such as:
  - page title
  - breadcrumbs
  - tables
  - images
  - content blocks

---

## Current scraper flow

1. Start from seed URLs:
   - `https://alekspodshipnik.ru/katalog-produkczii/`
   - `https://alekspodshipnik.ru/manufacturers/`
2. Discover listing pages recursively.
3. Walk paginated listing pages.
4. Detect product/detail page links.
5. Open each product page.
6. Parse structured product data.
7. Update the UI tree live.
8. Save collected data to JSON.
9. Optionally download images.

---

## Important fix applied

### Problem

The UI originally showed progress, but no products were being scraped.

### Root cause

The original product URL detection was too strict and only accepted product URLs containing:

- `/katalog-produkczii/`

That caused product pages linked through manufacturer branches to be ignored.

### Corrected logic

Product detection must accept any same-site `.html` page unless it is an obvious non-product page.

Recommended logic:

```python
def is_product_url(url: str) -> bool:
    if not is_same_site(url):
        return False

    path = urlparse(url).path.lower().rstrip("/")

    if not path.endswith(".html"):
        return False

    excluded = [
        "/news/",
        "/articles/",
        "/contacts/",
        "/about/",
    ]
    if any(x in path for x in excluded):
        return False

    return True
```

Listing detection should exclude product pages and allow catalog/manufacturer branches:

```python
def is_listing_url(url: str) -> bool:
    if not is_same_site(url):
        return False

    path = urlparse(url).path.lower().rstrip("/")

    if is_product_url(url):
        return False

    return (
        "/katalog-produkczii" in path
        or "/manufacturers" in path
    )
```

Product link extraction should be more aggressive:

```python
def extract_product_links(base_url: str, soup: BeautifulSoup) -> List[str]:
    links = []

    for a in soup.select("a[href]"):
        href = absolute_url(base_url, a.get("href"))
        if href and is_product_url(href):
            links.append(href)

    for sel in [
        "table a[href]",
        ".product a[href]",
        ".products a[href]",
        ".catalog a[href]",
        ".item a[href]",
        ".items a[href]",
        ".content a[href]",
    ]:
        for a in soup.select(sel):
            href = absolute_url(base_url, a.get("href"))
            if href and is_product_url(href):
                links.append(href)

    return dedupe_keep_order(links)
```

---

## Parsed product structure

Each product is stored as structured JSON, for example:

```json
{
  "url": "https://alekspodshipnik.ru/.../product.html",
  "title": "NU206 EG15 J30",
  "article": "NU206 EG15 J30",
  "manufacturer": "SNR",
  "price": "от 2 904",
  "status": "в наличии",
  "inner_d_mm": "30",
  "outer_D_mm": "62",
  "height_h_mm": "16",
  "weight_g": "220",
  "breadcrumbs": ["Каталог продукции", "Роликовые подшипники"],
  "description": "...",
  "attributes": {
    "Производитель": "SNR",
    "Внутренний d, мм": "30"
  },
  "image_urls": [
    "https://alekspodshipnik.ru/upload/...jpg"
  ],
  "scraped_at": 1770000000
}
```

---

## UI behavior

### Left tree

The tree contains:

- `Current scraped item`
- `Scraped products`

When a product is parsed:

- the current item tree is replaced with the newest structured data
- a summary node is appended to the scraped products list

### Right preview

The preview loads the currently scraped page and highlights selectors such as:

- `h1`
- `.breadcrumb`
- `.breadcrumbs`
- `table`
- `img`
- `.content`
- `.entry-content`

Note:
The visual highlighting is based on browser-side CSS selectors, while parsing is done using `requests + BeautifulSoup`. This is intended for debugging and visual verification.

---

## Progress and logging

The UI should clearly show whether the scraper is really finding products.

Recommended log messages:

### During listing discovery

```python
self.log.emit(
    f"LISTING done: {listing_url} | "
    f"+listing {new_listing_count} | "
    f"+product {new_product_count} | "
    f"TOTAL products={len(self.product_urls)}"
)
```

### During pagination

```python
self.log.emit(
    f"PAGINATION DISCOVERED {paged}: "
    f"product_links_on_page={len(product_links)}, new_products={new_page_products}, "
    f"products_total={len(self.product_urls)}"
)
```

### Before product scraping starts

```python
self.log.emit(f"START PRODUCT SCRAPE: total_product_urls={len(urls)}")
```

### Before each product page

```python
self.log.emit(f"SCRAPING PRODUCT URL: {product_url}")
```

These messages make it obvious whether the scraper is:

- only walking listings
- actually discovering product pages
- entering product parsing phase

---

## Output files

Current main output:

- `data_ui/products.json`
- `data_ui/images/...` if image download is enabled

Planned or optional outputs:

- CSV export
- Markdown export
- SQL export
- SQLite cache/state DB

---

## Installation

```bash
pip install PySide6 PySide6-Addons requests beautifulsoup4 lxml
```

---

## Run

```bash
python alekspodshipnik_ui_scraper.py
```

---

## Planned improvements

### UI improvements

- click a field in the left tree and highlight the matching element on the right
- pause/resume scraping
- product search/filter in the UI
- tabbed interface for data/log/export
- counters for discovered vs parsed vs failed URLs

### Scraper improvements

- save resume state between runs
- change detection between runs
- normalize prices to numeric values
- normalize manufacturer names
- split dimensions into numeric fields
- stronger product/detail page detection
- export only changed/new products

### Export improvements

- CSV export for database import
- Markdown per product
- SQL insert generation for MySQL/PostgreSQL
- separate images table export

---

## Recommended next milestone

The next best step is a `v2` desktop application with:

- the corrected product detection logic
- persistent crawl state
- exact field-to-element highlighting
- CSV + SQL export from the UI
- controls for start, stop, pause, resume, and export

---

## Notes

This scraper is intended for structured data extraction from a store that does not provide inventory export.

Because site structure may change, product detection and parsing selectors should be kept easy to update.
