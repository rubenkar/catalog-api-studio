"""
Configuration for shop.podshipnik.ru scraper.

Site structure (Bitrix CMS):
- Main page has parametric search with selects: INNER_D, OUTER_D, WIDTH
- Search results: GET /?INNER_D={d}&OUTER_D={D}&WIDTH={B}&PAGEN_1={page}
  → returns HTML table with product rows
- AJAX autocomplete: POST /ajax/search.php  body: name={marking}
  → returns HTML dropdown with product cards
- Product detail: GET /article/{code}/
  → full specs, images, analogs, price
"""

BASE_URL = "https://shop.podshipnik.ru"
SEARCH_URL = f"{BASE_URL}/ajax/search.php"

# Request settings
REQUEST_DELAY = 1.5  # seconds between requests
MAX_RETRIES = 3
TIMEOUT = 30

# Pagination
PAGE_PARAM = "PAGEN_1"

# ── URL params for parametric catalog ──
CATALOG_PARAMS = {
    'name': 'NAME',
    'inner_d': 'INNER_D',
    'outer_d': 'OUTER_D',
    'width': 'WIDTH',
    'page': PAGE_PARAM,
}

# ── CSS selectors: catalog results table ──
CATALOG_SELECTORS = {
    # Results table
    'result_count': '.catalog-result__count, .catalog-result .count',
    'table': '#catalog_items, .basket__table.main-catalog__table',
    'table_row': 'tr.basket__table-item',

    # Row data (each <td> by index)
    # Columns: Type | Marking(link) | Brand(link) | Analogs(link) | d | D | B | Price | Stock | Qty | Cost | Cart
    'row_link': 'td:nth-child(2) a',          # article link → /article/{code}/
    'row_type': 'td:nth-child(1)',
    'row_brand': 'td:nth-child(3)',
    'row_analogs_link': 'td:nth-child(4) a',  # → /analog/{code}/
    'row_d': 'td:nth-child(5)',
    'row_D': 'td:nth-child(6)',
    'row_B': 'td:nth-child(7)',
    'row_price': 'td:nth-child(8)',
    'row_stock': 'td:nth-child(9)',

    # Pagination
    'pagination_next': '.modern-page-next a, .bx-pag-next a',
    'pagination_links': '.modern-page-navigation a, .bx-pagination-container a',
}

# ── CSS selectors: product detail page ──
DETAIL_SELECTORS = {
    'h1': 'h1',
    'article_code': '.js-popup-title',
    'brand_name': '.product-card__brand-name',
    'brand_img': '.product-card__brand-img img',
    'price': '.product-card__price-block-head-value',

    # Image gallery (slick slider)
    'images_big': '.product-card__swiper-big img',
    'images_small': '.product-card__swiper-small img',

    # Main characteristics sidebar
    'main_chars': '.product-card__info-main-characteristics-item',

    # Spec blocks (under "Описание и характеристики" tab)
    'spec_blocks': '.product-card__info-content-character-block',
    'spec_block_title': ':scope > *:first-child',
    'spec_block_items': 'li',

    # Analogs table (under "Аналоги" tab)
    'analogs_table': '#catalog_items, .product-card__info-content-body-analogue table',
    'analogs_row': 'tr.basket__table-item',

    # Blueprint/drawing tab
    'blueprint': '.product-card__info-content-body-blueprint img',

    # Tabs
    'tabs': '.product-card__info-content-tabs-link',
}

# ── AJAX search selectors ──
SEARCH_SELECTORS = {
    'item': '.searchByMarking-dropdown-item__body',
    'title': '.searchByMarking-dropdown-item__title',
    'brand': '.searchByMarking-dropdown-item__title',  # brand is sibling text
    'type': '.searchByMarking-dropdown-item__type',
    'size': '.searchByMarking-dropdown-item__size',
    'price': '.searchByMarking-dropdown-item__price',
    'availability': '.searchByMarking-dropdown-item__availability',
    'image': 'img',
}

# Known product categories on shop.podshipnik.ru
PRODUCT_CATEGORIES = [
    'Вся продукция',
    'Подшипники',
    'Подшипниковые узлы, корпуса и принадлежности',
    'Уплотнения',
    'Ремни',
    'Смазочные материалы',
    'Опорно-поворотные устройства',
    'Инструменты',
    'Авто',
    'Системы линейного перемещения',
    'Прочие изделия',
    'Подшипники шариковые высокоточные',
]
