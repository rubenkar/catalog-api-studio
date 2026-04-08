"""Configuration for prutki.ru scraper."""

BASE_URL = "http://prutki.ru"
CATALOG_URL = BASE_URL + "/"
PRICE_XLS_URL = BASE_URL + "/price.xls"

# Rate limiting — friendly to own competitor site
REQUEST_DELAY = 1.0  # seconds between requests

# CSS selectors for homepage category parsing
# Homepage has sections: "Алюминиевые сплавы", "Сталь", etc.
# Each section has subsections like "Алюминиевые прутки Д16Т" with dimension links

# Product page selectors
PRODUCT_TABLE_SELECTOR = "table"  # main product table with items
SPECS_TABLE_SELECTOR = "table"    # specifications table

# Known material categories on the homepage
# The homepage is organized as flat sections with links to product pages
# URL pattern: /{shape}-{material}-{grade}-diametr-{size}-mm/
