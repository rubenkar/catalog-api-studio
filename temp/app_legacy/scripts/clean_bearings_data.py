#!/usr/bin/env python3
"""
Bearings Data Cleaning Script

Loads scraped data from alekspodshipnik.ru and transforms it
into a clean format ready for database import.

Fixes:
- Text encoding issues
- Manufacturer extraction
- Article/SKU extraction
- Price normalization
- Weight normalization
- Category tree building
"""

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Optional, Any

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent  # app/scripts → app → project root
DATA_DIR = PROJECT_ROOT / "data" / "bearings" / "alekspodshipnik.ru" / "data_ui"
INPUT_FILE = DATA_DIR / "products.json"
OUTPUT_FILE = DATA_DIR / "cleaned_import_data.json"


def load_products() -> List[Dict]:
    """Load products from scraped JSON"""
    print(f"Loading products from: {INPUT_FILE}")
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        products = json.load(f)
    print(f"[OK] Loaded {len(products)} products")
    return products


def safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float"""
    if value is None:
        return None
    try:
        return float(str(value).replace(',', '.'))
    except (ValueError, TypeError):
        return None


def extract_article(title: str) -> Optional[str]:
    """
    Extract bearing article/designation from title

    Examples:
        "Подшипник NU206 EG15 J30" -> "NU206 EG15 J30"
        "3206 B 2RSR TVH FAG" -> "3206 B 2RSR TVH"
    """
    if not title:
        return None

    # Common bearing designation patterns
    patterns = [
        # Standard patterns like NU206, 6205, 32010
        r'\b([A-Z]{0,4}\d{4,5}[A-Z\d\s\-/\.]*(?:[A-Z]{1,4}\d{0,3})?)\b',
        # Numeric with suffixes: 3206 B 2RSR
        r'\b(\d{4,5}[A-Z\d\s\-/]*)\b',
    ]

    for pattern in patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            article = match.group(1).strip()
            # Clean up excessive spaces
            article = re.sub(r'\s+', ' ', article)
            # Limit length to reasonable size
            if 3 <= len(article) <= 50:
                return article

    return None


def extract_manufacturer(product: Dict) -> Optional[str]:
    """
    Extract manufacturer from product data

    Priority:
    1. attributes['Производитель']
    2. Parse from title (last word, usually manufacturer)
    3. manufacturer field (if not corrupted)
    """
    # Try attributes first
    attrs = product.get('attributes', {})
    if isinstance(attrs, dict):
        manuf = attrs.get('Производитель') or attrs.get('производитель')
        if manuf and manuf not in ['Внутренний d, мм:', 'Внутренний D, мм:']:
            return manuf.strip()

    # Try manufacturer field (check if not corrupted)
    manuf_field = product.get('manufacturer', '')
    if manuf_field and 'мм' not in manuf_field and len(manuf_field) < 30:
        return manuf_field.strip()

    # Try extracting from title (common manufacturers at end)
    title = product.get('title', '')
    known_manufacturers = [
        'SKF', 'FAG', 'NSK', 'NTN', 'KOYO', 'TIMKEN', 'INA', 'SNR',
        'ZWZ', 'HRB', 'LYC', 'CX', 'FBJ', 'NACHI', 'THK', 'NMB',
    ]

    for manuf in known_manufacturers:
        if re.search(rf'\b{manuf}\b', title, re.IGNORECASE):
            return manuf

    # Fallback: try last word if it looks like a manufacturer
    words = title.split()
    if words:
        last_word = words[-1].strip('()[]')
        if last_word.isupper() and 2 <= len(last_word) <= 10:
            return last_word

    return None


def normalize_price(price_str: Any) -> Optional[float]:
    """
    Normalize price string to float

    Examples:
        "4500" -> 4500.0
        "от 2 904" -> 2904.0
        "12,500.50" -> 12500.50
    """
    if not price_str:
        return None

    price_str = str(price_str)
    # Remove Russian text
    price_str = re.sub(r'от\s+', '', price_str, flags=re.IGNORECASE)
    price_str = re.sub(r'руб.*', '', price_str, flags=re.IGNORECASE)

    # Remove spaces and non-numeric except . and ,
    price_str = re.sub(r'[^\d.,]', '', price_str)

    # Replace comma with period
    price_str = price_str.replace(',', '.')

    try:
        return float(price_str)
    except ValueError:
        return None


def normalize_weight(weight_str: Any) -> Optional[float]:
    """
    Normalize weight to grams

    If weight < 5, assume it's in kg and convert to g
    """
    weight = safe_float(weight_str)
    if weight is None:
        return None

    # If suspiciously small (< 5), likely in kg
    if 0 < weight < 5:
        return weight * 1000

    return weight


def clean_product(product: Dict) -> Dict:
    """Transform scraped product into import-ready format"""

    article = extract_article(product.get('title', ''))
    manufacturer = extract_manufacturer(product)
    price = normalize_price(product.get('price'))

    cleaned = {
        'source_url': product.get('url'),
        'title': product.get('title', '').strip(),
        'article': article,
        'manufacturer': manufacturer,

        # Pricing
        'price': price,
        'currency': product.get('currency', 'RUB'),
        'wholesale_prices': product.get('wholesale_prices', []),

        # Dimensions
        'inner_diameter_mm': safe_float(product.get('inner_d_mm')),
        'outer_diameter_mm': safe_float(product.get('outer_D_mm')),
        'height_mm': safe_float(product.get('height_h_mm')),
        'weight_g': normalize_weight(product.get('weight_g')),

        # Availability
        'availability_status': product.get('status'),
        'in_stock': bool(price),  # If has price, assume in stock

        # Category and metadata
        'category_path': product.get('category_path', []),
        'breadcrumbs': product.get('breadcrumbs', []),
        'description': product.get('description', '').strip(),
        'attributes': product.get('attributes', {}),

        # Images
        'images': product.get('image_urls', []),

        # Timestamps
        'scraped_at': product.get('scraped_at'),
    }

    return cleaned


def build_category_tree(products: List[Dict]) -> List[Dict]:
    """
    Extract unique category paths and build tree structure

    Returns list of category nodes with:
    - path: full category path
    - name: category name
    - parent_path: parent category path
    - depth: tree depth level
    """
    paths = set()

    for product in products:
        cat_path = product.get('category_path', [])
        if not cat_path:
            continue

        # Add all prefixes of the path
        for i in range(1, len(cat_path) + 1):
            path_tuple = tuple(cat_path[:i])
            paths.add(path_tuple)

    # Sort by depth (breadth-first)
    sorted_paths = sorted(paths, key=len)

    tree = []
    for path in sorted_paths:
        node = {
            'path': list(path),
            'name': path[-1],
            'parent_path': list(path[:-1]) if len(path) > 1 else None,
            'depth': len(path),
            'node_type': 'Group' if len(path) < 3 else 'Category',
        }
        tree.append(node)

    return tree


def generate_stats(cleaned_products: List[Dict]) -> Dict:
    """Generate statistics about cleaned data"""
    total = len(cleaned_products)

    return {
        'total_products': total,
        'with_article': sum(1 for p in cleaned_products if p['article']),
        'with_manufacturer': sum(1 for p in cleaned_products if p['manufacturer']),
        'with_price': sum(1 for p in cleaned_products if p['price']),
        'with_dimensions': sum(1 for p in cleaned_products if p['inner_diameter_mm'] and p['outer_diameter_mm']),
        'with_images': sum(1 for p in cleaned_products if p['images']),
        'in_stock': sum(1 for p in cleaned_products if p['in_stock']),

        # Percentages
        'article_pct': round(sum(1 for p in cleaned_products if p['article']) / total * 100, 1) if total else 0,
        'manufacturer_pct': round(sum(1 for p in cleaned_products if p['manufacturer']) / total * 100, 1) if total else 0,
        'price_pct': round(sum(1 for p in cleaned_products if p['price']) / total * 100, 1) if total else 0,
    }


def main():
    """Main execution"""
    print("=" * 60)
    print("Bearings Data Cleaning Script")
    print("=" * 60)
    print()

    # Load raw data
    products = load_products()

    # Clean each product
    print(f"\nCleaning {len(products)} products...")
    cleaned = [clean_product(p) for p in products]
    print("[OK] Products cleaned")

    # Build category tree
    print("\nBuilding category tree...")
    categories = build_category_tree(products)
    print(f"[OK] Created {len(categories)} category nodes")

    # Generate statistics
    stats = generate_stats(cleaned)

    # Prepare output
    output = {
        'meta': {
            'source': 'alekspodshipnik.ru',
            'scraped_file': str(INPUT_FILE),
            'cleaned_at': None,  # Will be set by import script
            'version': '1.0',
        },
        'stats': stats,
        'categories': categories,
        'products': cleaned,
    }

    # Save to file
    print(f"\nSaving cleaned data to: {OUTPUT_FILE}")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("[OK] Cleaned data saved")

    # Print summary
    print("\n" + "=" * 60)
    print("CLEANING SUMMARY")
    print("=" * 60)
    print(f"Total products:       {stats['total_products']}")
    print(f"With article:         {stats['with_article']} ({stats['article_pct']}%)")
    print(f"With manufacturer:    {stats['with_manufacturer']} ({stats['manufacturer_pct']}%)")
    print(f"With price:           {stats['with_price']} ({stats['price_pct']}%)")
    print(f"With dimensions:      {stats['with_dimensions']}")
    print(f"With images:          {stats['with_images']}")
    print(f"In stock:             {stats['in_stock']}")
    print(f"\nCategories created:   {len(categories)}")
    print("=" * 60)
    print("\n[SUCCESS] Data cleaning complete!")
    print(f"\nNext step: Run import script to load into database")


if __name__ == '__main__':
    main()
