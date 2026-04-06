"""
Import bearing images from scraped data into the media library.

Scans data/alekspodshipnik.ru/data_ui/images/ folders, extracts model codes
from folder names, selects the best image per model, copies to media/bearings/,
and writes media/bearings/_manifest.json for the PHP Artisan command to consume.

Usage:
    python tools/import_bearing_images.py [--dry-run]
"""

import os
import sys
import json
import shutil
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IMAGES_SOURCE = os.path.join(BASE_DIR, 'data', 'alekspodshipnik.ru', 'data_ui', 'images')
MEDIA_TARGET  = os.path.join(BASE_DIR, 'media', 'bearings')
MANIFEST_PATH = os.path.join(MEDIA_TARGET, '_manifest.json')

DRY_RUN = '--dry-run' in sys.argv

# ── Helpers ──────────────────────────────────────────────────────────────────

def safe_filename(model_code: str) -> str:
    """Convert model code to a safe filename (e.g. 'NK12/16' → 'NK12-16')."""
    return re.sub(r'[/\\:*?"<>|]', '-', model_code)

def is_thumbnail(filename: str) -> bool:
    return '_25x25_cc5' in filename or '_64x64' in filename

def best_image(folder_path: str) -> str | None:
    """Return path to the best (largest non-thumbnail) image in the folder."""
    candidates = []
    for f in os.listdir(folder_path):
        if is_thumbnail(f):
            continue
        ext = f.rsplit('.', 1)[-1].lower() if '.' in f else ''
        if ext not in ('jpg', 'jpeg', 'png', 'webp'):
            continue
        full = os.path.join(folder_path, f)
        try:
            candidates.append((os.path.getsize(full), full, f))
        except OSError:
            pass
    if not candidates:
        return None
    candidates.sort(reverse=True)  # largest first
    return candidates[0][1]        # return full path

def extract_model_code(folder_name: str) -> str:
    """
    Extract bearing model code from folder name.
    Patterns observed:
      '1204_Подшипник_FBJ'      → '1204'
      '6202_QE6_SKF'            → '6202_QE6' or '6202 QE6'?  Handled below.
      'Однорядный_..._6202_QE6_SKF' → parsed for known codes

    Strategy: split on '_', take tokens until we hit a Cyrillic word or brand.
    """
    parts = folder_name.split('_')

    # If the first part is purely Cyrillic (description-first pattern), skip leading words
    collected = []
    for part in parts:
        # Stop at purely-Cyrillic segment (description words), brand names handled separately
        if re.fullmatch(r'[А-Яа-яЁё\-]+', part) and len(part) > 3:
            if not collected:
                continue  # skip leading Cyrillic descriptions
            else:
                break     # stop once we've started collecting the code
        collected.append(part)
        if len(collected) >= 3:
            # Don't over-collect; real model codes are at most 2-3 segments
            break

    if not collected:
        return ''

    # Join collected parts; handle single vs multi-segment codes
    # If second part looks like an execution suffix (letters/digits ≤ 4 chars), include it
    code = collected[0]
    if len(collected) >= 2:
        second = collected[1]
        # Include second part if it looks like part of the code (e.g. 'QE6', '2RS', 'ZZ', 'K')
        if re.fullmatch(r'[A-Z0-9]{1,6}', second):
            code = code + ' ' + second
    return code.strip()

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f'Source : {IMAGES_SOURCE}')
    print(f'Target : {MEDIA_TARGET}')
    print(f'Dry run: {DRY_RUN}')
    print()

    if not os.path.isdir(IMAGES_SOURCE):
        print(f'ERROR: source directory not found: {IMAGES_SOURCE}')
        sys.exit(1)

    if not DRY_RUN:
        os.makedirs(MEDIA_TARGET, exist_ok=True)

    # ── Load existing DB model codes from JSON dump if available ──────────────
    # We try to load from the normalized catalog to validate matches.
    known_codes: set[str] = set()
    catalog_path = os.path.join(BASE_DIR, 'data', 'bearings-normalized-clean.json')
    # We'll do a loose match — not strictly requiring DB validation here;
    # the Artisan command will skip codes not in the DB.

    # ── Scan source folders ───────────────────────────────────────────────────
    folders = sorted(os.listdir(IMAGES_SOURCE))
    manifest: dict[str, dict] = {}

    skipped_no_image = 0
    skipped_no_code  = 0
    copied           = 0
    already_exists   = 0

    for folder_name in folders:
        folder_path = os.path.join(IMAGES_SOURCE, folder_name)
        if not os.path.isdir(folder_path):
            continue

        model_code = extract_model_code(folder_name)
        if not model_code:
            skipped_no_code += 1
            continue

        src = best_image(folder_path)
        if not src:
            skipped_no_image += 1
            continue

        ext       = src.rsplit('.', 1)[-1].lower()
        safe_name = safe_filename(model_code) + '.' + ext
        dst       = os.path.join(MEDIA_TARGET, safe_name)

        # Skip if we already have an entry for this model_code (first match wins)
        if model_code in manifest:
            continue

        manifest[model_code] = {
            'filename': safe_name,
            'source':   src,
            'path':     'bearings/' + safe_name,
        }

        if DRY_RUN:
            copied += 1
            continue

        if os.path.exists(dst):
            already_exists += 1
        else:
            shutil.copy2(src, dst)
            copied += 1

    # ── Write manifest ────────────────────────────────────────────────────────
    if not DRY_RUN:
        with open(MANIFEST_PATH, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        print(f'Manifest written: {MANIFEST_PATH}')

    print(f'Folders processed : {len(folders)}')
    print(f'Models found      : {len(manifest)}')
    print(f'Images copied     : {copied}')
    print(f'Already existed   : {already_exists}')
    print(f'Skipped (no code) : {skipped_no_code}')
    print(f'Skipped (no image): {skipped_no_image}')

    if DRY_RUN:
        print()
        print('--- DRY RUN sample (first 10 matches) ---')
        for code, info in list(manifest.items())[:10]:
            print(f"  {code!r:20s} -> {info['filename']}")

if __name__ == '__main__':
    main()
