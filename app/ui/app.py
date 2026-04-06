#!/usr/bin/env python3
"""
Catalog Extractor UI — Web-based tool for debugging PDF catalog extraction.

Usage:
    python app.py
    # Opens at http://localhost:5050

Features:
- Browse PDF catalogs from _src/catalogs/
- Preview pages as rendered images
- Run table analysis (PyMuPDF find_tables)
- Layout analysis: draw/resize rectangular sections on pages
"""

import json
import os
import sys
import subprocess
import threading
import time
import uuid
from pathlib import Path
from io import BytesIO
import base64

# Force UTF-8 on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

try:
    import fitz  # PyMuPDF
except ImportError:
    print("ERROR: pip install PyMuPDF")
    sys.exit(1)

try:
    from flask import Flask, render_template, jsonify, request, send_file
except ImportError:
    print("ERROR: pip install flask")
    sys.exit(1)

try:
    import cv2
    import numpy as np
    HAS_CV2 = True
except ImportError:
    HAS_CV2 = False
    print("WARNING: pip install opencv-python-headless numpy — bitmap analysis disabled")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # app/ui → project root
CATALOGS_DIR = PROJECT_ROOT / "catalogs"
INDEX_DIR = CATALOGS_DIR / "index"
EXTRACTION_DIR = PROJECT_ROOT / "app" / "extraction"
BRAND_MAP_PATH = PROJECT_ROOT / "app" / "cli" / "brand_catalog_map.json"

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Background process tracking
# ---------------------------------------------------------------------------
# Known scripts that can be run
SCRIPTS = {
    "catalog_extractor": {
        "name": "Catalog Extractor",
        "path": EXTRACTION_DIR / "catalog_extractor.py",
        "description": "Extract tables from PDF catalogs (PyMuPDF find_tables)",
    },
    "catalog_watcher": {
        "name": "Catalog Watcher",
        "path": EXTRACTION_DIR / "catalog_watcher.py",
        "description": "Watch for new catalogs and auto-process",
    },
    "ocr_extractor": {
        "name": "OCR Extractor",
        "path": EXTRACTION_DIR / "ocr_extractor.py",
        "description": "OCR-based extraction for scanned PDFs",
    },
    # NOTE: meili_sync.py was not moved — it remains in the stockshop project.
    # "meili_sync": {
    #     "name": "Meilisearch Sync",
    #     "path": ...,
    #     "description": "Sync extracted data to Meilisearch",
    # },
    "drawing_pipeline": {
        "name": "Drawing Pipeline",
        "path": EXTRACTION_DIR / "drawing_pipeline.py",
        "description": "Extract bearing drawings from catalogs",
    },
}

# Active background processes: {id: {script, proc, output, status, started, catalog, ...}}
bg_processes = {}
bg_lock = threading.Lock()


def _run_bg_process(proc_id, script_key, args):
    """Run a script in background thread, capturing output."""
    script = SCRIPTS[script_key]
    cmd = [sys.executable, str(script["path"])] + args

    with bg_lock:
        bg_processes[proc_id]["status"] = "running"

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(script["path"].parent),
        )
        with bg_lock:
            bg_processes[proc_id]["proc"] = proc

        output_lines = []
        for line in proc.stdout:
            line = line.rstrip()
            output_lines.append(line)
            with bg_lock:
                bg_processes[proc_id]["output"] = output_lines[-200:]  # keep last 200 lines
                # Try to parse progress from output
                bg_processes[proc_id]["last_line"] = line

        proc.wait()
        with bg_lock:
            bg_processes[proc_id]["status"] = "done" if proc.returncode == 0 else "error"
            bg_processes[proc_id]["returncode"] = proc.returncode
            bg_processes[proc_id]["finished"] = time.time()

    except Exception as e:
        with bg_lock:
            bg_processes[proc_id]["status"] = "error"
            bg_processes[proc_id]["output"].append(f"ERROR: {e}")
            bg_processes[proc_id]["finished"] = time.time()


@app.route("/api/scripts")
def api_scripts():
    """List available scripts."""
    result = {}
    for key, info in SCRIPTS.items():
        result[key] = {
            "name": info["name"],
            "description": info["description"],
            "exists": info["path"].exists(),
        }
    return jsonify(result)


@app.route("/api/scripts/run", methods=["POST"])
def api_scripts_run():
    """Start a script in background."""
    data = request.get_json() or {}
    script_key = data.get("script")
    args = data.get("args", [])

    if script_key not in SCRIPTS:
        return jsonify({"error": f"Unknown script: {script_key}"}), 400

    if not SCRIPTS[script_key]["path"].exists():
        return jsonify({"error": f"Script not found: {SCRIPTS[script_key]['path']}"}), 404

    proc_id = str(uuid.uuid4())[:8]
    with bg_lock:
        bg_processes[proc_id] = {
            "id": proc_id,
            "script": script_key,
            "script_name": SCRIPTS[script_key]["name"],
            "args": args,
            "status": "starting",
            "output": [],
            "last_line": "",
            "started": time.time(),
            "finished": None,
            "proc": None,
            "returncode": None,
        }

    thread = threading.Thread(target=_run_bg_process, args=(proc_id, script_key, args), daemon=True)
    thread.start()

    return jsonify({"id": proc_id, "status": "starting"})


@app.route("/api/scripts/status")
def api_scripts_status():
    """Get status of all background processes."""
    with bg_lock:
        result = []
        for pid, info in bg_processes.items():
            result.append({
                "id": info["id"],
                "script": info["script"],
                "script_name": info["script_name"],
                "args": info["args"],
                "status": info["status"],
                "last_line": info["last_line"],
                "started": info["started"],
                "finished": info["finished"],
                "returncode": info["returncode"],
                "output_lines": len(info["output"]),
            })
    return jsonify(result)


@app.route("/api/scripts/output/<proc_id>")
def api_scripts_output(proc_id):
    """Get output of a background process."""
    with bg_lock:
        if proc_id not in bg_processes:
            return jsonify({"error": "Process not found"}), 404
        info = bg_processes[proc_id]
        return jsonify({
            "id": proc_id,
            "status": info["status"],
            "output": info["output"],
            "returncode": info["returncode"],
        })


@app.route("/api/scripts/stop/<proc_id>", methods=["POST"])
def api_scripts_stop(proc_id):
    """Stop a background process."""
    with bg_lock:
        if proc_id not in bg_processes:
            return jsonify({"error": "Process not found"}), 404
        info = bg_processes[proc_id]
        if info["proc"] and info["status"] == "running":
            info["proc"].terminate()
            info["status"] = "stopped"
            info["finished"] = time.time()
    return jsonify({"status": "stopped"})


def get_catalogs():
    """List all PDF catalogs, grouped by brand."""
    catalogs = []
    if not CATALOGS_DIR.exists():
        return catalogs

    # Load brand map for grouping
    brand_map = {}
    if BRAND_MAP_PATH.exists():
        with open(BRAND_MAP_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for brand, info in raw.items():
            for cat_file in info.get("catalogs", []):
                brand_map[cat_file] = brand

    for pdf in sorted(CATALOGS_DIR.glob("*.pdf")):
        brand = brand_map.get(pdf.name, pdf.name.split(" - ")[0] if " - " in pdf.name else "Other")
        size_mb = pdf.stat().st_size / (1024 * 1024)
        catalogs.append({
            "filename": pdf.name,
            "brand": brand,
            "size_mb": round(size_mb, 1),
        })

    return catalogs


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/catalogs")
def api_catalogs():
    return jsonify(get_catalogs())


@app.route("/api/catalog/info")
def api_catalog_info():
    """Get page count and metadata for a catalog."""
    filename = request.args.get("file")
    if not filename:
        return jsonify({"error": "No file specified"}), 400

    pdf_path = CATALOGS_DIR / filename
    if not pdf_path.exists():
        return jsonify({"error": "File not found"}), 404

    doc = fitz.open(str(pdf_path))
    # Collect page dimensions + lightweight stats for every page
    # Only fast ops: text length, image count, text block count
    # Skips find_tables() and get_drawings() (too slow for bulk)
    pages = []
    for i in range(doc.page_count):
        page = doc[i]
        rect = page.rect
        text = page.get_text("text").strip()
        images = page.get_images(full=True)
        blocks = page.get_text("dict")["blocks"]
        text_blocks = sum(1 for b in blocks if b["type"] == 0)
        bitmap = len(text) < 20 and len(images) >= 1
        spread = rect.width / rect.height > 1.25 if rect.height > 0 else False
        pages.append({
            "w": round(rect.width, 1),
            "h": round(rect.height, 1),
            "images": len(images),
            "text_blocks": text_blocks,
            "chars": len(text),
            "bitmap": bitmap,
            "spread": spread,
        })
    info = {
        "filename": filename,
        "page_count": doc.page_count,
        "metadata": doc.metadata,
        "pages": pages,
    }
    doc.close()
    return jsonify(info)


@app.route("/api/catalog/page")
def api_catalog_page():
    """Render a catalog page as PNG image."""
    filename = request.args.get("file")
    page_num = int(request.args.get("page", 0))
    zoom = float(request.args.get("zoom", 2.0))

    if not filename:
        return jsonify({"error": "No file specified"}), 400

    pdf_path = CATALOGS_DIR / filename
    if not pdf_path.exists():
        return jsonify({"error": "File not found"}), 404

    doc = fitz.open(str(pdf_path))
    if page_num < 0 or page_num >= doc.page_count:
        doc.close()
        return jsonify({"error": "Page out of range"}), 400

    page = doc[page_num]
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat)

    img_bytes = BytesIO(pix.tobytes("png"))
    img_bytes.seek(0)
    doc.close()

    return send_file(img_bytes, mimetype="image/png")


@app.route("/api/catalog/page-stats")
def api_page_stats():
    """Quick per-page stats: image count, text blocks, drawings, tables, bitmap flag."""
    filename = request.args.get("file")
    page_num = int(request.args.get("page", 0))

    if not filename:
        return jsonify({"error": "No file specified"}), 400

    pdf_path = CATALOGS_DIR / filename
    if not pdf_path.exists():
        return jsonify({"error": "File not found"}), 404

    doc = fitz.open(str(pdf_path))
    if page_num < 0 or page_num >= doc.page_count:
        doc.close()
        return jsonify({"error": "Page out of range"}), 400

    page = doc[page_num]
    text = page.get_text("text").strip()
    images = page.get_images(full=True)
    blocks = page.get_text("dict")["blocks"]
    text_blocks = [b for b in blocks if b["type"] == 0]
    img_blocks = [b for b in blocks if b["type"] == 1]

    # Count drawings (vector paths)
    drawings = page.get_drawings()

    # Tables
    try:
        tables = page.find_tables()
        table_count = len(tables.tables)
    except Exception:
        table_count = 0

    bitmap = _is_bitmap_page(page)
    char_count = len(text)

    doc.close()

    return jsonify({
        "page": page_num,
        "images": len(images),
        "text_blocks": len(text_blocks),
        "drawings": len(drawings),
        "tables": table_count,
        "chars": char_count,
        "bitmap": bitmap,
    })


@app.route("/api/catalog/bulk-stats")
def api_bulk_stats():
    """Quick stats for a range of pages (for heading bars)."""
    filename = request.args.get("file")
    start = int(request.args.get("start", 0))
    end = int(request.args.get("end", 10))

    if not filename:
        return jsonify({"error": "No file specified"}), 400

    pdf_path = CATALOGS_DIR / filename
    if not pdf_path.exists():
        return jsonify({"error": "File not found"}), 404

    doc = fitz.open(str(pdf_path))
    end = min(end, doc.page_count)
    results = {}

    for i in range(start, end):
        page = doc[i]
        text = page.get_text("text").strip()
        images = page.get_images(full=True)
        blocks = page.get_text("dict")["blocks"]
        text_blocks_count = sum(1 for b in blocks if b["type"] == 0)
        drawings_count = len(page.get_drawings())

        try:
            tables = page.find_tables()
            table_count = len(tables.tables)
        except Exception:
            table_count = 0

        bitmap = _is_bitmap_page(page)

        results[str(i)] = {
            "images": len(images),
            "text_blocks": text_blocks_count,
            "drawings": drawings_count,
            "tables": table_count,
            "chars": len(text),
            "bitmap": bitmap,
        }

    doc.close()
    return jsonify(results)


@app.route("/api/catalog/analyze")
def api_catalog_analyze():
    """Run table analysis on a specific page using PyMuPDF find_tables()."""
    filename = request.args.get("file")
    page_num = int(request.args.get("page", 0))

    if not filename:
        return jsonify({"error": "No file specified"}), 400

    pdf_path = CATALOGS_DIR / filename
    if not pdf_path.exists():
        return jsonify({"error": "File not found"}), 404

    doc = fitz.open(str(pdf_path))
    if page_num < 0 or page_num >= doc.page_count:
        doc.close()
        return jsonify({"error": "Page out of range"}), 400

    page = doc[page_num]
    page_rect = page.rect

    # Find tables
    tables_result = page.find_tables()
    tables = []
    for i, table in enumerate(tables_result.tables):
        bbox = table.bbox  # (x0, y0, x1, y1)
        # Extract table data
        try:
            data = table.extract()
        except Exception:
            data = []

        tables.append({
            "index": i,
            "bbox": {
                "x0": bbox[0] / page_rect.width,
                "y0": bbox[1] / page_rect.height,
                "x1": bbox[2] / page_rect.width,
                "y1": bbox[3] / page_rect.height,
            },
            "bbox_abs": {
                "x0": round(bbox[0], 1),
                "y0": round(bbox[1], 1),
                "x1": round(bbox[2], 1),
                "y1": round(bbox[3], 1),
            },
            "rows": len(data),
            "cols": len(data[0]) if data else 0,
            "data": data[:10],  # first 10 rows for preview
        })

    # Find text blocks
    text_blocks = []
    blocks = page.get_text("dict")["blocks"]
    for block in blocks:
        if block["type"] == 0:  # text block
            bbox = block["bbox"]
            text_blocks.append({
                "bbox": {
                    "x0": bbox[0] / page_rect.width,
                    "y0": bbox[1] / page_rect.height,
                    "x1": bbox[2] / page_rect.width,
                    "y1": bbox[3] / page_rect.height,
                },
                "text": " ".join(
                    span["text"]
                    for line in block.get("lines", [])
                    for span in line.get("spans", [])
                )[:200],
            })

    # Find images
    images = []
    for img_info in page.get_images(full=True):
        xref = img_info[0]
        # Get image bbox via get_image_rects
        rects = page.get_image_rects(xref)
        for rect in rects:
            images.append({
                "bbox": {
                    "x0": rect.x0 / page_rect.width,
                    "y0": rect.y0 / page_rect.height,
                    "x1": rect.x1 / page_rect.width,
                    "y1": rect.y1 / page_rect.height,
                },
                "width": img_info[2],
                "height": img_info[3],
            })

    doc.close()

    return jsonify({
        "page": page_num,
        "page_size": {"width": page_rect.width, "height": page_rect.height},
        "tables": tables,
        "text_blocks": text_blocks,
        "images": images,
    })


# ---------------------------------------------------------------------------
# Section types for layout analysis
# ---------------------------------------------------------------------------
SECTION_TYPES = [
    "header", "drawing", "dimension_table", "notes",
    "footer", "page_number", "logo", "specs",
]

SECTION_COLORS = {
    "header":          "#e63946",
    "drawing":         "#f4a261",
    "dimension_table": "#00b4d8",
    "notes":           "#2ec4b6",
    "footer":          "#8338ec",
    "page_number":     "#ff006e",
    "logo":            "#fb5607",
    "specs":           "#3a86a7",
    "bitmap":          "#9d4edd",
}


def _is_bitmap_page(page):
    """Detect if a page is fully bitmapped (single image, no/minimal text)."""
    text = page.get_text("text").strip()
    images = page.get_images(full=True)
    # Bitmap page: very little text (<20 chars) and at least one image
    if len(text) < 20 and len(images) >= 1:
        return True
    # Or: single large image covering >80% of page area
    if len(images) == 1:
        rects = page.get_image_rects(images[0][0])
        if rects:
            r = rects[0]
            img_area = (r.x1 - r.x0) * (r.y1 - r.y0)
            page_area = page.rect.width * page.rect.height
            if img_area / page_area > 0.8:
                return True
    return False


import re as _re


def _detect_page_numbers_text(page, page_rect):
    """Detect page numbers from embedded text (non-bitmap pages).

    Scans bottom 12% of page for short numeric strings.
    For spreads, checks left and right corners separately.
    Returns list of {"number": int, "side": "left"|"right"|"center", "bbox": {...}}
    """
    pw, ph = page_rect.width, page_rect.height
    results = []

    # Define corner regions: bottom 12%, left 20% and right 20%
    regions = [
        ("left",   fitz.Rect(0,          ph * 0.88, pw * 0.25, ph)),
        ("right",  fitz.Rect(pw * 0.75,  ph * 0.88, pw,        ph)),
        ("center", fitz.Rect(pw * 0.35,  ph * 0.88, pw * 0.65, ph)),
    ]

    for side, clip in regions:
        text = page.get_text("text", clip=clip).strip()
        # Look for standalone numbers (1-4 digits)
        nums = _re.findall(r'\b(\d{1,4})\b', text)
        for n in nums:
            val = int(n)
            if 1 <= val <= 9999:
                results.append({
                    "number": val,
                    "side": side,
                    "bbox": {
                        "x0": clip.x0 / pw, "y0": clip.y0 / ph,
                        "x1": clip.x1 / pw, "y1": clip.y1 / ph,
                    },
                })
                break  # one number per region is enough

    return results


def _detect_page_numbers_bitmap(gray, h, w):
    """Detect page numbers from bitmap via contour analysis on bottom corners.

    Looks for small isolated character groups at the bottom corners.
    Returns approximate positions and raw crop images for debugging.
    No OCR — just detects presence and position of digit-like content.
    """
    if not HAS_CV2:
        return []

    results = []
    _, binary = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY_INV)

    # Check bottom corners: bottom 10%, left/right 18%
    corners = [
        ("left",  0, int(w * 0.18), int(h * 0.88), h),
        ("right", int(w * 0.82), w, int(h * 0.88), h),
    ]

    for side, x0, x1, y0, y1 in corners:
        crop = binary[y0:y1, x0:x1]
        # Find contours in the corner
        contours, _ = cv2.findContours(crop, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        # Filter for digit-sized contours (height 8-60px, aspect 0.2-1.5)
        digit_contours = []
        for cnt in contours:
            bx, by, bw, bh = cv2.boundingRect(cnt)
            if 8 <= bh <= 60 and 3 <= bw <= 50:
                aspect = bw / bh
                if 0.15 <= aspect <= 1.5:
                    digit_contours.append((bx, by, bw, bh))

        if digit_contours:
            # Group nearby contours on the same baseline (within 5px vertical)
            digit_contours.sort(key=lambda c: c[0])  # sort by x
            groups = []
            current_group = [digit_contours[0]]
            for dc in digit_contours[1:]:
                prev = current_group[-1]
                # Same line if vertical centers are close and horizontal gap is small
                if abs((dc[1] + dc[3]//2) - (prev[1] + prev[3]//2)) < 10 and dc[0] - (prev[0] + prev[2]) < 20:
                    current_group.append(dc)
                else:
                    groups.append(current_group)
                    current_group = [dc]
            groups.append(current_group)

            # Take the group most likely to be a page number (1-4 chars, near bottom)
            for group in groups:
                if 1 <= len(group) <= 4:
                    gx0 = min(c[0] for c in group)
                    gy0 = min(c[1] for c in group)
                    gx1 = max(c[0] + c[2] for c in group)
                    gy1 = max(c[1] + c[3] for c in group)

                    results.append({
                        "side": side,
                        "digit_count": len(group),
                        "bbox": {
                            "x0": (x0 + gx0) / w,
                            "y0": (y0 + gy0) / h,
                            "x1": (x0 + gx1) / w,
                            "y1": (y0 + gy1) / h,
                        },
                    })
                    break  # one per corner

    return results


def _detect_gutter(gray, w):
    """Check if a wide image has a vertical gutter (white strip) near the center.

    Returns the gutter x-position (in pixels) or None.
    """
    # Only check pages wider than tall (landscape / 2-page spread)
    h = gray.shape[0]
    if w / h < 1.2:
        return None

    # Scan center 40%-60% for a vertical white strip
    _, binary = cv2.threshold(gray, 235, 255, cv2.THRESH_BINARY)
    best_x = None
    best_white = 0
    for cx_frac in [i / 100.0 for i in range(42, 58)]:
        cx = int(w * cx_frac)
        strip = binary[:, max(0, cx - 3):cx + 3]  # 6px wide strip
        white_ratio = np.mean(strip) / 255
        if white_ratio > best_white:
            best_white = white_ratio
            best_x = cx

    # Need at least 70% white to count as a gutter
    if best_white > 0.70:
        return best_x
    return None


def _merge_caption_sections(sections):
    """Merge small content/notes bands into the drawing or table directly above them.

    Catches the pattern: [drawing 25%] [content 8%] → merge into [drawing 33%]
    Also merges: [table] [content <12%] → merge into [table]
    """
    if len(sections) < 2:
        return sections

    merged = [sections[0]]
    for sec in sections[1:]:
        prev = merged[-1]
        # Check if current is a small band right below a drawing or table
        gap = abs(sec["bbox"]["y0"] - prev["bbox"]["y1"])
        sec_height = sec["bbox"]["y1"] - sec["bbox"]["y0"]

        is_small = sec_height < 0.12
        is_adjacent = gap < 0.03
        prev_is_visual = prev["type"] in ("drawing", "dimension_table")
        cur_is_content = sec["type"] in ("notes", "drawing")

        if is_adjacent and is_small and prev_is_visual and cur_is_content:
            # Absorb into previous section
            prev["bbox"]["y1"] = sec["bbox"]["y1"]
            prev["label"] = prev["label"].rstrip(")") + "+caption)" if "(" in prev["label"] else prev["label"] + " (+caption)"
        else:
            merged.append(sec)

    return merged


def _analyze_half(gray, binary, h, w, x_offset_frac, x_width_frac, side_label):
    """Analyze one half of a 2-page spread. Returns sections with adjusted bboxes."""
    sections = []

    # Crop to this half
    x0 = int(gray.shape[1] * x_offset_frac)
    x1 = int(gray.shape[1] * (x_offset_frac + x_width_frac))
    half_gray = gray[:, x0:x1]
    half_binary = binary[:, x0:x1]
    hh, hw = half_gray.shape

    # Horizontal projection profile
    row_sums = np.sum(half_binary, axis=1).astype(float) / (hw * 255)
    GAP_THRESH = 0.01
    MIN_GAP = int(hh * 0.015)
    in_gap = False
    gap_start = 0
    gaps = []
    for y in range(hh):
        if row_sums[y] < GAP_THRESH:
            if not in_gap:
                gap_start = y
                in_gap = True
        else:
            if in_gap:
                if y - gap_start >= MIN_GAP:
                    gaps.append((gap_start, y))
                in_gap = False
    if in_gap and hh - gap_start >= MIN_GAP:
        gaps.append((gap_start, hh))

    # Build bands
    bands = []
    prev_end = 0
    for gs, ge in gaps:
        if gs > prev_end:
            bands.append((prev_end, gs))
        prev_end = ge
    if prev_end < hh:
        bands.append((prev_end, hh))

    # Line detection for tables
    edges = cv2.Canny(half_gray, 50, 150, apertureSize=3)
    h_lines = cv2.HoughLinesP(edges, 1, np.pi / 180, threshold=80,
                              minLineLength=int(hw * 0.2), maxLineGap=10)
    v_lines = cv2.HoughLinesP(edges, 1, np.pi / 2, threshold=80,
                              minLineLength=int(hh * 0.03), maxLineGap=10)

    h_line_ys = set()
    if h_lines is not None:
        for line in h_lines:
            lx1, ly1, lx2, ly2 = line[0]
            if abs(ly2 - ly1) < 5:
                h_line_ys.add((ly1 + ly2) // 2)

    def band_has_table(y0, y1):
        hl = sum(1 for y in h_line_ys if y0 <= y <= y1)
        vl = 0
        if v_lines is not None:
            for line in v_lines:
                lx1, ly1, lx2, ly2 = line[0]
                if abs(lx2 - lx1) < 5 and y0 <= (ly1 + ly2) // 2 <= y1:
                    vl += 1
        return hl >= 3 and vl >= 2

    # Classify bands
    for y0, y1 in bands:
        band_h_frac = (y1 - y0) / hh
        center_y_frac = ((y0 + y1) / 2) / hh

        band_binary = half_binary[y0:y1, :]
        ink_ratio = np.sum(band_binary) / (band_binary.size * 255) if band_binary.size > 0 else 0
        col_sums = np.sum(band_binary, axis=0).astype(float) / ((y1 - y0) * 255)
        content_spread = np.sum(col_sums > 0.01) / hw if hw > 0 else 0

        is_table = band_has_table(y0, y1)

        # Convert to full-page coordinates
        bbox = {
            "x0": x_offset_frac,
            "y0": y0 / hh,
            "x1": x_offset_frac + x_width_frac,
            "y1": y1 / hh,
        }

        if band_h_frac < 0.02 or (ink_ratio < 0.02 and band_h_frac < 0.05):
            continue
        elif is_table:
            sections.append({"type": "dimension_table", "label": f"{side_label} Table (lines)", "bbox": bbox})
        elif center_y_frac < 0.15 and band_h_frac < 0.18:
            sections.append({"type": "header", "label": f"{side_label} Header", "bbox": bbox})
        elif center_y_frac > 0.90 and band_h_frac < 0.10:
            sections.append({"type": "footer", "label": f"{side_label} Footer", "bbox": bbox})
        elif band_h_frac > 0.25 and ink_ratio > 0.05 and content_spread > 0.4:
            sections.append({"type": "drawing", "label": f"{side_label} Drawing ({int(band_h_frac*100)}%)", "bbox": bbox})
        elif content_spread > 0.6 and band_h_frac > 0.15:
            sections.append({"type": "dimension_table", "label": f"{side_label} Data region", "bbox": bbox})
        else:
            sections.append({"type": "notes", "label": f"{side_label} Content ({int(band_h_frac*100)}%)", "bbox": bbox})

    # ── Merge adjacent drawing/table + small caption bands ──
    sections = _merge_caption_sections(sections)

    # ── Detect page numbers in this half via contour analysis ──
    half_pn = _detect_page_numbers_bitmap(half_gray, hh, hw)
    for pn in half_pn:
        # Remap bbox to full page coords
        sections.append({
            "type": "page_number",
            "label": f"{side_label} Page# ({pn['side']}, ~{pn['digit_count']} digits)",
            "bbox": {
                "x0": x_offset_frac + pn["bbox"]["x0"] * x_width_frac,
                "y0": pn["bbox"]["y0"],
                "x1": x_offset_frac + pn["bbox"]["x1"] * x_width_frac,
                "y1": pn["bbox"]["y1"],
            },
        })

    return sections


def _classify_bitmap_sections(page, page_rect):
    """Detect layout sections on a bitmapped page using OpenCV.

    For wide pages (2-page spreads), detects the center gutter and
    analyzes left and right halves independently.

    Approach:
    1. Render page to image
    2. Detect gutter for 2-page spreads → split into halves
    3. Horizontal projection profile to find row-based sections
    4. Line detection (Hough) to find table borders
    5. Contour analysis for drawings/logos
    """
    if not HAS_CV2:
        return [{
            "type": "bitmap",
            "label": "Bitmap page (OpenCV not installed)",
            "bbox": {"x0": 0, "y0": 0, "x1": 1, "y1": 1},
        }]

    pw, ph = page_rect.width, page_rect.height

    # Render page at 1.5x for analysis (balance speed vs accuracy)
    mat = fitz.Matrix(1.5, 1.5)
    pix = page.get_pixmap(matrix=mat)
    img_data = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
    if pix.n == 4:
        gray = cv2.cvtColor(img_data, cv2.COLOR_BGRA2GRAY)
    elif pix.n == 3:
        gray = cv2.cvtColor(img_data, cv2.COLOR_BGR2GRAY)
    else:
        gray = img_data

    h, w = gray.shape

    # Invert for content detection
    _, binary = cv2.threshold(gray, 240, 255, cv2.THRESH_BINARY_INV)

    # ── Check for 2-page spread (wide page with center gutter) ──
    gutter_x = _detect_gutter(gray, w)
    if gutter_x is not None:
        gutter_frac = gutter_x / w
        # Analyze left and right halves separately
        left_sections = _analyze_half(gray, binary, h, w, 0, gutter_frac, "L")
        right_sections = _analyze_half(gray, binary, h, w, gutter_frac, 1.0 - gutter_frac, "R")

        sections = left_sections + right_sections

        # Add gutter marker
        sections.append({
            "type": "page_number",
            "label": f"Gutter (2-page spread, split at {int(gutter_frac*100)}%)",
            "bbox": {"x0": gutter_frac - 0.005, "y0": 0, "x1": gutter_frac + 0.005, "y1": 1},
        })

        if not sections:
            sections.append({
                "type": "bitmap",
                "label": "2-page bitmap (no structure detected)",
                "bbox": {"x0": 0, "y0": 0, "x1": 1, "y1": 1},
            })
        return sections

    # ── Single-page analysis (original flow) ──
    sections = []

    # ── 1. Horizontal projection profile → find horizontal section bands ──
    # Invert: white background → 0, content → 255
    _, binary = cv2.threshold(gray, 240, 255, cv2.THRESH_BINARY_INV)

    # Row sums — how much "ink" per row
    row_sums = np.sum(binary, axis=1).astype(float)
    row_sums /= (w * 255)  # normalize to 0-1

    # Find horizontal gaps (whitespace bands) — threshold at 1% ink
    GAP_THRESH = 0.01
    MIN_GAP = int(h * 0.015)  # minimum gap height ~1.5% of page
    in_gap = False
    gap_start = 0
    gaps = []
    for y in range(h):
        if row_sums[y] < GAP_THRESH:
            if not in_gap:
                gap_start = y
                in_gap = True
        else:
            if in_gap:
                if y - gap_start >= MIN_GAP:
                    gaps.append((gap_start, y))
                in_gap = False
    if in_gap and h - gap_start >= MIN_GAP:
        gaps.append((gap_start, h))

    # Build section bands from gaps
    bands = []
    prev_end = 0
    for gs, ge in gaps:
        if gs > prev_end:
            bands.append((prev_end, gs))
        prev_end = ge
    if prev_end < h:
        bands.append((prev_end, h))

    # ── 2. Line detection → find table regions ──
    edges = cv2.Canny(gray, 50, 150, apertureSize=3)
    # Detect horizontal lines
    h_lines = cv2.HoughLinesP(edges, 1, np.pi / 180, threshold=80,
                              minLineLength=int(w * 0.2), maxLineGap=10)
    # Detect vertical lines
    v_lines = cv2.HoughLinesP(edges, 1, np.pi / 2, threshold=80,
                              minLineLength=int(h * 0.03), maxLineGap=10)

    # Collect horizontal line y-coordinates
    h_line_ys = set()
    if h_lines is not None:
        for line in h_lines:
            x1, y1, x2, y2 = line[0]
            if abs(y2 - y1) < 5:  # nearly horizontal
                h_line_ys.add((y1 + y2) // 2)

    # Collect vertical line x-coordinates
    v_line_xs = set()
    if v_lines is not None:
        for line in v_lines:
            x1, y1, x2, y2 = line[0]
            if abs(x2 - x1) < 5:  # nearly vertical
                v_line_xs.add((x1 + x2) // 2)

    # A band is likely a table if it has >=3 horizontal lines AND >=2 vertical lines within it
    def band_has_table(y0, y1):
        hl = sum(1 for y in h_line_ys if y0 <= y <= y1)
        # Check vertical lines spanning this band
        vl = 0
        if v_lines is not None:
            for line in v_lines:
                lx1, ly1, lx2, ly2 = line[0]
                if abs(lx2 - lx1) < 5:
                    mid_y = (ly1 + ly2) // 2
                    if y0 <= mid_y <= y1:
                        vl += 1
        return hl >= 3 and vl >= 2

    # ── 3. Classify each band ──
    for i, (y0, y1) in enumerate(bands):
        band_h_frac = (y1 - y0) / h
        center_y_frac = ((y0 + y1) / 2) / h

        # Analyze content density in band
        band_binary = binary[y0:y1, :]
        ink_ratio = np.sum(band_binary) / (band_binary.size * 255) if band_binary.size > 0 else 0

        # Column analysis: vertical projection within band
        col_sums = np.sum(band_binary, axis=0).astype(float) / ((y1 - y0) * 255)
        # How spread is the content?
        content_cols = np.sum(col_sums > 0.01)
        content_spread = content_cols / w if w > 0 else 0

        # Check for table
        is_table = band_has_table(y0, y1)

        # Classify
        bbox = {"x0": 0, "y0": y0 / h, "x1": 1, "y1": y1 / h}

        if is_table:
            sec_type = "dimension_table"
            label = f"Table region (lines detected)"
        elif center_y_frac < 0.15 and band_h_frac < 0.18:
            sec_type = "header"
            label = "Header (top band)"
        elif center_y_frac > 0.90 and band_h_frac < 0.10:
            sec_type = "footer"
            label = "Footer (bottom band)"
        elif band_h_frac < 0.02 or (ink_ratio < 0.02 and band_h_frac < 0.05):
            continue  # skip tiny or nearly empty bands
        elif band_h_frac > 0.25 and ink_ratio > 0.05 and content_spread > 0.4:
            # Large content area with spread = likely drawing
            sec_type = "drawing"
            label = f"Drawing ({int(band_h_frac*100)}% height)"
        elif is_table or (content_spread > 0.6 and band_h_frac > 0.15):
            # Wide structured content = likely table even without line detection
            sec_type = "dimension_table"
            label = "Data region (structured)"
        else:
            sec_type = "notes"
            label = f"Content band ({int(band_h_frac*100)}% height)"

        sections.append({
            "type": sec_type,
            "label": label,
            "bbox": bbox,
        })

    # ── 4. Refine: find sub-regions within large bands via contour analysis ──
    # Find large contours that might be drawings or logos
    contours, _ = cv2.findContours(binary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    for cnt in contours:
        area = cv2.contourArea(cnt)
        page_area_px = h * w
        if area < page_area_px * 0.01:
            continue  # skip small contours

        x, y, cw, ch = cv2.boundingRect(cnt)
        aspect = cw / ch if ch > 0 else 0
        area_frac = area / page_area_px

        # Large square-ish contour not already covered = drawing
        if area_frac > 0.05 and 0.3 < aspect < 3.0:
            bbox = {"x0": x / w, "y0": y / h, "x1": (x + cw) / w, "y1": (y + ch) / h}
            # Check if this overlaps an existing section significantly
            overlaps = False
            for sec in sections:
                sb = sec["bbox"]
                ox = max(0, min(bbox["x1"], sb["x1"]) - max(bbox["x0"], sb["x0"]))
                oy = max(0, min(bbox["y1"], sb["y1"]) - max(bbox["y0"], sb["y0"]))
                overlap_area = ox * oy
                bbox_area = (bbox["x1"] - bbox["x0"]) * (bbox["y1"] - bbox["y0"])
                if bbox_area > 0 and overlap_area / bbox_area > 0.5:
                    overlaps = True
                    break
            if not overlaps:
                # Small at corners = logo
                center_y_frac = (y + ch / 2) / h
                if area_frac < 0.04 and (center_y_frac < 0.12 or center_y_frac > 0.88):
                    sections.append({"type": "logo", "label": "Logo (contour)", "bbox": bbox})
                else:
                    sections.append({"type": "drawing", "label": f"Figure (contour, {int(area_frac*100)}%)", "bbox": bbox})

    # ── Merge caption bands into their parent drawing/table ──
    sections = _merge_caption_sections(sections)

    # ── Detect page numbers via contour analysis on bottom corners ──
    page_nums = _detect_page_numbers_bitmap(gray, h, w)
    for pn in page_nums:
        sections.append({
            "type": "page_number",
            "label": f"Page# ({pn['side']}, ~{pn['digit_count']} digits)",
            "bbox": pn["bbox"],
        })

    # If no sections found, mark entire page as bitmap
    if not sections:
        sections.append({
            "type": "bitmap",
            "label": "Bitmap page (no structure detected)",
            "bbox": {"x0": 0, "y0": 0, "x1": 1, "y1": 1},
        })

    return sections


def _classify_sections(page, page_rect):
    """Auto-detect layout sections on a PDF page.

    If the page is fully bitmapped (single image, no text), delegates
    to _classify_bitmap_sections() which uses OpenCV edge/line detection.

    Heuristics for text pages:
    - Top 15% with large font = header
    - Bottom 8% = footer / page_number
    - Tables = dimension_table
    - Large images (>20% of page area) = drawing
    - Small images in corners = logo
    - Remaining text blocks grouped by proximity = notes / specs
    """
    # Check if this is a bitmap page or a wide 2-page spread
    if _is_bitmap_page(page):
        return _classify_bitmap_sections(page, page_rect)

    # Wide pages (even with text) may be 2-page spreads — use bitmap analyzer
    if page_rect.width / page_rect.height > 1.25 and HAS_CV2:
        return _classify_bitmap_sections(page, page_rect)

    pw, ph = page_rect.width, page_rect.height
    sections = []

    # ── Tables → dimension_table ──
    table_rects = []
    try:
        tables_result = page.find_tables()
        for i, table in enumerate(tables_result.tables):
            bbox = table.bbox
            try:
                data = table.extract()
            except Exception:
                data = []
            rect = {"x0": bbox[0], "y0": bbox[1], "x1": bbox[2], "y1": bbox[3]}
            table_rects.append(rect)
            sections.append({
                "type": "dimension_table",
                "label": f"Table {i+1} ({len(data)}x{len(data[0]) if data else 0})",
                "bbox": {
                    "x0": bbox[0] / pw, "y0": bbox[1] / ph,
                    "x1": bbox[2] / pw, "y1": bbox[3] / ph,
                },
                "bbox_abs": {
                    "x0": round(bbox[0], 1), "y0": round(bbox[1], 1),
                    "x1": round(bbox[2], 1), "y1": round(bbox[3], 1),
                },
                "data": data[:10] if data else [],
                "rows": len(data),
                "cols": len(data[0]) if data else 0,
            })
    except Exception:
        pass

    # ── Images ──
    page_area = pw * ph
    for img_info in page.get_images(full=True):
        xref = img_info[0]
        rects = page.get_image_rects(xref)
        for rect in rects:
            img_area = (rect.x1 - rect.x0) * (rect.y1 - rect.y0)
            ratio = img_area / page_area if page_area > 0 else 0

            # Classify: large image = drawing, small in corner = logo
            if ratio < 0.005:
                continue  # skip tiny images (artifacts)

            center_y = (rect.y0 + rect.y1) / 2 / ph

            if ratio > 0.15:
                sec_type = "drawing"
                label = f"Drawing ({img_info[2]}x{img_info[3]})"
            elif ratio < 0.04 and (center_y < 0.15 or center_y > 0.85):
                sec_type = "logo"
                label = f"Logo ({img_info[2]}x{img_info[3]})"
            elif ratio > 0.05:
                sec_type = "drawing"
                label = f"Figure ({img_info[2]}x{img_info[3]})"
            else:
                sec_type = "drawing"
                label = f"Image ({img_info[2]}x{img_info[3]})"

            sections.append({
                "type": sec_type,
                "label": label,
                "bbox": {
                    "x0": rect.x0 / pw, "y0": rect.y0 / ph,
                    "x1": rect.x1 / pw, "y1": rect.y1 / ph,
                },
            })

    # ── Text blocks ──
    blocks = page.get_text("dict")["blocks"]
    text_blocks = []
    for block in blocks:
        if block["type"] != 0:
            continue
        bbox = block["bbox"]
        # Skip if inside a table rect
        bx0, by0, bx1, by1 = bbox
        in_table = False
        for tr in table_rects:
            if bx0 >= tr["x0"] - 2 and by0 >= tr["y0"] - 2 and bx1 <= tr["x1"] + 2 and by1 <= tr["y1"] + 2:
                in_table = True
                break
        if in_table:
            continue

        # Get max font size in block
        max_font = 0
        total_text = ""
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                max_font = max(max_font, span.get("size", 0))
                total_text += span.get("text", "") + " "
        total_text = total_text.strip()
        if not total_text:
            continue

        center_y = (by0 + by1) / 2 / ph
        block_height = (by1 - by0) / ph
        block_width = (bx1 - bx0) / pw

        text_blocks.append({
            "bbox": bbox,
            "text": total_text[:200],
            "max_font": max_font,
            "center_y": center_y,
            "block_height": block_height,
            "block_width": block_width,
        })

    # Classify text blocks
    for tb in text_blocks:
        bbox = tb["bbox"]
        sec_bbox = {
            "x0": bbox[0] / pw, "y0": bbox[1] / ph,
            "x1": bbox[2] / pw, "y1": bbox[3] / ph,
        }

        # Header: top 18% of page, large font or wide block
        if tb["center_y"] < 0.18 and (tb["max_font"] > 14 or tb["block_width"] > 0.5):
            sec_type = "header"
        # Footer: bottom 10%
        elif tb["center_y"] > 0.90:
            # Page number: very short text at bottom
            if len(tb["text"]) < 10 and any(c.isdigit() for c in tb["text"]):
                sec_type = "page_number"
            else:
                sec_type = "footer"
        # Specs: short blocks near tables with specific keywords
        elif any(kw in tb["text"].lower() for kw in ["load", "rating", "speed", "rpm", "grease", "oil", "tolerance", "clearance", "precision"]):
            sec_type = "specs"
        # Notes: remaining text blocks
        else:
            sec_type = "notes"

        sections.append({
            "type": sec_type,
            "label": f"{sec_type.replace('_', ' ').title()}: {tb['text'][:60]}",
            "bbox": sec_bbox,
            "text": tb["text"],
        })

    # ── Detect page numbers from text ──
    page_nums = _detect_page_numbers_text(page, page_rect)
    for pn in page_nums:
        # Check if this overlaps an existing page_number section
        already = any(s["type"] == "page_number" and abs(s["bbox"]["y0"] - pn["bbox"]["y0"]) < 0.05 for s in sections)
        if not already:
            sections.append({
                "type": "page_number",
                "label": f"Page {pn['number']} ({pn['side']})",
                "bbox": pn["bbox"],
            })

    return sections


@app.route("/api/catalog/layout-analyze")
def api_layout_analyze():
    """Auto-detect layout sections on a page."""
    filename = request.args.get("file")
    page_num = int(request.args.get("page", 0))

    if not filename:
        return jsonify({"error": "No file specified"}), 400

    pdf_path = CATALOGS_DIR / filename
    if not pdf_path.exists():
        return jsonify({"error": "File not found"}), 404

    doc = fitz.open(str(pdf_path))
    if page_num < 0 or page_num >= doc.page_count:
        doc.close()
        return jsonify({"error": "Page out of range"}), 400

    page = doc[page_num]
    page_rect = page.rect

    sections = _classify_sections(page, page_rect)

    doc.close()

    # Collect unique section types found
    found_types = sorted(set(s["type"] for s in sections))

    return jsonify({
        "page": page_num,
        "page_size": {"width": page_rect.width, "height": page_rect.height},
        "sections": sections,
        "section_types": found_types,
        "section_colors": SECTION_COLORS,
    })


@app.route("/api/catalog/table-recognize")
def api_table_recognize():
    """Deep table recognition: find tables, extract cell grid, link headings.

    For text pages: uses PyMuPDF find_tables() with cell-level data.
    For bitmap pages: uses OpenCV line detection to find cell grid.
    For each table, looks upward for the nearest text block as heading.
    """
    filename = request.args.get("file")
    page_num = int(request.args.get("page", 0))

    if not filename:
        return jsonify({"error": "No file specified"}), 400

    pdf_path = CATALOGS_DIR / filename
    if not pdf_path.exists():
        return jsonify({"error": "File not found"}), 404

    doc = fitz.open(str(pdf_path))
    if page_num < 0 or page_num >= doc.page_count:
        doc.close()
        return jsonify({"error": "Page out of range"}), 400

    page = doc[page_num]
    page_rect = page.rect
    pw, ph = page_rect.width, page_rect.height
    is_bitmap = _is_bitmap_page(page)
    is_spread = pw / ph > 1.25

    tables = []

    if not is_bitmap:
        # ── Text-based: PyMuPDF find_tables with cell boundaries ──
        tables_result = page.find_tables()
        for i, table in enumerate(tables_result.tables):
            bbox = table.bbox
            try:
                data = table.extract()
            except Exception:
                data = []

            # Get cell boundaries
            cells = []
            if hasattr(table, 'cells') and table.cells:
                for cell in table.cells:
                    cells.append({
                        "x0": cell[0] / pw, "y0": cell[1] / ph,
                        "x1": cell[2] / pw, "y1": cell[3] / ph,
                    })

            tables.append({
                "index": i,
                "bbox": {
                    "x0": bbox[0] / pw, "y0": bbox[1] / ph,
                    "x1": bbox[2] / pw, "y1": bbox[3] / ph,
                },
                "rows": len(data),
                "cols": len(data[0]) if data else 0,
                "data": data[:20],
                "cells": cells,
                "heading": None,
                "source": "pymupdf",
            })

        # ── Link headings: find text blocks above each table ──
        blocks = page.get_text("dict")["blocks"]
        text_blocks = []
        for block in blocks:
            if block["type"] != 0:
                continue
            bb = block["bbox"]
            text = " ".join(
                span["text"]
                for line in block.get("lines", [])
                for span in line.get("spans", [])
            ).strip()
            if text:
                max_font = max(
                    (span.get("size", 0) for line in block.get("lines", []) for span in line.get("spans", [])),
                    default=0,
                )
                text_blocks.append({
                    "bbox": {"x0": bb[0] / pw, "y0": bb[1] / ph, "x1": bb[2] / pw, "y1": bb[3] / ph},
                    "text": text[:200],
                    "font_size": max_font,
                })

        for tbl in tables:
            tb = tbl["bbox"]
            best_heading = None
            best_gap = 0.35  # max gap: 35% of page height (covers header→table distance)

            for txt in text_blocks:
                txb = txt["bbox"]
                # Text must be above the table
                gap = tb["y0"] - txb["y1"]
                if gap < 0 or gap > best_gap:
                    continue
                # Text must overlap horizontally with the table
                x_overlap = min(tb["x1"], txb["x1"]) - max(tb["x0"], txb["x0"])
                if x_overlap < 0.05:
                    continue
                # Prefer closest text block
                if gap < best_gap:
                    best_gap = gap
                    best_heading = txt

            if best_heading:
                tbl["heading"] = {
                    "text": best_heading["text"],
                    "font_size": best_heading["font_size"],
                    "bbox": best_heading["bbox"],
                }

    if (is_bitmap or is_spread) and HAS_CV2:
        # ── Bitmap/spread: OpenCV grid detection ──
        mat = fitz.Matrix(1.5, 1.5)
        pix = page.get_pixmap(matrix=mat)
        img_data = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
        if pix.n == 4:
            gray = cv2.cvtColor(img_data, cv2.COLOR_BGRA2GRAY)
        elif pix.n == 3:
            gray = cv2.cvtColor(img_data, cv2.COLOR_BGR2GRAY)
        else:
            gray = img_data

        h, w = gray.shape
        cv_tables = _detect_tables_opencv(gray, h, w)

        for i, cvt in enumerate(cv_tables):
            tables.append({
                "index": len(tables),
                "bbox": cvt["bbox"],
                "rows": cvt.get("rows", 0),
                "cols": cvt.get("cols", 0),
                "data": [],
                "cells": cvt.get("cells", []),
                "heading": cvt.get("heading"),
                "source": "opencv",
            })

    doc.close()

    return jsonify({
        "page": page_num,
        "tables": tables,
        "table_count": len(tables),
    })


def _detect_tables_opencv(gray, h, w):
    """Detect table grids in a bitmap image using OpenCV line detection.

    Returns list of tables with bbox, cells, and row/col counts.
    """
    tables = []

    # Edge detection
    edges = cv2.Canny(gray, 50, 150, apertureSize=3)

    # Morphological operations to enhance horizontal and vertical lines
    # Horizontal kernel
    h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (max(40, w // 15), 1))
    h_lines_img = cv2.morphologyEx(edges, cv2.MORPH_CLOSE, h_kernel)
    h_lines_img = cv2.dilate(h_lines_img, cv2.getStructuringElement(cv2.MORPH_RECT, (1, 3)))

    # Vertical kernel
    v_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, max(20, h // 30)))
    v_lines_img = cv2.morphologyEx(edges, cv2.MORPH_CLOSE, v_kernel)
    v_lines_img = cv2.dilate(v_lines_img, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 1)))

    # Combine H+V lines
    grid = cv2.bitwise_and(h_lines_img, v_lines_img)

    # Find intersection points (where H and V lines cross)
    # Also find the bounding boxes of grid regions
    combined = cv2.bitwise_or(h_lines_img, v_lines_img)

    # Find contours of the combined grid
    contours, _ = cv2.findContours(combined, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    for cnt in contours:
        area = cv2.contourArea(cnt)
        page_area = h * w
        if area < page_area * 0.02:
            continue  # skip small non-table regions

        x, y, cw, ch = cv2.boundingRect(cnt)

        # Must have reasonable table proportions
        if cw < w * 0.15 or ch < h * 0.05:
            continue

        # Count horizontal and vertical lines within this bounding box
        roi_h = h_lines_img[y:y+ch, x:x+cw]
        roi_v = v_lines_img[y:y+ch, x:x+cw]

        # Horizontal lines: find rows with high white pixel count
        h_proj = np.sum(roi_h, axis=1) / 255
        h_threshold = cw * 0.2
        h_line_rows = []
        in_line = False
        for row_idx in range(len(h_proj)):
            if h_proj[row_idx] > h_threshold:
                if not in_line:
                    h_line_rows.append(row_idx)
                    in_line = True
            else:
                in_line = False

        # Vertical lines: find cols with high white pixel count
        v_proj = np.sum(roi_v, axis=0) / 255
        v_threshold = ch * 0.15
        v_line_cols = []
        in_line = False
        for col_idx in range(len(v_proj)):
            if v_proj[col_idx] > v_threshold:
                if not in_line:
                    v_line_cols.append(col_idx)
                    in_line = True
            else:
                in_line = False

        if len(h_line_rows) < 2 or len(v_line_cols) < 2:
            continue  # not a table

        # Build cell grid from line positions
        cells = []
        for ri in range(len(h_line_rows) - 1):
            for ci in range(len(v_line_cols) - 1):
                cells.append({
                    "x0": (x + v_line_cols[ci]) / w,
                    "y0": (y + h_line_rows[ri]) / h,
                    "x1": (x + v_line_cols[ci + 1]) / w,
                    "y1": (y + h_line_rows[ri + 1]) / h,
                })

        # Look for heading text above table (within bitmap: check for content band)
        heading = None
        heading_region = gray[max(0, y - int(h * 0.06)):y, x:x+cw]
        if heading_region.size > 0:
            _, heading_bin = cv2.threshold(heading_region, 200, 255, cv2.THRESH_BINARY_INV)
            ink = np.sum(heading_bin) / (heading_bin.size * 255) if heading_bin.size > 0 else 0
            if ink > 0.01:  # has content above
                heading = {
                    "text": "(bitmap heading detected)",
                    "bbox": {
                        "x0": x / w, "y0": max(0, y - int(h * 0.06)) / h,
                        "x1": (x + cw) / w, "y1": y / h,
                    },
                }

        tables.append({
            "bbox": {"x0": x / w, "y0": y / h, "x1": (x + cw) / w, "y1": (y + ch) / h},
            "rows": max(0, len(h_line_rows) - 1),
            "cols": max(0, len(v_line_cols) - 1),
            "cells": cells,
            "heading": heading,
        })

    return tables


@app.route("/api/catalog/extract-region")
def api_extract_region():
    """Extract text from a user-defined region on a page."""
    filename = request.args.get("file")
    page_num = int(request.args.get("page", 0))
    # Coordinates as fractions of page size (0-1)
    x0 = float(request.args.get("x0", 0))
    y0 = float(request.args.get("y0", 0))
    x1 = float(request.args.get("x1", 1))
    y1 = float(request.args.get("y1", 1))

    if not filename:
        return jsonify({"error": "No file specified"}), 400

    pdf_path = CATALOGS_DIR / filename
    if not pdf_path.exists():
        return jsonify({"error": "File not found"}), 404

    doc = fitz.open(str(pdf_path))
    page = doc[page_num]
    rect = page.rect

    # Convert fractional coords to absolute
    clip = fitz.Rect(
        x0 * rect.width,
        y0 * rect.height,
        x1 * rect.width,
        y1 * rect.height,
    )

    text = page.get_text("text", clip=clip)

    # Try table extraction in region
    tables_result = page.find_tables(clip=clip)
    table_data = []
    for table in tables_result.tables:
        try:
            table_data.append(table.extract())
        except Exception:
            pass

    doc.close()

    return jsonify({
        "text": text,
        "tables": table_data,
        "region": {"x0": x0, "y0": y0, "x1": x1, "y1": y1},
    })


if __name__ == "__main__":
    print(f"Catalogs dir: {CATALOGS_DIR}")
    print(f"Found {len(list(CATALOGS_DIR.glob('*.pdf')))} PDF catalogs")
    print(f"Starting Catalog Extractor UI at http://localhost:5050")
    app.run(host="127.0.0.1", port=5050, debug=True)
