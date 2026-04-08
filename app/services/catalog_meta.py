"""Per-catalog JSON metadata: objects with stable IDs, view settings, and technical info."""

import json
import logging
from pathlib import Path

from app.config.settings import settings

logger = logging.getLogger(__name__)

META_DIR = settings.data_dir / "catalog_meta"
META_DIR.mkdir(parents=True, exist_ok=True)


def _meta_path(catalog_path: Path) -> Path:
    """Return JSON metadata path for a given catalog PDF."""
    return META_DIR / f"{catalog_path.stem}.json"


def load_meta(catalog_path: Path) -> dict:
    """Load catalog metadata, returning defaults if file doesn't exist."""
    path = _meta_path(catalog_path)
    if path.exists():
        try:
            meta = json.loads(path.read_text(encoding="utf-8"))
            # Migrate legacy format
            meta = _migrate(meta)
            return meta
        except Exception as e:
            logger.error("Failed to load catalog meta %s: %s", path, e)
    return {
        "catalog": catalog_path.name,
        "next_id": 1,
        "objects": [],
        "templates": [],
        "template_config": {},
        "metadata": {},
    }


def _migrate(meta: dict) -> dict:
    """Migrate legacy hidden_objects/type_overrides to unified objects list."""
    meta.setdefault("templates", [])
    meta.setdefault("template_config", {})
    if "objects" in meta:
        return meta

    next_id = 1
    objects = []

    for h in meta.pop("hidden_objects", []):
        objects.append({
            "id": f"obj_{next_id}",
            "page": h["page"],
            "type": h.get("type", "unknown"),
            "label": h.get("label", ""),
            "pts": h.get("pts", []),
            "hidden": True,
        })
        next_id += 1

    for ov in meta.pop("type_overrides", []):
        # Check if already added from hidden
        found = False
        for obj in objects:
            if obj["page"] == ov["page"] and obj["pts"] == ov["pts"]:
                obj["type"] = ov["type"]
                found = True
                break
        if not found:
            objects.append({
                "id": f"obj_{next_id}",
                "page": ov["page"],
                "type": ov["type"],
                "label": "",
                "pts": ov["pts"],
                "hidden": False,
            })
            next_id += 1

    meta["objects"] = objects
    meta["next_id"] = next_id
    return meta


def save_meta(catalog_path: Path, meta: dict) -> None:
    """Save catalog metadata to JSON."""
    path = _meta_path(catalog_path)
    meta.setdefault("catalog", catalog_path.name)
    try:
        path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.error("Failed to save catalog meta %s: %s", path, e)


def _find_obj(meta: dict, obj_id: str) -> dict | None:
    """Find object by ID."""
    for obj in meta.get("objects", []):
        if obj["id"] == obj_id:
            return obj
    return None


def _iou(a: list, b: list) -> float:
    """Intersection-over-union for two [x0, y0, x1, y1] rects."""
    ix0 = max(a[0], b[0])
    iy0 = max(a[1], b[1])
    ix1 = min(a[2], b[2])
    iy1 = min(a[3], b[3])
    if ix1 <= ix0 or iy1 <= iy0:
        return 0.0
    inter = (ix1 - ix0) * (iy1 - iy0)
    area_a = (a[2] - a[0]) * (a[3] - a[1])
    area_b = (b[2] - b[0]) * (b[3] - b[1])
    union = area_a + area_b - inter
    return inter / union if union > 0 else 0.0


def merge_detected(catalog_path: Path, page: int, detected: list[dict]) -> list[dict]:
    """Merge newly detected bboxes with saved objects for a page.

    - Match by IoU overlap (>=0.3) — preserve ID, hidden, user overrides.
    - Unmatched saved objects on this page are kept (user may have positioned them).
    - New detections get fresh IDs.
    - Returns merged bbox list with 'id' assigned to each.
    """
    meta = load_meta(catalog_path)
    saved = [o for o in meta.get("objects", []) if o["page"] == page]
    other = [o for o in meta.get("objects", []) if o["page"] != page]
    next_id = meta.get("next_id", 1)

    matched_saved_ids: set[str] = set()
    result: list[dict] = []

    for det in detected:
        det_pts = list(det.get("pts", []))
        best_match = None
        best_iou = 0.3  # threshold

        for sv in saved:
            if sv["id"] in matched_saved_ids:
                continue
            iou = _iou(det_pts, sv["pts"])
            if iou > best_iou:
                best_iou = iou
                best_match = sv

        if best_match:
            # Matched: keep ID and user modifications, update detected geometry
            matched_saved_ids.add(best_match["id"])
            det["id"] = best_match["id"]
            det["hidden"] = best_match.get("hidden", False)
            # If user overrode type, keep it
            if best_match.get("user_type"):
                det["type"] = best_match["user_type"]
                det["user_type"] = best_match["user_type"]
            # If user moved/resized, keep user pts
            if best_match.get("user_pts"):
                det["pts"] = tuple(best_match["user_pts"])
                det["user_pts"] = best_match["user_pts"]
            result.append(det)
        else:
            # New detection
            det["id"] = f"obj_{next_id}"
            det["hidden"] = False
            next_id += 1
            result.append(det)

    # Keep unmatched saved objects (user-positioned or from previous sessions)
    for sv in saved:
        if sv["id"] not in matched_saved_ids:
            result.append({
                "id": sv["id"],
                "type": sv.get("user_type") or sv.get("type", "unknown"),
                "label": sv.get("label", ""),
                "pts": tuple(sv.get("user_pts") or sv.get("pts", [])),
                "hidden": sv.get("hidden", False),
                "user_type": sv.get("user_type"),
                "user_pts": sv.get("user_pts"),
            })

    # Save updated objects back
    save_objects = []
    for obj in result:
        save_objects.append({
            "id": obj["id"],
            "page": page,
            "type": obj.get("user_type") or obj.get("type", "unknown"),
            "label": obj.get("label", ""),
            "pts": list(obj.get("user_pts") or obj.get("pts", [])),
            "hidden": obj.get("hidden", False),
            "user_type": obj.get("user_type"),
            "user_pts": list(obj["user_pts"]) if obj.get("user_pts") else None,
        })

    meta["objects"] = other + save_objects
    meta["next_id"] = next_id
    save_meta(catalog_path, meta)

    return result


def update_object(catalog_path: Path, obj_id: str, **fields) -> None:
    """Update specific fields on a saved object by ID."""
    meta = load_meta(catalog_path)
    obj = _find_obj(meta, obj_id)
    if not obj:
        logger.warning("Object %s not found in meta", obj_id)
        return
    for k, v in fields.items():
        obj[k] = v
    save_meta(catalog_path, meta)


def get_object(meta: dict, obj_id: str) -> dict | None:
    """Get object from loaded meta by ID."""
    return _find_obj(meta, obj_id)
