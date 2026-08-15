"""Reusable table line detection extracted from PreviewView._extract_table_lines.

Algorithm is locked — do not change the logic (see project memory:
scanline/grow and border detection). Verbatim port of the body that used to
live at app/ui/preview_view.py:8858-8932.
"""

from collections import defaultdict

import fitz


def extract_table_lines(
    page, table_rect: "fitz.Rect",
) -> tuple[list[tuple[float, float, float]], list[tuple[float, float, float]]]:
    """Extract exact horizontal and vertical line segments from PDF drawings.

    Returns:
      h_segments: list of (x0, x1, y) — horizontal line segments
      v_segments: list of (x, y0, y1) — vertical line segments

    Filters out drawing/icon noise by grouping segments by position
    and only keeping positions with significant total coverage.
    """
    tr = table_rect
    table_w = tr.x1 - tr.x0
    table_h = tr.y1 - tr.y0

    # Collect all raw segments
    raw_h: dict[float, list[tuple[float, float]]] = defaultdict(list)
    raw_v: dict[float, list[tuple[float, float]]] = defaultdict(list)

    for d in page.get_drawings():
        for item in d.get("items", []):
            if item[0] != "l":
                continue
            p1, p2 = item[1], item[2]
            dx = abs(p1.x - p2.x)
            dy = abs(p1.y - p2.y)

            # Horizontal segment
            if dy < 2 and dx > 5:
                y = (p1.y + p2.y) / 2
                if y < tr.y0 - 5 or y > tr.y1 + 5:
                    continue
                x0, x1 = min(p1.x, p2.x), max(p1.x, p2.x)
                if x0 > tr.x1 + 5 or x1 < tr.x0 - 5:
                    continue
                ry = round(y * 2) / 2
                raw_h[ry].append((x0, x1))

            # Vertical segment
            elif dx < 2 and dy > 3:
                x = (p1.x + p2.x) / 2
                if x < tr.x0 - 5 or x > tr.x1 + 5:
                    continue
                y0, y1 = min(p1.y, p2.y), max(p1.y, p2.y)
                if y0 > tr.y1 + 5 or y1 < tr.y0 - 5:
                    continue
                rx = round(x * 2) / 2
                raw_v[rx].append((y0, y1))

    # Filter H: keep Y positions where longest segment >= 10% of table width
    h_segs: list[tuple[float, float, float]] = []
    for ry in sorted(raw_h.keys()):
        segs = raw_h[ry]
        max_w = max(s[1] - s[0] for s in segs)
        if max_w >= table_w * 0.1:
            for x0, x1 in segs:
                h_segs.append((x0, x1, ry))

    # Filter V: cluster nearby X (within 3pt), keep tight clusters
    # (spread < 5pt) where total height >= 30% of table height
    v_segs: list[tuple[float, float, float]] = []
    sorted_rxs = sorted(raw_v.keys())
    if sorted_rxs:
        x_clusters: list[list[float]] = [[sorted_rxs[0]]]
        for rx in sorted_rxs[1:]:
            if rx - x_clusters[-1][-1] < 3:
                x_clusters[-1].append(rx)
            else:
                x_clusters.append([rx])

        for cluster_xs in x_clusters:
            # Reject wide clusters — real borders are tight
            if cluster_xs[-1] - cluster_xs[0] > 5:
                continue
            all_segs = []
            for rx in cluster_xs:
                all_segs.extend(raw_v[rx])
            total_h = sum(s[1] - s[0] for s in all_segs)
            if total_h >= table_h * 0.3:
                for rx in cluster_xs:
                    for y0, y1 in raw_v[rx]:
                        v_segs.append((rx, y0, y1))

    return h_segs, v_segs


def column_bounds(
    v_segments: list[tuple[float, float, float]], merge_pt: float = 3.0
) -> list[float]:
    """Cluster vertical segment x-positions into column border coordinates."""
    xs = sorted({s[0] for s in v_segments})
    if not xs:
        return []
    clusters: list[list[float]] = [[xs[0]]]
    for x in xs[1:]:
        if x - clusters[-1][-1] <= merge_pt:
            clusters[-1].append(x)
        else:
            clusters.append([x])
    return [sum(c) / len(c) for c in clusters]
