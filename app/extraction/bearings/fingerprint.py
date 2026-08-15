"""Structural page fingerprint: histogram of word x-positions, quantized."""

from collections import defaultdict


def page_fingerprint(
    words: list[tuple],
    page_width: float,
    v_xs: list[float] | None = None,
    bins: int = 24,
) -> str:
    if not words or page_width <= 0:
        return "empty"
    hist = [0] * bins
    for w in words:
        idx = max(0, min(bins - 1, int(w[0] / page_width * bins)))
        hist[idx] += 1
    peak = max(hist)
    if peak == 0:
        return "empty"
    # quantize each bin to 0..3 relative to the page's own peak
    sig = "".join(str(min(3, h * 4 // (peak + 1))) for h in hist)
    if v_xs:
        border_bins = sorted({max(0, min(bins - 1, int(x / page_width * bins))) for x in v_xs})
        sig += "|" + ",".join(str(b) for b in border_bins)
    return sig


def cluster_pages(fingerprints: dict[int, str]) -> dict[str, list[int]]:
    clusters: dict[str, list[int]] = defaultdict(list)
    for page_no in sorted(fingerprints):
        clusters[fingerprints[page_no]].append(page_no)
    return dict(clusters)
