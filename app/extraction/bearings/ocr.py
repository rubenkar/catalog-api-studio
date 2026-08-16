"""OCR fallback for scanned catalog pages (PaddleOCR, lazy import)."""

import json
import logging
import os
from pathlib import Path

import fitz

from .pagetext import Word

logger = logging.getLogger(__name__)

_engine = None


def _get_engine():
    global _engine
    if _engine is None:
        os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")
        from paddleocr import PaddleOCR

        logger.info("Initializing PaddleOCR engine (first use)")
        _engine = PaddleOCR(lang="en", use_textline_orientation=False)
    return _engine


def result_to_words(
    rec_texts,
    rec_polys,
    scale: float,
    rec_scores=None,
    min_score: float = 0.5,
) -> list[Word]:
    """Перевод распознанных областей PaddleOCR (пиксели) в Word-кортежи (pt PDF)."""
    words: list[Word] = []
    for i, (text, poly) in enumerate(zip(rec_texts, rec_polys)):
        text = str(text).strip()
        if not text:
            continue
        if rec_scores is not None and rec_scores[i] < min_score:
            continue
        xs = [float(p[0]) for p in poly]
        ys = [float(p[1]) for p in poly]
        words.append(
            (min(xs) * scale, min(ys) * scale, max(xs) * scale, max(ys) * scale,
             text, 0, 0, i)
        )
    return sorted(words, key=lambda w: (round(w[1], 1), w[0]))


def ocr_page_words(
    page: fitz.Page, dpi: int = 300, cache_dir: Path | None = None
) -> list[Word]:
    """OCR одной страницы; результат кэшируется на диск (p<no>.json)."""
    cache_file = None
    if cache_dir is not None:
        cache_file = cache_dir / f"p{page.number + 1}.json"
        if cache_file.exists():
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            return [tuple(w) for w in data]

    import numpy as np

    pix = page.get_pixmap(dpi=dpi)
    img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    if pix.n == 4:
        img = img[:, :, :3]
    scale = 72.0 / dpi
    words: list[Word] = []
    for res in _get_engine().predict(img):
        words.extend(
            result_to_words(
                res["rec_texts"], res["rec_polys"], scale,
                rec_scores=res.get("rec_scores"),
            )
        )
    logger.info("OCR page %d: %d words", page.number + 1, len(words))

    if cache_file is not None:
        cache_dir.mkdir(parents=True, exist_ok=True)
        tmp = cache_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(words), encoding="utf-8")
        os.replace(tmp, cache_file)
    return words
