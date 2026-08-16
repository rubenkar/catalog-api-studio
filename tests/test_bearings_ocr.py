"""OCR fallback: конвертация результата PaddleOCR в Word-кортежи, кэш, интеграция."""

import json

import fitz

from app.extraction.bearings.ocr import ocr_page_words, result_to_words


def test_result_to_words_converts_and_scales():
    # координаты в пикселях при dpi=300 -> pt PDF (scale = 72/300 = 0.24)
    rec_texts = ["6205", "", "25"]
    rec_polys = [
        [[100, 200], [200, 200], [200, 250], [100, 250]],
        [[0, 0], [10, 0], [10, 10], [0, 10]],           # пустой текст — пропустить
        [[300, 200], [350, 200], [350, 250], [300, 250]],
    ]
    words = result_to_words(rec_texts, rec_polys, scale=0.24)
    assert len(words) == 2
    x0, y0, x1, y1, text = words[0][:5]
    assert (x0, y0, x1, y1) == (24.0, 48.0, 48.0, 60.0)
    assert text == "6205"
    assert words[1][4] == "25"


def test_result_to_words_skips_low_score():
    rec_texts = ["good", "noise"]
    rec_polys = [
        [[0, 0], [50, 0], [50, 10], [0, 10]],
        [[0, 20], [50, 20], [50, 30], [0, 30]],
    ]
    words = result_to_words(rec_texts, rec_polys, scale=1.0, rec_scores=[0.9, 0.3])
    assert [w[4] for w in words] == ["good"]


def test_ocr_page_words_reads_cache_without_engine(tmp_path):
    """Если кэш страницы существует — движок OCR не нужен вообще."""
    cache_dir = tmp_path / "ocr_cache"
    cache_dir.mkdir()
    cached = [[10.0, 20.0, 30.0, 28.0, "6205", 0, 0, 0]]
    (cache_dir / "p1.json").write_text(json.dumps(cached), encoding="utf-8")

    doc = fitz.open()
    page = doc.new_page(width=595, height=842)  # page.number == 0 -> p1
    words = ocr_page_words(page, cache_dir=cache_dir)
    doc.close()

    assert words == [(10.0, 20.0, 30.0, 28.0, "6205", 0, 0, 0)]


def test_page_data_uses_ocr_fallback_for_scanned_pages():
    """_page_data: страница без текстового слоя получает слова через ocr_fn."""
    from app.extraction.bearings.cli import _page_data

    doc = fitz.open()
    doc.new_page(width=595, height=842)  # «скан»: ни одного слова

    def fake_ocr(page):
        words = []
        y = 100.0
        for i in range(12):
            words.append((50.0, y, 90.0, y + 10, f"62{i:02d}", 0, 0, 0))
            words.append((150.0, y, 180.0, y + 10, str(20 + i), 0, 0, 1))
            words.append((220.0, y, 250.0, y + 10, str(47 + i), 0, 0, 2))
            words.append((290.0, y, 310.0, y + 10, "14", 0, 0, 3))
            y += 20
        return words

    pages = _page_data(doc, ocr_fn=fake_ocr)
    doc.close()

    assert 1 in pages
    assert len(pages[1]["words"]) == 48
    assert "6200[50]" in pages[1]["text"]
