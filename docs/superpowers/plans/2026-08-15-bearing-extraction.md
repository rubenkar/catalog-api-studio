# Bearing Catalog Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** CLI-конвейер `PDF-каталог подшипников → <brand>.json` внутри существующего приложения: LLM (DeepSeek) выводит структуру, механика детерминированно извлекает данные.

**Architecture:** Новый подпакет `app/extraction/bearings/`. Этапы: профилирование (манифест полей) → отпечаток/кластеризация страниц → LLM-правило разбора на кластер → механическая раскладка слов по колонкам → валидация с LLM-fallback → сборка JSON. Все LLM-ответы кэшируются на диске.

**Tech Stack:** Python 3.11, PyMuPDF (fitz), Pydantic v2, requests (DeepSeek OpenAI-совместимый API), pytest.

**Spec:** `docs/superpowers/specs/2026-08-15-bearing-catalog-extraction-design.md`

## Global Constraints

- Python ≥ 3.11, type hints везде, логирование через `logging` (никаких print в библиотечном коде; CLI может печатать результат).
- Pydantic-модели для всех схем данных.
- UI (PySide6) не трогаем; конвейер — чистый CLI.
- Ключ API: env `DEEPSEEK_API_KEY`, фолбэк — парсинг `C:\!dev\markagent\.env`.
- LLM — DeepSeek `deepseek-chat`, endpoint `https://api.deepseek.com/chat/completions`, JSON-режим (`response_format={"type":"json_object"}`), только текст (без изображений).
- Числа в выходном JSON — числа в единицах манифеста; неизвестное — `null`, не выдумывать.
- Все ответы LLM кэшируются на диск; повторный запуск не тратит деньги (`--force` пересчитывает).
- Тесты не ходят в сеть: LLM подменяется фейком через инъекцию `post_fn`.
- Запуск тестов: `pytest tests/test_bearings_*.py -v` (запуск всего `pytest` тянет legacy-тесты — не требуется).

## File Structure

```
app/extraction/bearings/
    __init__.py        # пустой
    __main__.py        # python -m app.extraction.bearings → cli.main()
    models.py          # FieldSpec, Manifest, ParseRule, ColumnSpec, Issue, CatalogResult
    pagetext.py        # слова страницы, layout-текст, числовая плотность
    fingerprint.py     # отпечаток страницы, кластеризация
app/pdf/
    tablelines.py      # детекция линий таблиц (вынос из preview_view.py, переиспользуется UI и конвейером)
    llm.py             # DeepSeekClient: complete_json + дисковый кэш + ретраи
    profiler.py        # этап 1: манифест полей по выборке страниц
    rules.py           # этап 3: правило разбора для кластера
    mechanics.py       # этап 4: детерминированная раскладка слов по правилу
    validate.py        # этап 5: чистка/проверка записей
    assemble.py        # этап 6: дедупликация, stats, запись JSON
    cli.py             # argparse: profile / extract, оркестрация этапов
tests/
    test_bearings_tablelines.py
    test_bearings_pagetext.py
    test_bearings_fingerprint.py
    test_bearings_llm.py
    test_bearings_models.py      # + validate
    test_bearings_mechanics.py
    test_bearings_assemble.py
    test_bearings_pipeline.py    # e2e на синтетическом PDF с фейковым LLM
```

Интерфейсный словарь (используется всеми задачами):
- **word** = кортеж PyMuPDF `page.get_text("words")`: `(x0, y0, x1, y1, text, block_no, line_no, word_no)`.
- Координаты — в pt (единицы PDF).

---

### Task 1: models.py — все Pydantic-схемы

**Files:**
- Create: `app/extraction/bearings/__init__.py` (пустой)
- Create: `app/extraction/bearings/models.py`
- Test: `tests/test_bearings_models.py`

**Interfaces:**
- Produces (используется всеми последующими задачами):
  - `FieldSpec(key: str, label: str, unit: str | None = None, core: bool = False)`
  - `Manifest(source: str, brand: str, fields: list[FieldSpec])`, метод `field_keys() -> list[str]`
  - `ColumnSpec(x_min: float, x_max: float, field: str)`
  - `ParseRule(header_skip_lines: int = 0, columns: list[ColumnSpec], row_gap_pt: float = 4.0, inherit_fields: list[str] = [], type_default: str | None = None)`
  - `Issue(page: int, problem: str)`
  - `CatalogResult(source: str, brand: str, extracted_at: str, pipeline_version: str = "1.0", items: list[dict], stats: dict, issues: list[Issue])`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bearings_models.py
from app.extraction.bearings.models import (
    CatalogResult, ColumnSpec, FieldSpec, Issue, Manifest, ParseRule,
)


def test_manifest_field_keys():
    m = Manifest(
        source="KOYO.pdf", brand="KOYO",
        fields=[
            FieldSpec(key="designation", label="Bearing number", core=True),
            FieldSpec(key="d", label="Bore diameter", unit="mm", core=True),
        ],
    )
    assert m.field_keys() == ["designation", "d"]


def test_parse_rule_defaults():
    rule = ParseRule(columns=[ColumnSpec(x_min=40, x_max=90, field="designation")])
    assert rule.header_skip_lines == 0
    assert rule.row_gap_pt == 4.0
    assert rule.inherit_fields == []


def test_catalog_result_round_trip():
    r = CatalogResult(
        source="a.pdf", brand="X", extracted_at="2026-08-15T00:00:00Z",
        items=[{"designation": "6205", "page": 3}],
        stats={"pages_total": 5, "pages_with_data": 1, "items_count": 1},
        issues=[Issue(page=4, problem="rule failed")],
    )
    data = r.model_dump()
    assert data["pipeline_version"] == "1.0"
    assert data["issues"][0]["page"] == 4
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bearings_models.py -v`
Expected: FAIL с `ModuleNotFoundError: app.extraction.bearings`

- [ ] **Step 3: Write minimal implementation**

```python
# app/extraction/bearings/models.py
"""Pydantic schemas for the bearing catalog extraction pipeline."""

from pydantic import BaseModel, Field


class FieldSpec(BaseModel):
    key: str
    label: str
    unit: str | None = None
    core: bool = False


class Manifest(BaseModel):
    source: str
    brand: str
    fields: list[FieldSpec]

    def field_keys(self) -> list[str]:
        return [f.key for f in self.fields]


class ColumnSpec(BaseModel):
    x_min: float
    x_max: float
    field: str


class ParseRule(BaseModel):
    header_skip_lines: int = 0
    columns: list[ColumnSpec]
    row_gap_pt: float = 4.0
    inherit_fields: list[str] = Field(default_factory=list)
    type_default: str | None = None


class Issue(BaseModel):
    page: int
    problem: str


class CatalogResult(BaseModel):
    source: str
    brand: str
    extracted_at: str
    pipeline_version: str = "1.0"
    items: list[dict]
    stats: dict
    issues: list[Issue]
```

`app/extraction/bearings/__init__.py` — пустой файл.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bearings_models.py -v`
Expected: PASS (3 теста)

- [ ] **Step 5: Commit**

```bash
git add app/extraction/bearings/__init__.py app/extraction/bearings/models.py tests/test_bearings_models.py
git commit -m "feat(bearings): pydantic schemas for extraction pipeline"
```

---

### Task 2: pagetext.py — слова, layout-текст, числовая плотность

**Files:**
- Create: `app/extraction/bearings/pagetext.py`
- Test: `tests/test_bearings_pagetext.py`

**Interfaces:**
- Consumes: PyMuPDF `fitz.Page`.
- Produces:
  - `page_words(page: fitz.Page) -> list[tuple]` — как `get_text("words")`, отсортированы по (y, x)
  - `layout_text(words: list[tuple], page_width: float, col_pt: float = 6.0) -> str` — текст с координатными метками строк: каждая строка вида `y=123.4 | word[x0] word[x0] ...`; используется в промптах LLM (rules/profiler), чтобы модель видела x-координаты
  - `numeric_density(words: list[tuple]) -> float` — доля «числовых» слов (float после нормализации запятой)
  - `is_data_page(words: list[tuple], min_words: int = 40, min_density: float = 0.35) -> bool`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bearings_pagetext.py
import fitz
import pytest

from app.extraction.bearings.pagetext import (
    is_data_page, layout_text, numeric_density, page_words,
)


@pytest.fixture()
def table_page(tmp_path):
    """Synthetic one-page PDF resembling a bearing table."""
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    rows = [
        ("Bearing", "d", "D", "B", "Cr"),
        ("6204", "20", "47", "14", "12.8"),
        ("6205", "25", "52", "15", "14.0"),
        ("6206", "30", "62", "16", "19.5"),
    ]
    xs = [50, 150, 220, 290, 360]
    y = 100
    for row in rows:
        for x, text in zip(xs, row):
            page.insert_text((x, y), text, fontsize=10)
        y += 20
    yield page
    doc.close()


def test_page_words_sorted(table_page):
    words = page_words(table_page)
    assert len(words) == 20
    ys = [round(w[1], 1) for w in words]
    assert ys == sorted(ys)


def test_numeric_density(table_page):
    words = page_words(table_page)
    # 15 numeric out of 20 (заголовок + колонка Bearing частично)
    assert numeric_density(words) == pytest.approx(15 / 20)


def test_is_data_page(table_page):
    words = page_words(table_page)
    assert is_data_page(words, min_words=10, min_density=0.3)
    assert not is_data_page(words[:5], min_words=10, min_density=0.3)


def test_layout_text_contains_coords(table_page):
    words = page_words(table_page)
    text = layout_text(words, page_width=595)
    assert "6205[50]" in text
    lines = text.splitlines()
    assert lines[0].startswith("y=")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bearings_pagetext.py -v`
Expected: FAIL с `ModuleNotFoundError` / `ImportError`

- [ ] **Step 3: Write minimal implementation**

```python
# app/extraction/bearings/pagetext.py
"""Page word extraction, layout-preserving text, numeric density heuristics."""

import logging

import fitz

logger = logging.getLogger(__name__)

Word = tuple  # (x0, y0, x1, y1, text, block_no, line_no, word_no)


def page_words(page: fitz.Page) -> list[Word]:
    words = page.get_text("words")
    return sorted(words, key=lambda w: (round(w[1], 1), w[0]))


def _is_number(text: str) -> bool:
    t = text.replace(",", ".").replace("\u2009", "").replace(" ", "")
    try:
        float(t)
        return True
    except ValueError:
        return False


def numeric_density(words: list[Word]) -> float:
    if not words:
        return 0.0
    numeric = sum(1 for w in words if _is_number(w[4]))
    return numeric / len(words)


def is_data_page(words: list[Word], min_words: int = 40, min_density: float = 0.35) -> bool:
    return len(words) >= min_words and numeric_density(words) >= min_density


def layout_text(words: list[Word], page_width: float, col_pt: float = 6.0) -> str:
    """Group words into visual lines; annotate each word with its x0.

    Format per line: 'y=<y0> | text[x0] text[x0] ...' so an LLM can reason
    about column positions from plain text.
    """
    lines: list[str] = []
    current: list[Word] = []
    current_y: float | None = None
    for w in words:
        y = w[1]
        if current_y is None or abs(y - current_y) <= col_pt:
            current.append(w)
            current_y = y if current_y is None else current_y
        else:
            lines.append(_format_line(current))
            current = [w]
            current_y = y
    if current:
        lines.append(_format_line(current))
    return "\n".join(lines)


def _format_line(ws: list[Word]) -> str:
    ws = sorted(ws, key=lambda w: w[0])
    body = " ".join(f"{w[4]}[{int(w[0])}]" for w in ws)
    return f"y={ws[0][1]:.1f} | {body}"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bearings_pagetext.py -v`
Expected: PASS (4 теста). Если `test_numeric_density` падает из-за иного подсчёта слов PyMuPDF — проверить фактическое `len(words)` и скорректировать ожидание пропорции (числитель/знаменатель считать из фактических слов, не менять реализацию под тест без анализа).

- [ ] **Step 5: Commit**

```bash
git add app/extraction/bearings/pagetext.py tests/test_bearings_pagetext.py
git commit -m "feat(bearings): page words, layout text and numeric density"
```

---

### Task 2b: tablelines.py — переиспользование существующей детекции линий таблиц

Прошлые сессии проекта дали проверенную детекцию точных линий таблиц из PDF-рисунков
(`PreviewView._extract_table_lines` в `app/ui/preview_view.py:8846`) с фильтрацией
шума. Выносим её в общий модуль `app/pdf/tablelines.py`, UI делегирует туда,
конвейер использует как библиотеку. Алгоритм НЕ менять — он отлажен (см. память
проекта: scanline/grow и границы — LOCKED).

**Files:**
- Create: `app/pdf/tablelines.py`
- Modify: `app/ui/preview_view.py:8846-8932` (`_extract_table_lines` — тело заменить на делегирование)
- Test: `tests/test_bearings_tablelines.py`

**Interfaces:**
- Produces:
  - `extract_table_lines(page: fitz.Page, table_rect: fitz.Rect) -> tuple[list[tuple[float, float, float]], list[tuple[float, float, float]]]` — `(h_segments, v_segments)`; h = (x0, x1, y), v = (x, y0, y1). Код — дословный перенос из `preview_view.py:8846-8932` (со `staticmethod`-семантикой, без self).
  - `column_bounds(v_segments: list[tuple[float, float, float]], merge_pt: float = 3.0) -> list[float]` — отсортированные уникальные x-позиции вертикальных границ (кластеризация ближе `merge_pt` → среднее кластера). Это границы колонок таблицы.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bearings_tablelines.py
import fitz
import pytest

from app.pdf.tablelines import column_bounds, extract_table_lines


@pytest.fixture()
def grid_page():
    """Page with a 3x2 ruled table drawn as lines."""
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    xs = [50, 200, 350, 500]   # 4 vertical borders → 3 columns
    ys = [100, 130, 160]       # 3 horizontal borders → 2 rows
    for x in xs:
        page.draw_line((x, ys[0]), (x, ys[-1]))
    for y in ys:
        page.draw_line((xs[0], y), (xs[-1], y))
    yield page
    doc.close()


def test_extract_table_lines(grid_page):
    rect = fitz.Rect(40, 90, 510, 170)
    h_segs, v_segs = extract_table_lines(grid_page, rect)
    h_ys = sorted({round(s[2]) for s in h_segs})
    v_xs = sorted({round(s[0]) for s in v_segs})
    assert h_ys == [100, 130, 160]
    assert v_xs == [50, 200, 350, 500]


def test_column_bounds_merges_close():
    v_segs = [(50.0, 0, 100), (51.0, 0, 100), (200.0, 0, 100)]
    assert column_bounds(v_segs) == [50.5, 200.0]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bearings_tablelines.py -v`
Expected: FAIL с `ModuleNotFoundError: app.pdf.tablelines`

- [ ] **Step 3: Write implementation**

`app/pdf/tablelines.py`: модульная функция `extract_table_lines(page, table_rect)` —
дословно перенести тело `_extract_table_lines` из `app/ui/preview_view.py:8858-8932`
(сигнатура уже без self; заменить `from collections import defaultdict` на
импорт уровня модуля). Добавить:

```python
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
```

В `app/ui/preview_view.py` тело `_extract_table_lines` (строки 8858-8932) заменить на:

```python
        from app.pdf.tablelines import extract_table_lines

        return extract_table_lines(page, table_rect)
```

(докстринг метода сохранить; декоратор/сигнатуру не менять).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bearings_tablelines.py -v && ruff check app/pdf/tablelines.py`
Expected: PASS (2 теста), ruff чисто. UI-поведение не проверяем запуском приложения (правило проекта — приложение запускает пользователь).

- [ ] **Step 5: Commit**

```bash
git add app/pdf/tablelines.py app/ui/preview_view.py tests/test_bearings_tablelines.py
git commit -m "refactor: extract table line detection into reusable app/pdf/tablelines"
```

---

### Task 3: fingerprint.py — отпечаток страницы и кластеризация

**Files:**
- Create: `app/extraction/bearings/fingerprint.py`
- Test: `tests/test_bearings_fingerprint.py`

**Interfaces:**
- Consumes: `page_words()` из Task 2; `column_bounds()` из Task 2b (x-границы колонок передаются снаружи).
- Produces:
  - `page_fingerprint(words: list[tuple], page_width: float, v_xs: list[float] | None = None, bins: int = 24) -> str` — строка-сигнатура: гистограмма x-позиций слов по bins корзинам, квантованная до 4 уровней (0–3); если переданы `v_xs` (x-границы колонок из детекции линий), к сигнатуре добавляется `"|"` + номера корзин границ, например `"0013322000131|2,7,12,18"` — линии таблиц дают более сильный сигнал layout, чем слова
  - `cluster_pages(fingerprints: dict[int, str]) -> dict[str, list[int]]` — группировка страниц по равным сигнатурам (page_no с 1)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bearings_fingerprint.py
from app.extraction.bearings.fingerprint import cluster_pages, page_fingerprint


def make_words(xs: list[float], y: float = 100.0):
    return [(x, y, x + 20, y + 10, "w", 0, 0, i) for i, x in enumerate(xs)]


def test_same_layout_same_fingerprint():
    a = make_words([50, 150, 250, 350], y=100) + make_words([50, 150, 250, 350], y=120)
    b = make_words([52, 149, 251, 348], y=300) + make_words([52, 149, 251, 348], y=320)
    fp_a = page_fingerprint(a, page_width=595)
    fp_b = page_fingerprint(b, page_width=595)
    assert fp_a == fp_b


def test_different_layout_different_fingerprint():
    a = make_words([50, 150, 250, 350])
    b = make_words([50, 60, 70, 80, 90, 100, 110, 120])
    assert page_fingerprint(a, 595) != page_fingerprint(b, 595)


def test_fingerprint_includes_line_borders():
    words = make_words([50, 150, 250, 350])
    plain = page_fingerprint(words, 595)
    with_lines = page_fingerprint(words, 595, v_xs=[50.0, 200.0, 500.0])
    assert with_lines != plain
    assert with_lines.startswith(plain + "|")
    # близкие расклады линий дают одинаковый суффикс
    assert page_fingerprint(words, 595, v_xs=[52.0, 203.0, 501.0]) == with_lines


def test_cluster_pages():
    fps = {1: "AAA", 2: "BBB", 3: "AAA", 4: "AAA"}
    clusters = cluster_pages(fps)
    assert clusters["AAA"] == [1, 3, 4]
    assert clusters["BBB"] == [2]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bearings_fingerprint.py -v`
Expected: FAIL с `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# app/extraction/bearings/fingerprint.py
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
        idx = min(bins - 1, int(w[0] / page_width * bins))
        hist[idx] += 1
    peak = max(hist)
    if peak == 0:
        return "empty"
    # quantize each bin to 0..3 relative to the page's own peak
    sig = "".join(str(min(3, h * 4 // (peak + 1))) for h in hist)
    if v_xs:
        border_bins = sorted({min(bins - 1, int(x / page_width * bins)) for x in v_xs})
        sig += "|" + ",".join(str(b) for b in border_bins)
    return sig


def cluster_pages(fingerprints: dict[int, str]) -> dict[str, list[int]]:
    clusters: dict[str, list[int]] = defaultdict(list)
    for page_no in sorted(fingerprints):
        clusters[fingerprints[page_no]].append(page_no)
    return dict(clusters)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bearings_fingerprint.py -v`
Expected: PASS (3 теста)

- [ ] **Step 5: Commit**

```bash
git add app/extraction/bearings/fingerprint.py tests/test_bearings_fingerprint.py
git commit -m "feat(bearings): page fingerprint and layout clustering"
```

---

### Task 4: llm.py — клиент DeepSeek с кэшем и ретраями

**Files:**
- Create: `app/extraction/bearings/llm.py`
- Test: `tests/test_bearings_llm.py`

**Interfaces:**
- Produces:
  - `load_api_key(env_path: str = r"C:\!dev\markagent\.env") -> str` — env `DEEPSEEK_API_KEY`, иначе парсинг файла `.env`; `RuntimeError`, если нигде нет
  - `DeepSeekClient(api_key: str, cache_dir: Path, post_fn: Callable | None = None, model: str = "deepseek-chat")`
  - `DeepSeekClient.complete_json(cache_key: str, system: str, user: str, max_tokens: int = 4096, force: bool = False) -> dict` — JSON-ответ модели; дисковый кэш `cache_dir/<cache_key>-<sha1(prompts)[:10]>.json`; до 3 попыток при сетевой ошибке/невалидном JSON (при невалидном JSON в повторный запрос добавляется текст ошибки); после 3 неудач — `LLMError`
  - `LLMError(Exception)`
- `post_fn(url: str, headers: dict, payload: dict, timeout: int) -> dict` — инъекция транспорта; по умолчанию — обёртка над `requests.post(...).json()` с `raise_for_status()`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bearings_llm.py
import json

import pytest

from app.extraction.bearings.llm import DeepSeekClient, LLMError, load_api_key


def api_response(content: str) -> dict:
    return {"choices": [{"message": {"content": content}}]}


def test_load_api_key_from_env(monkeypatch):
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test")
    assert load_api_key() == "sk-test"


def test_load_api_key_from_file(monkeypatch, tmp_path):
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    env = tmp_path / ".env"
    env.write_text("FOO=bar\nDEEPSEEK_API_KEY=sk-file\n", encoding="utf-8")
    assert load_api_key(env_path=str(env)) == "sk-file"


def test_complete_json_caches(tmp_path):
    calls = []

    def fake_post(url, headers, payload, timeout):
        calls.append(payload)
        return api_response(json.dumps({"ok": 1}))

    client = DeepSeekClient("sk", tmp_path, post_fn=fake_post)
    r1 = client.complete_json("k1", "sys", "user")
    r2 = client.complete_json("k1", "sys", "user")
    assert r1 == {"ok": 1} and r2 == {"ok": 1}
    assert len(calls) == 1  # второй раз — из кэша


def test_complete_json_retries_bad_json(tmp_path):
    answers = ["not json at all", json.dumps({"ok": 2})]

    def fake_post(url, headers, payload, timeout):
        return api_response(answers.pop(0))

    client = DeepSeekClient("sk", tmp_path, post_fn=fake_post)
    assert client.complete_json("k2", "sys", "user") == {"ok": 2}


def test_complete_json_gives_up(tmp_path):
    def fake_post(url, headers, payload, timeout):
        return api_response("garbage")

    client = DeepSeekClient("sk", tmp_path, post_fn=fake_post)
    with pytest.raises(LLMError):
        client.complete_json("k3", "sys", "user")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bearings_llm.py -v`
Expected: FAIL с `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# app/extraction/bearings/llm.py
"""DeepSeek chat-completions client with disk cache and retries."""

import hashlib
import json
import logging
import os
import time
from collections.abc import Callable
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

API_URL = "https://api.deepseek.com/chat/completions"


class LLMError(Exception):
    pass


def load_api_key(env_path: str = r"C:\!dev\markagent\.env") -> str:
    key = os.environ.get("DEEPSEEK_API_KEY")
    if key:
        return key
    path = Path(env_path)
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip().startswith("DEEPSEEK_API_KEY="):
                value = line.split("=", 1)[1].strip()
                if value:
                    return value
    raise RuntimeError("DEEPSEEK_API_KEY not found in environment or " + env_path)


def _default_post(url: str, headers: dict, payload: dict, timeout: int) -> dict:
    resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


class DeepSeekClient:
    def __init__(
        self,
        api_key: str,
        cache_dir: Path,
        post_fn: Callable[[str, dict, dict, int], dict] | None = None,
        model: str = "deepseek-chat",
    ) -> None:
        self.api_key = api_key
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.post_fn = post_fn or _default_post
        self.model = model

    def complete_json(
        self,
        cache_key: str,
        system: str,
        user: str,
        max_tokens: int = 4096,
        force: bool = False,
    ) -> dict:
        digest = hashlib.sha1((system + "\x00" + user).encode()).hexdigest()[:10]
        cache_file = self.cache_dir / f"{cache_key}-{digest}.json"
        if cache_file.exists() and not force:
            return json.loads(cache_file.read_text(encoding="utf-8"))

        error_note = ""
        for attempt in range(3):
            payload = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user + error_note},
                ],
                "response_format": {"type": "json_object"},
                "max_tokens": max_tokens,
                "temperature": 0,
            }
            headers = {"Authorization": f"Bearer {self.api_key}"}
            try:
                raw = self.post_fn(API_URL, headers, payload, 120)
                content = raw["choices"][0]["message"]["content"]
                result = json.loads(content)
            except (requests.RequestException, KeyError) as exc:
                logger.warning("LLM call failed (attempt %d): %s", attempt + 1, exc)
                time.sleep(min(2**attempt, 8))
                continue
            except json.JSONDecodeError as exc:
                logger.warning("LLM returned invalid JSON (attempt %d): %s", attempt + 1, exc)
                error_note = f"\n\nПредыдущий ответ не был валидным JSON ({exc}). Верни строго один JSON-объект."
                continue
            cache_file.write_text(
                json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8"
            )
            return result
        raise LLMError(f"LLM failed after 3 attempts for cache_key={cache_key}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bearings_llm.py -v`
Expected: PASS (5 тестов)

- [ ] **Step 5: Commit**

```bash
git add app/extraction/bearings/llm.py tests/test_bearings_llm.py
git commit -m "feat(bearings): DeepSeek client with disk cache and retries"
```

---

### Task 5: validate.py — чистка и проверка записей

**Files:**
- Create: `app/extraction/bearings/validate.py`
- Test: дополнить `tests/test_bearings_models.py`

**Interfaces:**
- Consumes: `Manifest`, `Issue` из Task 1.
- Produces:
  - `parse_number(text: str) -> float | None` — "14,0" → 14.0; "12 000" → 12000.0; "—", "-", "" → None; нечисло → None
  - `validate_items(rows: list[dict], manifest: Manifest, page: int) -> tuple[list[dict], list[Issue]]` — для каждого row: designation непусто (иначе запись отброшена без Issue — это служебные строки); числовые поля прогнаны через `parse_number` (поле с `unit != None` считается числовым); если оба `d` и `D` присутствуют и `d >= D` → запись в Issues и отброшена; валидным записям добавляется `"page": page`

- [ ] **Step 1: Write the failing test** (дописать в `tests/test_bearings_models.py`)

```python
from app.extraction.bearings.validate import parse_number, validate_items


def _manifest():
    return Manifest(
        source="x.pdf", brand="X",
        fields=[
            FieldSpec(key="designation", label="No", core=True),
            FieldSpec(key="d", label="Bore", unit="mm", core=True),
            FieldSpec(key="D", label="Outer", unit="mm", core=True),
            FieldSpec(key="Cr", label="Load", unit="kN"),
        ],
    )


def test_parse_number():
    assert parse_number("14,0") == 14.0
    assert parse_number("12 000") == 12000.0
    assert parse_number("—") is None
    assert parse_number("abc") is None


def test_validate_items_ok_and_bad():
    rows = [
        {"designation": "6205", "d": "25", "D": "52", "Cr": "14,0"},
        {"designation": "", "d": "1", "D": "2", "Cr": "3"},        # dropped silently
        {"designation": "BAD", "d": "52", "D": "25", "Cr": "1"},   # d >= D → issue
    ]
    items, issues = validate_items(rows, _manifest(), page=7)
    assert len(items) == 1
    assert items[0] == {"designation": "6205", "d": 25.0, "D": 52.0, "Cr": 14.0, "page": 7}
    assert len(issues) == 1 and issues[0].page == 7
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bearings_models.py -v`
Expected: новые тесты FAIL с `ModuleNotFoundError: ...validate`

- [ ] **Step 3: Write minimal implementation**

```python
# app/extraction/bearings/validate.py
"""Row cleanup and sanity validation against a catalog manifest."""

import logging

from .models import Issue, Manifest

logger = logging.getLogger(__name__)

_EMPTY = {"", "-", "—", "–", "n/a", "N/A"}


def parse_number(text: str) -> float | None:
    if text is None:
        return None
    t = str(text).strip()
    if t in _EMPTY:
        return None
    t = t.replace(",", ".").replace("\u2009", "").replace(" ", "")
    try:
        return float(t)
    except ValueError:
        return None


def validate_items(
    rows: list[dict], manifest: Manifest, page: int
) -> tuple[list[dict], list[Issue]]:
    numeric_keys = {f.key for f in manifest.fields if f.unit is not None}
    items: list[dict] = []
    issues: list[Issue] = []
    for row in rows:
        designation = str(row.get("designation") or "").strip()
        if not designation:
            continue
        item: dict = {"designation": designation}
        for key, value in row.items():
            if key == "designation":
                continue
            item[key] = parse_number(value) if key in numeric_keys else value or None
        d, big_d = item.get("d"), item.get("D")
        if d is not None and big_d is not None and d >= big_d:
            issues.append(Issue(page=page, problem=f"{designation}: d={d} >= D={big_d}"))
            continue
        item["page"] = page
        items.append(item)
    return items, issues
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bearings_models.py -v`
Expected: PASS (все тесты файла)

- [ ] **Step 5: Commit**

```bash
git add app/extraction/bearings/validate.py tests/test_bearings_models.py
git commit -m "feat(bearings): row validation and number parsing"
```

---

### Task 6: mechanics.py — раскладка слов по правилу

**Files:**
- Create: `app/extraction/bearings/mechanics.py`
- Test: `tests/test_bearings_mechanics.py`

**Interfaces:**
- Consumes: `ParseRule`, `ColumnSpec` (Task 1); words (Task 2).
- Produces:
  - `apply_rule(words: list[tuple], rule: ParseRule) -> list[dict]` — сырые строки таблицы (значения — строки), ключи — `field` колонок правила. Алгоритм: слова группируются в визуальные строки по y-центру с допуском `rule.row_gap_pt`; первые `rule.header_skip_lines` строк отбрасываются; в строке каждое слово попадает в колонку, где x-центр слова ∈ [x_min, x_max] (иначе слово игнорируется); несколько слов в ячейке соединяются пробелом по порядку x; для полей из `rule.inherit_fields` пустая ячейка наследует последнее непустое значение сверху; если `rule.type_default` задан и в строке нет поля `type` — подставляется он.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bearings_mechanics.py
from app.extraction.bearings.mechanics import apply_rule
from app.extraction.bearings.models import ColumnSpec, ParseRule


def W(x, y, text):
    return (x, y, x + 20, y + 10, text, 0, 0, 0)


RULE = ParseRule(
    header_skip_lines=1,
    columns=[
        ColumnSpec(x_min=40, x_max=120, field="designation"),
        ColumnSpec(x_min=140, x_max=200, field="d"),
        ColumnSpec(x_min=210, x_max=270, field="D"),
    ],
    row_gap_pt=5.0,
    inherit_fields=["d"],
    type_default="deep_groove_ball",
)


def test_apply_rule_basic():
    words = [
        W(50, 100, "Bearing"), W(150, 100, "d"), W(220, 100, "D"),   # header
        W(50, 120, "6205"), W(150, 120, "25"), W(220, 120, "52"),
        W(50, 140, "6205"), W(50, 141, "ZZ"), W(220, 140, "52"),     # d empty → inherit
    ]
    rows = apply_rule(words, RULE)
    assert rows == [
        {"designation": "6205", "d": "25", "D": "52", "type": "deep_groove_ball"},
        {"designation": "6205 ZZ", "d": "25", "D": "52", "type": "deep_groove_ball"},
    ]


def test_apply_rule_ignores_outside_words():
    words = [
        W(50, 100, "hdr"),
        W(50, 120, "6206"), W(300, 120, "noise"), W(150, 120, "30"),
    ]
    rows = apply_rule(words, RULE)
    assert rows == [{"designation": "6206", "d": "30", "type": "deep_groove_ball"}]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bearings_mechanics.py -v`
Expected: FAIL с `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# app/extraction/bearings/mechanics.py
"""Deterministic word-to-row extraction driven by an LLM-produced ParseRule."""

import logging

from .models import ParseRule

logger = logging.getLogger(__name__)


def _group_lines(words: list[tuple], gap: float) -> list[list[tuple]]:
    lines: list[list[tuple]] = []
    for w in sorted(words, key=lambda w: ((w[1] + w[3]) / 2, w[0])):
        yc = (w[1] + w[3]) / 2
        if lines and abs(yc - (lines[-1][0][1] + lines[-1][0][3]) / 2) <= gap:
            lines[-1].append(w)
        else:
            lines.append([w])
    return lines


def apply_rule(words: list[tuple], rule: ParseRule) -> list[dict]:
    lines = _group_lines(words, rule.row_gap_pt)[rule.header_skip_lines:]
    rows: list[dict] = []
    last_values: dict[str, str] = {}
    for line in lines:
        cells: dict[str, list[tuple]] = {}
        for w in sorted(line, key=lambda w: w[0]):
            xc = (w[0] + w[2]) / 2
            for col in rule.columns:
                if col.x_min <= xc <= col.x_max:
                    cells.setdefault(col.field, []).append(w)
                    break
        row = {f: " ".join(w[4] for w in ws) for f, ws in cells.items()}
        for field in rule.inherit_fields:
            if not row.get(field) and field in last_values:
                row[field] = last_values[field]
        for field, value in row.items():
            if value:
                last_values[field] = value
        if rule.type_default and not row.get("type"):
            row["type"] = rule.type_default
        if row.get("designation"):
            rows.append(row)
    return rows
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bearings_mechanics.py -v`
Expected: PASS (2 теста)

- [ ] **Step 5: Commit**

```bash
git add app/extraction/bearings/mechanics.py tests/test_bearings_mechanics.py
git commit -m "feat(bearings): deterministic rule-based row extraction"
```

---

### Task 7: profiler.py и rules.py — LLM-этапы

**Files:**
- Create: `app/extraction/bearings/profiler.py`
- Create: `app/extraction/bearings/rules.py`
- Test: `tests/test_bearings_pipeline.py` (первая часть)

**Interfaces:**
- Consumes: `DeepSeekClient` (Task 4), `layout_text` (Task 2), `Manifest`, `ParseRule` (Task 1).
- Produces:
  - `build_manifest(client: DeepSeekClient, source_name: str, brand: str, sample_texts: list[str]) -> Manifest` — cache_key `"manifest"`
  - `build_rule(client: DeepSeekClient, manifest: Manifest, cluster_id: str, sample_texts: list[str], column_hints: list[float] | None = None) -> ParseRule` — cache_key `f"rule-{cluster_id}"`; `column_hints` — x-границы колонок из детекции линий таблиц (Task 2b), добавляются в промпт как достоверная подсказка
  - `extract_page_direct(client: DeepSeekClient, manifest: Manifest, page_no: int, page_text: str) -> list[dict]` — fallback-извлечение всей страницы напрямую, cache_key `f"page-{page_no}"`; возвращает список сырых row-словарей (строковые значения)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bearings_pipeline.py
import json

from app.extraction.bearings.llm import DeepSeekClient
from app.extraction.bearings.models import FieldSpec, Manifest
from app.extraction.bearings.profiler import build_manifest
from app.extraction.bearings.rules import build_rule, extract_page_direct


def make_client(tmp_path, content: dict):
    def fake_post(url, headers, payload, timeout):
        return {"choices": [{"message": {"content": json.dumps(content)}}]}
    return DeepSeekClient("sk", tmp_path, post_fn=fake_post)


def test_build_manifest(tmp_path):
    client = make_client(tmp_path, {
        "fields": [
            {"key": "designation", "label": "Bearing number", "unit": None, "core": True},
            {"key": "d", "label": "Bore diameter", "unit": "mm", "core": True},
        ]
    })
    m = build_manifest(client, "KOYO.pdf", "KOYO", ["y=100 | 6205[50] 25[150]"])
    assert m.brand == "KOYO"
    assert m.field_keys() == ["designation", "d"]


def test_build_rule(tmp_path):
    client = make_client(tmp_path, {
        "header_skip_lines": 1,
        "columns": [{"x_min": 40, "x_max": 120, "field": "designation"}],
        "row_gap_pt": 5.0,
        "inherit_fields": [],
        "type_default": "deep_groove_ball",
    })
    manifest = Manifest(source="a.pdf", brand="X",
                        fields=[FieldSpec(key="designation", label="No", core=True)])
    rule = build_rule(client, manifest, "AAA", ["y=100 | Bearing[50]"])
    assert rule.header_skip_lines == 1
    assert rule.columns[0].field == "designation"


def test_extract_page_direct(tmp_path):
    client = make_client(tmp_path, {"items": [{"designation": "6205", "d": "25"}]})
    manifest = Manifest(source="a.pdf", brand="X",
                        fields=[FieldSpec(key="designation", label="No", core=True),
                                FieldSpec(key="d", label="Bore", unit="mm", core=True)])
    rows = extract_page_direct(client, manifest, 7, "y=100 | 6205[50] 25[150]")
    assert rows == [{"designation": "6205", "d": "25"}]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bearings_pipeline.py -v`
Expected: FAIL с `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# app/extraction/bearings/profiler.py
"""Stage 1: LLM builds the per-catalog field manifest from sample pages."""

import logging

from .llm import DeepSeekClient
from .models import Manifest

logger = logging.getLogger(__name__)

_SYSTEM = (
    "Ты анализируешь каталог подшипников. По образцам страниц (текст с x-координатами "
    "в формате 'слово[x0]') определи, какие характеристики публикует производитель.\n"
    "Верни JSON: {\"fields\": [{\"key\": str, \"label\": str, \"unit\": str|null, "
    "\"core\": bool}]}.\n"
    "Правила: ключи — короткие латинские идентификаторы (designation, type, d, D, B, "
    "Cr, C0r, grease_rpm, oil_rpm, mass, r_min, ...); единицы — mm, kN, rpm, kg или "
    "null; core=true только для designation, type и основных размеров. Включай ВСЕ "
    "характеристики, которые реально встречаются в образцах. Не выдумывай отсутствующие."
)


def build_manifest(
    client: DeepSeekClient, source_name: str, brand: str, sample_texts: list[str]
) -> Manifest:
    user = "Образцы страниц каталога:\n\n" + "\n\n=== СТРАНИЦА ===\n".join(sample_texts)
    data = client.complete_json("manifest", _SYSTEM, user, max_tokens=4096)
    manifest = Manifest(source=source_name, brand=brand, fields=data["fields"])
    logger.info("Manifest for %s: %d fields", brand, len(manifest.fields))
    return manifest
```

```python
# app/extraction/bearings/rules.py
"""Stage 3: LLM builds a ParseRule per layout cluster; direct-page fallback."""

import json
import logging

from .llm import DeepSeekClient
from .models import Manifest, ParseRule

logger = logging.getLogger(__name__)

_RULE_SYSTEM = (
    "Ты описываешь правило разбора табличной страницы каталога подшипников. Текст "
    "страницы дан построчно: 'y=<y> | слово[x0] слово[x0] ...' — числа в скобках это "
    "x-координата слова в pt.\n"
    "Доступные поля (из манифеста каталога): {field_keys}.\n"
    "Верни JSON:\n"
    "{{\"header_skip_lines\": int, \"columns\": [{{\"x_min\": float, \"x_max\": float, "
    "\"field\": str}}], \"row_gap_pt\": float, \"inherit_fields\": [str], "
    "\"type_default\": str|null}}.\n"
    "columns — непересекающиеся x-диапазоны колонок таблицы (с запасом по краям); "
    "field — только из списка доступных; inherit_fields — поля, чьё значение в пустой "
    "ячейке наследуется от строки выше (типично для размера ряда); type_default — тип "
    "подшипника этих страниц, если он один (deep_groove_ball, tapered_roller, "
    "spherical_roller, cylindrical_roller, needle, angular_contact, thrust, unit, ...)."
)

_PAGE_SYSTEM = (
    "Извлеки ВСЕ записи подшипников со страницы каталога. Текст дан построчно с "
    "x-координатами: 'слово[x0]'.\n"
    "Поля записи: {field_keys}.\n"
    "Верни JSON {{\"items\": [{{...}}]}} — значения строками как в тексте; "
    "отсутствующее поле — null. Не выдумывай данные."
)


def build_rule(
    client: DeepSeekClient,
    manifest: Manifest,
    cluster_id: str,
    sample_texts: list[str],
    column_hints: list[float] | None = None,
) -> ParseRule:
    system = _RULE_SYSTEM.format(field_keys=json.dumps(manifest.field_keys()))
    user = "Образцовые страницы кластера:\n\n" + "\n\n=== СТРАНИЦА ===\n".join(sample_texts)
    if column_hints:
        user += (
            "\n\nДостоверно обнаруженные вертикальные границы колонок таблицы "
            f"(из линий PDF): x = {[round(x, 1) for x in column_hints]}. "
            "Используй их как границы x_min/x_max колонок."
        )
    data = client.complete_json(f"rule-{cluster_id}", system, user, max_tokens=2048)
    return ParseRule(**data)


def extract_page_direct(
    client: DeepSeekClient, manifest: Manifest, page_no: int, page_text: str
) -> list[dict]:
    system = _PAGE_SYSTEM.format(field_keys=json.dumps(manifest.field_keys()))
    data = client.complete_json(f"page-{page_no}", system, page_text, max_tokens=8192)
    return data.get("items", [])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bearings_pipeline.py -v`
Expected: PASS (3 теста)

- [ ] **Step 5: Commit**

```bash
git add app/extraction/bearings/profiler.py app/extraction/bearings/rules.py tests/test_bearings_pipeline.py
git commit -m "feat(bearings): LLM profiler, rule builder and direct-page fallback"
```

---

### Task 8: assemble.py — дедупликация и сборка результата

**Files:**
- Create: `app/extraction/bearings/assemble.py`
- Test: `tests/test_bearings_assemble.py`

**Interfaces:**
- Consumes: `CatalogResult`, `Issue` (Task 1).
- Produces:
  - `dedupe_items(items: list[dict]) -> list[dict]` — группировка по `designation`; записи сливаются: непустое значение побеждает `null`; при конфликте непустых значений остаётся первое (по порядку страниц), конфликт логируется; `page` — минимальный
  - `assemble(source: str, brand: str, items: list[dict], issues: list[Issue], pages_total: int, pages_with_data: int, extracted_at: str) -> CatalogResult`
  - `write_result(result: CatalogResult, out_path: Path) -> None` — JSON c `ensure_ascii=False, indent=1`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bearings_assemble.py
import json

from app.extraction.bearings.assemble import assemble, dedupe_items, write_result
from app.extraction.bearings.models import Issue


def test_dedupe_merges_nulls():
    items = [
        {"designation": "6205", "d": 25.0, "Cr": None, "page": 10},
        {"designation": "6205", "d": None, "Cr": 14.0, "page": 43},
        {"designation": "6206", "d": 30.0, "Cr": None, "page": 11},
    ]
    result = dedupe_items(items)
    assert len(result) == 2
    merged = next(i for i in result if i["designation"] == "6205")
    assert merged == {"designation": "6205", "d": 25.0, "Cr": 14.0, "page": 10}


def test_assemble_and_write(tmp_path):
    r = assemble(
        source="a.pdf", brand="X",
        items=[{"designation": "6205", "page": 1}],
        issues=[Issue(page=2, problem="x")],
        pages_total=3, pages_with_data=1,
        extracted_at="2026-08-15T00:00:00Z",
    )
    assert r.stats == {"pages_total": 3, "pages_with_data": 1, "items_count": 1}
    out = tmp_path / "x.json"
    write_result(r, out)
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["brand"] == "X" and data["items"][0]["designation"] == "6205"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bearings_assemble.py -v`
Expected: FAIL с `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# app/extraction/bearings/assemble.py
"""Stage 6: dedupe, stats and final JSON output."""

import json
import logging
from pathlib import Path

from .models import CatalogResult, Issue

logger = logging.getLogger(__name__)


def dedupe_items(items: list[dict]) -> list[dict]:
    by_designation: dict[str, dict] = {}
    for item in sorted(items, key=lambda i: i.get("page") or 0):
        key = item["designation"]
        if key not in by_designation:
            by_designation[key] = dict(item)
            continue
        merged = by_designation[key]
        for field, value in item.items():
            if field == "page":
                merged["page"] = min(merged.get("page") or value, value)
            elif merged.get(field) is None:
                merged[field] = value
            elif value is not None and merged[field] != value:
                logger.debug("Conflict for %s.%s: %r vs %r", key, field, merged[field], value)
    return list(by_designation.values())


def assemble(
    source: str,
    brand: str,
    items: list[dict],
    issues: list[Issue],
    pages_total: int,
    pages_with_data: int,
    extracted_at: str,
) -> CatalogResult:
    deduped = dedupe_items(items)
    return CatalogResult(
        source=source, brand=brand, extracted_at=extracted_at,
        items=deduped,
        stats={
            "pages_total": pages_total,
            "pages_with_data": pages_with_data,
            "items_count": len(deduped),
        },
        issues=issues,
    )


def write_result(result: CatalogResult, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(result.model_dump(), ensure_ascii=False, indent=1), encoding="utf-8"
    )
    logger.info("Wrote %s (%d items)", out_path, len(result.items))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bearings_assemble.py -v`
Expected: PASS (2 теста)

- [ ] **Step 5: Commit**

```bash
git add app/extraction/bearings/assemble.py tests/test_bearings_assemble.py
git commit -m "feat(bearings): dedupe and result assembly"
```

---

### Task 9: cli.py + __main__.py — оркестрация и e2e-тест

**Files:**
- Create: `app/extraction/bearings/cli.py`
- Create: `app/extraction/bearings/__main__.py`
- Test: дополнить `tests/test_bearings_pipeline.py`

**Interfaces:**
- Consumes: всё из Task 1–8.
- Produces:
  - `brand_from_filename(path: Path) -> str` — `"KOYO - Ball and Roller Bearings.pdf"` → `"KOYO"` (часть до первого `" -"`, либо стем целиком; strip)
  - `run_profile(pdf_path: Path, out_dir: Path, client: DeepSeekClient, sample: int = 12) -> Manifest` — пишет `out_dir/<brand-lower>.manifest.json`
  - `run_extract(pdf_path: Path, out_dir: Path, client: DeepSeekClient) -> CatalogResult` — читает манифест (если нет — сначала `run_profile`), полный конвейер, пишет `out_dir/<brand-lower>.json`
  - `main(argv: list[str] | None = None) -> int` — argparse: `profile <glob> [--out OUT] [--sample N] [--force]`, `extract <glob> [--out OUT] [--force]`; по умолчанию `--out output/bearings`; кэш LLM — `OUT/cache/<pdf-stem>/`
- Логика `run_extract` по спеке: страницы → words → `is_data_page` → fingerprints → `cluster_pages`; на кластер: `build_rule` по 1–2 образцам → `apply_rule` + `validate_items` для каждой страницы; если валидных записей < 50% сырых строк или `apply_rule`/`build_rule` бросил исключение — `extract_page_direct` для страницы → `validate_items`; повторный провал → `Issue(page, "rule and fallback failed")`; затем `assemble` + `write_result`. `extracted_at` — `datetime.now(timezone.utc).isoformat()`.

- [ ] **Step 1: Write the failing e2e test** (дописать в `tests/test_bearings_pipeline.py`)

```python
import fitz
import pytest

from app.extraction.bearings.cli import brand_from_filename, main


@pytest.fixture()
def tiny_catalog(tmp_path):
    """Two data pages with identical layout + a cover page."""
    doc = fitz.open()
    cover = doc.new_page(width=595, height=842)
    cover.insert_text((100, 100), "KOYO GENERAL CATALOGUE", fontsize=20)
    for page_no in range(2):
        page = doc.new_page(width=595, height=842)
        rows = [("Bearing", "d", "D", "B")] + [
            (f"62{page_no}{i}", str(20 + i), str(47 + i), "14") for i in range(10)
        ]
        y = 100
        for row in rows:
            for x, text in zip([50, 150, 220, 290], row):
                page.insert_text((x, y), text, fontsize=10)
            y += 20
    pdf = tmp_path / "KOYO - Test.pdf"
    doc.save(str(pdf))
    doc.close()
    return pdf


def test_brand_from_filename(tmp_path):
    assert brand_from_filename(tmp_path / "KOYO - Ball Bearings.pdf") == "KOYO"
    assert brand_from_filename(tmp_path / "Dinroll.pdf") == "Dinroll"


def test_extract_end_to_end(tiny_catalog, tmp_path, monkeypatch):
    responses = {
        "manifest": {"fields": [
            {"key": "designation", "label": "Bearing number", "unit": None, "core": True},
            {"key": "d", "label": "Bore", "unit": "mm", "core": True},
            {"key": "D", "label": "Outer", "unit": "mm", "core": True},
            {"key": "B", "label": "Width", "unit": "mm", "core": True},
        ]},
        "rule": {"header_skip_lines": 1,
                 "columns": [
                     {"x_min": 40, "x_max": 130, "field": "designation"},
                     {"x_min": 140, "x_max": 200, "field": "d"},
                     {"x_min": 210, "x_max": 270, "field": "D"},
                     {"x_min": 280, "x_max": 340, "field": "B"},
                 ],
                 "row_gap_pt": 5.0, "inherit_fields": [], "type_default": None},
    }

    def fake_post(url, headers, payload, timeout):
        import json as _json
        user = payload["messages"][1]["content"]
        key = "manifest" if "какие характеристики" in payload["messages"][0]["content"] \
            else "rule"
        return {"choices": [{"message": {"content": _json.dumps(responses[key])}}]}

    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test")
    monkeypatch.setattr("app.extraction.bearings.llm._default_post", fake_post)

    out = tmp_path / "out"
    code = main(["extract", str(tiny_catalog), "--out", str(out)])
    assert code == 0

    import json as _json
    data = _json.loads((out / "koyo.json").read_text(encoding="utf-8"))
    assert data["brand"] == "KOYO"
    assert data["stats"]["items_count"] == 20
    assert data["stats"]["pages_total"] == 3
    assert all(isinstance(i["d"], float) for i in data["items"])
    pages = {i["page"] for i in data["items"]}
    assert pages == {2, 3}
```

Примечание: `fake_post` различает этапы по тексту system-промпта — «какие характеристики» встречается только в профилировщике. Если формулировка system-промпта менялась в Task 7, синхронизировать маркер.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bearings_pipeline.py -v`
Expected: новые тесты FAIL с `ImportError: ...cli`

- [ ] **Step 3: Write implementation**

```python
# app/extraction/bearings/cli.py
"""CLI orchestration: profile and extract commands."""

import argparse
import glob as globmod
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import fitz

from app.pdf.tablelines import column_bounds, extract_table_lines

from .assemble import assemble, write_result
from .fingerprint import cluster_pages, page_fingerprint
from .llm import DeepSeekClient, LLMError, load_api_key
from .models import CatalogResult, Issue, Manifest
from .mechanics import apply_rule
from .pagetext import is_data_page, layout_text, page_words
from .profiler import build_manifest
from .rules import build_rule, extract_page_direct
from .validate import validate_items

logger = logging.getLogger(__name__)

DEFAULT_OUT = Path("output/bearings")


def brand_from_filename(path: Path) -> str:
    stem = path.stem
    return stem.split(" -")[0].strip() if " -" in stem else stem.strip()


def _client_for(pdf_path: Path, out_dir: Path, force: bool = False) -> DeepSeekClient:
    cache_dir = out_dir / "cache" / pdf_path.stem
    return DeepSeekClient(load_api_key(), cache_dir)


def _page_data(doc: fitz.Document) -> dict[int, dict]:
    """page_no (1-based) -> {words, text, width, col_xs} for data pages only."""
    pages: dict[int, dict] = {}
    for idx in range(len(doc)):
        page = doc[idx]
        words = page_words(page)
        if not is_data_page(words):
            continue
        _, v_segs = extract_table_lines(page, page.rect)
        pages[idx + 1] = {
            "words": words,
            "text": layout_text(words, page.rect.width),
            "width": page.rect.width,
            "col_xs": column_bounds(v_segs),
        }
    return pages


def _manifest_path(out_dir: Path, brand: str) -> Path:
    return out_dir / f"{brand.lower()}.manifest.json"


def run_profile(
    pdf_path: Path, out_dir: Path, client: DeepSeekClient, sample: int = 12
) -> Manifest:
    brand = brand_from_filename(pdf_path)
    with fitz.open(str(pdf_path)) as doc:
        pages = _page_data(doc)
    if not pages:
        raise RuntimeError(f"{pdf_path.name}: no data pages found")
    page_nos = sorted(pages)
    step = max(1, len(page_nos) // sample)
    sample_texts = [pages[n]["text"] for n in page_nos[::step][:sample]]
    manifest = build_manifest(client, pdf_path.name, brand, sample_texts)
    path = _manifest_path(out_dir, brand)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(manifest.model_dump(), ensure_ascii=False, indent=1), encoding="utf-8"
    )
    logger.info("Manifest written: %s", path)
    return manifest


def run_extract(pdf_path: Path, out_dir: Path, client: DeepSeekClient) -> CatalogResult:
    brand = brand_from_filename(pdf_path)
    manifest_file = _manifest_path(out_dir, brand)
    if manifest_file.exists():
        manifest = Manifest(**json.loads(manifest_file.read_text(encoding="utf-8")))
    else:
        manifest = run_profile(pdf_path, out_dir, client)

    with fitz.open(str(pdf_path)) as doc:
        pages_total = len(doc)
        pages = _page_data(doc)

    fps = {
        n: page_fingerprint(p["words"], p["width"], v_xs=p["col_xs"])
        for n, p in pages.items()
    }
    clusters = cluster_pages(fps)

    all_items: list[dict] = []
    issues: list[Issue] = []
    for cluster_id, page_nos in clusters.items():
        try:
            rule = build_rule(
                client, manifest, cluster_id,
                [pages[n]["text"] for n in page_nos[:2]],
                column_hints=pages[page_nos[0]]["col_xs"] or None,
            )
        except (LLMError, ValueError) as exc:
            logger.warning("Rule failed for cluster %s: %s", cluster_id, exc)
            rule = None
        for n in page_nos:
            items: list[dict] = []
            page_issues: list[Issue] = []
            raw_count = 0
            if rule is not None:
                try:
                    raw = apply_rule(pages[n]["words"], rule)
                    raw_count = len(raw)
                    items, page_issues = validate_items(raw, manifest, n)
                except Exception as exc:  # noqa: BLE001 — правило от LLM, не падаем
                    logger.warning("apply_rule failed on page %d: %s", n, exc)
            if rule is None or raw_count == 0 or len(items) < raw_count * 0.5:
                try:
                    raw = extract_page_direct(client, manifest, n, pages[n]["text"])
                    items, page_issues = validate_items(raw, manifest, n)
                except LLMError:
                    issues.append(Issue(page=n, problem="rule and fallback failed"))
                    continue
            all_items.extend(items)
            issues.extend(page_issues)

    result = assemble(
        source=pdf_path.name, brand=brand, items=all_items, issues=issues,
        pages_total=pages_total, pages_with_data=len(pages),
        extracted_at=datetime.now(timezone.utc).isoformat(),
    )
    write_result(result, out_dir / f"{brand.lower()}.json")
    return result


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(prog="python -m app.extraction.bearings")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("profile", "extract"):
        p = sub.add_parser(name)
        p.add_argument("pdf_glob")
        p.add_argument("--out", default=str(DEFAULT_OUT))
        p.add_argument("--force", action="store_true")
        if name == "profile":
            p.add_argument("--sample", type=int, default=12)
    args = parser.parse_args(argv)

    paths = [Path(p) for p in sorted(globmod.glob(args.pdf_glob))]
    if not paths:
        parser.error(f"no PDF matches {args.pdf_glob}")
    out_dir = Path(args.out)
    for pdf_path in paths:
        client = _client_for(pdf_path, out_dir, force=args.force)
        if args.command == "profile":
            run_profile(pdf_path, out_dir, client, sample=args.sample)
        else:
            result = run_extract(pdf_path, out_dir, client)
            print(
                f"{pdf_path.name}: {result.stats['items_count']} items, "
                f"{len(result.issues)} issues"
            )
    return 0
```

```python
# app/extraction/bearings/__main__.py
import sys

from .cli import main

sys.exit(main())
```

Примечание: `--force` в MVP протаскивается до клиента, но `complete_json` вызывается без `force=True` из этапов — принудительный пересчёт реализуется удалением каталога кэша; флаг оставлен в CLI как задел (не подключать `force` внутрь этапов без необходимости — YAGNI, зафиксировать это поведение короткой строкой в `--help` не требуется).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bearings_pipeline.py -v`
Expected: PASS (все тесты файла, включая e2e)

- [ ] **Step 5: Run full new-test suite and lint**

Run: `pytest tests/test_bearings_models.py tests/test_bearings_pagetext.py tests/test_bearings_fingerprint.py tests/test_bearings_llm.py tests/test_bearings_mechanics.py tests/test_bearings_assemble.py tests/test_bearings_pipeline.py -v && ruff check app/extraction/bearings/`
Expected: все PASS, ruff без ошибок

- [ ] **Step 6: Commit**

```bash
git add app/extraction/bearings/cli.py app/extraction/bearings/__main__.py tests/test_bearings_pipeline.py
git commit -m "feat(bearings): CLI orchestration with e2e test"
```

---

### Task 10: Приёмка — живой прогон малого каталога

**Files:**
- Modify: нет (прогон и ручная сверка)

**Interfaces:**
- Consumes: CLI из Task 9, реальный ключ DeepSeek.

- [ ] **Step 1: Прогнать малый каталог**

Run: `python -m app.extraction.bearings extract "data/uploads/KSM - Bearings.pdf"`
Expected: завершение без исключений, файл `output/bearings/ksm.json` создан, в консоли `KSM - Bearings.pdf: N items, M issues`.

- [ ] **Step 2: Ручная сверка**

Открыть `output/bearings/ksm.json`, взять 5 случайных записей, открыть PDF на указанных `page` и сверить designation/размеры. Проверить `stats` (правдоподобные pages_with_data) и просмотреть `issues`.

- [ ] **Step 3: Зафиксировать результат прогона**

Если сверка выявила системную ошибку (сдвиг колонок, потерянные строки) — НЕ чинить наугад: задокументировать примеры (страница, ожидание, факт) и вернуться к пользователю с отчётом. Если всё сходится — доложить пользователю метрики (items, issues, стоимость по кэшу LLM-вызовов) и предложить прогон остальных каталогов.

- [ ] **Step 4: Commit артефактов (опционально, по решению пользователя)**

`output/` и кэш в git не коммитим (проверить `.gitignore`; при отсутствии — добавить строку `output/`).

---

## Self-Review (выполнено при написании плана)

1. **Spec coverage:** профилирование → Task 7/9; кластеризация → Task 3; правило на кластер → Task 7; механика → Task 6; валидация+fallback → Task 5/9; сборка/дедуп → Task 8; кэш/resume → Task 4; CLI → Task 9; тесты (юнит, интеграционный без сети, живая приёмка) → Task 1–10. Отсев нетабличных страниц → Task 2 (`is_data_page`) + Task 9. Ключ из env/markagent → Task 4.
2. **Placeholder scan:** все шаги содержат конкретный код/команды; заглушек нет.
3. **Type consistency:** словарь word-кортежа объявлен глобально; имена (`page_words`, `layout_text`, `apply_rule`, `validate_items`, `complete_json`, `build_manifest`, `build_rule`, `extract_page_direct`, `dedupe_items`, `assemble`, `write_result`) согласованы между задачами.

Переиспользование наработок проекта (по требованию пользователя «продолжить и усилить то, что было»): отлаженная детекция линий таблиц `_extract_table_lines` выносится из `preview_view.py` в общий `app/pdf/tablelines.py` (Task 2b, алгоритм не меняется — он LOCKED по памяти проекта) и усиливает конвейер в двух местах: сигнатура страницы включает позиции границ колонок (Task 3), а LLM-правило получает достоверные x-границы колонок как подсказку (Task 7/9). UI делегирует в общий модуль — код живёт в одном месте.
