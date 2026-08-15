"""Page word extraction, layout-preserving text, numeric density heuristics."""

import logging

import fitz

logger = logging.getLogger(__name__)

Word = tuple  # (x0, y0, x1, y1, text, block_no, line_no, word_no)


def page_words(page: fitz.Page) -> list[Word]:
    words = page.get_text("words")
    return sorted(words, key=lambda w: (round(w[1], 1), w[0]))


def _is_number(text: str) -> bool:
    t = text.replace(",", ".").replace(" ", "").replace(" ", "")
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
