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
