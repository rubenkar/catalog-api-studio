"""Pure helpers for presenting extraction results as a table (no Qt imports)."""


def result_table_rows(
    result: dict, manifest: dict | None = None
) -> tuple[list[str], list[list[str]]]:
    """Build table headers and string rows from a CatalogResult-shaped dict.

    Headers follow the manifest field order when a manifest is given
    (with ``page`` appended last); otherwise they are derived from item
    keys in first-seen order with ``designation`` first and ``page`` last.
    ``None`` values render as empty strings.
    """
    items: list[dict] = result.get("items", [])
    if not items:
        return [], []

    if manifest and manifest.get("fields"):
        headers = [f["key"] for f in manifest["fields"]]
        if "page" not in headers:
            headers.append("page")
    else:
        headers = []
        for item in items:
            for key in item:
                if key not in headers:
                    headers.append(key)
        for special, position in (("designation", 0), ("page", len(headers) - 1)):
            if special in headers:
                headers.remove(special)
                headers.insert(position if special == "designation" else len(headers), special)

    rows = [
        ["" if item.get(h) is None else str(item.get(h)) for h in headers]
        for item in items
    ]
    return headers, rows
