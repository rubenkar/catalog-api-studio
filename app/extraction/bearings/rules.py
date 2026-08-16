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
    "отсутствующие поля ПРОПУСКАЙ (не пиши null — это раздувает ответ). "
    "Не выдумывай данные."
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
