"""Stage 1: LLM builds the per-catalog field manifest from sample pages."""

import logging
import re

from .llm import DeepSeekClient
from .models import Manifest

logger = logging.getLogger(__name__)

_SYSTEM = (
    "Ты анализируешь каталог подшипников. По образцам страниц (текст с x-координатами "
    "в формате 'слово[x0]') определи, какие характеристики публикует производитель.\n"
    "Верни JSON: {\"fields\": [{\"key\": str, \"label\": str, \"unit\": str|null, "
    "\"core\": bool}]}.\n"
    "Правила: ключи — короткие латинские идентификаторы (designation, type, d, D, B, "
    "Cr, C0r, grease_rpm, oil_rpm, mass, r_min, ...); единицы — mm, kN, daN, rpm, kg "
    "или null; core=true только для designation, type и основных размеров. Включай ВСЕ "
    "характеристики, которые реально встречаются в образцах. Не выдумывай отсутствующие. "
    "Единицы указывай ТОЧНО как напечатано в каталоге: если нагрузки даны в daN — "
    "пиши daN, НЕ пересчитывай в kN."
)


def fix_units(manifest: Manifest, sample_texts: list[str]) -> Manifest:
    """Детерминированная коррекция единиц нагрузок по маркерам в тексте.

    LLM склонен писать «kN» по привычке; если в образцах страниц встречается
    только «daN» — исправляем kN -> daN у всех полей.
    """
    blob = "\n".join(sample_texts)
    has_dan = "daN" in blob
    has_kn = bool(re.search(r"(?<![a-zA-Z])kN", blob))
    if has_dan and not has_kn:
        for f in manifest.fields:
            if f.unit == "kN":
                logger.info("Unit fix: %s kN -> daN (catalog prints daN)", f.key)
                f.unit = "daN"
    return manifest


def build_manifest(
    client: DeepSeekClient, source_name: str, brand: str, sample_texts: list[str]
) -> Manifest:
    user = "Образцы страниц каталога:\n\n" + "\n\n=== СТРАНИЦА ===\n".join(sample_texts)
    data = client.complete_json("manifest", _SYSTEM, user, max_tokens=4096)
    manifest = Manifest(source=source_name, brand=brand, fields=data["fields"])
    fix_units(manifest, sample_texts)
    logger.info("Manifest for %s: %d fields", brand, len(manifest.fields))
    return manifest
