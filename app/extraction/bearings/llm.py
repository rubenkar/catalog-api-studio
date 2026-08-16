"""DeepSeek chat-completions client with disk cache and retries."""

import hashlib
import json
import logging
import os
import re
import time
from collections.abc import Callable
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

API_URL = "https://api.deepseek.com/chat/completions"


def _sanitize_cache_key(key: str) -> str:
    """Sanitize cache_key for Windows filesystem.

    Keeps only [A-Za-z0-9._-], replaces others with _.
    If result is longer than 60 chars, truncates to 60.
    SHA1 suffix preserves uniqueness across truncations.
    """
    sanitized = re.sub(r"[^A-Za-z0-9._-]", "_", key)
    if len(sanitized) > 60:
        sanitized = sanitized[:60]
    return sanitized


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
        sanitized_key = _sanitize_cache_key(cache_key)
        cache_file = self.cache_dir / f"{sanitized_key}-{digest}.json"
        if cache_file.exists() and not force:
            try:
                return json.loads(cache_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning(
                    "Cache read failed for %s: %s — treating as cache miss", cache_file, exc
                )

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
                try:
                    result = json.loads(content)
                except json.JSONDecodeError:
                    # «Extra data»: валидный JSON + мусорный хвост — берём объект
                    result, _ = json.JSONDecoder().raw_decode(content.strip())
            except (requests.RequestException, KeyError, IndexError) as exc:
                logger.warning("LLM call failed (attempt %d): %s", attempt + 1, exc)
                time.sleep(min(2**attempt, 8))
                continue
            except json.JSONDecodeError as exc:
                logger.warning("LLM returned invalid JSON (attempt %d): %s", attempt + 1, exc)
                error_note = f"\n\nПредыдущий ответ не был валидным JSON ({exc}). Верни строго один JSON-объект."
                continue
            tmp_file = cache_file.with_name(cache_file.name + ".tmp")
            tmp_file.write_text(
                json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8"
            )
            os.replace(tmp_file, cache_file)
            return result
        raise LLMError(f"LLM failed after 3 attempts for cache_key={cache_key}")
