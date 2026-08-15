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
