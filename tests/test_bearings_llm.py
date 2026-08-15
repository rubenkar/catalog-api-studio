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
