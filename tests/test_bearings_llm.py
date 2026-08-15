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


def test_complete_json_empty_choices(tmp_path, monkeypatch):
    monkeypatch.setattr("app.extraction.bearings.llm.time.sleep", lambda s: None)

    def fake_post(url, headers, payload, timeout):
        return {"choices": []}

    client = DeepSeekClient("sk", tmp_path, post_fn=fake_post)
    with pytest.raises(LLMError):
        client.complete_json("k4", "sys", "user")


def test_complete_json_sanitizes_cache_key(tmp_path):
    calls = []

    def fake_post(url, headers, payload, timeout):
        calls.append(payload)
        return api_response(json.dumps({"ok": 3}))

    client = DeepSeekClient("sk", tmp_path, post_fn=fake_post)
    # Use cache_key with special characters invalid in Windows filenames
    dirty_key = "rule-0303|1,2:4*8"
    r1 = client.complete_json(dirty_key, "sys", "user")
    r2 = client.complete_json(dirty_key, "sys", "user")
    assert r1 == {"ok": 3} and r2 == {"ok": 3}
    assert len(calls) == 1  # second call from cache
    # Verify cache file was created with sanitized name (pipes/colons/asterisks replaced with underscores)
    cache_files = list(tmp_path.glob("*.json"))
    assert len(cache_files) == 1
    assert "rule-0303_1_2_4_8" in cache_files[0].name
