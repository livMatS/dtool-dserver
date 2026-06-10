"""Read flow (signed URL fetching/caching), listing, and configuration."""

import json
import logging
import os

import pytest

import dtool_dserver.storagebroker as sbmod
from dtool_dserver.storagebroker import (
    DServerStorageBroker,
    DServerAuthenticationError,
)

from .conftest import (
    BUCKET_BASE,
    DATASET_URI,
    SERVER,
    UUID,
    FakeResponse,
)


def test_fetch_signed_urls_caches_and_logs(broker, caplog):
    """Also a regression test: a stale variable in the debug log used to
    raise NameError on every fetch."""
    calls = []

    def fake_api_request(method, path, **kwargs):
        calls.append(path)
        return FakeResponse({"readme_url": "https://s3/x"})

    broker._api_request = fake_api_request

    with caplog.at_level(logging.DEBUG):
        first = broker._fetch_signed_urls()
        second = broker._fetch_signed_urls()

    assert first == second == {"readme_url": "https://s3/x"}
    assert len(calls) == 1  # second call served from cache
    encoded = f"s3%3A%2F%2Fmy-bucket%2F{UUID}"
    assert calls[0] == f"/signed-urls/dataset/{encoded}"


def test_fetch_signed_urls_requires_dataset_uri(monkeypatch):
    monkeypatch.setenv("DSERVER_TOKEN", "test-token")
    broker = DServerStorageBroker(BUCKET_BASE)
    with pytest.raises(ValueError):
        broker._fetch_signed_urls()


def _paged_post(pages):
    """Return a fake requests.post serving the given pages of /uris."""
    calls = []

    def fake_post(url, headers=None, params=None, json=None):
        calls.append(params)
        page = params["page"]
        items, total_pages = pages(page)
        return FakeResponse(
            items,
            headers={"X-Pagination": __import__("json").dumps(
                {"page": page, "last_page": total_pages})},
        )

    return fake_post, calls


def test_list_dataset_uris_follows_pagination(monkeypatch):
    monkeypatch.setenv("DSERVER_TOKEN", "test-token")

    def pages(page):
        if page == 1:
            return [{"uri": f"s3://my-bucket/uuid-{i}"} for i in range(3)], 2
        return [{"uri": "s3://my-bucket/uuid-last"}], 2

    fake_post, calls = _paged_post(pages)
    monkeypatch.setattr(sbmod.requests, "post", fake_post)

    uris = DServerStorageBroker.list_dataset_uris(BUCKET_BASE)

    assert len(uris) == 4
    assert uris[-1] == f"dserver://{SERVER}/s3/my-bucket/uuid-last"
    assert [c["page"] for c in calls] == [1, 2]


def test_list_dataset_uris_skips_unrepresentable(monkeypatch, caplog):
    monkeypatch.setenv("DSERVER_TOKEN", "test-token")

    def pages(page):
        return [
            {"uri": "s3://my-bucket/uuid-ok"},
            {"uri": "s3://my-bucket/prefix/uuid-nested"},  # path prefix
            {"uri": "file:///home/user/uuid-file"},        # empty netloc
        ], 1

    fake_post, _ = _paged_post(pages)
    monkeypatch.setattr(sbmod.requests, "post", fake_post)

    with caplog.at_level(logging.WARNING):
        uris = DServerStorageBroker.list_dataset_uris(BUCKET_BASE)

    assert uris == [f"dserver://{SERVER}/s3/my-bucket/uuid-ok"]
    assert sum("cannot be expressed" in r.message for r in caplog.records) == 2


def test_list_dataset_uris_rejects_invalid_base(monkeypatch):
    monkeypatch.setenv("DSERVER_TOKEN", "test-token")
    with pytest.raises(ValueError):
        DServerStorageBroker.list_dataset_uris(f"dserver://{SERVER}/")


def test_token_from_config_file(tmp_path, monkeypatch):
    monkeypatch.delenv("DSERVER_TOKEN", raising=False)
    monkeypatch.delenv("DSERVER_TOKEN_FILE", raising=False)
    config = tmp_path / "dtool.json"
    config.write_text(json.dumps({"DSERVER_TOKEN": "config-token"}))

    broker = DServerStorageBroker(DATASET_URI, config_path=str(config))
    assert broker._get_token() == "config-token"


def test_protocol_from_config_file(tmp_path, monkeypatch):
    monkeypatch.delenv("DSERVER_PROTOCOL", raising=False)
    config = tmp_path / "dtool.json"
    config.write_text(json.dumps({"DSERVER_PROTOCOL": "https"}))

    broker = DServerStorageBroker(DATASET_URI, config_path=str(config))
    assert broker._get_server_url("/x") == f"https://{SERVER}/x"


def test_missing_token_raises(monkeypatch, tmp_path):
    monkeypatch.delenv("DSERVER_TOKEN", raising=False)
    monkeypatch.delenv("DSERVER_TOKEN_FILE", raising=False)
    config = tmp_path / "empty.json"
    config.write_text("{}")

    broker = DServerStorageBroker(DATASET_URI, config_path=str(config))
    with pytest.raises(DServerAuthenticationError):
        broker._get_token()


def test_get_size_hash_pending_vs_manifest(broker, item_file):
    from dtoolcore.utils import generate_identifier
    import hashlib

    broker.put_item(item_file, "data.txt")
    # Pending item: read from local file.
    assert broker.get_size_in_bytes("data.txt") == os.path.getsize(item_file)
    assert broker.get_hash("data.txt") == \
        hashlib.md5(b"item content").hexdigest()
    assert broker.get_relpath("data.txt") == "data.txt"

    # Non-pending: read from (mocked) manifest.
    identifier = generate_identifier("other.txt")
    broker._manifest_cache = {"items": {identifier: {
        "relpath": "other.txt", "size_in_bytes": 7, "hash": "abc"}}}
    assert broker.get_size_in_bytes("other.txt") == 7
    assert broker.get_hash("other.txt") == "abc"
