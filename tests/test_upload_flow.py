"""Upload flow: manifest reuse, request payload, pinned headers."""

import hashlib
import os

import pytest

from dtoolcore.utils import generate_identifier

import dtool_dserver.storagebroker as sbmod
from dtool_dserver.storagebroker import DServerStorageBroker

from .conftest import (
    DATASET_URI,
    FakeResponse,
    TrapHasher,
    make_manifest,
)


@pytest.fixture
def upload_broker(broker, item_file):
    broker.put_item(item_file, "data.txt")
    broker._admin_metadata_cache = {
        "name": "my-dataset",
        "creator_username": "lars",
        "frozen_at": 1234.5,
    }
    return broker


def capture_api_request(broker, response_json):
    calls = []

    def fake_api_request(method, path, **kwargs):
        calls.append({"method": method, "path": path, **kwargs})
        return FakeResponse(response_json)

    broker._api_request = fake_api_request
    return calls


def test_upload_request_reuses_manifest_hashes(upload_broker):
    identifier, manifest = make_manifest("data.txt")
    upload_broker._manifest_cache = manifest
    upload_broker.hasher = TrapHasher()  # fails on any re-hashing

    calls = capture_api_request(
        upload_broker, {"upload_urls": {"readme": "u", "items": {}}})
    upload_broker._fetch_upload_urls()

    sent = calls[0]["json"]
    assert sent["name"] == "my-dataset"
    assert sent["creator_username"] == "lars"
    assert sent["frozen_at"] == 1234.5
    assert sent["hash_function"] == "md5sum_hexdigest"
    item = sent["items"][0]
    assert item["hash"] == manifest["items"][identifier]["hash"]
    assert item["size_in_bytes"] == len(b"item content")


def test_upload_request_falls_back_to_hashing(upload_broker, item_file):
    # No manifest cache: hashes must be computed from the file.
    calls = capture_api_request(
        upload_broker, {"upload_urls": {"readme": "u", "items": {}}})
    upload_broker._fetch_upload_urls()

    item = calls[0]["json"]["items"][0]
    assert item["hash"] == hashlib.md5(b"item content").hexdigest()
    assert item["size_in_bytes"] == os.path.getsize(item_file)


def test_upload_request_dataset_name_not_uuid(upload_broker):
    """Regression: dataset name used to be sent as the UUID."""
    calls = capture_api_request(
        upload_broker, {"upload_urls": {"readme": "u", "items": {}}})
    upload_broker._fetch_upload_urls()
    assert calls[0]["json"]["name"] == "my-dataset"
    assert calls[0]["json"]["name"] != upload_broker.uuid


def test_upload_request_includes_staged_metadata(upload_broker):
    upload_broker.put_tag("science")
    upload_broker.put_annotation("project", {"id": 42})
    upload_broker.put_overlay("quality", {"id1": "good"})

    calls = capture_api_request(
        upload_broker, {"upload_urls": {"readme": "u", "items": {}}})
    upload_broker._fetch_upload_urls()

    sent = calls[0]["json"]
    assert sent["tags"] == ["science"]
    assert sent["annotations"] == {"project": {"id": 42}}
    assert sent["overlays"] == {"quality": {"id1": "good"}}


def test_post_freeze_hook_sends_pinned_headers_verbatim(
        upload_broker, monkeypatch):
    identifier = generate_identifier("data.txt")
    pinned = {"Content-MD5": "AAA=", "x-amz-meta-checksum": "ff",
              "x-amz-meta-handle": "ZGF0YS50eHQ="}
    upload_broker._upload_urls_cache = {"upload_urls": {
        "readme": "https://s3/readme?sig",
        "items": {identifier: {
            "url": "https://s3/item?sig", "relpath": "data.txt",
            "headers": pinned,
        }},
    }}
    upload_broker._readme_content = "description: test"
    upload_broker._signal_upload_complete = lambda: None

    puts = []
    monkeypatch.setattr(
        sbmod.requests, "put",
        lambda url, **kw: (puts.append((url, kw.get("headers"))),
                           FakeResponse())[1])

    upload_broker.post_freeze_hook()

    item_puts = [p for p in puts if "item" in p[0]]
    assert item_puts[0][1] == pinned
    readme_puts = [p for p in puts if "readme" in p[0]]
    assert len(readme_puts) == 1


def test_post_freeze_hook_skips_empty_readme(upload_broker, monkeypatch):
    upload_broker._upload_urls_cache = {"upload_urls": {
        "readme": "https://s3/readme?sig", "items": {},
    }}
    upload_broker._readme_content = None
    upload_broker._pending_items = {}
    upload_broker._signal_upload_complete = lambda: None

    puts = []
    monkeypatch.setattr(
        sbmod.requests, "put",
        lambda url, **kw: (puts.append(url), FakeResponse())[1])

    upload_broker.post_freeze_hook()
    assert puts == []


def test_signal_upload_complete_posts_backend_uri(broker):
    calls = capture_api_request(broker, {})
    broker._signal_upload_complete()
    assert calls[0]["path"] == "/signed-urls/upload-complete"
    assert calls[0]["json"] == {"uri": broker._backend_uri}
