"""Shared fixtures for dtool-dserver tests."""

import hashlib
import json
import os
import tempfile

import pytest

from dtoolcore.utils import generate_identifier

from dtool_dserver.storagebroker import DServerStorageBroker

SERVER = "example.org:5000"
BUCKET_BASE = f"dserver://{SERVER}/s3/my-bucket"
UUID = "550e8400-e29b-41d4-a716-446655440000"
DATASET_URI = f"{BUCKET_BASE}/{UUID}"


class FakeResponse:
    """Minimal stand-in for requests.Response."""

    def __init__(self, json_data=None, headers=None, text=""):
        self._json = json_data if json_data is not None else {}
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._json

    def raise_for_status(self):
        pass


class TrapHasher:
    """Hasher that fails the test if invoked, but mimics FileHasher."""

    name = "md5sum_hexdigest"

    def __call__(self, fpath):
        raise AssertionError(f"File was re-hashed unexpectedly: {fpath}")


@pytest.fixture
def broker(monkeypatch):
    """Dataset-URI broker with token configured via environment."""
    monkeypatch.setenv("DSERVER_TOKEN", "test-token")
    return DServerStorageBroker(DATASET_URI)


@pytest.fixture
def item_file():
    """Temporary file representing a dataset item."""
    with tempfile.NamedTemporaryFile(
            mode="wb", suffix=".txt", delete=False) as fh:
        fh.write(b"item content")
        fpath = fh.name
    yield fpath
    os.unlink(fpath)


def md5_hexdigest(content):
    return hashlib.md5(content).hexdigest()


def make_manifest(relpath, content=b"item content"):
    identifier = generate_identifier(relpath)
    return identifier, {
        "dtoolcore_version": "3.0.0",
        "hash_function": "md5sum_hexdigest",
        "items": {
            identifier: {
                "relpath": relpath,
                "size_in_bytes": len(content),
                "hash": md5_hexdigest(content),
                "utc_timestamp": 1700000000.0,
            }
        },
    }
