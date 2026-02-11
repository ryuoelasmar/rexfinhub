"""
Tests for the digest subscribe page.
"""
import pytest
from pathlib import Path


def test_subscribe_page_renders(client):
    r = client.get("/digest/subscribe")
    assert r.status_code == 200
    assert "Subscribe" in r.text
    assert "Request Access" in r.text


def test_subscribe_invalid_email(client):
    r = client.post("/digest/subscribe", data={"email": "not-an-email"})
    assert r.status_code == 200
    assert "valid email" in r.text


def test_subscribe_valid_email(client, tmp_path, monkeypatch):
    """Valid email should be appended to the subscribers file."""
    sub_file = tmp_path / "digest_subscribers.txt"
    monkeypatch.setattr("webapp.routers.digest.SUBSCRIBERS_FILE", sub_file)

    r = client.post("/digest/subscribe", data={"email": "test@example.com"})
    assert r.status_code == 200
    assert "submitted" in r.text.lower() or "review" in r.text.lower()

    content = sub_file.read_text()
    assert "test@example.com" in content
    assert "PENDING" in content


def test_subscribe_duplicate_blocked(client, tmp_path, monkeypatch):
    """Duplicate emails should be rejected."""
    sub_file = tmp_path / "digest_subscribers.txt"
    sub_file.write_text("PENDING|dupe@example.com|2025-01-01T00:00:00\n")
    monkeypatch.setattr("webapp.routers.digest.SUBSCRIBERS_FILE", sub_file)

    r = client.post("/digest/subscribe", data={"email": "dupe@example.com"})
    assert r.status_code == 200
    assert "already" in r.text.lower()
