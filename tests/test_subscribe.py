"""
Tests for the digest subscribe page (DB-based).
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


def test_subscribe_valid_email(client):
    """Valid email should be saved to the database."""
    r = client.post("/digest/subscribe", data={"email": "test-new@example.com"})
    assert r.status_code == 200
    assert "submitted" in r.text.lower() or "review" in r.text.lower()


def test_subscribe_duplicate_blocked(client):
    """Duplicate emails should be rejected."""
    # First submission
    client.post("/digest/subscribe", data={"email": "dupe-test@example.com"})
    # Second submission of same email
    r = client.post("/digest/subscribe", data={"email": "dupe-test@example.com"})
    assert r.status_code == 200
    assert "already" in r.text.lower()
