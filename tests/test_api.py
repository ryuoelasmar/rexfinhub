"""
Tests for web pages and REST API endpoints.
"""
import pytest

API_KEY = "rex-etp-api-2026-kJw9xPm4"
API_HEADERS = {"X-API-Key": API_KEY}


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"


def test_dashboard(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "ETP Filing Tracker" in r.text
    assert "Test Trust" in r.text


def test_fund_list(client):
    r = client.get("/funds/")
    assert r.status_code == 200
    assert "Test Ultra Fund" in r.text
    assert "TULF" in r.text


def test_fund_list_search(client):
    r = client.get("/funds/?q=Ultra")
    assert r.status_code == 200
    assert "Test Ultra Fund" in r.text


def test_fund_list_status_filter(client):
    r = client.get("/funds/?status=EFFECTIVE")
    assert r.status_code == 200
    assert "Test Ultra Fund" in r.text


def test_fund_detail(client):
    r = client.get("/funds/S000111111")
    assert r.status_code == 200
    assert "Test Ultra Fund" in r.text
    assert "EFFECTIVE" in r.text


def test_fund_detail_404(client):
    r = client.get("/funds/S000NONEXISTENT")
    assert r.status_code == 404


def test_trust_detail(client):
    r = client.get("/trusts/test-trust")
    assert r.status_code == 200
    assert "Test Trust" in r.text


def test_trust_detail_404(client):
    r = client.get("/trusts/nonexistent-trust")
    assert r.status_code == 404


def test_filings_page(client):
    r = client.get("/filings/")
    assert r.status_code == 200
    assert "485BPOS" in r.text


def test_downloads_page(client):
    r = client.get("/downloads/")
    assert r.status_code == 200
    assert "Downloads" in r.text
    assert "Live Exports" in r.text


def test_downloads_export_funds_csv(client):
    r = client.get("/downloads/export/funds")
    assert r.status_code == 200
    assert r.headers["content-type"] == "text/csv; charset=utf-8"
    assert "Test Ultra Fund" in r.text
    assert "TULF" in r.text


def test_subscribe_page(client):
    r = client.get("/digest/subscribe")
    assert r.status_code == 200
    assert "Subscribe" in r.text


# --- REST API tests (require API key) ---

def test_api_health(client):
    r = client.get("/api/v1/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_api_rejects_no_key(client):
    """API should reject requests without a valid key."""
    r = client.get("/api/v1/trusts")
    assert r.status_code == 401


def test_api_trusts(client):
    r = client.get("/api/v1/trusts", headers=API_HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert len(data) >= 1
    names = [t["name"] for t in data]
    assert "Test Trust" in names


def test_api_funds(client):
    r = client.get("/api/v1/funds", headers=API_HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert len(data) >= 1


def test_api_filings_recent(client):
    r = client.get("/api/v1/filings/recent?days=9999", headers=API_HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert len(data) >= 1


def test_api_pipeline_status(client):
    r = client.get("/api/v1/pipeline/status", headers=API_HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "never_run"
