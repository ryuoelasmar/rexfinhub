"""
Shared test fixtures - in-memory SQLite DB + FastAPI TestClient.
"""
from __future__ import annotations

import pytest
from datetime import date, datetime

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from webapp.database import Base
from webapp.models import Trust, Filing, FundStatus, FundExtraction, NameHistory


# ---------------------------------------------------------------------------
# In-memory SQLite engine (fresh per test session)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def engine():
    eng = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})

    @event.listens_for(eng, "connect")
    def _set_pragma(dbapi_conn, _):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    Base.metadata.create_all(bind=eng)
    return eng


@pytest.fixture()
def db_session(engine):
    """Yields a DB session that rolls back after each test."""
    connection = engine.connect()
    transaction = connection.begin()
    Session = sessionmaker(bind=connection)
    session = Session()

    yield session

    session.close()
    transaction.rollback()
    connection.close()


# ---------------------------------------------------------------------------
# Seeded DB session (has sample data)
# ---------------------------------------------------------------------------
@pytest.fixture()
def seeded_db(db_session):
    """DB session pre-loaded with sample trust, filings, funds."""
    trust = Trust(
        cik="0001234567",
        name="Test Trust",
        slug="test-trust",
        is_rex=True,
        is_active=True,
    )
    db_session.add(trust)
    db_session.flush()

    trust2 = Trust(
        cik="0009999999",
        name="Other Trust",
        slug="other-trust",
        is_rex=False,
        is_active=True,
    )
    db_session.add(trust2)
    db_session.flush()

    # Filing
    filing = Filing(
        trust_id=trust.id,
        accession_number="0001234567-25-000001",
        form="485BPOS",
        filing_date=date(2025, 6, 15),
        primary_link="https://sec.gov/test",
        cik="0001234567",
        registrant="Test Trust",
        processed=True,
    )
    db_session.add(filing)
    db_session.flush()

    filing2 = Filing(
        trust_id=trust.id,
        accession_number="0001234567-25-000002",
        form="485APOS",
        filing_date=date(2025, 5, 1),
        primary_link="https://sec.gov/test2",
        cik="0001234567",
        registrant="Test Trust",
        processed=True,
    )
    db_session.add(filing2)
    db_session.flush()

    # Fund status
    fund = FundStatus(
        trust_id=trust.id,
        series_id="S000111111",
        class_contract_id="C000222222",
        fund_name="Test Ultra Fund",
        ticker="TULF",
        status="EFFECTIVE",
        effective_date=date(2025, 6, 15),
        latest_form="485BPOS",
        latest_filing_date=date(2025, 6, 15),
    )
    db_session.add(fund)

    fund2 = FundStatus(
        trust_id=trust.id,
        series_id="S000333333",
        class_contract_id="C000444444",
        fund_name="Test Pending Fund",
        ticker="TPND",
        status="PENDING",
        latest_form="485APOS",
        latest_filing_date=date(2025, 5, 1),
    )
    db_session.add(fund2)

    # Fund extraction
    extraction = FundExtraction(
        filing_id=filing.id,
        series_id="S000111111",
        series_name="Test Ultra Fund",
        class_contract_id="C000222222",
        class_contract_name="Test Ultra Fund",
        class_symbol="TULF",
        effective_date=date(2025, 6, 15),
        effective_date_confidence="HIGH",
    )
    db_session.add(extraction)

    # Name history
    name = NameHistory(
        series_id="S000111111",
        name="Test Ultra Fund",
        name_clean="Test Ultra Fund",
        first_seen_date=date(2025, 1, 1),
        last_seen_date=date(2025, 6, 15),
        is_current=True,
        source_form="485BPOS",
    )
    db_session.add(name)

    db_session.commit()
    return db_session


# ---------------------------------------------------------------------------
# FastAPI test client
# ---------------------------------------------------------------------------
@pytest.fixture()
def client(seeded_db):
    """TestClient wired to the seeded in-memory DB."""
    from fastapi.testclient import TestClient
    from webapp.main import create_app
    from webapp.dependencies import get_db

    app = create_app()

    def _override_db():
        yield seeded_db

    app.dependency_overrides[get_db] = _override_db

    with TestClient(app) as c:
        yield c
