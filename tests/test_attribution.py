"""Attribution audit validation tests.

Ensures CSV rules are internally consistent, schemas match actual values,
no orphan entries exist, and category_display derivation produces clean groups.
"""
import pandas as pd
import pytest

from market.config import RULES_DIR


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def fund_mapping():
    return pd.read_csv(RULES_DIR / "fund_mapping.csv", engine="python", on_bad_lines="skip")


@pytest.fixture(scope="module")
def attr_cc():
    return pd.read_csv(RULES_DIR / "attributes_CC.csv", engine="python", on_bad_lines="skip")


@pytest.fixture(scope="module")
def attr_defined():
    return pd.read_csv(RULES_DIR / "attributes_Defined.csv", engine="python", on_bad_lines="skip")


@pytest.fixture(scope="module")
def attr_li():
    return pd.read_csv(RULES_DIR / "attributes_LI.csv", engine="python", on_bad_lines="skip")


@pytest.fixture(scope="module")
def attr_crypto():
    return pd.read_csv(RULES_DIR / "attributes_Crypto.csv", engine="python", on_bad_lines="skip")


@pytest.fixture(scope="module")
def attr_thematic():
    return pd.read_csv(RULES_DIR / "attributes_Thematic.csv", engine="python", on_bad_lines="skip")


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

def test_cc_categories_match_schema(attr_cc):
    """Every cc_category value in CSV must be in schemas.py CC_CATEGORIES."""
    from tools.rules_editor.schemas import CC_CATEGORIES
    csv_vals = set(attr_cc["cc_category"].dropna().unique())
    schema_vals = set(CC_CATEGORIES)
    assert csv_vals == schema_vals, f"Mismatch: CSV extra={csv_vals - schema_vals}, Schema extra={schema_vals - csv_vals}"


def test_defined_categories_match_schema(attr_defined):
    """Every map_defined_category value in CSV must be in schemas.py DEFINED_CATEGORIES."""
    from tools.rules_editor.schemas import DEFINED_CATEGORIES
    csv_vals = set(attr_defined["map_defined_category"].dropna().unique())
    schema_vals = set(DEFINED_CATEGORIES)
    assert csv_vals == schema_vals, f"Mismatch: CSV extra={csv_vals - schema_vals}, Schema extra={schema_vals - csv_vals}"


def test_no_autocallable_in_defined(attr_defined):
    """Defined Outcome should never have 'Autocallable' category."""
    vals = attr_defined["map_defined_category"].dropna().unique()
    assert "Autocallable" not in vals


def test_no_shield_in_defined(attr_defined):
    """Shield category should be retired (merged into Buffer/Hedged Equity)."""
    vals = attr_defined["map_defined_category"].dropna().unique()
    assert "Shield" not in vals


# ---------------------------------------------------------------------------
# Orphan checks (attribute entries without fund_mapping)
# ---------------------------------------------------------------------------

def _orphan_check(fund_mapping, attr_df, category):
    mapped = set(fund_mapping[fund_mapping["etp_category"] == category]["ticker"].values)
    attr_tickers = set(attr_df["ticker"].values)
    orphans = attr_tickers - mapped
    return orphans


def test_no_cc_orphans(fund_mapping, attr_cc):
    orphans = _orphan_check(fund_mapping, attr_cc, "CC")
    assert not orphans, f"CC orphans: {orphans}"


def test_no_defined_orphans(fund_mapping, attr_defined):
    orphans = _orphan_check(fund_mapping, attr_defined, "Defined")
    assert not orphans, f"Defined orphans: {orphans}"


def test_no_li_orphans(fund_mapping, attr_li):
    orphans = _orphan_check(fund_mapping, attr_li, "LI")
    assert not orphans, f"LI orphans: {orphans}"


def test_no_crypto_orphans(fund_mapping, attr_crypto):
    orphans = _orphan_check(fund_mapping, attr_crypto, "Crypto")
    assert not orphans, f"Crypto orphans: {orphans}"


def test_no_thematic_orphans(fund_mapping, attr_thematic):
    orphans = _orphan_check(fund_mapping, attr_thematic, "Thematic")
    assert not orphans, f"Thematic orphans: {orphans}"


# ---------------------------------------------------------------------------
# Coverage checks (fund_mapping entries should have attribute data)
# ---------------------------------------------------------------------------

def _coverage_check(fund_mapping, attr_df, category):
    mapped = set(fund_mapping[fund_mapping["etp_category"] == category]["ticker"].values)
    attr_tickers = set(attr_df["ticker"].values)
    missing = mapped - attr_tickers
    return missing


def test_all_cc_have_attributes(fund_mapping, attr_cc):
    missing = _coverage_check(fund_mapping, attr_cc, "CC")
    assert not missing, f"CC missing attributes: {missing}"


def test_all_defined_have_attributes(fund_mapping, attr_defined):
    missing = _coverage_check(fund_mapping, attr_defined, "Defined")
    assert not missing, f"Defined missing attributes: {missing}"


def test_all_li_have_attributes(fund_mapping, attr_li):
    missing = _coverage_check(fund_mapping, attr_li, "LI")
    assert not missing, f"LI missing attributes: {missing}"


def test_all_crypto_have_attributes(fund_mapping, attr_crypto):
    missing = _coverage_check(fund_mapping, attr_crypto, "Crypto")
    assert not missing, f"Crypto missing attributes: {missing}"


def test_all_thematic_have_attributes(fund_mapping, attr_thematic):
    missing = _coverage_check(fund_mapping, attr_thematic, "Thematic")
    assert not missing, f"Thematic missing attributes: {missing}"


# ---------------------------------------------------------------------------
# Data integrity
# ---------------------------------------------------------------------------

def test_no_duplicate_fund_mapping(fund_mapping):
    """Primary key (ticker, etp_category) must be unique."""
    dupes = fund_mapping.duplicated(subset=["ticker", "etp_category"], keep=False)
    dupe_rows = fund_mapping[dupes]
    assert dupe_rows.empty, f"Duplicate fund_mapping entries:\n{dupe_rows}"


def test_no_negative_leverage(attr_li):
    """Leverage amounts must be non-negative (direction handles sign)."""
    lev = attr_li["map_li_leverage_amount"].dropna()
    negatives = lev[lev < 0]
    assert negatives.empty, f"Negative leverage values: {attr_li[attr_li['map_li_leverage_amount'] < 0]['ticker'].tolist()}"


def test_no_blank_crypto_types(attr_crypto):
    """Every crypto entry should have a map_crypto_type value."""
    blanks = attr_crypto["map_crypto_type"].isna().sum()
    assert blanks == 0, f"{blanks} crypto entries have blank type"


def test_crypto_column_renamed(attr_crypto):
    """map_crypto_is_spot should be renamed to map_crypto_type."""
    assert "map_crypto_type" in attr_crypto.columns, "CSV still uses old name map_crypto_is_spot"
    assert "map_crypto_is_spot" not in attr_crypto.columns, "Old column name map_crypto_is_spot still present"


def test_valid_li_directions(attr_li):
    """LI direction should be Long, Short, or Tactical."""
    valid = {"Long", "Short", "Tactical"}
    actual = set(attr_li["map_li_direction"].dropna().unique())
    invalid = actual - valid
    assert not invalid, f"Invalid LI directions: {invalid}"


def test_valid_cc_types(attr_cc):
    """CC type should be Traditional or Synthetic."""
    valid = {"Traditional", "Synthetic"}
    actual = set(attr_cc["cc_type"].dropna().unique())
    invalid = actual - valid
    assert not invalid, f"Invalid CC types: {invalid}"


# ---------------------------------------------------------------------------
# Tactical direction
# ---------------------------------------------------------------------------

def test_tactical_is_valid_direction(attr_li):
    """Tactical should be a recognized LI direction alongside Long/Short."""
    valid = {"Long", "Short", "Tactical"}
    actual = set(attr_li["map_li_direction"].dropna().unique())
    assert actual.issubset(valid), f"Unexpected directions: {actual - valid}"


def test_tactical_count(attr_li):
    """There should be some Tactical products (not zero)."""
    tactical = attr_li[attr_li["map_li_direction"] == "Tactical"]
    assert len(tactical) > 0, "No Tactical direction products found"


# ---------------------------------------------------------------------------
# ACTV filter logic
# ---------------------------------------------------------------------------

def test_actv_filter_helper():
    """_is_actv should correctly identify active vs liquidated funds."""
    from webapp.services.market_data import _is_actv
    df = pd.DataFrame({
        "market_status": ["ACTV", "LIQU", "DLST", None, "ACTV", ""],
    })
    mask = _is_actv(df)
    assert mask.tolist() == [True, False, False, True, True, False]


def test_actv_filter_no_column():
    """_is_actv should return all True when market_status column is missing."""
    from webapp.services.market_data import _is_actv
    df = pd.DataFrame({"ticker": ["A", "B", "C"]})
    mask = _is_actv(df)
    assert mask.all()


# ---------------------------------------------------------------------------
# Schema consistency: crypto rename propagation
# ---------------------------------------------------------------------------

def test_crypto_schema_uses_new_column_name():
    """schemas.py should reference map_crypto_type, not map_crypto_is_spot."""
    from tools.rules_editor.schemas import SCHEMAS
    crypto_schema = SCHEMAS["attributes_Crypto.csv"]
    col_names = [c.name for c in crypto_schema.columns]
    assert "map_crypto_type" in col_names, "Schema still uses old name"
    assert "map_crypto_is_spot" not in col_names, "Schema still has old name"


def test_market_config_uses_new_column_name():
    """market/config.py CRYPTO_ATTR_COLS should use map_crypto_type."""
    from market.config import CRYPTO_ATTR_COLS
    assert "map_crypto_type" in CRYPTO_ATTR_COLS
    assert "map_crypto_is_spot" not in CRYPTO_ATTR_COLS
