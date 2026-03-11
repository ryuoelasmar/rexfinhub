"""Validation functions for rules CSV files."""

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from .schemas import SCHEMAS, FileSchema, ETP_CATEGORIES, get_schema

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_RULES = PROJECT_ROOT / "config" / "rules"

# Map etp_category -> attributes filename
CATEGORY_ATTR_FILES = {
    "LI": "attributes_LI.csv",
    "CC": "attributes_CC.csv",
    "Crypto": "attributes_Crypto.csv",
    "Defined": "attributes_Defined.csv",
    "Thematic": "attributes_Thematic.csv",
}


@dataclass
class ValidationIssue:
    file: str
    severity: str  # error, warning
    message: str
    rows: list[int] = field(default_factory=list)  # 0-indexed row numbers


def _read(filename: str) -> pd.DataFrame:
    path = CONFIG_RULES / filename
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, engine="python", on_bad_lines="skip")


# -- Per-file validators --


def check_duplicate_keys(df: pd.DataFrame, schema: FileSchema) -> list[ValidationIssue]:
    """Find rows with duplicate primary key values."""
    issues = []
    pk = schema.primary_key
    if not pk or df.empty:
        return issues
    available = [c for c in pk if c in df.columns]
    if not available:
        return issues
    dupes = df[df.duplicated(subset=available, keep=False)]
    if not dupes.empty:
        groups = dupes.groupby(available).apply(lambda g: g.index.tolist())
        for key_vals, rows in groups.items():
            key_str = dict(zip(available, key_vals if isinstance(key_vals, tuple) else [key_vals]))
            issues.append(ValidationIssue(
                schema.filename, "error",
                f"Duplicate key {key_str}",
                rows=list(rows),
            ))
    return issues


def check_required_fields(df: pd.DataFrame, schema: FileSchema) -> list[ValidationIssue]:
    """Find rows with blank required fields."""
    issues = []
    for col_def in schema.columns:
        if not col_def.required or col_def.name not in df.columns:
            continue
        blank = df[df[col_def.name].isna() | (df[col_def.name].astype(str).str.strip() == "")]
        if not blank.empty:
            issues.append(ValidationIssue(
                schema.filename, "error",
                f"Missing required field '{col_def.name}' in {len(blank)} row(s)",
                rows=blank.index.tolist(),
            ))
    return issues


def check_enum_values(df: pd.DataFrame, schema: FileSchema) -> list[ValidationIssue]:
    """Find rows with invalid enum values in dropdown columns."""
    issues = []
    for col_def in schema.columns:
        if col_def.choices is None or col_def.name not in df.columns:
            continue
        valid_set = set(col_def.choices)
        filled = df[df[col_def.name].notna() & (df[col_def.name].astype(str).str.strip() != "")]
        if filled.empty:
            continue
        invalid = filled[~filled[col_def.name].astype(str).isin(valid_set)]
        if not invalid.empty:
            bad_vals = invalid[col_def.name].unique().tolist()
            issues.append(ValidationIssue(
                schema.filename, "error",
                f"Invalid '{col_def.name}' values: {bad_vals}",
                rows=invalid.index.tolist(),
            ))
    return issues


def validate_file(df: pd.DataFrame, schema: FileSchema) -> list[ValidationIssue]:
    """Run all per-file validations."""
    issues = []
    issues.extend(check_duplicate_keys(df, schema))
    issues.extend(check_required_fields(df, schema))
    issues.extend(check_enum_values(df, schema))
    return issues


# -- Cross-file validators --


def check_orphan_tickers() -> list[ValidationIssue]:
    """Tickers in attributes files not present in fund_mapping with matching category."""
    issues = []
    fm = _read("fund_mapping.csv")
    if fm.empty:
        return issues

    for cat, attr_file in CATEGORY_ATTR_FILES.items():
        attr_df = _read(attr_file)
        if attr_df.empty or "ticker" not in attr_df.columns:
            continue
        mapped = set(fm.loc[fm["etp_category"] == cat, "ticker"].dropna())
        attr_tickers = set(attr_df["ticker"].dropna())
        orphans = attr_tickers - mapped
        if orphans:
            sorted_orphans = sorted(orphans)
            issues.append(ValidationIssue(
                attr_file, "warning",
                f"{len(orphans)} ticker(s) not in fund_mapping with category={cat}: "
                f"{sorted_orphans[:10]}{'...' if len(sorted_orphans) > 10 else ''}",
            ))
    return issues


def check_rex_consistency() -> list[ValidationIssue]:
    """rex_suite_mapping tickers must be in rex_funds; rex_funds tickers should be in fund_mapping."""
    issues = []
    rex_funds = _read("rex_funds.csv")
    rex_suite = _read("rex_suite_mapping.csv")
    fm = _read("fund_mapping.csv")

    rex_tickers = set(rex_funds["ticker"].dropna()) if not rex_funds.empty and "ticker" in rex_funds.columns else set()
    suite_tickers = set(rex_suite["ticker"].dropna()) if not rex_suite.empty and "ticker" in rex_suite.columns else set()
    fm_tickers = set(fm["ticker"].dropna()) if not fm.empty and "ticker" in fm.columns else set()

    # suite tickers not in rex_funds
    missing_in_rex = suite_tickers - rex_tickers
    if missing_in_rex:
        issues.append(ValidationIssue(
            "rex_suite_mapping.csv", "error",
            f"{len(missing_in_rex)} ticker(s) in rex_suite_mapping but not in rex_funds: "
            f"{sorted(missing_in_rex)[:10]}",
        ))

    # rex_funds tickers not in fund_mapping
    missing_in_fm = rex_tickers - fm_tickers
    if missing_in_fm:
        issues.append(ValidationIssue(
            "rex_funds.csv", "warning",
            f"{len(missing_in_fm)} REX ticker(s) not in fund_mapping: "
            f"{sorted(missing_in_fm)[:10]}",
        ))

    return issues


def check_unmapped_issuers(bbg_df: pd.DataFrame | None) -> list[ValidationIssue]:
    """(etp_category, issuer) pairs in Bloomberg data not in issuer_mapping."""
    issues = []
    if bbg_df is None or "issuer" not in bbg_df.columns:
        return issues

    im = _read("issuer_mapping.csv")
    fm = _read("fund_mapping.csv")
    if im.empty or fm.empty:
        return issues

    # Build set of mapped (category, issuer) pairs
    mapped_pairs = set()
    if "etp_category" in im.columns and "issuer" in im.columns:
        for _, row in im.iterrows():
            if pd.notna(row["etp_category"]) and pd.notna(row["issuer"]):
                mapped_pairs.add((str(row["etp_category"]).strip(), str(row["issuer"]).strip()))

    # Build (category, issuer) pairs from Bloomberg + fund_mapping
    bbg_with_cat = bbg_df.merge(fm, on="ticker", how="inner")
    if "etp_category" not in bbg_with_cat.columns or "issuer" not in bbg_with_cat.columns:
        return issues

    bbg_pairs = set()
    for _, row in bbg_with_cat.iterrows():
        if pd.notna(row["etp_category"]) and pd.notna(row["issuer"]):
            bbg_pairs.add((str(row["etp_category"]).strip(), str(row["issuer"]).strip()))

    unmapped = bbg_pairs - mapped_pairs
    if unmapped:
        issues.append(ValidationIssue(
            "issuer_mapping.csv", "warning",
            f"{len(unmapped)} (category, issuer) pair(s) in Bloomberg data not in issuer_mapping",
        ))
    return issues


def check_unmapped_tickers(bbg_df: pd.DataFrame | None) -> list[ValidationIssue]:
    """Tickers in Bloomberg data not in fund_mapping, sorted by AUM desc."""
    issues = []
    if bbg_df is None or "ticker" not in bbg_df.columns:
        return issues

    fm = _read("fund_mapping.csv")
    fm_tickers = set(fm["ticker"].dropna()) if not fm.empty and "ticker" in fm.columns else set()
    bbg_tickers = set(bbg_df["ticker"].dropna())
    unmapped = bbg_tickers - fm_tickers

    if unmapped:
        # Sort by AUM descending if available
        unmapped_df = bbg_df[bbg_df["ticker"].isin(unmapped)].copy()
        if "aum" in unmapped_df.columns:
            unmapped_df = unmapped_df.sort_values("aum", ascending=False)
        top = unmapped_df["ticker"].head(20).tolist()
        issues.append(ValidationIssue(
            "fund_mapping.csv", "warning",
            f"{len(unmapped)} Bloomberg ticker(s) not in fund_mapping. "
            f"Top by AUM: {top}",
        ))
    return issues


def run_full_validation(bbg_df: pd.DataFrame | None = None) -> dict[str, list[ValidationIssue]]:
    """Run all per-file and cross-file validations.

    Returns dict keyed by filename -> list of issues.
    """
    all_issues: dict[str, list[ValidationIssue]] = {}

    # Per-file
    for filename, schema in SCHEMAS.items():
        df = _read(filename)
        file_issues = validate_file(df, schema)
        if file_issues:
            all_issues.setdefault(filename, []).extend(file_issues)

    # Cross-file
    for issue in check_orphan_tickers():
        all_issues.setdefault(issue.file, []).append(issue)

    for issue in check_rex_consistency():
        all_issues.setdefault(issue.file, []).append(issue)

    # Bloomberg cross-reference
    for issue in check_unmapped_issuers(bbg_df):
        all_issues.setdefault(issue.file, []).append(issue)

    for issue in check_unmapped_tickers(bbg_df):
        all_issues.setdefault(issue.file, []).append(issue)

    return all_issues
