"""REX Rules Editor -- Standalone Streamlit App.

Multi-page app for ETP categorization management and rules CSV editing.

Pages:
  1. Queues        - Review queue: approve/deny unmapped funds and new issuers
  2. Dashboard     - Coverage stats, category distribution
  3. Auto-Classify - Run classification engine, review, bulk apply
  4. Fund Mapping  - Edit fund_mapping.csv with expanded categories
  5. Rules Editor  - Generic CSV editor for other rules files
  6. Sync & Validate - config/rules/ <-> data/rules/ sync + validation

Launch: python -m streamlit run tools/rules_editor/app.py
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# Ensure project root importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.rules_editor.schemas import (
    SCHEMAS, get_grouped_files, get_schema, FileSchema, ETP_CATEGORIES,
)
from tools.rules_editor.validators import validate_file, run_full_validation
from tools.rules_editor.sync import compare_all, sync_config_to_data
from tools.rules_editor.categorize import (
    load_bloomberg, run_classification, load_fund_mapping, save_fund_mapping,
    get_coverage_stats, VALID_CATEGORIES, CATEGORY_LABELS, STRATEGY_TO_CODE,
)

RULES_DIR = PROJECT_ROOT / "config" / "rules"

st.set_page_config(page_title="REX Rules Editor", layout="wide")


# ===================================================================
# Sidebar
# ===================================================================

st.sidebar.title("REX Rules Editor")

page = st.sidebar.radio(
    "Navigation",
    ["Classification Queue", "Dashboard", "Auto-Classify", "Fund Mapping",
     "Rules Editor", "Sync & Validate"],
    index=0,
)

# Quick stats
fm_sidebar = load_fund_mapping()
st.sidebar.markdown("---")
st.sidebar.caption("Fund Mapping")
c1, c2 = st.sidebar.columns(2)
c1.metric("Entries", f"{len(fm_sidebar):,}")
c2.metric("Tickers", f"{fm_sidebar['ticker'].nunique():,}" if not fm_sidebar.empty else "0")


# ===================================================================
# Helpers
# ===================================================================

def load_csv(filename: str) -> pd.DataFrame:
    path = RULES_DIR / filename
    if not path.exists():
        schema = get_schema(filename)
        return pd.DataFrame(columns=[c.name for c in schema.columns])
    return pd.read_csv(path, engine="python", on_bad_lines="skip", keep_default_na=False)


def save_csv(filename: str, df: pd.DataFrame):
    path = RULES_DIR / filename
    df.to_csv(path, index=False)


def file_info(filename: str) -> dict:
    path = RULES_DIR / filename
    if path.exists():
        stat = path.stat()
        return {
            "exists": True,
            "rows": len(load_csv(filename)),
            "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
        }
    return {"exists": False, "rows": 0, "modified": "N/A"}


def compute_diff(original: pd.DataFrame, edited: pd.DataFrame, schema: FileSchema):
    """Compute added, deleted, modified rows by primary key."""
    pk = schema.primary_key
    available_pk = [c for c in pk if c in original.columns and c in edited.columns]
    if not available_pk:
        return {"added": edited, "deleted": original, "modified": pd.DataFrame()}

    orig = original.copy().fillna("").astype(str)
    edit = edited.copy().fillna("").astype(str)
    orig["_pk"] = orig[available_pk].apply(lambda r: "|".join(r), axis=1)
    edit["_pk"] = edit[available_pk].apply(lambda r: "|".join(r), axis=1)

    orig_keys = set(orig["_pk"])
    edit_keys = set(edit["_pk"])

    added = edit[edit["_pk"].isin(edit_keys - orig_keys)].drop(columns="_pk")
    deleted = orig[orig["_pk"].isin(orig_keys - edit_keys)].drop(columns="_pk")

    # Modified: same key, different values
    modified_rows = []
    common_keys = orig_keys & edit_keys
    if common_keys:
        orig_idx = orig.set_index("_pk")
        edit_idx = edit.set_index("_pk")
        data_cols = [c for c in orig.columns if c != "_pk"]
        for key in common_keys:
            o = orig_idx.loc[key, data_cols]
            e = edit_idx.loc[key, data_cols]
            if isinstance(o, pd.DataFrame):
                o = o.iloc[0]
            if isinstance(e, pd.DataFrame):
                e = e.iloc[0]
            if not o.equals(e):
                modified_rows.append(e)

    modified = pd.DataFrame(modified_rows) if modified_rows else pd.DataFrame()
    return {"added": added, "deleted": deleted, "modified": modified}


# ===================================================================
# Page: Queues (Classification Queue)
# ===================================================================

# The 5 tracked categories -- everything else is ignored
_TRACKED_CATEGORIES = {"LI", "CC", "Crypto", "Defined", "Thematic"}


def page_queues():
    st.header("Classification Queue")
    st.caption(
        "Keyword-matched candidates for your 5 tracked categories. "
        "Review the matched rule, then Approve or Deny each fund."
    )

    # Load + classify button
    if "queue_candidates" not in st.session_state:
        st.markdown(
            "Loads Bloomberg data, finds unmapped funds, and runs keyword rules "
            "to identify candidates for **LI, CC, Crypto, Defined, and Thematic**. "
            "Only HIGH and MEDIUM confidence matches are shown."
        )
        if st.button("Scan for New Candidates", type="primary"):
            _run_scan()
        else:
            # Show stale counts from last pipeline run if available
            queue_path = RULES_DIR / "_queues_report.json"
            if queue_path.exists():
                mtime = datetime.fromtimestamp(queue_path.stat().st_mtime)
                st.caption(f"Last pipeline queue: {mtime.strftime('%Y-%m-%d %H:%M')}")
        return

    candidates = st.session_state["queue_candidates"]
    new_issuers = st.session_state.get("queue_issuers", pd.DataFrame())
    fm = st.session_state["queue_fm"]

    # Refresh button
    if st.button("Rescan"):
        st.session_state.pop("queue_candidates", None)
        st.session_state.pop("queue_issuers", None)
        st.session_state.pop("queue_fm", None)
        st.cache_data.clear()
        st.rerun()

    if candidates.empty and new_issuers.empty:
        st.success("All relevant funds are mapped. Nothing to review.")
        return

    # KPIs per category
    cols = st.columns(6)
    for i, cat in enumerate(["LI", "CC", "Crypto", "Defined", "Thematic"]):
        n = len(candidates[candidates["etp_category"] == cat]) if not candidates.empty else 0
        cols[i].metric(CATEGORY_LABELS.get(cat, cat), f"{n}")
    cols[5].metric("Total", f"{len(candidates):,}")

    # Tabs: one per category + issuers
    tab_names = []
    tab_cats = []
    for cat in ["LI", "CC", "Crypto", "Defined", "Thematic"]:
        n = len(candidates[candidates["etp_category"] == cat]) if not candidates.empty else 0
        if n > 0:
            tab_names.append(f"{CATEGORY_LABELS.get(cat, cat)} ({n})")
            tab_cats.append(cat)

    if not new_issuers.empty:
        tab_names.append(f"New Issuers ({len(new_issuers)})")
        tab_cats.append("_issuers")

    if not tab_names:
        st.success("No candidates to review.")
        return

    tabs = st.tabs(tab_names)

    for tab, cat in zip(tabs, tab_cats):
        with tab:
            if cat == "_issuers":
                _render_issuer_queue(new_issuers)
            else:
                subset = candidates[candidates["etp_category"] == cat].copy()
                _render_category_queue(subset, fm, cat)


def _run_scan():
    """Load Bloomberg, classify unmapped funds, store in session."""
    with st.spinner("Loading Bloomberg data..."):
        try:
            etp = load_bloomberg()
        except Exception as e:
            st.error(f"Failed to load Bloomberg data: {e}")
            return

    if etp.empty:
        st.error("Bloomberg data is empty. Check bloomberg_daily_file.xlsm.")
        return

    fm = load_fund_mapping()
    mapped = set(fm["ticker"].astype(str).str.strip()) if not fm.empty else set()

    excl = load_csv("exclusions.csv")
    excluded = set()
    if not excl.empty and "ticker" in excl.columns:
        excluded = set(excl["ticker"].astype(str).str.strip())

    # Only classify unmapped, non-excluded funds
    unmapped_etp = etp[~etp["ticker"].isin(mapped | excluded)].copy()

    if unmapped_etp.empty:
        st.session_state["queue_candidates"] = pd.DataFrame()
        st.session_state["queue_issuers"] = pd.DataFrame()
        st.session_state["queue_fm"] = fm
        st.rerun()
        return

    with st.spinner(f"Classifying {len(unmapped_etp):,} unmapped funds..."):
        classified = run_classification(unmapped_etp)

    # Filter to 5 tracked categories + HIGH/MEDIUM confidence only
    candidates = classified[
        (classified["etp_category"].isin(_TRACKED_CATEGORIES))
        & (classified["confidence"].isin(["HIGH", "MEDIUM"]))
    ].copy()

    # Sort: HIGH first, then by AUM descending
    conf_order = {"HIGH": 0, "MEDIUM": 1}
    candidates["_sort"] = candidates["confidence"].map(conf_order).fillna(2)
    if "aum" in candidates.columns:
        candidates = candidates.sort_values(
            ["etp_category", "_sort", "aum"], ascending=[True, True, False]
        )
    candidates = candidates.drop(columns=["_sort"]).reset_index(drop=True)

    # New issuers (from _queues_report.json if available)
    new_issuers = pd.DataFrame()
    queue_path = RULES_DIR / "_queues_report.json"
    if queue_path.exists():
        try:
            with open(queue_path, "r", encoding="utf-8") as f:
                q = json.load(f)
            issuers_raw = q.get("new_issuers", [])
            if issuers_raw:
                new_issuers = pd.DataFrame(issuers_raw)
                # Filter out already-mapped issuers
                im = load_csv("issuer_mapping.csv")
                if not im.empty and "etp_category" in im.columns:
                    known = set(zip(im["etp_category"].astype(str), im["issuer"].astype(str)))
                    new_issuers = new_issuers[
                        ~new_issuers.apply(
                            lambda r: (str(r["etp_category"]), str(r["issuer"])) in known, axis=1
                        )
                    ]
        except Exception:
            pass

    st.session_state["queue_candidates"] = candidates
    st.session_state["queue_issuers"] = new_issuers
    st.session_state["queue_fm"] = fm
    st.rerun()


def _render_category_queue(df: pd.DataFrame, fm: pd.DataFrame, cat: str):
    """Render candidates for one category with per-row approve/deny."""
    if df.empty:
        st.info("No candidates.")
        return

    # Filters
    fc1, fc2 = st.columns(2)
    with fc1:
        conf_filter = st.selectbox(
            "Confidence", ["All", "HIGH", "MEDIUM"], key=f"q_{cat}_conf"
        )
    with fc2:
        min_aum = st.number_input("Min AUM ($M)", value=0.0, step=10.0, key=f"q_{cat}_aum")

    if conf_filter != "All":
        df = df[df["confidence"] == conf_filter]
    if min_aum > 0 and "aum" in df.columns:
        df = df[pd.to_numeric(df["aum"], errors="coerce").fillna(0) >= min_aum]

    if df.empty:
        st.info("No candidates match filters.")
        return

    df = df.copy().reset_index(drop=True)
    df.insert(0, "action", "")
    # Editable category override (defaults to auto-classified category)
    df["category"] = df["etp_category"]

    display_cols = ["action", "ticker", "fund_name", "issuer", "aum",
                    "confidence", "reason", "category"]
    available = [c for c in display_cols if c in df.columns]

    st.markdown(f"**{len(df)}** candidates -- review the **Matched Rule** column")

    edited = st.data_editor(
        df[available],
        use_container_width=True,
        height=min(500, 50 + 35 * len(df)),
        column_config={
            "action": st.column_config.SelectboxColumn(
                "Action", options=["", "Approve", "Deny"], width="small",
            ),
            "category": st.column_config.SelectboxColumn(
                "Override", options=VALID_CATEGORIES, width="small",
            ),
            "aum": st.column_config.NumberColumn("AUM ($M)", format="%.1f"),
            "confidence": st.column_config.TextColumn("Conf.", disabled=True, width="small"),
            "reason": st.column_config.TextColumn("Matched Rule", disabled=True, width="large"),
            "ticker": st.column_config.TextColumn("Ticker", disabled=True),
            "fund_name": st.column_config.TextColumn("Fund Name", disabled=True, width="large"),
            "issuer": st.column_config.TextColumn("Issuer", disabled=True),
        },
        key=f"q_{cat}_editor",
    )

    approvals = edited[edited["action"] == "Approve"]
    denials = edited[edited["action"] == "Deny"]

    if approvals.empty and denials.empty:
        st.caption("Set **Action** to Approve or Deny, then click Apply below.")
        return

    st.markdown(f"**{len(approvals)}** to approve, **{len(denials)}** to deny")

    if st.button("Apply Decisions", type="primary", key=f"q_{cat}_apply"):
        n_approved, n_denied = 0, 0

        # Approvals -> fund_mapping.csv
        if not approvals.empty:
            valid = approvals[approvals["category"].astype(str).str.strip() != ""]
            if not valid.empty:
                new_rows = valid[["ticker", "category"]].copy()
                new_rows.columns = ["ticker", "etp_category"]
                new_rows["is_primary"] = 1
                new_rows["source"] = "manual"
                updated_fm = pd.concat([fm, new_rows], ignore_index=True)
                updated_fm = updated_fm.drop_duplicates(
                    subset=["ticker", "etp_category"], keep="first"
                )
                save_fund_mapping(updated_fm)
                n_approved = len(valid)

        # Denials -> exclusions.csv
        if not denials.empty:
            valid = denials[denials["category"].astype(str).str.strip() != ""]
            if not valid.empty:
                excl = load_csv("exclusions.csv")
                new_excl = valid[["ticker", "category"]].copy()
                new_excl.columns = ["ticker", "etp_category"]
                updated_excl = pd.concat([excl, new_excl], ignore_index=True)
                updated_excl = updated_excl.drop_duplicates(
                    subset=["ticker", "etp_category"], keep="first"
                )
                save_csv("exclusions.csv", updated_excl)
                n_denied = len(valid)

        st.success(f"Done: {n_approved} approved, {n_denied} denied")

        # Remove processed tickers from candidates (no rescan needed)
        processed = set()
        if not approvals.empty:
            processed.update(approvals["ticker"].tolist())
        if not denials.empty:
            processed.update(denials["ticker"].tolist())
        all_cands = st.session_state["queue_candidates"]
        st.session_state["queue_candidates"] = (
            all_cands[~all_cands["ticker"].isin(processed)].reset_index(drop=True)
        )
        st.session_state["queue_fm"] = load_fund_mapping()
        st.cache_data.clear()

        # Point user to the attributes file for this category
        _ATTR_FILES = {
            "LI": "attributes_LI.csv",
            "CC": "attributes_CC.csv",
            "Crypto": "attributes_Crypto.csv",
            "Defined": "attributes_Defined.csv",
            "Thematic": "attributes_Thematic.csv",
        }
        attr_file = _ATTR_FILES.get(cat)
        if attr_file and n_approved > 0:
            st.info(
                f"Next: go to **Rules Editor** > **[Attributes] {CATEGORY_LABELS.get(cat, cat)}** "
                f"to add subcategory/underlier details for the {n_approved} newly mapped fund(s)."
            )

        st.rerun()


def _render_issuer_queue(new_issuers: pd.DataFrame):
    """Render the new issuers queue with display name input."""
    if new_issuers.empty:
        st.success("No new issuers to review.")
        return

    st.markdown(
        "New Bloomberg issuer strings that need display name mappings. "
        "Enter a short nickname and click Approve."
    )

    df = new_issuers.copy().reset_index(drop=True)
    df.insert(0, "action", "")
    df["display_name"] = ""

    display_cols = ["action", "issuer", "etp_category", "product_count",
                    "total_aum", "display_name"]
    available = [c for c in display_cols if c in df.columns]

    edited = st.data_editor(
        df[available],
        use_container_width=True,
        height=min(400, 50 + 35 * len(df)),
        column_config={
            "action": st.column_config.SelectboxColumn(
                "Action", options=["", "Approve"], width="small",
            ),
            "display_name": st.column_config.TextColumn("Display Name", width="medium"),
            "issuer": st.column_config.TextColumn("Bloomberg Issuer", disabled=True),
            "etp_category": st.column_config.TextColumn("Category", disabled=True),
            "product_count": st.column_config.NumberColumn("Products", disabled=True),
            "total_aum": st.column_config.NumberColumn(
                "Total AUM ($M)", format="%.1f", disabled=True
            ),
        },
        key="q_issuer_editor",
    )

    approvals = edited[
        (edited["action"] == "Approve")
        & (edited["display_name"].astype(str).str.strip() != "")
    ]

    if approvals.empty:
        st.caption("Set **Action** to Approve and enter a **Display Name** for each issuer.")
        return

    st.markdown(f"**{len(approvals)}** issuer(s) to approve")

    if st.button("Apply Issuer Mappings", type="primary", key="q_apply_issuers"):
        im = load_csv("issuer_mapping.csv")
        new_rows = approvals[["etp_category", "issuer", "display_name"]].copy()
        new_rows.columns = ["etp_category", "issuer", "issuer_nickname"]
        updated = pd.concat([im, new_rows], ignore_index=True)
        updated = updated.drop_duplicates(
            subset=["etp_category", "issuer"], keep="first"
        )
        save_csv("issuer_mapping.csv", updated)
        st.success(f"Added {len(approvals)} issuer mapping(s)")
        st.session_state.pop("queue_issuers", None)
        st.cache_data.clear()
        st.rerun()


# ===================================================================
# Page: Dashboard
# ===================================================================

def page_dashboard():
    st.header("ETP Category Dashboard")

    fm = load_fund_mapping()

    # Summary row
    if not fm.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Entries", f"{len(fm):,}")
        c2.metric("Unique Tickers", f"{fm['ticker'].nunique():,}")

        multi = fm.groupby("ticker").size()
        c3.metric("Multi-Category", f"{(multi > 1).sum():,}")
        c4.metric("Categories Used", f"{fm['etp_category'].nunique()}")

        # Category distribution
        st.subheader("Category Distribution (Manual Mappings)")
        cat_counts = fm["etp_category"].value_counts().reset_index()
        cat_counts.columns = ["Category", "Count"]
        cat_counts["Label"] = cat_counts["Category"].map(
            lambda c: CATEGORY_LABELS.get(c, c)
        )
        st.bar_chart(cat_counts.set_index("Label")["Count"])

        # Source breakdown
        if "source" in fm.columns:
            st.subheader("Source Breakdown")
            source_counts = fm["source"].value_counts()
            sc1, sc2 = st.columns(2)
            sc1.metric("Manual", f"{source_counts.get('manual', 0):,}")
            sc2.metric("Auto", f"{source_counts.get('auto', 0):,}")
    else:
        st.info("Fund mapping is empty. Use **Auto-Classify** to get started.")

    # Bloomberg coverage
    st.markdown("---")
    if st.button("Load Bloomberg Data for Full Coverage Analysis"):
        try:
            etp = load_bloomberg()
        except Exception as e:
            st.error(f"Failed to load Bloomberg data: {e}")
            return

        if etp.empty:
            st.error("Bloomberg data is empty. Check bloomberg_daily_file.xlsm path.")
            return

        classified = run_classification(etp)
        stats = get_coverage_stats(classified, fm)

        cc1, cc2, cc3 = st.columns(3)
        cc1.metric("Active ETPs", f"{stats['total']:,}")
        cc2.metric(
            "Manually Mapped",
            f"{stats['mapped']:,}",
            delta=f"{stats['pct_mapped']}%",
        )
        cc3.metric("Unmapped", f"{stats['unmapped']:,}")

        # Confidence breakdown
        if stats["confidence"]:
            st.subheader("Unmapped ETPs by Auto-Classify Confidence")
            conf_df = pd.DataFrame(
                list(stats["confidence"].items()),
                columns=["Confidence", "Count"],
            )
            st.dataframe(conf_df, hide_index=True, use_container_width=True)

        # Auto category distribution for unmapped
        if stats["auto_categories"]:
            st.subheader("Auto-Suggested Categories (Unmapped ETPs)")
            auto_df = pd.DataFrame(
                list(stats["auto_categories"].items()),
                columns=["Category", "Count"],
            ).sort_values("Count", ascending=False)
            st.bar_chart(auto_df.set_index("Category")["Count"])


# ===================================================================
# Page: Auto-Classify
# ===================================================================

def page_classify():
    st.header("Auto-Classification Engine")
    st.markdown(
        "Run the auto-classification engine on Bloomberg data to suggest "
        "categories for all active ETPs. Review results and apply to fund mapping."
    )

    # Run button
    if st.button("Run Auto-Classification", type="primary"):
        try:
            etp = load_bloomberg()
        except Exception as e:
            st.error(f"Failed to load Bloomberg data: {e}")
            return
        if etp.empty:
            st.error("Bloomberg data is empty.")
            return

        with st.spinner("Classifying..."):
            classified = run_classification(etp)
        st.session_state["classified"] = classified
        st.success(f"Classified {len(classified):,} active ETPs")

    if "classified" not in st.session_state:
        st.info("Click **Run Auto-Classification** to start.")
        return

    classified = st.session_state["classified"]
    fm = load_fund_mapping()
    mapped_tickers = set(fm["ticker"].astype(str).str.strip()) if not fm.empty else set()

    # Add mapping status
    classified = classified.copy()
    classified["is_mapped"] = classified["ticker"].isin(mapped_tickers)

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        status_filter = st.selectbox(
            "Status",
            ["All", "Unmapped Only", "Mapped Only", "Needs Review (LOW)"],
        )
    with col2:
        cat_options = ["All"] + sorted(classified["etp_category"].dropna().unique().tolist())
        cat_filter = st.selectbox("Category", cat_options)
    with col3:
        conf_filter = st.selectbox("Confidence", ["All", "HIGH", "MEDIUM", "LOW"])

    # Apply filters
    filtered = classified.copy()
    if status_filter == "Unmapped Only":
        filtered = filtered[~filtered["is_mapped"]]
    elif status_filter == "Mapped Only":
        filtered = filtered[filtered["is_mapped"]]
    elif status_filter == "Needs Review (LOW)":
        filtered = filtered[(~filtered["is_mapped"]) & (filtered["confidence"] == "LOW")]

    if cat_filter != "All":
        filtered = filtered[filtered["etp_category"] == cat_filter]
    if conf_filter != "All":
        filtered = filtered[filtered["confidence"] == conf_filter]

    # Sort by AUM descending
    if "aum" in filtered.columns:
        filtered = filtered.sort_values("aum", ascending=False, na_position="last")

    # Summary
    n_total = len(classified)
    n_mapped = classified["is_mapped"].sum()
    n_unmapped = n_total - n_mapped
    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.metric("Total", f"{n_total:,}")
    sc2.metric("Mapped", f"{n_mapped:,}")
    sc3.metric("Unmapped", f"{n_unmapped:,}")
    sc4.metric("Showing", f"{len(filtered):,}")

    # Display table
    display_cols = [
        "ticker", "fund_name", "issuer", "aum",
        "etp_category", "confidence", "reason", "is_mapped",
    ]
    available = [c for c in display_cols if c in filtered.columns]

    st.dataframe(
        filtered[available].reset_index(drop=True),
        use_container_width=True,
        height=450,
        column_config={
            "aum": st.column_config.NumberColumn("AUM ($M)", format="%.1f"),
            "is_mapped": st.column_config.CheckboxColumn("Mapped"),
            "etp_category": st.column_config.TextColumn("Category"),
            "confidence": st.column_config.TextColumn("Conf."),
        },
    )

    # Apply actions
    st.markdown("---")
    st.subheader("Apply to Fund Mapping")

    ac1, ac2, ac3 = st.columns(3)

    with ac1:
        high_unmapped = classified[
            (~classified["is_mapped"]) & (classified["confidence"] == "HIGH")
        ]
        st.markdown(f"**{len(high_unmapped):,}** HIGH confidence unmapped")
        if st.button("Apply HIGH Only"):
            if high_unmapped.empty:
                st.warning("No HIGH confidence unmapped ETPs.")
            else:
                _apply_to_mapping(fm, high_unmapped)

    with ac2:
        med_unmapped = classified[
            (~classified["is_mapped"])
            & (classified["confidence"].isin(["HIGH", "MEDIUM"]))
        ]
        st.markdown(f"**{len(med_unmapped):,}** HIGH+MEDIUM unmapped")
        if st.button("Apply HIGH + MEDIUM"):
            if med_unmapped.empty:
                st.warning("No HIGH/MEDIUM unmapped ETPs.")
            else:
                _apply_to_mapping(fm, med_unmapped)

    with ac3:
        all_unmapped = classified[~classified["is_mapped"]]
        st.markdown(f"**{len(all_unmapped):,}** total unmapped")
        if st.button("Apply ALL Unmapped"):
            if all_unmapped.empty:
                st.warning("All ETPs already mapped.")
            else:
                _apply_to_mapping(fm, all_unmapped)


def _apply_to_mapping(fm: pd.DataFrame, new_entries: pd.DataFrame):
    """Add auto-classified entries to fund_mapping.csv."""
    new_rows = new_entries[["ticker", "etp_category"]].copy()
    new_rows["is_primary"] = 1
    new_rows["source"] = "auto"

    updated = pd.concat([fm, new_rows], ignore_index=True)
    updated = updated.drop_duplicates(subset=["ticker", "etp_category"], keep="first")

    save_fund_mapping(updated)
    st.success(f"Added {len(new_rows):,} entries to fund_mapping.csv")
    st.cache_data.clear()
    # Clear classified cache to force re-check of mapping status
    st.session_state.pop("classified", None)


# ===================================================================
# Page: Fund Mapping Editor
# ===================================================================

def page_mapping():
    st.header("Fund Mapping Editor")
    st.caption("Edit ETP category assignments. Multi-category tickers have multiple rows.")

    fm = load_fund_mapping()

    # Stats
    c1, c2, c3 = st.columns(3)
    c1.metric("Entries", f"{len(fm):,}")
    c2.metric("Unique Tickers", f"{fm['ticker'].nunique():,}" if not fm.empty else "0")
    c3.metric("Categories", f"{fm['etp_category'].nunique()}" if not fm.empty else "0")

    # Search + filter
    fc1, fc2 = st.columns([3, 1])
    with fc1:
        search = st.text_input("Search ticker", "", key="fm_search")
    with fc2:
        cat_filter = st.selectbox(
            "Category",
            ["All"] + VALID_CATEGORIES,
            key="fm_cat_filter",
        )

    # Apply filters
    display_df = fm.copy()
    if search:
        mask = display_df["ticker"].astype(str).str.contains(
            search.upper(), case=False, na=False,
        )
        display_df = display_df[mask]
    if cat_filter != "All":
        display_df = display_df[display_df["etp_category"] == cat_filter]

    # Editable table
    edited = st.data_editor(
        display_df,
        use_container_width=True,
        num_rows="dynamic",
        height=500,
        column_config={
            "ticker": st.column_config.TextColumn("Ticker", required=True),
            "etp_category": st.column_config.SelectboxColumn(
                "Category",
                options=VALID_CATEGORIES + ["Unclassified"],
                required=True,
            ),
            "is_primary": st.column_config.CheckboxColumn("Primary", default=True),
            "source": st.column_config.SelectboxColumn(
                "Source",
                options=["manual", "auto"],
                default="manual",
            ),
        },
        key="fm_editor",
    )

    # Save / Revert
    bc1, bc2, bc3 = st.columns([1, 1, 2])
    with bc1:
        if st.button("Save Changes", type="primary", key="fm_save"):
            # If filtered view: merge edits back into full df
            if search or cat_filter != "All":
                # Remove old filtered rows, add edited
                remaining = fm[~fm.index.isin(display_df.index)]
                final = pd.concat([remaining, edited], ignore_index=True)
            else:
                final = edited
            save_fund_mapping(final)
            st.success(f"Saved {len(final):,} entries")
            st.cache_data.clear()
    with bc2:
        if st.button("Revert", key="fm_revert"):
            st.cache_data.clear()
            st.rerun()
    with bc3:
        st.caption(f"Showing {len(display_df):,} of {len(fm):,} entries")


# ===================================================================
# Page: Rules Editor (generic CSV editor for other files)
# ===================================================================

def page_editor():
    st.header("Rules CSV Editor")
    st.caption("Edit attribute files, issuer mappings, REX funds, and other rules.")

    # File selector (exclude fund_mapping -- has its own page)
    grouped = get_grouped_files()
    file_options = []
    schema_lookup = {}
    for group, schemas_list in grouped.items():
        for s in schemas_list:
            if s.filename == "fund_mapping.csv":
                continue
            label = f"[{group}] {s.label}"
            file_options.append(label)
            schema_lookup[label] = s

    if not file_options:
        st.info("No rule files available.")
        return

    selected_label = st.selectbox("Select file", file_options)
    schema = schema_lookup[selected_label]
    filename = schema.filename
    info = file_info(filename)

    st.markdown(f"**{info['rows']} rows** | Last modified: {info['modified']}")
    if schema.description:
        st.caption(schema.description)

    # --- Missing attributes detection for attributes_*.csv files ---
    _ATTR_CAT_MAP = {
        "attributes_LI.csv": "LI",
        "attributes_CC.csv": "CC",
        "attributes_Crypto.csv": "Crypto",
        "attributes_Defined.csv": "Defined",
        "attributes_Thematic.csv": "Thematic",
    }
    if filename in _ATTR_CAT_MAP:
        _attr_cat = _ATTR_CAT_MAP[filename]
        _fm = load_fund_mapping()
        _fm_tickers = set(
            _fm[_fm["etp_category"] == _attr_cat]["ticker"].astype(str).str.strip()
        ) if not _fm.empty else set()
        _attr_df = load_csv(filename)
        _attr_tickers = set(
            _attr_df["ticker"].astype(str).str.strip()
        ) if not _attr_df.empty and "ticker" in _attr_df.columns else set()
        _missing = sorted(_fm_tickers - _attr_tickers)

        if _missing:
            st.warning(
                f"**{len(_missing)} fund(s)** in fund_mapping as {_attr_cat} "
                f"but missing from this attributes file."
            )
            with st.expander(f"Show {len(_missing)} missing tickers"):
                st.code(", ".join(_missing))
            if st.button(
                f"Add {len(_missing)} stub rows", key=f"stub_{filename}"
            ):
                # Create stub rows with just the ticker column filled
                stub_cols = [c.name for c in schema.columns]
                stubs = pd.DataFrame({"ticker": _missing})
                for col in stub_cols:
                    if col != "ticker" and col not in stubs.columns:
                        stubs[col] = ""
                updated = pd.concat([_attr_df, stubs], ignore_index=True)
                save_csv(filename, updated)
                st.success(f"Added {len(_missing)} stub rows. Fill in the details below.")
                st.session_state.pop(f"original_{filename}", None)
                st.rerun()
        else:
            st.success(f"All {_attr_cat} funds have attribute entries.")

    # Load
    state_key = f"original_{filename}"
    if state_key not in st.session_state or st.session_state.get("_loaded_file") != filename:
        st.session_state[state_key] = load_csv(filename)
        st.session_state["_loaded_file"] = filename
        st.session_state.pop("preview_diff", None)

    original_df = st.session_state[state_key]

    # Search
    search = st.text_input("Search", key=f"search_{filename}")
    display_df = original_df.copy()
    if search:
        mask = display_df.apply(
            lambda row: row.astype(str).str.contains(search, case=False).any(), axis=1
        )
        display_df = display_df[mask]

    # Column configs
    column_config = {}
    for col_def in schema.columns:
        if col_def.choices:
            column_config[col_def.name] = st.column_config.SelectboxColumn(
                col_def.name, options=col_def.choices, required=col_def.required,
            )
        elif col_def.dtype == "float":
            column_config[col_def.name] = st.column_config.NumberColumn(
                col_def.name, format="%.2f",
            )

    # Data editor
    edited_df = st.data_editor(
        display_df,
        column_config=column_config,
        num_rows="dynamic",
        use_container_width=True,
        key=f"editor_{filename}",
    )

    # Actions
    col1, col2, col3 = st.columns(3)

    with col1:
        preview_clicked = st.button("Preview & Save", type="primary", key=f"preview_{filename}")
    with col2:
        if st.button("Revert", key=f"revert_{filename}"):
            st.session_state[state_key] = load_csv(filename)
            st.session_state.pop("preview_diff", None)
            st.rerun()
    with col3:
        if st.button("Validate", key=f"validate_{filename}"):
            issues = validate_file(edited_df, schema)
            if not issues:
                st.success("No issues found")
            else:
                for issue in issues:
                    if issue.severity == "error":
                        st.error(issue.message)
                    else:
                        st.warning(issue.message)

    # Preview diff
    if preview_clicked:
        if search:
            full_df = original_df.copy()
            full_df = full_df[~full_df.index.isin(display_df.index)]
            final_df = pd.concat([full_df, edited_df], ignore_index=True)
        else:
            final_df = edited_df

        diff = compute_diff(original_df, final_df, schema)
        st.session_state["preview_diff"] = diff
        st.session_state["preview_df"] = final_df

    if "preview_diff" in st.session_state and st.session_state.get("_loaded_file") == filename:
        diff = st.session_state["preview_diff"]
        st.divider()
        st.subheader("Preview Changes")

        added, deleted, modified = diff["added"], diff["deleted"], diff["modified"]
        has_changes = not added.empty or not deleted.empty or not modified.empty

        if not has_changes:
            st.info("No changes detected")
        else:
            if not added.empty:
                st.markdown(f"**Added ({len(added)} rows):**")
                st.dataframe(added, use_container_width=True)
            if not deleted.empty:
                st.markdown(f"**Deleted ({len(deleted)} rows):**")
                st.dataframe(deleted, use_container_width=True)
            if not modified.empty:
                st.markdown(f"**Modified ({len(modified)} rows):**")
                st.dataframe(modified, use_container_width=True)

            s1, s2 = st.columns(2)
            with s1:
                if st.button("Confirm Save", type="primary", key=f"confirm_{filename}"):
                    save_csv(filename, st.session_state["preview_df"])
                    st.session_state[state_key] = st.session_state["preview_df"].copy()
                    st.session_state.pop("preview_diff", None)
                    st.session_state.pop("preview_df", None)
                    st.success(f"Saved {filename}")
                    st.rerun()
            with s2:
                if st.button("Cancel", key=f"cancel_{filename}"):
                    st.session_state.pop("preview_diff", None)
                    st.session_state.pop("preview_df", None)
                    st.rerun()


# ===================================================================
# Page: Sync & Validate
# ===================================================================

def page_sync():
    st.header("Sync & Validate")

    # Sync panel
    st.subheader("Sync: config/rules/ vs data/rules/")
    sc1, sc2 = st.columns(2)
    with sc1:
        if st.button("Compare"):
            results = compare_all()
            st.session_state["sync_results"] = results

        if "sync_results" in st.session_state:
            for status in st.session_state["sync_results"]:
                icon = {
                    "identical": "[OK]",
                    "modified": "[DIFF]",
                    "config_only": "[+CONFIG]",
                    "data_only": "[+DATA]",
                }
                st.text(
                    f"{icon.get(status.status, '?')} {status.filename}"
                    f" (config:{status.config_rows} / data:{status.data_rows})"
                )
    with sc2:
        if st.button("Sync config/ -> data/", type="primary"):
            copied = sync_config_to_data()
            st.success(f"Synced {len(copied)} file(s)")
            st.session_state.pop("sync_results", None)

    # Validation panel
    st.markdown("---")
    st.subheader("Full Validation")

    if st.button("Run All Validations"):
        with st.spinner("Validating..."):
            try:
                from tools.rules_editor.bloomberg import load_bloomberg_tickers
                bbg = load_bloomberg_tickers()
            except Exception:
                bbg = None

            all_issues = run_full_validation(bbg)

        if not all_issues:
            st.success("All checks passed!")
        else:
            for filename, issues in all_issues.items():
                errors = [i for i in issues if i.severity == "error"]
                warnings = [i for i in issues if i.severity == "warning"]
                if errors:
                    with st.expander(f"[ERROR] {filename} -- {len(errors)} error(s), {len(warnings)} warning(s)"):
                        for issue in issues:
                            if issue.severity == "error":
                                st.error(issue.message)
                            else:
                                st.warning(issue.message)
                elif warnings:
                    with st.expander(f"[WARN] {filename} -- {len(warnings)} warning(s)"):
                        for issue in issues:
                            st.warning(issue.message)


# ===================================================================
# Page dispatch
# ===================================================================

if page == "Classification Queue":
    page_queues()
elif page == "Dashboard":
    page_dashboard()
elif page == "Auto-Classify":
    page_classify()
elif page == "Fund Mapping":
    page_mapping()
elif page == "Rules Editor":
    page_editor()
elif page == "Sync & Validate":
    page_sync()
