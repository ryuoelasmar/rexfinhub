from __future__ import annotations
import pandas as pd
from .sec_client import SECClient
from .utils import is_prospectus_form, safe_str
from .paths import output_paths_for_trust, build_primary_link, build_submission_txt_link
from .csvio import write_csv

def _extract_filings_from_recent(rec: dict) -> tuple[list, list, list, list, list]:
    """Extract parallel arrays from a 'recent' filings block."""
    return (
        rec.get("form", []) or [],
        rec.get("accessionNumber", []) or [],
        rec.get("primaryDocument", []) or [],
        rec.get("filingDate", []) or [],
        rec.get("isInlineXBRL", []) or [],
    )


def load_all_submissions_for_cik(client: SECClient, cik: str, overrides: dict | None = None,
                                 since: str | None = None, until: str | None = None,
                                 refresh_submissions: bool = True, refresh_max_age_hours: int = 6,
                                 refresh_force_now: bool = False) -> tuple[str, pd.DataFrame]:
    data = client.load_submissions_json(cik, refresh_submissions, refresh_max_age_hours, refresh_force_now)
    trust_name = (overrides or {}).get(str(cik)) or data.get("name") or f"CIK {int(str(cik))}"

    # Collect filings from the "recent" block (up to ~1,000 filings)
    rec = data.get("filings", {}).get("recent", {})
    forms, accession, files, dates, is_ixbrl_list = _extract_filings_from_recent(rec)

    # Paginated older filings: SEC puts overflow in filings.files[] as separate JSON files
    filings_files = data.get("filings", {}).get("files", []) or []
    if filings_files:
        cik_padded = f"{int(str(cik)):010d}"
        for file_entry in filings_files:
            fname = file_entry.get("name", "")
            if not fname:
                continue
            url = f"https://data.sec.gov/submissions/{fname}"
            try:
                extra = client.fetch_json(url)
            except Exception:
                continue
            ef, ea, epd, ed, ei = _extract_filings_from_recent(extra)
            forms.extend(ef)
            accession.extend(ea)
            files.extend(epd)
            dates.extend(ed)
            is_ixbrl_list.extend(ei)

    rows = []
    for i in range(len(forms)):
        form = safe_str(forms[i])
        accn = safe_str(accession[i])
        fdt  = safe_str(dates[i])
        prim = safe_str(files[i])
        ixbrl = str(is_ixbrl_list[i]) if i < len(is_ixbrl_list) else "0"
        row = {
            "Filing Date": fdt,
            "Form": form,
            "Accession Number": accn,
            "Primary Document": prim,
            "Primary Link": build_primary_link(cik, accn, prim) if prim else "",
            "Full Submission TXT": build_submission_txt_link(cik, accn),
            "CIK": str(int(str(cik))),
            "Registrant": trust_name,
            "isInlineXBRL": ixbrl,
        }
        rows.append(row)
    df1 = pd.DataFrame(rows, columns=[
        "Filing Date","Form","Accession Number","Primary Document",
        "Primary Link","Full Submission TXT","CIK","Registrant","isInlineXBRL"
    ])
    if since or until:
        d = pd.to_datetime(df1["Filing Date"], errors="coerce")
        if since: df1 = df1[d >= pd.to_datetime(since, errors="coerce")]
        if until: df1 = df1[d <= pd.to_datetime(until, errors="coerce")]
    return trust_name, df1

def step2_submissions_and_prospectus(client: SECClient, output_root, cik_list: list[str],
                                     overrides: dict | None = None, since: str | None = None, until: str | None = None,
                                     refresh_submissions: bool = True, refresh_max_age_hours: int = 6,
                                     refresh_force_now: bool = False) -> list[str]:
    trusts_done = []
    for cik in cik_list:
        trust_name, df1 = load_all_submissions_for_cik(
            client, cik, overrides, since, until, refresh_submissions, refresh_max_age_hours, refresh_force_now
        )
        paths = output_paths_for_trust(output_root, trust_name)
        write_csv(paths["all_filings"], df1)
        df2 = df1[df1["Form"].apply(is_prospectus_form)].copy()
        write_csv(paths["prospectus_base"], df2)
        trusts_done.append(trust_name)
    return trusts_done
