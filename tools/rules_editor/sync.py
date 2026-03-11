"""Sync logic: compare and copy config/rules/ -> data/rules/."""

import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .schemas import SCHEMAS

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_RULES = PROJECT_ROOT / "config" / "rules"
DATA_RULES = PROJECT_ROOT / "data" / "rules"


@dataclass
class FileSyncStatus:
    filename: str
    status: str  # identical, modified, config_only, data_only
    config_rows: int = 0
    data_rows: int = 0


def _read_csv_safe(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_csv(path, engine="python", on_bad_lines="skip")


def compare_all() -> list[FileSyncStatus]:
    """Compare config/rules/ vs data/rules/ for every known rules file."""
    results = []
    for filename in SCHEMAS:
        config_path = CONFIG_RULES / filename
        data_path = DATA_RULES / filename
        config_df = _read_csv_safe(config_path)
        data_df = _read_csv_safe(data_path)

        if config_df is None and data_df is None:
            continue

        if config_df is not None and data_df is None:
            results.append(FileSyncStatus(
                filename, "config_only", len(config_df), 0,
            ))
        elif config_df is None and data_df is not None:
            results.append(FileSyncStatus(
                filename, "data_only", 0, len(data_df),
            ))
        else:
            # Both exist -- compare content
            try:
                identical = config_df.fillna("").equals(data_df.fillna(""))
            except Exception:
                identical = False
            results.append(FileSyncStatus(
                filename,
                "identical" if identical else "modified",
                len(config_df),
                len(data_df),
            ))
    return results


def sync_config_to_data() -> list[str]:
    """Copy all rules CSVs from config/rules/ to data/rules/.

    Returns list of filenames that were copied.
    """
    DATA_RULES.mkdir(parents=True, exist_ok=True)
    copied = []
    for filename in SCHEMAS:
        src = CONFIG_RULES / filename
        if src.exists():
            shutil.copy2(src, DATA_RULES / filename)
            copied.append(filename)
    return copied
