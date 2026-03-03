#!/bin/bash
# Render build step: stash git-tracked data files + export CSVs for low-memory loading.
# At build time, data/ contains the git repo files (disk not yet mounted).
# We copy them to .data_sync/ so render_start.sh can restore them at runtime.
# We also export Excel sheets to CSV so the app never needs to open the 25MB xlsm.

set -e

SRC="/opt/render/project/src"
SYNC="$SRC/.data_sync"

mkdir -p "$SYNC/rules" "$SYNC/sheets"

# Stash rules CSVs
if [ -d "$SRC/data/rules" ]; then
    cp -r "$SRC/data/rules/"* "$SYNC/rules/" 2>/dev/null || true
    echo "Stashed $(ls "$SYNC/rules/" | wc -l) rules files"
fi

# Export Excel sheets to CSV (much lower memory at runtime)
XLSM="$SRC/data/DASHBOARD/bloomberg_daily_file.xlsm"
if [ -f "$XLSM" ]; then
    echo "Exporting Excel sheets to CSV..."
    python3 "$SRC/scripts/export_sheets.py" "$XLSM" "$SYNC/sheets"
    echo "CSV export complete: $(ls "$SYNC/sheets/" | wc -l) files"
    # Also keep the raw xlsm as fallback
    cp "$XLSM" "$SYNC/bloomberg_daily_file.xlsm"
fi

echo "Build stash complete"
