#!/bin/bash
# Render build step: stash git-tracked data files before the persistent disk hides them.
# At build time, data/ contains the git repo files (disk not yet mounted).
# We copy them to .data_sync/ so render_start.sh can restore them at runtime.

set -e

SRC="/opt/render/project/src"
SYNC="$SRC/.data_sync"

mkdir -p "$SYNC/rules"

# Stash rules CSVs
if [ -d "$SRC/data/rules" ]; then
    cp -r "$SRC/data/rules/"* "$SYNC/rules/" 2>/dev/null || true
    echo "Stashed $(ls "$SYNC/rules/" | wc -l) rules files"
fi

# Stash bloomberg_daily_file.xlsm
if [ -f "$SRC/data/DASHBOARD/bloomberg_daily_file.xlsm" ]; then
    cp "$SRC/data/DASHBOARD/bloomberg_daily_file.xlsm" "$SYNC/bloomberg_daily_file.xlsm"
    echo "Stashed bloomberg_daily_file.xlsm ($(stat -c%s "$SYNC/bloomberg_daily_file.xlsm") bytes)"
fi

echo "Build stash complete"
