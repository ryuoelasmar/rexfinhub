#!/bin/bash
# Render startup: sync build-stashed data to persistent disk, then start app.

set -e

SRC="/opt/render/project/src"
DISK="$SRC/data"

# 1. Copy rules CSVs
mkdir -p "$DISK/rules"
if [ -d "$SRC/.data_sync/rules" ]; then
    cp -r "$SRC/.data_sync/rules/"* "$DISK/rules/" 2>/dev/null || true
    echo "Synced $(ls "$DISK/rules/" | wc -l) rules files"
fi

# 2. Copy pre-exported CSV sheets (fast, low-memory alternative to xlsm)
mkdir -p "$DISK/DASHBOARD/sheets"
if [ -d "$SRC/.data_sync/sheets" ]; then
    cp -r "$SRC/.data_sync/sheets/"* "$DISK/DASHBOARD/sheets/" 2>/dev/null || true
    echo "Synced $(ls "$DISK/DASHBOARD/sheets/" | wc -l) CSV sheets"
fi

# 3. Copy bloomberg_daily_file.xlsm if needed (fallback)
mkdir -p "$DISK/DASHBOARD"
if [ -f "$SRC/.data_sync/bloomberg_daily_file.xlsm" ]; then
    SRC_SIZE=$(stat -c%s "$SRC/.data_sync/bloomberg_daily_file.xlsm" 2>/dev/null || echo 0)
    DST_SIZE=$(stat -c%s "$DISK/DASHBOARD/bloomberg_daily_file.xlsm" 2>/dev/null || echo 0)
    if [ "$SRC_SIZE" != "$DST_SIZE" ] || [ "$DST_SIZE" = "0" ]; then
        echo "Syncing bloomberg_daily_file.xlsm ($SRC_SIZE bytes)"
        cp "$SRC/.data_sync/bloomberg_daily_file.xlsm" "$DISK/DASHBOARD/bloomberg_daily_file.xlsm"
    fi
fi

# 4. Init DB
python -c "from webapp.database import init_db; init_db()" 2>/dev/null || true

# 5. Start app
exec uvicorn webapp.main:app --host 0.0.0.0 --port $PORT
