#!/bin/bash
# Render startup: sync git-tracked data files to persistent disk, then start app.
# The persistent disk at /opt/render/project/src/data overlays the git repo's data/ dir,
# so git-tracked files under data/ must be copied from the build cache.

set -e

SRC="/opt/render/project/src"
DISK="$SRC/data"
# Render caches the git repo at build time; the build output lives at $SRC.
# Git-tracked files under data/ are in the build but hidden by the disk mount.
# The buildCommand runs before the disk is mounted, so we stash copies during build.
# But since we can't change the build easily, we embed the files we need.

# 1. Copy rules CSVs (tracked in git, small files)
mkdir -p "$DISK/rules"
if [ -d "$SRC/.data_sync/rules" ]; then
    cp -r "$SRC/.data_sync/rules/"* "$DISK/rules/" 2>/dev/null || true
fi

# 2. Copy bloomberg_daily_file.xlsm if not already on disk (or if stale)
mkdir -p "$DISK/DASHBOARD"
if [ -f "$SRC/.data_sync/bloomberg_daily_file.xlsm" ]; then
    SRC_SIZE=$(stat -c%s "$SRC/.data_sync/bloomberg_daily_file.xlsm" 2>/dev/null || echo 0)
    DST_SIZE=$(stat -c%s "$DISK/DASHBOARD/bloomberg_daily_file.xlsm" 2>/dev/null || echo 0)
    if [ "$SRC_SIZE" != "$DST_SIZE" ] || [ "$DST_SIZE" = "0" ]; then
        echo "Syncing bloomberg_daily_file.xlsm to persistent disk ($SRC_SIZE bytes)"
        cp "$SRC/.data_sync/bloomberg_daily_file.xlsm" "$DISK/DASHBOARD/bloomberg_daily_file.xlsm"
    else
        echo "bloomberg_daily_file.xlsm already up to date on persistent disk"
    fi
fi

# 3. Init DB
python -c "from webapp.database import init_db; init_db()" 2>/dev/null || true

# 4. Start app
exec uvicorn webapp.main:app --host 0.0.0.0 --port $PORT
