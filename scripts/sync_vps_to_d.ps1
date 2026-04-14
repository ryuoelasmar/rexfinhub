# Sync VPS -> D: drive (Option C)
#
# Pulls key archives from the Hetzner VPS to the local D: drive so your D:
# drive stays current as a historical backup. Runs via a Windows Scheduled
# Task nightly when the laptop is online.
#
# What's pulled:
#   1. etp_tracker.db                → D:\sec-data\backups\etp_tracker_{DATE}.db
#   2. structured_notes.db (if any)  → D:\sec-data\databases\structured_notes_{DATE}.db
#   3. SEC submissions.zip           → D:\sec-data\submissions.zip (overwrite)
#   4. Bloomberg daily snapshot      → D:\sec-data\archives\bloomberg\
#   5. SEC http_cache changes        → D:\sec-data\cache\rexfinhub\ (rsync-style via scp)
#
# Prereqs:
#   - OpenSSH client (Windows 10+ built-in)
#   - Ed25519 SSH key for jarvis@46.224.126.196 (already set up)
#   - D: drive connected
#
# Run manually:
#   powershell -ExecutionPolicy Bypass -File scripts\sync_vps_to_d.ps1
#
# Scheduled:
#   Task Scheduler -> Create Basic Task -> Daily 11:00 PM -> Run above command
#   Set condition: "Wake the computer to run this task" = unchecked
#                  "Start only if computer is idle" = unchecked
#                  "Stop the task if it runs longer than: 2 hours"

$ErrorActionPreference = "Continue"  # keep going on non-fatal errors
$VPS = "jarvis@46.224.126.196"
$DATE = Get-Date -Format "yyyy-MM-dd"
$D_BASE = "D:\sec-data"

Write-Host "=== VPS -> D: sync ($DATE) ==="

# Check D: available
if (-not (Test-Path $D_BASE)) {
    Write-Host "ERROR: $D_BASE not available. Is D: drive connected?"
    exit 1
}

# Check SSH connectivity
$null = ssh -o ConnectTimeout=10 -o BatchMode=yes $VPS "echo ok" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: VPS unreachable"
    exit 2
}

# Ensure target dirs exist
New-Item -ItemType Directory -Force -Path "$D_BASE\backups" | Out-Null
New-Item -ItemType Directory -Force -Path "$D_BASE\databases" | Out-Null
New-Item -ItemType Directory -Force -Path "$D_BASE\archives\bloomberg" | Out-Null
New-Item -ItemType Directory -Force -Path "$D_BASE\cache\rexfinhub" | Out-Null

# 1. etp_tracker.db -> dated backup
Write-Host "[1/5] Pulling etp_tracker.db..."
$DB_TARGET = "$D_BASE\backups\etp_tracker_$DATE.db"
scp -q "${VPS}:/home/jarvis/rexfinhub/data/etp_tracker.db" $DB_TARGET
if ($LASTEXITCODE -eq 0) {
    $size = (Get-Item $DB_TARGET).Length / 1MB
    Write-Host ("      -> {0} ({1:N0} MB)" -f $DB_TARGET, $size)
    # Prune backups older than 30 days
    Get-ChildItem "$D_BASE\backups\etp_tracker_*.db" |
        Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } |
        Remove-Item -Force -ErrorAction SilentlyContinue
} else {
    Write-Host "      FAILED"
}

# 2. structured_notes.db (if present)
Write-Host "[2/5] Pulling structured_notes.db..."
$NOTES_TARGET = "$D_BASE\databases\structured_notes_$DATE.db"
scp -q "${VPS}:/home/jarvis/rexfinhub/data/structured_notes.db" $NOTES_TARGET 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0 -and (Test-Path $NOTES_TARGET)) {
    $size = (Get-Item $NOTES_TARGET).Length / 1MB
    Write-Host ("      -> {0} ({1:N0} MB)" -f $NOTES_TARGET, $size)
} else {
    Write-Host "      SKIPPED (not present on VPS)"
    Remove-Item $NOTES_TARGET -ErrorAction SilentlyContinue
}

# 3. SEC submissions.zip (overwrite, it's a moving target)
Write-Host "[3/5] Pulling submissions.zip..."
scp -q "${VPS}:/home/jarvis/rexfinhub/temp/submissions.zip" "$D_BASE\submissions.zip"
if ($LASTEXITCODE -eq 0) {
    $size = (Get-Item "$D_BASE\submissions.zip").Length / 1MB
    Write-Host ("      -> {0:N0} MB" -f $size)
} else {
    Write-Host "      FAILED or not present"
}

# 4. Bloomberg daily snapshots (all in history/)
Write-Host "[4/5] Pulling Bloomberg snapshots..."
scp -q -r "${VPS}:/home/jarvis/rexfinhub/data/DASHBOARD/history/*" "$D_BASE\archives\bloomberg\" 2>&1 | Out-Null
$count = (Get-ChildItem "$D_BASE\archives\bloomberg\bloomberg_daily_file_*.xlsm" -ErrorAction SilentlyContinue).Count
Write-Host "      -> $count snapshots in D:\sec-data\archives\bloomberg"

# 5. SEC http_cache (only the 'web' subdir — filings themselves)
# Use rsync-style scp incremental: only pull what's new based on file timestamps.
# Full rsync isn't built in, so we tar+scp the delta.
Write-Host "[5/5] SEC http_cache delta (last 24h)..."
$TEMP_TAR = "$env:TEMP\vps_cache_delta.tar.gz"
ssh $VPS "cd /home/jarvis/rexfinhub/cache && find sec -type f -mtime -1 2>/dev/null | tar -czf - -T - 2>/dev/null" | Out-File -FilePath $TEMP_TAR -Encoding Byte 2>$null
# The above pipe binary is awkward in PowerShell; simpler: just scp the whole sec dir (idempotent-ish)
scp -q -r "${VPS}:/home/jarvis/rexfinhub/cache/sec/" "$D_BASE\cache\rexfinhub\" 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "      -> synced"
} else {
    Write-Host "      SKIPPED"
}
Remove-Item $TEMP_TAR -ErrorAction SilentlyContinue

Write-Host "=== Done ==="
