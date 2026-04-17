# Sync VPS -> D: drive (ON-DEMAND, interactive)
#
# Run this when you plug the D: drive in. Shows a live progress bar,
# per-step elapsed time, and a toast notification when complete.
#
# Differences from sync_vps_to_d.ps1 (the scheduled version):
#   - Startup banner with VPS IP, target path, D: free space
#   - Per-step Write-Progress bar and byte/speed reporting
#   - Color-coded output (Green=OK, Yellow=WARN, Red=ERR)
#   - Windows toast at end: "VPS Sync Complete" (or MessageBox fallback)
#   - Aggregate stats: total bytes, files, duration, average speed
#   - Error summary listing any failed steps
#
# Usage:
#   Double-click, or:
#     powershell -ExecutionPolicy Bypass -File scripts\sync_vps_to_d_on_demand.ps1
#   Skip the slow SEC http_cache leg:
#     powershell -ExecutionPolicy Bypass -File scripts\sync_vps_to_d_on_demand.ps1 -SkipSecCache

[CmdletBinding()]
param(
    [switch]$SkipSecCache
)

$ErrorActionPreference = "Continue"

# --- Config -----------------------------------------------------------------
$VPS = "jarvis@46.224.126.196"
$DATE = Get-Date -Format "yyyy-MM-dd"
$D_BASE = "D:\sec-data"

# --- Helpers ----------------------------------------------------------------
function Write-Ok    ($msg) { Write-Host $msg -ForegroundColor Green }
function Write-Warn  ($msg) { Write-Host $msg -ForegroundColor Yellow }
function Write-Err   ($msg) { Write-Host $msg -ForegroundColor Red }
function Write-Info  ($msg) { Write-Host $msg -ForegroundColor Cyan }
function Write-Dim   ($msg) { Write-Host $msg -ForegroundColor DarkGray }

function Format-Bytes($bytes) {
    if ($bytes -ge 1GB) { return ("{0:N2} GB" -f ($bytes / 1GB)) }
    if ($bytes -ge 1MB) { return ("{0:N1} MB" -f ($bytes / 1MB)) }
    if ($bytes -ge 1KB) { return ("{0:N1} KB" -f ($bytes / 1KB)) }
    return "$bytes B"
}

function Format-Duration($seconds) {
    $ts = [TimeSpan]::FromSeconds([math]::Round($seconds))
    if ($ts.TotalHours -ge 1) {
        return ("{0}h {1}m {2}s" -f [int]$ts.TotalHours, $ts.Minutes, $ts.Seconds)
    }
    return ("{0}m {1}s" -f [int]$ts.TotalMinutes, $ts.Seconds)
}

function Show-Toast($title, $message) {
    # Preferred: BurntToast
    if (Get-Module -ListAvailable -Name BurntToast) {
        try {
            Import-Module BurntToast -ErrorAction Stop
            New-BurntToastNotification -Text $title, $message
            return
        } catch {
            Write-Dim "BurntToast failed: $_"
        }
    }
    # Fallback 1: WinRT toast via native XML (works on Windows 10/11 without extra modules)
    try {
        [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null
        $xmlDoc = New-Object Windows.Data.Xml.Dom.XmlDocument
        $escTitle = [System.Security.SecurityElement]::Escape($title)
        $escMsg   = [System.Security.SecurityElement]::Escape($message)
        $template = @"
<toast>
  <visual>
    <binding template="ToastGeneric">
      <text>$escTitle</text>
      <text>$escMsg</text>
    </binding>
  </visual>
</toast>
"@
        $xmlDoc.LoadXml($template)
        $toast = [Windows.UI.Notifications.ToastNotification]::new($xmlDoc)
        [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("VPS Sync").Show($toast)
        return
    } catch {
        Write-Dim "WinRT toast failed: $_"
    }
    # Fallback 2: MessageBox popup
    try {
        Add-Type -AssemblyName PresentationFramework -ErrorAction Stop
        [System.Windows.MessageBox]::Show($message, $title) | Out-Null
        return
    } catch {
        Write-Dim "MessageBox failed: $_"
    }
    # Last-resort: terminal beep + banner
    [Console]::Beep(880, 200)
    [Console]::Beep(1320, 400)
    Write-Host ""
    Write-Host "*****************************************" -ForegroundColor Green
    Write-Host "*   $title" -ForegroundColor Green
    Write-Host "*   $message" -ForegroundColor Green
    Write-Host "*****************************************" -ForegroundColor Green
}

# Tracking for aggregate stats
$Script:TotalBytes = 0L
$Script:TotalFiles = 0
$Script:Errors = New-Object System.Collections.Generic.List[string]

function Add-TransferStats($path) {
    if (Test-Path $path -PathType Leaf) {
        $Script:TotalBytes += (Get-Item $path).Length
        $Script:TotalFiles += 1
    } elseif (Test-Path $path -PathType Container) {
        $items = Get-ChildItem $path -Recurse -File -ErrorAction SilentlyContinue
        foreach ($i in $items) { $Script:TotalBytes += $i.Length }
        $Script:TotalFiles += $items.Count
    }
}

# --- Startup banner ---------------------------------------------------------
Clear-Host
Write-Host ""
Write-Host "===============================================================" -ForegroundColor Cyan
Write-Host "  VPS -> D: Drive Sync (On-Demand)" -ForegroundColor Cyan
Write-Host "===============================================================" -ForegroundColor Cyan
Write-Host ("  Date        : {0}" -f $DATE)
Write-Host ("  VPS         : {0}" -f $VPS)
Write-Host ("  Target path : {0}" -f $D_BASE)
if ($SkipSecCache) {
    Write-Host "  Mode        : SKIP SEC http_cache (-SkipSecCache)" -ForegroundColor Yellow
} else {
    Write-Host "  Mode        : Full (includes SEC http_cache)"
}

# D: free space
if (Test-Path "D:\") {
    try {
        $drv = Get-PSDrive D -ErrorAction Stop
        $freeGB = [math]::Round($drv.Free / 1GB, 1)
        $usedGB = [math]::Round($drv.Used / 1GB, 1)
        $totalGB = $freeGB + $usedGB
        Write-Host ("  D: drive    : {0} GB free / {1} GB total" -f $freeGB, $totalGB)
    } catch {
        Write-Warn "  D: drive    : could not read free space"
    }
} else {
    Write-Err "  D: drive    : NOT CONNECTED"
    Write-Host ""
    Write-Err "ERROR: D: drive is not available. Plug in the drive and retry."
    Read-Host "Press Enter to exit"
    exit 1
}
Write-Host "===============================================================" -ForegroundColor Cyan
Write-Host ""

# --- Preflight checks -------------------------------------------------------
if (-not (Test-Path $D_BASE)) {
    Write-Warn "Creating $D_BASE..."
    New-Item -ItemType Directory -Force -Path $D_BASE | Out-Null
}

Write-Info "Checking SSH connectivity to $VPS..."
$null = ssh -o ConnectTimeout=10 -o BatchMode=yes $VPS "echo ok" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Err "ERROR: VPS unreachable (SSH failed)"
    Read-Host "Press Enter to exit"
    exit 2
}
Write-Ok "  SSH OK"

# Ensure target dirs exist
New-Item -ItemType Directory -Force -Path "$D_BASE\backups" | Out-Null
New-Item -ItemType Directory -Force -Path "$D_BASE\databases" | Out-Null
New-Item -ItemType Directory -Force -Path "$D_BASE\archives\bloomberg" | Out-Null
New-Item -ItemType Directory -Force -Path "$D_BASE\cache\rexfinhub" | Out-Null

$RunStart = Get-Date
$TotalSteps = if ($SkipSecCache) { 4 } else { 5 }

# --- Step 1: etp_tracker.db -------------------------------------------------
$stepStart = Get-Date
Write-Host ""
Write-Info "[1/$TotalSteps] etp_tracker.db"
Write-Progress -Id 1 -Activity "VPS -> D: sync" -Status "etp_tracker.db" -PercentComplete (1 / $TotalSteps * 100)
$DB_TARGET = "$D_BASE\backups\etp_tracker_$DATE.db"
scp -q "${VPS}:/home/jarvis/rexfinhub/data/etp_tracker.db" $DB_TARGET
if ($LASTEXITCODE -eq 0 -and (Test-Path $DB_TARGET)) {
    $sz = (Get-Item $DB_TARGET).Length
    Add-TransferStats $DB_TARGET
    $elapsed = (Get-Date) - $stepStart
    Write-Ok ("      OK  {0}  ({1})  in {2}" -f $DB_TARGET, (Format-Bytes $sz), (Format-Duration $elapsed.TotalSeconds))
    # Prune backups older than 30 days
    Get-ChildItem "$D_BASE\backups\etp_tracker_*.db" |
        Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } |
        Remove-Item -Force -ErrorAction SilentlyContinue
} else {
    Write-Err "      FAILED"
    $Script:Errors.Add("Step 1: etp_tracker.db transfer failed")
}

# --- Step 2: structured_notes.db -------------------------------------------
$stepStart = Get-Date
Write-Host ""
Write-Info "[2/$TotalSteps] structured_notes.db"
Write-Progress -Id 1 -Activity "VPS -> D: sync" -Status "structured_notes.db" -PercentComplete (2 / $TotalSteps * 100)
$NOTES_TARGET = "$D_BASE\databases\structured_notes_$DATE.db"
scp -q "${VPS}:/home/jarvis/rexfinhub/data/structured_notes.db" $NOTES_TARGET 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0 -and (Test-Path $NOTES_TARGET)) {
    $sz = (Get-Item $NOTES_TARGET).Length
    Add-TransferStats $NOTES_TARGET
    $elapsed = (Get-Date) - $stepStart
    Write-Ok ("      OK  {0}  ({1})  in {2}" -f $NOTES_TARGET, (Format-Bytes $sz), (Format-Duration $elapsed.TotalSeconds))
} else {
    Write-Warn "      SKIPPED (not present on VPS)"
    Remove-Item $NOTES_TARGET -ErrorAction SilentlyContinue
}

# --- Step 3: submissions.zip ------------------------------------------------
$stepStart = Get-Date
Write-Host ""
Write-Info "[3/$TotalSteps] submissions.zip"
Write-Progress -Id 1 -Activity "VPS -> D: sync" -Status "submissions.zip" -PercentComplete (3 / $TotalSteps * 100)
$ZIP_TARGET = "$D_BASE\submissions.zip"
scp -q "${VPS}:/home/jarvis/rexfinhub/temp/submissions.zip" $ZIP_TARGET
if ($LASTEXITCODE -eq 0 -and (Test-Path $ZIP_TARGET)) {
    $sz = (Get-Item $ZIP_TARGET).Length
    Add-TransferStats $ZIP_TARGET
    $elapsed = (Get-Date) - $stepStart
    $mbps = if ($elapsed.TotalSeconds -gt 0) { ($sz / 1MB) / $elapsed.TotalSeconds } else { 0 }
    Write-Ok ("      OK  {0}  ({1})  in {2}  @ {3:N1} MB/s" -f $ZIP_TARGET, (Format-Bytes $sz), (Format-Duration $elapsed.TotalSeconds), $mbps)
} else {
    Write-Err "      FAILED or not present"
    $Script:Errors.Add("Step 3: submissions.zip transfer failed")
}

# --- Step 4: Bloomberg snapshots -------------------------------------------
$stepStart = Get-Date
Write-Host ""
Write-Info "[4/$TotalSteps] Bloomberg snapshots"
Write-Progress -Id 1 -Activity "VPS -> D: sync" -Status "Bloomberg snapshots" -PercentComplete (4 / $TotalSteps * 100)
$bloombergDir = "$D_BASE\archives\bloomberg"
$countBefore = (Get-ChildItem "$bloombergDir\bloomberg_daily_file_*.xlsm" -ErrorAction SilentlyContinue).Count
$bytesBefore = ((Get-ChildItem $bloombergDir -File -ErrorAction SilentlyContinue) | Measure-Object -Property Length -Sum).Sum
scp -q -r "${VPS}:/home/jarvis/rexfinhub/data/DASHBOARD/history/*" "$bloombergDir\" 2>&1 | Out-Null
$countAfter = (Get-ChildItem "$bloombergDir\bloomberg_daily_file_*.xlsm" -ErrorAction SilentlyContinue).Count
$bytesAfter = ((Get-ChildItem $bloombergDir -File -ErrorAction SilentlyContinue) | Measure-Object -Property Length -Sum).Sum
$newFiles = [math]::Max(0, $countAfter - $countBefore)
$newBytes = [math]::Max(0, ($bytesAfter - $bytesBefore))
$Script:TotalBytes += $newBytes
$Script:TotalFiles += $newFiles
$elapsed = (Get-Date) - $stepStart
if ($countAfter -gt 0) {
    Write-Ok ("      OK  {0} snapshots present (+{1} new, {2})  in {3}" -f $countAfter, $newFiles, (Format-Bytes $newBytes), (Format-Duration $elapsed.TotalSeconds))
} else {
    Write-Warn "      No snapshots present"
    $Script:Errors.Add("Step 4: no Bloomberg snapshots found after sync")
}

# --- Step 5: SEC http_cache -------------------------------------------------
if ($SkipSecCache) {
    Write-Host ""
    Write-Warn "[5/5] SEC http_cache  --  SKIPPED (-SkipSecCache flag)"
} else {
    $stepStart = Get-Date
    Write-Host ""
    Write-Info "[5/$TotalSteps] SEC http_cache (this is the big one)"
    Write-Progress -Id 1 -Activity "VPS -> D: sync" -Status "SEC http_cache" -PercentComplete (5 / $TotalSteps * 100)
    $cacheDir = "$D_BASE\cache\rexfinhub"
    $cacheBytesBefore = ((Get-ChildItem $cacheDir -Recurse -File -ErrorAction SilentlyContinue) | Measure-Object -Property Length -Sum).Sum
    $cacheFilesBefore = (Get-ChildItem $cacheDir -Recurse -File -ErrorAction SilentlyContinue).Count
    scp -q -r "${VPS}:/home/jarvis/rexfinhub/cache/sec/" "$cacheDir\" 2>&1 | Out-Null
    $scpExit = $LASTEXITCODE
    $cacheBytesAfter = ((Get-ChildItem $cacheDir -Recurse -File -ErrorAction SilentlyContinue) | Measure-Object -Property Length -Sum).Sum
    $cacheFilesAfter = (Get-ChildItem $cacheDir -Recurse -File -ErrorAction SilentlyContinue).Count
    $newFiles = [math]::Max(0, $cacheFilesAfter - $cacheFilesBefore)
    $newBytes = [math]::Max(0, ($cacheBytesAfter - $cacheBytesBefore))
    $Script:TotalBytes += $newBytes
    $Script:TotalFiles += $newFiles
    $elapsed = (Get-Date) - $stepStart
    $mbps = if ($elapsed.TotalSeconds -gt 0 -and $newBytes -gt 0) { ($newBytes / 1MB) / $elapsed.TotalSeconds } else { 0 }
    if ($scpExit -eq 0) {
        Write-Ok ("      OK  +{0} files ({1})  in {2}  @ {3:N1} MB/s" -f $newFiles, (Format-Bytes $newBytes), (Format-Duration $elapsed.TotalSeconds), $mbps)
    } else {
        Write-Warn ("      PARTIAL (scp exit {0}); +{1} files ({2})" -f $scpExit, $newFiles, (Format-Bytes $newBytes))
        $Script:Errors.Add("Step 5: SEC http_cache scp returned exit $scpExit (partial transfer possible)")
    }
}

Write-Progress -Id 1 -Activity "VPS -> D: sync" -Completed

# --- Summary ----------------------------------------------------------------
$runElapsed = (Get-Date) - $RunStart
$avgMbps = if ($runElapsed.TotalSeconds -gt 0 -and $Script:TotalBytes -gt 0) {
    ($Script:TotalBytes / 1MB) / $runElapsed.TotalSeconds
} else { 0 }

Write-Host ""
Write-Host "===============================================================" -ForegroundColor Green
Write-Host "  SYNC COMPLETE" -ForegroundColor Green
Write-Host "===============================================================" -ForegroundColor Green
Write-Host ("  Duration    : {0}" -f (Format-Duration $runElapsed.TotalSeconds))
Write-Host ("  Files copied: {0}" -f $Script:TotalFiles)
Write-Host ("  Total size  : {0}" -f (Format-Bytes $Script:TotalBytes))
Write-Host ("  Avg speed   : {0:N1} MB/s" -f $avgMbps)
Write-Host "==============================================================="  -ForegroundColor Green

if ($Script:Errors.Count -gt 0) {
    Write-Host ""
    Write-Err "ERRORS / WARNINGS ($($Script:Errors.Count)):"
    foreach ($e in $Script:Errors) {
        Write-Err "  - $e"
    }
}

# --- Toast notification -----------------------------------------------------
$sizeGB = [math]::Round($Script:TotalBytes / 1GB, 2)
$durMin = [math]::Round($runElapsed.TotalMinutes, 1)
$toastMsg = "Synced $sizeGB GB to D: in $durMin minutes. $($Script:TotalFiles) files copied."
if ($Script:Errors.Count -gt 0) {
    $toastMsg += " $($Script:Errors.Count) warning(s)."
}
Show-Toast "VPS Sync Complete" $toastMsg

Write-Host ""
if ($Host.Name -eq "ConsoleHost") {
    Read-Host "Press Enter to close"
}
