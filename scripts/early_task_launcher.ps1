<#
Entry point for the ImagePointe-EarlyRefresh scheduled task (weekdays 4:45 a.m. on CHI-TOWN, 2026-11-05).

Re-pulls open-order-lines.parquet and design-codes.parquet so the 5 a.m. Ops reports see second shift's
scans (they run until about midnight, after the 11 p.m. nightly). See scripts/early_refresh.py.
The -Deadline (default 04:56) keeps any file write clear of the 5:00 Due Dates run.

Same pattern as gdot_task_launcher.ps1: pull first so a pushed change takes effect, fail open on a failed
pull, explicit exit-code checks (no $ErrorActionPreference = "Stop": git writes routine lines to stderr).

Register (once, on CHI-TOWN; schtasks /ST writes local time, not UTC):
  schtasks /Create /TN "ImagePointe-EarlyRefresh" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 04:45 /F /TR "conhost.exe --headless powershell.exe -NoProfile -ExecutionPolicy Bypass -File \"<repo>\scripts\early_task_launcher.ps1\""
#>

param(
    [string]$Deadline = "04:56"
)

# Weekdays only, like the reports it feeds
if ((Get-Date).DayOfWeek -in 'Saturday', 'Sunday') { exit 0 }

$RepoRoot = Split-Path -Parent $PSScriptRoot
$PythonExe = "C:\Users\joshr\AppData\Local\Programs\Python\Python312\python.exe"
if (-not (Test-Path $PythonExe)) { $PythonExe = "python" }

Set-Location $RepoRoot

$logDir = Join-Path $RepoRoot "scripts\logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logFile = Join-Path $logDir ("early_launcher_" + (Get-Date -Format "yyyy-MM-dd_HHmmss") + ".log")

function Log($msg) {
    $line = "$(Get-Date -Format o)  $msg"
    Write-Output $line
    Add-Content -Path $logFile -Value $line
}

Log "pulling latest from origin/main"
$pullOutput = & git pull --ff-only origin main 2>&1
$pullExit = $LASTEXITCODE
$pullOutput | ForEach-Object { Log $_ }
if ($pullExit -ne 0) {
    Log "WARNING: git pull failed (exit $pullExit) -- continuing with local working copy as-is"
} else {
    Log "pull ok"
}

Log "launching early_refresh.py --deadline $Deadline"
& $PythonExe "$RepoRoot\scripts\early_refresh.py" --deadline $Deadline
$refreshExit = $LASTEXITCODE
Log "early_refresh.py exited with code $refreshExit"
exit $refreshExit
