<#
Entry point for the ImagePointe-GDOTHourlyRefresh scheduled task.

Mirrors nightly_task_launcher.ps1 -- see that file's comments for the full reasoning
(pull-before-run so a code change pushed from another machine takes effect on the
next scheduled run here, fail-open on a failed pull, explicit exit-code check instead
of relying on $ErrorActionPreference which turns git's routine stderr chatter into
terminating errors). Kept as a near-duplicate rather than a shared parameterized
launcher, deliberately: this hourly job is newer and less proven than the nightly one,
and a shared launcher script means a bug in it risks both jobs at once.
#>

$RepoRoot = Split-Path -Parent $PSScriptRoot
$PythonExe = "C:\Users\joshr\AppData\Local\Programs\Python\Python312\python.exe"

Set-Location $RepoRoot

$logDir = Join-Path $RepoRoot "scripts\logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logFile = Join-Path $logDir ("gdot_launcher_" + (Get-Date -Format "yyyy-MM-dd_HHmmss") + ".log")

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

Log "launching gdot_hourly_refresh.py"
& $PythonExe "$RepoRoot\scripts\gdot_hourly_refresh.py"
$refreshExit = $LASTEXITCODE
Log "gdot_hourly_refresh.py exited with code $refreshExit"

# Vendor PO confirmations (2026-10-13): scan the purchasing mailboxes and update the
# shared "Purchasing - PO confirmations for Team Check" folder. Runs after the refresh so
# it compares against fresh PO data. Fail-open: its result never changes this task's exit
# code, so a mail or sign-in problem can't stop the GDOT refresh.
Log "launching confirmations.py --scan"
# po-so-reconcile moved to tools\agent (2026-11-18, phase 3); tools sits next to this repo unless TOOLS_REPO says otherwise
$ToolsRoot = if ($env:TOOLS_REPO) { $env:TOOLS_REPO } else { Join-Path (Split-Path -Parent $RepoRoot) "tools" }
$confOutput = & $PythonExe "$ToolsRoot\agent\.claude\skills\po-so-reconcile\confirmations.py" --scan 2>&1
$confOutput | ForEach-Object { Log "  $_" }
Log "confirmations.py exited with code $LASTEXITCODE"
exit $refreshExit
