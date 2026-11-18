<#
Entry point for the ImagePointe-SalesDataRefresh scheduled task.

Pulls the repo before running daily_refresh.py, so a script/query change pushed
from another machine takes effect on the next scheduled run here without anyone
needing to RDP into this machine and sync manually.

Runs `git pull` and `python daily_refresh.py` as separate processes on purpose:
Python reads the whole script into memory before executing it, so pulling from
inside daily_refresh.py itself wouldn't affect the run already in progress --
the freshly pulled file would only take effect the run *after* next. Doing the
pull here, before daily_refresh.py is even launched, avoids that lag.

Fail-open on a failed pull: this machine already does other unattended git
pushes nightly (see daily_refresh.py's git_commit_and_push), so a pull failure
here is assumed to be transient (network blip) or a rare local conflict. We'd
rather run tonight's refresh with whatever script version is already on disk
than skip a night of data entirely -- same self-healing philosophy as the
LOOKBACK_DAYS / dedup design in daily_refresh.py. The log below makes a failed
pull visible so it doesn't go unnoticed for long.

Deliberately no `$ErrorActionPreference = "Stop"`. `git pull` writes routine
progress lines ("From https://...", branch info) to stderr even on success;
merging that into stdout via `2>&1` while ErrorActionPreference is Stop turns
those normal lines into terminating PowerShell exceptions, killing the script
before daily_refresh.py ever runs. (This is exactly what happened the first
two nights this launcher was live -- 2026-09-19 and 2026-09-20 both silently
no-op'd after logging one line.) Check $LASTEXITCODE explicitly instead of
relying on try/catch around native commands.
#>

$RepoRoot = Split-Path -Parent $PSScriptRoot
$PythonExe = "C:\Users\joshr\AppData\Local\Programs\Python\Python312\python.exe"

Set-Location $RepoRoot

# Dashboards moved to the tools repo (2026-11-17, phase 1 of the repo split; see tools\dashboards\CUTOVER.md).
# tools sits next to this repo unless TOOLS_REPO says otherwise; its builders read data from this repo (SALES_REPO).
$ToolsRoot = if ($env:TOOLS_REPO) { $env:TOOLS_REPO } else { Join-Path (Split-Path -Parent $RepoRoot) "tools" }
$Dash = Join-Path $ToolsRoot "dashboards"
$env:EXTRACTION_REPO = $RepoRoot
# The data folder (not a repo since 2026-11-18, phase 5): written here, read by tools and Answers; nothing is committed.
$env:SALES_DATA_DIR = if ($env:SALES_DATA_DIR) { $env:SALES_DATA_DIR } else { Join-Path (Split-Path -Parent $RepoRoot) "data" }
# The customer-webstores page build is a skill in tools\agent (phase 3); its pulls are here (phase 4).
$AgentSkills = Join-Path $ToolsRoot "agent\.claude\skills"

$logDir = Join-Path $RepoRoot "scripts\logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logFile = Join-Path $logDir ("launcher_" + (Get-Date -Format "yyyy-MM-dd_HHmmss") + ".log")

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

Log "launching daily_refresh.py"
& $PythonExe "$RepoRoot\scripts\daily_refresh.py"
$refreshExit = $LASTEXITCODE
Log "daily_refresh.py exited with code $refreshExit"

# DA Wholesale (2026-10-12): wholesale is sold on the WooCommerce wholesale store, not entered in Impress. Pull its
# completed orders into data/dashboard/wholesale-orders.csv (read-only REST; key in %USERPROFILE%\.woocommerce\sites.json).
# Fail-open: no key or an API error keeps the committed CSV. Committed below with the other dashboard inputs.
Log "pulling DA Wholesale orders (WooCommerce)"
$whOutput = & $PythonExe "$RepoRoot\scripts\pull_wholesale.py" 2>&1
$whOutput | ForEach-Object { Log $_ }

# HubSpot contact activity (2026-10-18): last call / email / visit for every contact on an Impress order since 2023,
# shown in the dashboard's customer panel. READ-ONLY (search and batch-read calls only); private-app token in
# %USERPROFILE%\.hubspot\credentials.json. Fail-open: no token or an API error keeps the committed files. Runs after
# daily_refresh.py so the ordering-email list is tonight's.
Log "pulling HubSpot contact activity"
$hsOutput = & $PythonExe "$RepoRoot\scripts\pull_hubspot_activity.py" 2>&1
$hsOutput | ForEach-Object { Log $_ }

# Sales-dashboard inputs (2026-10-10): pull goals, management-dashboard and customer-health
# numbers out of the newest source workbooks into data/dashboard/*.json. Fail-open: the folders
# in data/dashboard/sources.json are OneDrive shortcuts that may not exist yet; a missing one is
# skipped and the committed JSON stays as-is. Never affects the exit code of the data refresh.
Log "extracting sales-dashboard inputs"
$extractOutput = & $PythonExe "$RepoRoot\scripts\dashboard_inputs\extract_inputs.py" --scan 2>&1
$extractOutput | ForEach-Object { Log $_ }

# Owners folder on the dashboards site (2026-10-30): pages Josh dropped in dashboard-site\owners\ today go up encrypted;
# commits and pushes only when they changed. Fail-open.
$ownersOut = & $PythonExe "$Dash\publish\owners_site_publish.py" 2>&1
$ownersOut | ForEach-Object { Log "owners folder: $_" }
# Site access watch (2026-10-30): email Josh if anyone else changed who can open dashboards-site pages or how it
# deploys (no branch protection on this GitHub plan). Silent when nothing changed. Fail-open.
$watchOut = & $PythonExe "$Dash\publish\site_access_watch.py" 2>&1
$watchOut | ForEach-Object { Log "site access watch: $_" }

# Sales dashboard (2026-10-10): rebuild tonight's data file (plain Python, no Claude; --split writes the page shell
# plus dashboard-data.js) and hand both to the cloud publish routine on the `dashboard-page` branch
# (scripts/push_dashboard_page.ps1). Headless Claude can't publish artifacts, so a claude.ai routine replaces the
# artifact's dashboard-data.js around 4:15am Central; the shell is published by hand when the template changes.
# Both steps fail-open: a failure is logged, yesterday's dashboard stays up, and the data refresh's exit code
# is unchanged.
if ($refreshExit -eq 0) {
    Log "building sales dashboard"
    $buildOutput = & $PythonExe "$Dash\sales-dashboard\scripts\build.py" "$Dash\out\sales-dashboard" --split 2>&1
    $buildExit = $LASTEXITCODE
    $buildOutput | Select-Object -Last 3 | ForEach-Object { Log $_ }
    if ($buildExit -eq 0) {
        # Home Hub business card (2026-11-08, Josh): export the kitchen hub's daily card and its Business-mode brief from
        # tonight's dashboard data (tools\home-hub\export_hub_metrics.py since 2026-11-18; no network), then POST both to the hub Worker on
        # Josh's personal Cloudflare (push_hub_metrics.py; key only in Credential Manager HUB_PUSH, the Worker keeps just
        # its SHA-256 and accepts it on /api/metrics only). Fail-open: a failure is logged, the hub keeps yesterday's
        # card (it shows its as-of date) and the rest of the night carries on. See scripts/hub/README.md.
        try {
            Log "exporting Home Hub business card"
            $hubOut = & $PythonExe "$ToolsRoot\home-hub\export_hub_metrics.py" 2>&1
            $hubExit = $LASTEXITCODE
            $hubOut | ForEach-Object { Log "hub: $_" }
            if ($hubExit -eq 0) {
                $hubPush = & $PythonExe "$ToolsRoot\home-hub\push_hub_metrics.py" 2>&1
                $hubPushExit = $LASTEXITCODE
                $hubPush | ForEach-Object { Log "hub push: $_" }
                Log "hub push exited with code $hubPushExit"
            } else {
                Log "WARNING: hub export failed (exit $hubExit) -- not pushing; the hub keeps yesterday's card"
            }
        } catch {
            Log "WARNING: hub step failed: $_ -- continuing"
        }
        # Rep two-week pages (2026-10-18): each rep's plan with goals (frozen at the window start, reps/goals/) vs results
        # since then; pushed with the dashboard page into the site's rep folders. Fail-open.
        Log "building rep two-week pages"
        $repOut = & $PythonExe "$Dash\sales-dashboard\scripts\rep_pages.py" 2>&1
        $repOut | ForEach-Object { Log $_ }
        $goalStatus = & git -C $ToolsRoot status --porcelain dashboards/reps/goals 2>&1
        if ($goalStatus) {
            & git -C $ToolsRoot add dashboards/reps/goals 2>&1 | ForEach-Object { Log $_ }
            & git -C $ToolsRoot commit -m "Rep page goals frozen $(Get-Date -Format 'yyyy-MM-dd')" 2>&1 | ForEach-Object { Log $_ }
            & git -C $ToolsRoot push 2>&1 | ForEach-Object { Log $_ }
        }
        # KPI dashboard for the IP manager meeting (2026-11-02): pull each panel's numbers (reads tonight's sales
        # dashboard build, the SAA stock and A/R files, three SharePoint workbooks and Emily Nolting's weekly Money email
        # through read-only Graph; scripts/kpi/kpi_graph.py has its own token cache) into data/dashboard/kpi-<year>.json
        # and the daily stock history, commit those numbers, then build out\kpi\index.html. Every panel fails open: a
        # broken source keeps its last good values and the page shows its as-of date.
        Log "pulling KPI dashboard data"
        $kpiOut = & $PythonExe "$Dash\kpi\pull_kpi.py" 2>&1
        $kpiOut | ForEach-Object { Log "kpi: $_" }
        $kpiStatus = & git -C $ToolsRoot status --porcelain -- "dashboards/data/dashboard/kpi-*" 2>&1
        if ($kpiStatus) {
            & git -C $ToolsRoot add -- "dashboards/data/dashboard/kpi-*" 2>&1 | ForEach-Object { Log $_ }
            & git -C $ToolsRoot commit -m "Automated KPI dashboard data $(Get-Date -Format 'yyyy-MM-dd')" -- "dashboards/data/dashboard/kpi-*" 2>&1 | ForEach-Object { Log $_ }
            & git -C $ToolsRoot push 2>&1 | ForEach-Object { Log $_ }
            Log "KPI data committed (exit $LASTEXITCODE)"
        }
        # --owners also writes Jeff and Pat's copy into dashboard-site\owners\ (git-ignored; encrypted below)
        $kpiPage = & $PythonExe "$Dash\kpi\build_kpi_page.py" --owners 2>&1
        $kpiPage | ForEach-Object { Log "kpi page: $_" }
        # Quality Report (2026-11-03): Traci's Quality Report workbook rebuilt nightly. HubSpot Support Pipeline tickets
        # (read-only token), tonight's Impress write-offs and credit memos, and the workbook's typed-in tabs (read-only
        # Graph, the KPI sign-in) into data\quality\ (git-ignored: ticket text and names), then out\quality\index.html.
        # Each source fails open. push_dashboard_page.ps1 (next) carries it to /operations/quality/ on the IP Hub.
        Log "pulling Quality Report data"
        $qOut = & $PythonExe "$RepoRoot\scripts\quality\pull_quality.py" 2>&1
        $qOut | ForEach-Object { Log "quality: $_" }
        $qPage = & $PythonExe "$Dash\quality\build_quality_page.py" 2>&1
        $qPage | ForEach-Object { Log "quality page: $_" }
        Log "pushing the dashboard page for the publish routine"
        & powershell -NoProfile -ExecutionPolicy Bypass -File "$Dash\publish\push_dashboard_page.ps1" 2>&1 | ForEach-Object { Log $_ }
        # Owners' copy of the KPI page (2026-11-02): the owners publish above ran before the KPI build, so publish again
        # now. Only pushes when the copy changed. It runs after the branch push, so the deploy its push starts (the last
        # one tonight) has tonight's pages and tonight's owners folder. Fail-open.
        $ownersKpi = & $PythonExe "$Dash\publish\owners_site_publish.py" 2>&1
        $ownersKpi | ForEach-Object { Log "owners folder (KPI copy): $_" }
    } else {
        Log "WARNING: dashboard build failed (exit $buildExit) -- not pushing; yesterday's dashboard stays up"
    }
} else {
    Log "data refresh failed -- skipping the dashboard build"
}

# Customer webstores (2026-10-12): union webstore orders from WooCommerce and ShipStation label costs (API key in
# %USERPROFILE%\.shipstation\credentials.json) into data/webstores/ -- operations data, NOT
# sales; nothing here touches data/sales/, data/dashboard/ or the sales dashboard. Runs after the dashboard so it can
# never delay it. Pull (last 45 days, read-only; login in %USERPROFILE%\.woocommerce\sites.json, entry "Union webstores"),
# commit data/webstores/ on its own, build the page and hand it to its own publish routine (push_webstores_page.ps1).
# Independent of the Impress refresh, and every step fails open: no login or a failed store keeps the committed data.
Log "pulling ShipStation label costs for the webstore shipping check"
$ssOutput = & $PythonExe "$RepoRoot\scripts\webstores\pull_shipstation.py" 2>&1
$ssOutput | ForEach-Object { Log $_ }
Log "copying webstore inventory purchases from the Impress data"
$ivOutput = & $PythonExe "$RepoRoot\scripts\webstores\import_inventory.py" 2>&1
$ivOutput | ForEach-Object { Log $_ }
Log "pulling customer webstore orders (WooCommerce)"
$wsOutput = & $PythonExe "$RepoRoot\scripts\webstores\pull_webstores.py" 2>&1
$wsOutput | ForEach-Object { Log $_ }
Log "pulling webstore product stock (WooCommerce, read-only)"
$wiOutput = & $PythonExe "$RepoRoot\scripts\webstores\pull_inventory.py" 2>&1
$wiOutput | Select-Object -Last 3 | ForEach-Object { Log $_ }
Log "building customer webstores page"
$wsBuild = & $PythonExe "$AgentSkills\customer-webstores\scripts\build.py" "$Dash\out\customer-webstores" --split 2>&1
$wsBuildExit = $LASTEXITCODE
$wsBuild | Select-Object -Last 3 | ForEach-Object { Log $_ }
if ($wsBuildExit -eq 0) {
    & powershell -NoProfile -ExecutionPolicy Bypass -File "$Dash\publish\push_webstores_page.ps1" 2>&1 | ForEach-Object { Log $_ }
} else {
    Log "WARNING: customer webstores build failed (exit $wsBuildExit) -- not pushing; yesterday's page stays up"
}

# DA Iowa Production Schedule (2026-11-03, Hamilton): rebuild the page's nightly data file from tonight's Impress pull
# (scripts/da_iowa/build_scheduler.py: open DA Iowa orders, unpicked qty by size, M + D stock) and hand it to its own
# publish routine on the `da-iowa-page` branch (push_da_iowa_page.ps1). Plain Python, no Claude, no external calls.
# Only runs after a good refresh; every step fails open, so a failure leaves yesterday's page up.
if ($refreshExit -eq 0) {
    Log "building DA Iowa schedule page"
    $daOut = & $PythonExe "$Dash\da_iowa\build_scheduler.py" 2>&1
    $daExit = $LASTEXITCODE
    $daOut | Select-Object -Last 3 | ForEach-Object { Log "da iowa: $_" }
    if ($daExit -eq 0) {
        & powershell -NoProfile -ExecutionPolicy Bypass -File "$Dash\publish\push_da_iowa_page.ps1" 2>&1 | ForEach-Object { Log $_ }
    } else {
        Log "WARNING: DA Iowa schedule build failed (exit $daExit) -- not pushing; yesterday's page stays up"
    }
} else {
    Log "data refresh failed -- skipping the DA Iowa schedule build"
}

# Inventory search page (2026-11-07, Josh): one search across Impress (finished, unfinished, open POs), the DA Inventory
# Monitor, the DA Iowa pipeline workbook and webstore stock (scripts/inventory/build_inventory.py). Reads the two DA
# workbooks read-only through the KPI Graph sign-in and saves them to data/inventory/da-workbooks.json, committed here so
# the DA numbers live in the repo. Then hands the page to its publish routine on the `inventory-page` branch
# (push_inventory_page.ps1). Runs last, after the webstore stock pull, and even after a failed Impress refresh: every
# source on the page shows its own as-of date. Fails open: a failed build leaves yesterday's page up.
Log "building inventory search page"
Log "pulling the DA workbooks (Graph, read-only)"
$daWbOut = & $PythonExe "$RepoRoot\scripts\inventory\pull_da_workbooks.py" 2>&1
$daWbOut | Select-Object -Last 2 | ForEach-Object { Log "da workbooks: $_" }
$invOut = & $PythonExe "$Dash\inventory\build_inventory.py" 2>&1
$invExit = $LASTEXITCODE
$invOut | Select-Object -Last 4 | ForEach-Object { Log "inventory: $_" }
if ($invExit -eq 0) {
    & powershell -NoProfile -ExecutionPolicy Bypass -File "$Dash\publish\push_inventory_page.ps1" 2>&1 | ForEach-Object { Log $_ }
} else {
    Log "WARNING: inventory search build failed (exit $invExit) -- not pushing; yesterday's page stays up"
}

exit $refreshExit
