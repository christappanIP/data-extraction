# CHI-TOWN setup: daily sales data refresh

> **Admin-only.** This is for whoever manages the Impress data pipeline (currently
> Josh). Regular team members using this repo don't need anything here — see
> `docs/team-setup-guide.md` instead.

One-time setup to run the nightly Impress data refresh on CHI-TOWN (always-on),
scheduled for 11pm daily (2am until 2026-10-13). The refresh script itself is plain Python — no Claude
involved at runtime — so once this is wired up it just runs unattended.

**2026-10-04 (Josh): both tasks now run headless.** The action is `conhost.exe --headless powershell.exe ...` instead of `powershell.exe ...` directly -- launching PowerShell straight from Task Scheduler under an Interactive logon popped a console window on CHI-TOWN every 30 minutes. `conhost --headless` (built into Windows 11) gives PowerShell, Python, git and rclone a console with no visible window, while keeping the same Interactive logon so git credentials and rclone config work unchanged. Trade-off: conhost does not pass the exit code through, so Task Scheduler's "Last Run Result" always shows 0 -- check `scripts\logs\launcher_*.log` / `gdot_launcher_*.log` (last line records the real exit code). To switch an existing install: `Set-ScheduledTask -TaskName <name> -Action (New-ScheduledTaskAction -Execute "conhost.exe" -Argument "--headless powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"<launcher path>`"")`.

**2026-10-04 (Josh): both tasks now run headless.** The action is `conhost.exe --headless powershell.exe ...` instead of `powershell.exe ...` directly -- launching PowerShell straight from Task Scheduler under an Interactive logon popped a console window on CHI-TOWN every 30 minutes. `conhost --headless` (built into Windows 11) gives PowerShell, Python, git and rclone a console with no visible window, while keeping the same Interactive logon so git credentials and rclone config work unchanged. Trade-off: conhost does not pass the exit code through, so Task Scheduler's "Last Run Result" always shows 0 -- check `scripts\logs\launcher_*.log` / `gdot_launcher_*.log` (last line records the real exit code). To switch an existing install: `Set-ScheduledTask -TaskName <name> -Action (New-ScheduledTaskAction -Execute "conhost.exe" -Argument "--headless powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"<launcher path>`"")`.

## 1. Clone the repo and cache GitHub credentials

Open PowerShell and run:

```
git clone https://github.com/Image-Pointe/sales-analyst-agent.git "C:\Users\joshr\OneDrive - Image Pointe\IP Agents\sales-analyst-agent"
```

This should pop a browser window for GitHub sign-in. If `git config --global user.email`
comes back empty afterward, also run:

```
git config --global user.email "josh@imagepointe.com"
git config --global user.name "Josh"
```

## 2. Install Python dependencies

```
python -m pip install pandas pyarrow
```

## 3. Test it manually once

```
cd "C:\Users\joshr\OneDrive - Image Pointe\IP Agents\sales-analyst-agent"
python scripts\daily_refresh.py
```

Should take about 1.5-3 minutes and end with `done`. Check `scripts\logs\` for the
run's log file if anything looks off.

## 4. Register the Task Scheduler job (11pm daily)

**2026-10-13 (Josh): moved from 2am to 11pm.** Impress runs its nightly backup overnight and the
application server shuts down around 3am afterward; the 2am run sat inside the backup (the 2026-10-09
run timed out waiting on Impress). 11pm is clear of the backup, the ~5-6:40pm window when SQL Server
often stops accepting connections, and the 7:15pm database reboot. On an existing install:
`Set-ScheduledTask -TaskName "ImagePointe-SalesDataRefresh" -Trigger (New-ScheduledTaskTrigger -Daily -At 11pm)`.

**Updated 2026-09-22 to reflect what's actually deployed** — the registered action
runs `scripts\nightly_task_launcher.ps1`, not `daily_refresh.py` directly. The
launcher does `git pull --ff-only origin main` first, then runs `daily_refresh.py` --
this is what lets a script/query change pushed from another machine take effect here
on the next scheduled run without anyone RDPing in to sync manually (see that file's
own comments for why it's a separate launcher rather than a pull baked into
`daily_refresh.py` itself). This doc previously showed registering `python.exe`
directly, which is stale -- if a task on this machine is still wired that way, switch
it to the launcher below.

```powershell
$action = New-ScheduledTaskAction -Execute "conhost.exe" -Argument "--headless powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"C:\Users\joshr\OneDrive - Image Pointe\IP Agents\sales-analyst-agent\scripts\nightly_task_launcher.ps1`""
$trigger = New-ScheduledTaskTrigger -Daily -At 11pm
Register-ScheduledTask -TaskName "ImagePointe-SalesDataRefresh" -Action $action -Trigger $trigger -Description "Nightly Impress refresh for sales-analyst-agent (pulls latest code first)"
```

No wake-computer setting needed since this PC is always on.

## 5. Register the GDOT/art intraday refresh (7am-7pm, added 2026-09-22; every 30 min since 2026-10-01)

**2026-10-01 (Josh):** now polls and pushes every 30 minutes (was hourly) and also pulls art status events (ARTX/ARTRC etc.) for the `art-team-check` skill. The task keeps its original name. On an existing install, change only the interval: `$t = Get-ScheduledTask -TaskName "ImagePointe-GDOTHourlyRefresh"; $t.Triggers[0].Repetition.Interval = "PT30M"; Set-ScheduledTask -InputObject $t`. Polls seek from the newest OTEVENT_ID in the snapshot (no index leads with OTEVENT_TIME), skip if the previous run still holds `scripts/logs/intraday_refresh.lock`, and push at most every `REFRESH_PUSH_EVERY_MIN` (30).

A second, lighter job that polls hourly for new GDOT ("PO to Team Check") scans and
refreshes just the affected orders/POs — see `.claude/skills/po-so-reconcile/SKILL.md`'s
"GDOT hourly refresh and the pending queue" section for why this exists alongside the
nightly job above. Same launcher pattern as step 4 (`scripts\gdot_task_launcher.ps1`
pulls first, then runs `gdot_hourly_refresh.py`) so a same-day code push takes effect
on this job's very next hourly run too, not just at the 11pm nightly run.

```powershell
$action = New-ScheduledTaskAction -Execute "conhost.exe" -Argument "--headless powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"C:\Users\joshr\OneDrive - Image Pointe\IP Agents\sales-analyst-agent\scripts\gdot_task_launcher.ps1`""
$trigger = New-ScheduledTaskTrigger -Once -At 7am -RepetitionInterval (New-TimeSpan -Minutes 30) -RepetitionDuration (New-TimeSpan -Hours 12)
Register-ScheduledTask -TaskName "ImagePointe-GDOTHourlyRefresh" -Action $action -Trigger $trigger -Description "Hourly GDOT-triggered refresh for sales-analyst-agent (7am-7pm, pulls latest code first)"
```

The script also has its own business-hours check as a backstop (see its docstring),
but the trigger window above is the primary control — it repeats hourly starting 7am
for 12 hours (last fire at 6pm), covering the 7am-7pm window this job is scoped to.

## What the script actually does each night

- Pulls orders + design-codes received in the last 60 days from Impress (a generous
  lookback window, not an exact "since last run" pointer — self-healing if a run is
  ever missed), appends to the existing Parquet files, and deduplicates on each
  file's real key.
- Re-pulls order-tracking in full every time (cheap, ~85k rows) since an order's
  status/invoice/ship info can change long after it was received.
- Commits and pushes the updated Parquet files to GitHub automatically.

Full technical detail — the exact validated SQL, and three real bugs found and fixed
while building this (Art Request contamination, a mostly-blank invoice join field,
a non-unique dedup key) — is in `docs/impress-refresh-queries.md` in this repo.
