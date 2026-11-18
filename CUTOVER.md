# Phase 4 cutover: extraction from sales-analyst-agent (and tools) to data-extraction

Status 2026-11-18: **moved and tested on a developer PC; `git init` only, not on GitHub. On that PC,
sales-analyst-agent has everything but its committed `data/` removed, uncommitted. CHI-TOWN is unchanged.** Do this
with phases 1–3 (`tools/*/CUTOVER.md`). Phase 5 (the data folder outside git) comes with it on CHI-TOWN, but team PCs
wait for the Answers server (webapp TODO.md, "Phase 5 cutover").

## Done

| From | To |
|---|---|
| sales-analyst-agent `scripts/` (refreshes, launchers, DOL, UPS, Shipped Orders pull, `kpi/kpi_graph.py`, `set_hubspot_token.py`) | `scripts/`, same layout |
| sales-analyst-agent `.claude/skills/sales-dashboard/scripts/pull_wholesale.py`, `pull_hubspot_activity.py` | `scripts/` |
| sales-analyst-agent `docs/chi-town-setup.md` | `docs/` |
| tools `dashboards/sales-dashboard/scripts/extract_inputs.py`, `extract_opportunities.py` | `scripts/dashboard_inputs/` |
| tools `dashboards/quality/pull_quality.py` | `scripts/quality/` |
| tools `agent/.claude/skills/customer-webstores/scripts/` pulls and imports (6) | `scripts/webstores/` |
| tools `dashboards/data/dashboard/sources.json` | `config/` |
| the DA workbook read inside tools `dashboards/inventory/build_inventory.py` | `scripts/inventory/pull_da_workbooks.py` (new) |

- **Outputs to the data folder.** `sales-workbook-2026.json`, `management-dashboard-2026.json`, `health-snapshot.json`,
  `opportunities.json` (to `dashboard/`) and `da-workbooks.json` (to `inventory/`) moved out of tools. The Quality data
  (`quality/`) now lands there too. The sales dashboard build reads those four files from the data folder, the
  inventory page reads the DA extract (and the last pull's error) from it, the Quality page reads `quality/`.
- **Launcher.** `scripts/nightly_task_launcher.ps1` runs the moved pulls from here, runs `pull_da_workbooks.py` before
  the inventory build, and no longer commits dashboard inputs or the DA extract to tools (the KPI data and frozen rep
  goals are still committed to tools: `pull_kpi.py` stays there for now). It sets `EXTRACTION_REPO`, which tools'
  `paths.py` use to find `kpi_graph.py` and the Shipped Orders pull (default: `data-extraction` next to tools).
- **Fixed on the way.** The art Team Check re-runs itself when a pull changes its code; since phase 3 the changed paths
  start with `agent/`, which it didn't recognise.
- **Tested on the developer PC.**
  - Sales dashboard rebuilt with its inputs from the data folder: identical to the previous build.
  - Inventory page built from the data folder's DA extract.
  - The DA workbook pull failed safely without a Graph sign-in: kept the last extract, recorded the error, and the page
    showed it.
  - `extract_inputs --scan` read `config/sources.json`.
  - The webstores import wrote the data folder.
  - The wholesale and ShipStation pulls kept their files without keys.
  - The Due Dates report found `kpi_graph.py` and the Shipped Orders pull here.

  Not tested here: anything needing CHI-TOWN's sign-ins, keys, query bridge or OneDrive folders.

## Still a hybrid

`tools/dashboards/kpi/pull_kpi.py`: 6 panels read outside systems (stock-out, line of credit, DA TN, DA Iowa, CS
tickets, commitments), 5 derive from tonight's sales dashboard build and the data folder. Split later: the outside
panels into a pull here, writing the data folder.

## To do, in order (with phases 1–3)

1. **GitHub repo** `Image-Pointe/data-extraction` (private) created; push the first commit. Push rights for CHI-TOWN.
2. **CHI-TOWN.** Clone it next to `tools`. Choose the data folder (`SALES_DATA_DIR`, machine-wide), outside OneDrive if
   the repos are in it, and copy the current `sales-analyst-agent\data\` into it, plus `tools\dashboards\data\quality\`
   (to `quality\`). Run `tools\agent\setup.ps1` so the agent's link points at it.
3. **Scheduled tasks.** Point the nightly, early (`ImagePointe-EarlyRefresh`) and GDOT hourly tasks at
   `<data-extraction>\scripts\*_task_launcher.ps1`. The Shipped Orders hourly task (tools) needs no change.
4. **Sign-ins and keys** stay where they are (`%USERPROFILE%\.hubspot`, `.woocommerce`, `.shipstation`, the Graph token
   caches under `%LOCALAPPDATA%`); check each script's `--check` or a hand run once.
5. **sales-analyst-agent.** Commit the removals with Josh (all phases), stop its scheduled tasks, and archive it on GitHub
   once a week of nights runs clean from here.
