# data-extraction

Image Pointe's data extraction: every script that reads an outside system and writes the **data folder**. Split out of
`sales-analyst-agent` on 2026-11-18 (phase 4 of the repo split; see the webapp repo's TODO.md, "Target repository
layout"). Data flows one way: extraction writes the data folder; the `tools` repo's dashboards, reports and agent, and
Answers, only read it.

## The data folder

Not a repo. `SALES_DATA_DIR`, or the `data` folder next to the repos (`..\data`, the default). Every script finds it
through that setting (`paths.py` for the newer ones). Nothing here commits data to git.

| Folder | Written by |
|---|---|
| `sales/` | `daily_refresh.py` (nightly Impress), `early_refresh.py`, `gdot_hourly_refresh.py`, `ups_shipments.py`, `shipped_orders_pull.py`, `pull_hubspot_activity.py`, `backfill_*.py` |
| `webstores/` | `scripts/webstores/` (WooCommerce, ShipStation, imports) |
| `dol/` | `import_dol.py`, `dol_company.py`, `dol_payee_addresses.py` |
| `dashboard/` | `scripts/dashboard_inputs/` (sales workbooks, management dashboard, health snapshot, opportunities), `pull_wholesale.py`, `trim_dol_vendors.py` |
| `inventory/` | `scripts/inventory/pull_da_workbooks.py` (DA workbooks, Graph) |
| `quality/` | `scripts/quality/pull_quality.py` (HubSpot tickets, Impress, the Quality Report workbook) |

The data dictionary is still the `README.md` in each data subfolder.

## Layout

```
scripts/                    refreshes, pulls and the scheduled launchers (nightly, early, GDOT hourly)
scripts/kpi/kpi_graph.py    the shared read-only Microsoft Graph sign-in (also used by tools' KPI pull and reports)
scripts/dashboard_inputs/   inputs for the sales dashboard from workbooks and the sales model
scripts/quality/            the Quality Dashboard's data
scripts/webstores/          customer webstore orders, stock, labels and bills
scripts/inventory/          the DA workbooks for the inventory search page
config/sources.json         the folders scripts/dashboard_inputs/extract_inputs.py scans
paths.py                    where the data folder and tools are
docs/chi-town-setup.md      the scheduled jobs on CHI-TOWN
```

Some pulls read settings people keep in `tools` (next to this repo, or `TOOLS_REPO`): `holidays.json` and the
credit-memo classification for the Quality pull, and the account crosswalk for `trim_dol_vendors.py`. They never
write there. The `tools` nightly steps (dashboard builds, page pushes) run from this repo's
`scripts/nightly_task_launcher.ps1`.

## Setup

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
```

Keys and sign-ins stay where they were on the machine that runs the jobs (each script's docstring says where); none are
in this repo. Moving over is not finished: see `CUTOVER.md`.
