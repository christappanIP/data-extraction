# data-extraction

Image Pointe's data extraction: the scripts that read Impress (through the query bridge), HubSpot, WooCommerce,
ShipStation, UPS, the DOL files and Microsoft 365 (Graph), and write the data folder. See `README.md` for what writes
where.

- **Read-only against every source system.** Nothing here writes back to Impress, HubSpot, the stores, SharePoint or
  Outlook.
- **The data folder isn't a repo.** Write it through `SALES_DATA_DIR` (or the `data` folder next to the repos), never
  into a repo, and never commit data.
- **No secrets in the repo.** Keys and sign-in caches live on the machine that runs the jobs.
- **The data dictionary is each data subfolder's `README.md`.** Update it when a script adds or changes a file or column.
- The sales analyst agent, dashboards and reports are in the `tools` repo, and only read the data folder.
- Dates are `YYYY-MM-DD`.
