"""Where things are for data extraction.

Extraction writes the data folder (not a repo): SALES_DATA_DIR, or the 'data' folder next to the repos. A few pulls
also read settings people keep in the tools repo (next to this one, or TOOLS_REPO); they never write there.
"""

import os

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG = os.path.join(ROOT, "config")
GRAPH = os.path.join(ROOT, "scripts", "kpi")  # kpi_graph.py: the shared read-only Microsoft Graph sign-in

DATA = os.path.abspath(os.environ.get("SALES_DATA_DIR") or os.path.join(ROOT, "..", "data"))
SALES = os.path.join(DATA, "sales")
WEBSTORES = os.path.join(DATA, "webstores")
DATA_DASH = os.path.join(DATA, "dashboard")  # extracted dashboard inputs and the wholesale / DOL vendor files
QUALITY = os.path.join(DATA, "quality")      # ticket text and names: keep it out of any repo
INVENTORY = os.path.join(DATA, "inventory")

# Read-only, from the tools repo
TOOLS = os.path.abspath(os.environ.get("TOOLS_REPO") or os.path.join(ROOT, "..", "tools"))
TOOLS_DASH = os.path.join(TOOLS, "dashboards", "data", "dashboard")  # holidays.json
DOCS = os.path.join(TOOLS, "agent", "docs")  # the credit-memo classification
