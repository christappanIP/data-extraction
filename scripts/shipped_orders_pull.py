"""Hourly Shipped Orders pull, with the data (split out of shipped_orders_report.py on 2026-11-18; the report moved to
the tools repo, reports/shipped_orders/, which runs this first with --hourly).

Read-only against Impress (SELECT through the query bridge) and UPS. Writes:
  data/sales/so-shipments-intraday.parquet  boxes shipped in the last INTRADAY_DAYS days
  data/sales/so-header-extra.parquet        header fields of every order shipped or changed in that window
then runs ups_shipments.py --no-tracking (UPS Quantum View pull and match). Each step fails open.

Usage: python scripts/shipped_orders_pull.py
"""

import os, subprocess, sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = Path(os.environ.get("SALES_DATA_DIR") or REPO_ROOT.parent / "data")  # the data folder (not a repo since 2026-11-18): SALES_DATA_DIR, or 'data' next to the repos
DATA = DATA_ROOT / "sales"
LOG_DIR = REPO_ROOT / "scripts" / "logs"
INTRADAY_SHIP = DATA / "so-shipments-intraday.parquet"
HEADER_EXTRA = DATA / "so-header-extra.parquet"
INTRADAY_DAYS = 3


def log(msg, fh=None):
    line = f"{datetime.now():%Y-%m-%d %H:%M:%S} {msg}"
    print(line, flush=True)
    if fh:
        fh.write(line + "\n"); fh.flush()


# ---------------------------------------------------------------- hourly Impress pull

def impress_pull(fh):
    """Boxes shipped in the last INTRADAY_DAYS days, and the header fields of every order
    shipped or changed in that window. Same columns as the nightly so-shipments pull."""
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    import gdot_hourly_refresh as g
    tag, bridge = g.get_tag_and_bridge_dir()
    since = (datetime.now() - timedelta(days=INTRADAY_DAYS)).strftime("%Y-%m-%d")
    run = datetime.now().strftime("%H%M%S")
    ship_sql = f"""
SELECT s.SORDERS_SORDER_CODE AS sales_order, CONVERT(char(10), s.SORDERS_SHIPDATE, 120) AS ship_date,
       s.SORDERS_SHIPVIA_CODE AS ship_via_code, sv.SHIPVIA_DESC AS ship_via_desc,
       s.SORDERS_TRACKINGNBR AS tracking, s.SORDERS_BOX AS box, s.SORDERS_BOXES AS boxes,
       s.SORDERS_TOTALQTYSHIPPED AS qty_shipped, CAST(s.SORDERS_WEIGHT AS decimal(18,2)) AS weight,
       CAST(s.SORDERS_FREIGHT AS decimal(18,2)) AS freight, s.SORDERS_INVOICE_INVNBR AS invoice
FROM SORDERS s
LEFT JOIN SHIPVIA sv ON sv.SHIPVIA_CODE = s.SORDERS_SHIPVIA_CODE
WHERE s.SORDERS_SHIPDATE >= '{since}'""".strip()
    hdr_sql = f"""
SELECT o.SORDER_CODE AS sales_order, CONVERT(char(10), o.SORDER_RECDATE, 120) AS received_date,
       o.SORDER_CUSTOMER_CODE AS customer_number, o.SORDER_AGENT_CODE1 AS order_agent_code,
       o.SORDER_CUSTPO AS custpo, o.SORDER_ENDCUSTPO AS endcustpo,
       o.SORDER_SHIPREFERENCE1 AS shipref1, o.SORDER_SHIPREFENTRY1 AS shiprefentry1,
       o.SORDER_SHIPREFERENCE2 AS shipref2, o.SORDER_SHIPREFENTRY2 AS shiprefentry2
FROM SORDER o
WHERE o.SORDER_DATECHANGED >= '{since}' OR o.SORDER_DATEENTERED >= '{since}'
   OR o.SORDER_CODE IN (SELECT SORDERS_SORDER_CODE FROM SORDERS WHERE SORDERS_SHIPDATE >= '{since}')""".strip()
    names = {k: g.submit_query(tag, bridge, f"shiprpt_{k}_{run}", q, fh) for k, q in (("ship", ship_sql), ("hdr", hdr_sql))}
    ship = pd.read_csv(g.wait_for_result(tag, bridge, names["ship"], fh), dtype=str, encoding="utf-8-sig")
    hdr = pd.read_csv(g.wait_for_result(tag, bridge, names["hdr"], fh), dtype=str, encoding="utf-8-sig")
    for d in (ship, hdr):
        for c in d.columns:
            d[c] = d[c].str.strip()
    ship["ship_date"] = pd.to_datetime(ship.ship_date, errors="coerce")
    for c in ("boxes", "qty_shipped"):
        ship[c] = pd.to_numeric(ship[c], errors="coerce").fillna(0).astype("int64")
    for c in ("weight", "freight"):
        ship[c] = pd.to_numeric(ship[c], errors="coerce")
    ship.to_parquet(INTRADAY_SHIP, index=False)
    old = pd.read_parquet(HEADER_EXTRA) if HEADER_EXTRA.exists() else hdr.iloc[0:0]
    pd.concat([old[~old.sales_order.isin(hdr.sales_order)], hdr]).to_parquet(HEADER_EXTRA, index=False)
    log(f"impress: {len(ship)} boxes since {since}, {len(hdr)} order headers refreshed", fh)


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_DIR / f"shipped_orders_pull_{datetime.now():%Y-%m-%d_%H%M%S}.log", "w", encoding="utf-8") as fh:
        try:
            impress_pull(fh)
        except Exception as e:
            log(f"impress pull FAILED, the report builds from the last data: {e}", fh)
        r = subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "ups_shipments.py"), "--no-tracking"],
                           capture_output=True, text=True)
        log(f"ups_shipments.py --no-tracking exit {r.returncode}: {(r.stdout or '')[-400:]}{(r.stderr or '')[-400:]}", fh)


if __name__ == "__main__":
    main()
