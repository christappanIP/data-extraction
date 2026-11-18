"""
Hourly, business-hours-only refresh triggered by new GDOT ("PO to Team Check") scans.

Built for ImagePointe-OS's PO Team Check automation project (see that repo's
projects/In process/po-team-check-automation/po-team-check-automation.md) to close
the freshness gap `daily_refresh.py` can't: a PO scanned GDOT (ready for Purchasing
Lead review) at 9am shouldn't have to wait until the next 11pm nightly run to show up
in `data/sales/`, but a same-day PO Team Check gate also doesn't need the live query
bridge's zero-lag guarantee -- roughly hourly is the agreed middle tier (Josh,
2026-09-22).

Design, and why:
- Triggered by GDOT, not a date/received-date window. GDOT (OTEVENT_PRODACT_CODE =
  'GDOT') is the real "PO entered, purchaser has flagged it ready for Team Check"
  signal (confirmed 2026-09-23 against 23,895 historical events -- see
  ImagePointe-OS's knowledge/08_operations/po-team-check-sop-map-and-art-storage.md).
  It's logged at the SO level, so one poll pass is: find SOs with a *new* GDOT event,
  then refresh both sides (orders AND their linked POs/PO-lines) for exactly those
  SOs -- not a full-table pull. This is what keeps an hourly cadence cheap and avoids
  the PORDER full-table-stall problem daily_refresh.py's own docstring warns about.
- Lookback window (GDOT_LOOKBACK_HOURS), not an exact "since last successful poll"
  pointer -- same self-healing reasoning as daily_refresh.py's LOOKBACK_DAYS: a missed
  or failed run doesn't silently lose GDOT events, and re-pulling an already-seen
  event is harmless (dedup handles it, and the queue upsert is idempotent).
- Deliberately duplicates daily_refresh.py's bridge submit/poll/merge plumbing rather
  than importing it, so this new, less-proven script can't destabilize the working
  nightly job. Revisit consolidating into a shared module once this has run
  unattended for a while without incident.
- Business hours only (7am-7pm local, Josh's call 2026-09-22): GDOT scans only happen
  while purchasers are working, so this only guards against Task Scheduler's own
  catch-up/misfire behavior -- the trigger registration itself (see
  docs/chi-town-setup.md) is what actually limits *when* this runs; this check is a
  cheap belt-and-suspenders backstop, not the primary control.
- Also maintains data/sales/gdot_queue.json -- a small "orders pulled in via GDOT but
  not yet Team-Checked" queue that `reconcile.py --pending` reads to show what's
  waiting and how long it's been sitting. This script only ever adds/refreshes
  entries; `reconcile.py` is what marks one checked, once someone actually runs a
  reconciliation against it.

Usage: no arguments. Meant to be triggered by Windows Task Scheduler on CHI-TOWN,
same as daily_refresh.py -- see docs/chi-town-setup.md for the registration snippet.
"""
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = Path(os.environ.get("SALES_DATA_DIR") or REPO_ROOT.parent / "data")  # the data folder (not a repo since 2026-11-18): SALES_DATA_DIR, or 'data' next to the repos
DATA_DIR = DATA_ROOT / "sales"
LOG_DIR = REPO_ROOT / "scripts" / "logs"
RCLONE = r"C:\rclone\rclone.exe"
DROPBOX_REMOTE_ROOT = "dropbox:Claude/SQL database/_query_bridge"

BRIDGE_FOLDERS = {
    "XIA_WU": r"C:\Users\joshr\Dropbox\Claude\SQL database\_query_bridge",
    "CHI-TOWN": r"C:\Users\joshr\_query_bridge",
}

POLL_INTERVAL_SEC = 10
POLL_TIMEOUT_SEC = 300  # scoped queries are small -- fail fast, don't eat into the next hourly slot

# Trigger interval this script is scheduled at (Task Scheduler is the real control --
# keep this in step with it). Lookback stays at 4x the interval: same self-healing
# overlap as the original 4h-on-1h design, without re-pulling 4h of SOs every 15 min.
POLL_INTERVAL_MIN = int(os.environ.get("REFRESH_INTERVAL_MIN", "30"))  # 30 min: Josh, 2026-10-01
GDOT_LOOKBACK_HOURS = max(1.0, 4 * POLL_INTERVAL_MIN / 60)

# Art status events for the art-team-check skill (added 2026-10-01; A# art requests included
# 2026-10-02 -- confirmed live they log the same ARTX/ARTRC/ARTW/ARTD codes; the nightly
# events pull still keeps S% only). Kept in their own
# small rolling file instead of rewriting the ~29 MB order-tracking-events.parquet every
# poll; the nightly refresh still lands the same events in the big file.
ART_CODES = ("ARTX", "ARTRC", "ARTW", "ARTA", "ARTD", "ARTP", "ARTR", "ARTPP", "ARTT", "ARTBTT", "ARTHLD")
ART_READY_CODES = ("ARTX", "ARTRC")
ART_RECENT_DAYS = 14
ART_EVENTS_PATH = DATA_DIR / "art-events-recent.parquet"
ART_DESIGN_CODES_PATH = DATA_DIR / "art-design-codes-recent.parquet"
# Every status event (all OTEVENT_PRODACT_CODEs, S# and A#), added 2026-10-04 for the
# exact-reorder / art prep pipeline on Dante: it needs ORDW ("order team checked, sent for
# approval") the same day, plus ORDT/ORDA and anything else a consumer may want. One query
# feeds both files: art-events-recent keeps exactly ART_CODES so art-team-check sees the
# same data it always has; order-events-recent holds everything.
ORDER_EVENTS_PATH = DATA_DIR / "order-events-recent.parquet"
ORDW_CODES = ("ORDW",)
LOCK_PATH = LOG_DIR / "intraday_refresh.lock"
LOCK_STALE_MIN = 45
# ORDERTRACKINGEVENT (~1.95M rows) has no index leading with OTEVENT_TIME or the event
# code (checked live 2026-10-01: only OTEVENT_ID clustered PK, plus INVNBR- and
# SORDER_CODE-leading indexes), so a time-only filter scans the whole table every poll.
# OTEVENT_ID rises with time (2 backward jumps in 158k 2026 events, worst 15 min), so
# polls also filter OTEVENT_ID above the newest ID already in the snapshot minus this
# margin (~6 days of events) -- a clustered range seek over a few thousand rows.
EVENT_ID_MARGIN = 5000


def event_id_floor():
    best = 0
    for path in (DATA_DIR / "order-tracking-events.parquet", DATA_DIR / "art-events-recent.parquet",
                 DATA_DIR / "order-events-recent.parquet"):
        if path.exists():
            try:
                ids = pd.to_numeric(pd.read_parquet(path, columns=["event_id"])["event_id"], errors="coerce")
                best = max(best, int(ids.max()))
            except Exception:
                pass
    return max(0, best - EVENT_ID_MARGIN)

BUSINESS_HOUR_START = 7   # inclusive, local time
BUSINESS_HOUR_END = 19    # exclusive, local time

QUEUE_PATH = DATA_DIR / "gdot_queue.json"
LAST_POLL_PATH = DATA_DIR / "LAST_GDOT_POLL.json"


def log(msg, fh):
    line = f"{datetime.now().isoformat(timespec='seconds')}  {msg}"
    print(line)
    fh.write(line + "\n")
    fh.flush()


def get_tag_and_bridge_dir():
    computername = os.environ["COMPUTERNAME"]
    bridge_dir = BRIDGE_FOLDERS.get(computername)
    if not bridge_dir:
        raise RuntimeError(
            f"No known bridge folder for machine '{computername}'. "
            f"Add it to BRIDGE_FOLDERS in this script (see run-impress-queries skill)."
        )
    bridge_dir = Path(bridge_dir)
    tag_file = bridge_dir / "TAG.txt"
    tag = tag_file.read_text().strip()
    if tag != computername:
        raise RuntimeError(f"TAG.txt says '{tag}' but COMPUTERNAME is '{computername}' -- refusing to run.")
    return tag, bridge_dir


def submit_query(tag, bridge_dir, name, sql, fh):
    inbox = bridge_dir / "query_inbox"
    sql_path = inbox / f"{tag}__{name}.sql"
    sql_path.write_text(sql, encoding="utf-8")
    log(f"wrote {sql_path.name}", fh)
    subprocess.run(
        [RCLONE, "copy", str(sql_path), f"{DROPBOX_REMOTE_ROOT}/query_inbox/"],
        capture_output=True, text=True,
    )
    return name


def wait_for_result(tag, bridge_dir, name, fh):
    results = bridge_dir / "query_results"
    status_path = results / f"{tag}__{name}.status.txt"
    csv_path = results / f"{tag}__{name}.csv"
    deadline = time.time() + POLL_TIMEOUT_SEC
    while time.time() < deadline:
        subprocess.run(
            [RCLONE, "copy", f"{DROPBOX_REMOTE_ROOT}/query_results/", str(results)],
            capture_output=True, text=True,
        )
        if status_path.exists():
            status = status_path.read_text(encoding="utf-8-sig").strip()
            if status.startswith("OK"):
                if csv_path.exists():
                    log(f"{name}: {status}", fh)
                    return csv_path
            else:
                raise RuntimeError(f"{name} query failed: {status}")
        time.sleep(POLL_INTERVAL_SEC)
    raise TimeoutError(f"{name} query did not return within {POLL_TIMEOUT_SEC}s")


def parse_mixed_dates(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", format="mixed")
    return df


def merge_append_dedupe(existing_path, new_csv_path, date_cols, numeric_cols, key_cols, fh):
    try:
        new_df = pd.read_csv(new_csv_path, dtype=str, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        # The query bridge writes a header-less, fully empty CSV when a query matches
        # zero rows -- a real case here (e.g. order_notes_query_scoped() when none of
        # this hour's GDOT'd SOs have any internal notes yet). Same fix as the
        # gdot_events zero-result case in main() below -- nothing to merge, not an
        # error.
        log(f"{existing_path.name}: 0 rows returned, nothing to merge", fh)
        if existing_path.exists():
            return len(pd.read_parquet(existing_path))
        return 0
    new_df = parse_mixed_dates(new_df, date_cols)
    for col in numeric_cols:
        if col in new_df.columns:
            new_df[col] = pd.to_numeric(new_df[col], errors="coerce")

    if existing_path.exists():
        old_df = pd.read_parquet(existing_path)
        combined = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined = new_df

    before = len(combined)
    combined = combined.drop_duplicates(subset=key_cols, keep="last")
    log(f"{existing_path.name}: {before} -> {len(combined)} rows after dedup on {key_cols}", fh)
    combined.to_parquet(existing_path, index=False)
    return len(combined)


def merge_replace_by_group(existing_path, new_csv_path, date_cols, numeric_cols, group_col, fh):
    """Same fix as daily_refresh.py's copy of this function -- see its docstring
    for the full 2026-09-23 S0087649 finding (a deleted SO line, AS2775BLK/ORA/PUR,
    kept surfacing as a phantom false mismatch forever because key-based dedup
    alone can never detect a row that's simply absent from a new pull). For every
    `group_col` value present in this pull (this hour's GDOT'd SOs/their linked
    POs), drop all of that group's existing cached rows first, then add the fresh
    set, so a row genuinely deleted from Impress actually disappears here too."""
    try:
        new_df = pd.read_csv(new_csv_path, dtype=str, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        log(f"{existing_path.name}: 0 rows returned, nothing to merge", fh)
        if existing_path.exists():
            return len(pd.read_parquet(existing_path))
        return 0
    new_df = parse_mixed_dates(new_df, date_cols)
    for col in numeric_cols:
        if col in new_df.columns:
            new_df[col] = pd.to_numeric(new_df[col], errors="coerce")

    if existing_path.exists():
        old_df = pd.read_parquet(existing_path)
        refreshed_groups = set(new_df[group_col].dropna().unique())
        stale = old_df[group_col].isin(refreshed_groups)
        dropped = int(stale.sum())
        combined = pd.concat([old_df[~stale], new_df], ignore_index=True)
    else:
        dropped = 0
        combined = new_df

    log(
        f"{existing_path.name}: {len(new_df)} rows refreshed across "
        f"{new_df[group_col].nunique()} {group_col}(s), {dropped} stale row(s) dropped, "
        f"{len(combined)} total",
        fh,
    )
    combined.to_parquet(existing_path, index=False)
    return len(combined)


def sql_in_list(codes):
    """Format a list of Impress codes (SO#s) as a SQL IN(...) literal list. These are
    values we just pulled out of Impress ourselves (not user input), but quote-escape
    defensively anyway -- cheap insurance."""
    escaped = [c.replace("'", "''") for c in codes]
    return ", ".join(f"'{c}'" for c in escaped)


def gdot_events_query(since_iso, id_floor=0):
    return f"""
SELECT DISTINCT OTEVENT_SORDER_CODE AS sales_order
FROM ORDERTRACKINGEVENT
WHERE OTEVENT_ID > {int(id_floor)}
  AND OTEVENT_PRODACT_CODE = 'GDOT' AND OTEVENT_TIME >= '{since_iso}'
  AND OTEVENT_SORDER_CODE LIKE 'S%';
""".strip()


def orders_query_scoped(so_codes):
    """Same SELECT as daily_refresh.py's orders_query() -- keep the field list and
    column names identical so this merges into orders.parquet with the same schema.
    Only the WHERE clause differs: scoped to specific SO#s (this hour's GDOT'd
    orders), not a received_date lookback window."""
    return f"""
WITH orda AS (
    SELECT OTEVENT_SORDER_CODE AS c, MIN(OTEVENT_TIME) AS ord_approved
    FROM ORDERTRACKINGEVENT
    WHERE OTEVENT_PRODACT_CODE = 'ORDA'
      AND OTEVENT_SORDER_CODE IN ({sql_in_list(so_codes)})
    GROUP BY OTEVENT_SORDER_CODE
)
SELECT o.SORDER_CODE AS sales_order, o.SORDER_RECDATE AS received_date,
       oa.ord_approved,
       o.SORDER_CUSTOMER_CODE AS customer_number, c.CUSTOMER_NAME AS customer_name,
       o.SORDER_AGENT_CODE1 AS agent_1, a.AGENT_NAME AS agent_name_1,
       CAST(o.SORDER_PRODUCTTOTAL AS decimal(18,2)) AS order_sub_total,
       o.SORDER_SHORTED AS shorted,
       ot.OETYPE_DESC AS sales_order_type,
       o.SORDER_REQUEST AS due_date, o.SORDER_NOTBEFORE AS latest_ship_date,
       o.SORDER_SNAME AS ship_name, o.SORDER_SADDRESS1 AS ship_address1,
       o.SORDER_SCITY AS ship_city, o.SORDER_SSTATE AS ship_state, o.SORDER_SZIP AS ship_zip,
       o.SORDER_SHIPVIA_CODE1 AS ship_via_code, sv.SHIPVIA_DESC AS ship_via_desc,
       d.SORDERD_LINE AS line, d.SORDERD_RAWSKU_SKUNBR AS sku,
       d.SORDERD_DESC AS sku_description,
       d.SORDERD_PURCHASEDESC AS line_description, d.SORDERD_GCOLOR_CODE AS gcolor_code,
       d.SORDERD_TOTALQTYORDERED AS quantity_ordered,
       CAST(d.SORDERD_TOTALPRICE AS decimal(18,2)) AS sku_ordered_amount,
       sz.Size AS size, sz.Qty AS size_qty_ordered,
       CAST(sz.Price AS decimal(18,2)) * sz.Qty AS size_ordered_amount,
       sz.Wip AS size_qty_wip, sz.Shp AS size_qty_shipped,
       o.SORDER_BUYERNAME AS buyer_name, o.SORDER_BUYERPHONE AS buyer_phone,
       o.SORDER_BUYEREMAIL AS buyer_email
FROM SORDER o
JOIN SORDERD d ON d.SORDERD_SORDER_CODE = o.SORDER_CODE
CROSS APPLY (VALUES (d.SORDERD_SIZE1,d.SORDERD_QTYORDERED1,d.SORDERD_PRICE1,d.SORDERD_QTYWIP1,d.SORDERD_QTYSHIPPED1),
                    (d.SORDERD_SIZE2,d.SORDERD_QTYORDERED2,d.SORDERD_PRICE2,d.SORDERD_QTYWIP2,d.SORDERD_QTYSHIPPED2),
                    (d.SORDERD_SIZE3,d.SORDERD_QTYORDERED3,d.SORDERD_PRICE3,d.SORDERD_QTYWIP3,d.SORDERD_QTYSHIPPED3),
                    (d.SORDERD_SIZE4,d.SORDERD_QTYORDERED4,d.SORDERD_PRICE4,d.SORDERD_QTYWIP4,d.SORDERD_QTYSHIPPED4),
                    (d.SORDERD_SIZE5,d.SORDERD_QTYORDERED5,d.SORDERD_PRICE5,d.SORDERD_QTYWIP5,d.SORDERD_QTYSHIPPED5),
                    (d.SORDERD_SIZE6,d.SORDERD_QTYORDERED6,d.SORDERD_PRICE6,d.SORDERD_QTYWIP6,d.SORDERD_QTYSHIPPED6)) sz(Size,Qty,Price,Wip,Shp)
LEFT JOIN CUSTOMER c ON c.CUSTOMER_CODE = o.SORDER_CUSTOMER_CODE
LEFT JOIN AGENT a     ON a.AGENT_CODE = o.SORDER_AGENT_CODE1
LEFT JOIN OETYPES ot  ON ot.OETYPE_ID = o.SORDER_OETYPE_ID
LEFT JOIN orda oa     ON oa.c = o.SORDER_CODE
LEFT JOIN SHIPVIA sv  ON sv.SHIPVIA_CODE = o.SORDER_SHIPVIA_CODE1
WHERE o.SORDER_CODE IN ({sql_in_list(so_codes)}) AND sz.Qty > 0
ORDER BY o.SORDER_CODE, d.SORDERD_LINE;
""".strip()


def purchase_orders_query_scoped(so_codes):
    """Same SELECT as daily_refresh.py's purchase_orders_query(), scoped to POs
    linked to this hour's GDOT'd SOs (PORDER_SORDER_CODE IN (...)) instead of the
    date/still-open filter -- this is what pulls in 'any new orders that match to
    those POs' from the PO side (Josh, 2026-09-22): every PO tied to a just-GDOT'd
    SO gets refreshed here, and its own for_order feeds back into which SOs get
    pulled by orders_query_scoped() above.

    ship_days/exclude_weekends (PORDER_SHIPDAYS/PORDER_EXCLUDEWEEKENDS) added
    2026-09-22 -- mirrors daily_refresh.py's purchase_orders_query() fix the same
    day, the project's actual UPS transit-time answer (see data/sales/README.md's
    "Schema extension" section).

    The purchasing follow-up fields (entered_by through no_fu_needed) were added
    2026-10-30, mirroring daily_refresh.py's purchase_orders_query(). Without them,
    this merge would blank those fields on every PO it re-pulls."""
    return f"""
SELECT p.PORDER_CODE AS po,
       v.VENDOR_CODE AS vendor_code, v.VENDOR_NAME AS vendor,
       p.PORDER_ORDERDATE AS ordered_date,
       p.PORDER_FIRSTEXPECTEDRECEIVE AS expected_date,
       p.PORDER_LASTRECEIVE AS received_date,
       p.PORDER_SORDER_CODE AS for_order,
       p.PORDER_TOTALQTY AS qty_ordered,
       p.PORDER_TOTALQTYREC AS qty_received,
       CAST(p.PORDER_POTOTAL AS decimal(18,2)) AS po_total,
       p.PORDER_COMPLETE AS complete_flag,
       p.PORDER_SHORTED AS shorted,
       p.PORDER_SHIPNAME AS ship_name, p.PORDER_SHIPADDRESS1 AS ship_address1,
       p.PORDER_SHIPCITY AS ship_city, p.PORDER_SHIPSTATE AS ship_state, p.PORDER_SHIPZIP AS ship_zip,
       p.PORDER_SHIPVIA_CODE AS ship_via_code, sv.SHIPVIA_DESC AS ship_via_desc,
       p.PORDER_SHIPDAYS AS ship_days, p.PORDER_EXCLUDEWEEKENDS AS exclude_weekends,
       p.PORDER_WHSE_CODE AS whse_code,
       p.PORDER_TYPE AS po_type,
       p.PORDER_USERS_LOGIN AS entered_by, p.PORDER_DIVISION_CODE AS division_code,
       p.PORDER_USERDATE3 AS vendor_oc_date, p.PORDER_USERDATE4 AS in_hands_date,
       p.PORDER_USERDATE1 AS fu_1wk, p.PORDER_USERDATE2 AS fu_2day,
       CASE WHEN p.PORDER_USERBOOL1 = 1 THEN 'Yes' ELSE 'No' END AS no_fu_needed
FROM PORDER p
LEFT JOIN VENDOR v ON v.VENDOR_CODE = p.PORDER_VENDOR_CODE
LEFT JOIN SHIPVIA sv ON sv.SHIPVIA_CODE = p.PORDER_SHIPVIA_CODE
WHERE p.PORDER_SORDER_CODE IN ({sql_in_list(so_codes)})
ORDER BY p.PORDER_ORDERDATE;
""".strip()


def purchase_order_lines_query_scoped(so_codes):
    """Same SELECT as daily_refresh.py's purchase_order_lines_query(), scoped via a
    join back to PORDER filtered on PORDER_SORDER_CODE IN (...)."""
    return f"""
SELECT pd.PORDERD_ID AS po_line_id,
       pd.PORDERD_PORDER_CODE AS po,
       pd.PORDERD_RAWSKU_SKUNBR AS sku,
       pd.PORDERD_TOTALQTYORDERED AS qty_ordered,
       pd.PORDERD_TOTALQTYRECD AS qty_received,
       CAST(pd.PORDERD_TOTALPRICE AS decimal(18,2)) AS line_total_cost,
       CAST(pd.PORDERD_TOTALPRICE AS decimal(18,4))
           / NULLIF(pd.PORDERD_TOTALQTYORDERED, 0) AS unit_cost,
       pd.PORDERD_VENDORDESC AS description
FROM PORDERD pd
JOIN PORDER p ON p.PORDER_CODE = pd.PORDERD_PORDER_CODE
WHERE p.PORDER_SORDER_CODE IN ({sql_in_list(so_codes)})
ORDER BY pd.PORDERD_PORDER_CODE;
""".strip()


def purchase_order_line_sizes_query_scoped(so_codes):
    """Same SELECT as daily_refresh.py's purchase_order_line_sizes_query(), scoped
    to this hour's GDOT'd SOs -- see that function's docstring."""
    return f"""
SELECT pd.PORDERD_ID AS po_line_id,
       pd.PORDERD_PORDER_CODE AS po,
       pd.PORDERD_RAWSKU_SKUNBR AS sku,
       sz.size_seq, sz.size, sz.size_qty_ordered,
       CAST(sz.size_unit_price AS decimal(18,4)) AS size_unit_price
FROM PORDERD pd
JOIN PORDER p ON p.PORDER_CODE = pd.PORDERD_PORDER_CODE
CROSS APPLY (VALUES (1, pd.PORDERD_SIZE1, pd.PORDERD_QTYORDERED1, pd.PORDERD_PRICE1),
                    (2, pd.PORDERD_SIZE2, pd.PORDERD_QTYORDERED2, pd.PORDERD_PRICE2),
                    (3, pd.PORDERD_SIZE3, pd.PORDERD_QTYORDERED3, pd.PORDERD_PRICE3),
                    (4, pd.PORDERD_SIZE4, pd.PORDERD_QTYORDERED4, pd.PORDERD_PRICE4),
                    (5, pd.PORDERD_SIZE5, pd.PORDERD_QTYORDERED5, pd.PORDERD_PRICE5),
                    (6, pd.PORDERD_SIZE6, pd.PORDERD_QTYORDERED6, pd.PORDERD_PRICE6))
            sz(size_seq, size, size_qty_ordered, size_unit_price)
WHERE p.PORDER_SORDER_CODE IN ({sql_in_list(so_codes)})
  AND sz.size_qty_ordered <> 0
ORDER BY pd.PORDERD_PORDER_CODE, pd.PORDERD_ID, sz.size_seq;
""".strip()


def purchase_order_comments_query_scoped(so_codes):
    """Same SELECT as daily_refresh.py's purchase_order_comments_query(), scoped to
    this run's GDOT'd SOs -- see that function's docstring."""
    return f"""
SELECT c.PORDERC_PORDER_CODE AS po, c.PORDERC_LINE AS line,
       c.PORDERC_BEFOREAFTER AS before_after, c.PORDERC_TYPE AS type,
       c.PORDERC_TEXT AS text,
       d.PORDERD_ID AS po_line_id, d.PORDERD_RAWSKU_SKUNBR AS sku
FROM PORDERC c
JOIN PORDER p ON p.PORDER_CODE = c.PORDERC_PORDER_CODE
LEFT JOIN PORDERD d ON d.PORDERD_PORDER_CODE = c.PORDERC_PORDER_CODE AND d.PORDERD_LINE = c.PORDERC_LINE
WHERE p.PORDER_SORDER_CODE IN ({sql_in_list(so_codes)})
ORDER BY c.PORDERC_PORDER_CODE, c.PORDERC_LINE, c.PORDERC_BEFOREAFTER, c.PORDERC_TYPE;
""".strip()


def order_notes_query_scoped(so_codes):
    """Same SELECT as daily_refresh.py's order_notes_query(), scoped to this hour's
    GDOT'd SOs instead of a received_date window -- see that function's docstring
    for what SORDERC actually contains and why type 'I' only."""
    return f"""
SELECT sc.SORDERC_SORDER_CODE AS sales_order, sc.SORDERC_LINE AS seq,
       sc.SORDERC_TEXT AS note_text
FROM SORDERC sc
WHERE sc.SORDERC_TYPE = 'I' AND sc.SORDERC_SORDER_CODE IN ({sql_in_list(so_codes)})
ORDER BY sc.SORDERC_SORDER_CODE, sc.SORDERC_LINE;
""".strip()


def so_charge_lines_query_scoped(so_codes):
    """Same SELECT as daily_refresh.py's so_charge_lines_query() (2026-10-19), scoped to
    this hour's GDOT'd SOs: the SO's misc / service lines (PP-SETUP, PP-CPYCHG...)."""
    return f"""
SELECT d.SORDERD_SORDER_CODE AS sales_order, d.SORDERD_LINE AS line,
       d.SORDERD_ITEM_CODE AS item_code, d.SORDERD_DESC AS description,
       d.SORDERD_ITEMQTY AS qty, CAST(d.SORDERD_TOTALPRICE AS decimal(18,2)) AS amount,
       d.SORDERD_DIVISION_CODE AS division
FROM SORDERD d
WHERE d.SORDERD_FORMAT = 'M' AND d.SORDERD_SORDER_CODE IN ({sql_in_list(so_codes)})
ORDER BY d.SORDERD_SORDER_CODE, d.SORDERD_LINE;
""".strip()


def order_events_query(since_iso, id_floor=0):
    """Every order status event since the lookback cutoff -- same columns as daily_refresh.py's
    order_tracking_events_query(). (Was ART*-only until 2026-10-04; ART* is now filtered
    in Python so art-events-recent.parquet is unchanged.)"""
    return f"""
SELECT e.OTEVENT_ID AS event_id,
       e.OTEVENT_SORDER_CODE AS sales_order,
       e.OTEVENT_TIME AS event_time,
       e.OTEVENT_PRODACT_CODE AS code,
       p.PRODACT_DESC AS code_desc,
       e.OTEVENT_USERS_LOGIN AS user_login,
       e.OTEVENT_TEXT AS note,
       e.OTEVENT_INVNBR AS invoice_number
FROM ORDERTRACKINGEVENT e
LEFT JOIN PRODACT p ON p.PRODACT_CODE = e.OTEVENT_PRODACT_CODE
WHERE e.OTEVENT_ID > {int(id_floor)}
  AND (e.OTEVENT_SORDER_CODE LIKE 'S%' OR e.OTEVENT_SORDER_CODE LIKE 'A%')
  AND e.OTEVENT_TIME >= '{since_iso}'
ORDER BY e.OTEVENT_TIME;
""".strip()


def design_codes_query_scoped(so_codes):
    """Same SELECT as daily_refresh.py's design_codes_query(), scoped to the SOs that
    just reached ARTX/ARTRC -- artists often assign new decoration codes the same day."""
    return f"""
SELECT pd.SORDERPD_ID          AS decoration_line_id,
       pd.SORDERPD_SORDER_CODE AS ord,
       o.SORDER_RECDATE        AS received,
       o.SORDER_CUSTOMER_CODE  AS customer_number,
       c.CUSTOMER_NAME         AS customer_name,
       dt.DECOTYPE_DESCRIPTION AS deco_type,
       l.LOC_DESC              AS location,
       COALESCE(e.EMBRO_CODE, pr.PRINTS_CODE, dc.DECORATION_CODE) AS decoration_number,
       COALESCE(e.EMBRO_DESC, pr.PRINTS_DESC, dc.DECORATION_DESC) AS decoration_description,
       NULLIF(pd.SORDERPD_DECOCOUNT,0) AS stitches_or_screens
FROM SORDERPD pd
JOIN SORDER o          ON o.SORDER_CODE = pd.SORDERPD_SORDER_CODE
JOIN CUSTOMER c        ON c.CUSTOMER_CODE = o.SORDER_CUSTOMER_CODE
LEFT JOIN DECOTYPE dt  ON dt.DECOTYPE_CODE = pd.SORDERPD_DECOTYPE
LEFT JOIN LOC l        ON l.LOC_CODE = pd.SORDERPD_LOC_CODE
LEFT JOIN EMBRO e      ON pd.SORDERPD_DECOCLASS = 'E' AND e.EMBRO_ID  = pd.SORDERPD_DECOID
LEFT JOIN PRINTS pr    ON pd.SORDERPD_DECOCLASS = 'S' AND pr.PRINTS_ID = pd.SORDERPD_DECOID
LEFT JOIN DECORATION dc ON pd.SORDERPD_DECOCLASS = 'O' AND dc.DECORATION_ID = pd.SORDERPD_DECOID
WHERE o.SORDER_CODE IN ({sql_in_list(so_codes)})
ORDER BY pd.SORDERPD_SORDER_CODE, pd.SORDERPD_SEQUENCE;
""".strip()


def _roll(path, new_df, cutoff):
    old_df = pd.read_parquet(path) if path.exists() else new_df.iloc[0:0]
    combined = pd.concat([old_df, new_df], ignore_index=True).drop_duplicates("event_id", keep="last")
    combined = combined[pd.to_datetime(combined["event_time"]) >= cutoff]
    combined.to_parquet(path, index=False)
    return combined


def refresh_status_events(tag, bridge_dir, since_iso, run_id, fh, id_floor=0):
    """Pull every recent status event. Writes order-events-recent.parquet (all codes) and
    art-events-recent.parquet (ART_CODES only, unchanged contract), both rolling
    ART_RECENT_DAYS, then refreshes design codes for SOs that just became art-ready
    (ARTX/ARTRC) or just reached ORDW. Returns (art_ready, ordw)."""
    name = f"order_events_{run_id}"
    submit_query(tag, bridge_dir, name, order_events_query(since_iso, id_floor), fh)
    csv_path = wait_for_result(tag, bridge_dir, name, fh)
    try:
        new_df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        log("status events: none in the lookback window", fh)
        return [], []
    new_df = parse_mixed_dates(new_df, ["event_time"])
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=ART_RECENT_DAYS)
    all_kept = _roll(ORDER_EVENTS_PATH, new_df, cutoff)
    art_new = new_df[new_df["code"].isin(ART_CODES)]
    art_kept = _roll(ART_EVENTS_PATH, art_new, cutoff)
    ready = sorted(art_new.loc[art_new["code"].isin(ART_READY_CODES), "sales_order"].dropna().unique())
    ordw = sorted(new_df.loc[new_df["code"].isin(ORDW_CODES), "sales_order"].dropna().unique())
    log(f"{ORDER_EVENTS_PATH.name}: {len(new_df)} new event(s), {len(all_kept)} kept; "
        f"{ART_EVENTS_PATH.name}: {len(art_new)} new, {len(art_kept)} kept; "
        f"{len(ready)} SO(s) newly ARTX/ARTRC, {len(ordw)} newly ORDW", fh)
    dc_sos = sorted(set(ready) | set(ordw))
    if dc_sos:
        dc_name = f"art_design_codes_{run_id}"
        submit_query(tag, bridge_dir, dc_name, design_codes_query_scoped(dc_sos), fh)
        dc_csv = wait_for_result(tag, bridge_dir, dc_name, fh)
        merge_replace_by_group(ART_DESIGN_CODES_PATH, dc_csv, date_cols=["received"],
                               numeric_cols=["stitches_or_screens"], group_col="ord", fh=fh)
        dc = pd.read_parquet(ART_DESIGN_CODES_PATH)
        dc = dc[pd.to_datetime(dc["received"], errors="coerce") >= cutoff - pd.Timedelta(days=60)]
        dc.to_parquet(ART_DESIGN_CODES_PATH, index=False)
    return ready, ordw


def refresh_orders_for(tag, bridge_dir, so_codes, run_id, fh, label):
    """Re-pull order lines (SKU, style, color, quantities) for the given SOs into
    orders.parquet -- same query and merge the GDOT path uses."""
    name = f"{label}_orders_{run_id}"
    submit_query(tag, bridge_dir, name, orders_query_scoped(so_codes), fh)
    csv_path = wait_for_result(tag, bridge_dir, name, fh)
    merge_replace_by_group(
        DATA_DIR / "orders.parquet", csv_path,
        date_cols=["received_date", "ord_approved", "due_date", "latest_ship_date"],
        numeric_cols=["order_sub_total", "sku_ordered_amount", "quantity_ordered",
                      "size_qty_ordered", "size_ordered_amount", "size_qty_wip",
                      "size_qty_shipped"],
        group_col="sales_order",
        fh=fh,
    )


def acquire_lock(fh):
    """Skip this run if the previous one is still going (bridge latency can stretch a
    run past a 15-minute interval). A lock older than LOCK_STALE_MIN is treated as left
    behind by a crashed run."""
    if LOCK_PATH.exists():
        age_min = (time.time() - LOCK_PATH.stat().st_mtime) / 60
        if age_min < LOCK_STALE_MIN:
            log(f"previous run still in progress (lock {age_min:.0f} min old) -- skipping", fh)
            return False
        log(f"removing stale lock ({age_min:.0f} min old)", fh)
    LOCK_PATH.write_text(str(os.getpid()), encoding="utf-8")
    return True


def release_lock():
    try:
        LOCK_PATH.unlink()
    except FileNotFoundError:
        pass


def load_queue():
    if QUEUE_PATH.exists():
        return json.loads(QUEUE_PATH.read_text(encoding="utf-8"))
    return {}


def save_queue(queue):
    QUEUE_PATH.write_text(json.dumps(queue, indent=2, sort_keys=True), encoding="utf-8")


def upsert_queue(queue, so_to_pos, now_iso):
    """Add newly-seen SOs, refresh last_seen_utc + po list for ones already queued.
    Deliberately never resets `checked` back to False here -- if a new PO shows up on
    an SO that was already Team-Checked, that's a real (if rare) gap this v1 doesn't
    handle; flagged as a known limitation rather than silently guessed at."""
    for so_code, po_list in so_to_pos.items():
        entry = queue.get(so_code)
        if entry is None:
            queue[so_code] = {
                "pos": sorted(po_list),
                "first_seen_utc": now_iso,
                "last_seen_utc": now_iso,
                "checked": False,
                "checked_utc": None,
            }
        else:
            entry["pos"] = sorted(set(entry.get("pos", [])) | set(po_list))
            entry["last_seen_utc"] = now_iso
    return queue


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"gdot_refresh_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log"
    with open(log_path, "w", encoding="utf-8") as fh:
        locked = False
        try:
            local_hour = datetime.now().hour
            if not (BUSINESS_HOUR_START <= local_hour < BUSINESS_HOUR_END) \
                    and os.environ.get("REFRESH_IGNORE_HOURS") != "1":   # manual test runs only
                log(f"outside business hours ({local_hour}:00 local) -- exiting without polling", fh)
                return
            if not acquire_lock(fh):
                return
            locked = True

            tag, bridge_dir = get_tag_and_bridge_dir()
            log(f"running on {tag}, bridge dir {bridge_dir}", fh)

            # Local time, not UTC -- OTEVENT_TIME is stored/compared as local (Central)
            # time in SQL Server (confirmed live: GETDATE() returns local time), same
            # convention daily_refresh.py's own since_date uses. A UTC-aware `now` here
            # shifted the cutoff ~5h ahead of local time (Central is UTC-5 in September),
            # netting to roughly an hour in the future even after the 4h lookback -- the
            # query then matched nothing on every run today despite 10 real GDOT events
            # since 8:45am, all missed silently until caught 2026-09-23.
            #
            # Separate from now_utc below: this `now` is ONLY for the SQL cutoff, which
            # must match OTEVENT_TIME's local-time convention. The queue/poll bookkeeping
            # timestamps (first_seen_utc etc.) need to actually be UTC, since
            # reconcile.py's format_age() diffs them against datetime.now(timezone.utc) --
            # reusing one local `now` for both broke that the first time this ran for
            # real (2026-09-23), so don't collapse these back into a single variable.
            now = datetime.now()
            now_utc = datetime.now(timezone.utc)
            since_iso = (now - timedelta(hours=GDOT_LOOKBACK_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
            run_id = uuid.uuid4().hex[:8]

            # Art status events first, isolated: a failure here must not block the
            # GDOT/PO refresh this script was built for.
            id_floor = event_id_floor()
            log(f"event-id floor {id_floor} (seek, not a full scan)", fh)
            art_ready, ordw = [], []
            try:
                art_ready, ordw = refresh_status_events(tag, bridge_dir, since_iso, run_id, fh, id_floor)
            except Exception as e:
                log(f"status events: SKIPPED after error, rest of refresh continues: {e}", fh)
            # Order lines for orders that just reached ORDW (art prep / exact-reorder
            # pipeline needs garment style + color the same day). Isolated like above.
            if ordw:
                try:
                    refresh_orders_for(tag, bridge_dir, ordw, run_id, fh, "ordw")
                except Exception as e:
                    log(f"ORDW order lines: SKIPPED after error, rest of refresh continues: {e}", fh)

            gdot_name = f"gdot_events_{run_id}"
            submit_query(tag, bridge_dir, gdot_name, gdot_events_query(since_iso, id_floor), fh)
            gdot_csv = wait_for_result(tag, bridge_dir, gdot_name, fh)
            try:
                gdot_df = pd.read_csv(gdot_csv, dtype=str, encoding="utf-8-sig")
                so_codes = sorted(gdot_df["sales_order"].dropna().unique().tolist())
            except pd.errors.EmptyDataError:
                # The query bridge writes a header-less, fully empty CSV when a
                # SELECT DISTINCT matches zero rows -- the common case (most hours
                # have no new GDOT event), not an error. Without this, every
                # zero-result poll crashed here instead of reaching the
                # "nothing to refresh" path below.
                so_codes = []

            if not so_codes:
                log(f"no new GDOT events in the last {GDOT_LOOKBACK_HOURS:g}h -- nothing to refresh", fh)
                return

            log(f"{len(so_codes)} SO(s) with a new GDOT event: {', '.join(so_codes)}", fh)

            orders_name = f"gdot_orders_{run_id}"
            po_name = f"gdot_pos_{run_id}"
            pol_name = f"gdot_po_lines_{run_id}"
            pols_name = f"gdot_po_line_sizes_{run_id}"
            notes_name = f"gdot_order_notes_{run_id}"
            poc_name = f"gdot_po_comments_{run_id}"
            chg_name = f"gdot_so_charge_lines_{run_id}"
            submit_query(tag, bridge_dir, orders_name, orders_query_scoped(so_codes), fh)
            submit_query(tag, bridge_dir, po_name, purchase_orders_query_scoped(so_codes), fh)
            submit_query(tag, bridge_dir, pol_name, purchase_order_lines_query_scoped(so_codes), fh)
            submit_query(tag, bridge_dir, pols_name, purchase_order_line_sizes_query_scoped(so_codes), fh)
            submit_query(tag, bridge_dir, notes_name, order_notes_query_scoped(so_codes), fh)
            submit_query(tag, bridge_dir, poc_name, purchase_order_comments_query_scoped(so_codes), fh)
            submit_query(tag, bridge_dir, chg_name, so_charge_lines_query_scoped(so_codes), fh)

            orders_csv = wait_for_result(tag, bridge_dir, orders_name, fh)
            merge_replace_by_group(
                DATA_DIR / "orders.parquet", orders_csv,
                date_cols=["received_date", "ord_approved", "due_date", "latest_ship_date"],
                numeric_cols=["order_sub_total", "sku_ordered_amount", "quantity_ordered",
                              "size_qty_ordered", "size_ordered_amount", "size_qty_wip",
                      "size_qty_shipped"],
                group_col="sales_order",
                fh=fh,
            )

            po_csv = wait_for_result(tag, bridge_dir, po_name, fh)
            po_df = pd.read_csv(po_csv, dtype=str, encoding="utf-8-sig")
            merge_append_dedupe(
                DATA_DIR / "purchase-orders.parquet", po_csv,
                date_cols=["ordered_date", "expected_date", "received_date",
                           "vendor_oc_date", "in_hands_date", "fu_1wk", "fu_2day"],
                numeric_cols=["qty_ordered", "qty_received", "po_total", "ship_days"],
                key_cols=["po"],
                fh=fh,
            )

            pol_csv = wait_for_result(tag, bridge_dir, pol_name, fh)
            merge_replace_by_group(
                DATA_DIR / "purchase-order-lines.parquet", pol_csv,
                date_cols=[],
                numeric_cols=["qty_ordered", "qty_received", "line_total_cost", "unit_cost"],
                group_col="po",
                fh=fh,
            )

            # Size runs (2026-10-01) -- isolated, see daily_refresh.py.
            try:
                pols_csv = wait_for_result(tag, bridge_dir, pols_name, fh)
                merge_replace_by_group(
                    DATA_DIR / "purchase-order-line-sizes.parquet", pols_csv,
                    date_cols=[],
                    numeric_cols=["size_seq", "size_qty_ordered", "size_unit_price"],
                    group_col="po",
                    fh=fh,
                )
            except Exception as e:
                log(f"purchase-order-line-sizes: SKIPPED after error, rest of refresh continues: {e}", fh)

            # PO comments (2026-10-02) -- isolated the same way.
            try:
                poc_csv = wait_for_result(tag, bridge_dir, poc_name, fh)
                merge_replace_by_group(
                    DATA_DIR / "purchase-order-comments.parquet", poc_csv,
                    date_cols=[],
                    numeric_cols=["line"],
                    group_col="po",
                    fh=fh,
                )
            except Exception as e:
                log(f"purchase-order-comments: SKIPPED after error, rest of refresh continues: {e}", fh)

            notes_csv = wait_for_result(tag, bridge_dir, notes_name, fh)
            merge_append_dedupe(
                DATA_DIR / "order-notes.parquet", notes_csv,
                date_cols=[],
                numeric_cols=[],
                key_cols=["sales_order", "seq"],
                fh=fh,
            )

            # SO charge lines (2026-10-19) -- isolated, see daily_refresh.py.
            try:
                chg_csv = wait_for_result(tag, bridge_dir, chg_name, fh)
                merge_replace_by_group(
                    DATA_DIR / "so-charge-lines.parquet", chg_csv,
                    date_cols=[],
                    numeric_cols=["line", "qty", "amount"],
                    group_col="sales_order",
                    fh=fh,
                )
            except Exception as e:
                log(f"so-charge-lines: SKIPPED after error, rest of refresh continues: {e}", fh)

            # Union each PO's own for_order back into the queue's SO set (Josh,
            # 2026-09-22) -- normally identical to so_codes, but this catches any
            # PO whose for_order disagrees with the GDOT event's own SO code instead
            # of silently trusting the two always match.
            so_to_pos = {}
            for so_code in so_codes:
                so_to_pos.setdefault(so_code, set())
            for _, row in po_df.iterrows():
                for_order = row.get("for_order")
                po_num = row.get("po")
                if pd.notna(for_order) and pd.notna(po_num):
                    so_to_pos.setdefault(for_order, set()).add(po_num)

            queue = load_queue()
            queue = upsert_queue(queue, so_to_pos, now_utc.isoformat(timespec="seconds"))
            save_queue(queue)
            log(f"gdot_queue.json: {len(so_to_pos)} SO(s) upserted", fh)

            LAST_POLL_PATH.write_text(
                json.dumps({"last_poll_utc": now_utc.isoformat(timespec="seconds"),
                            "sos_refreshed": so_codes}, indent=2),
                encoding="utf-8",
            )

            log("done", fh)
        except Exception as e:
            log(f"FAILED: {e}", fh)
            raise
        finally:
            if locked:
                release_lock()


if __name__ == "__main__":
    main()
