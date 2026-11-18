"""
Unattended nightly refresh of data/sales/*.parquet from live Impress data.

No AI/Claude involvement at runtime -- deterministic script meant to be triggered by
Windows Task Scheduler (see docs/impress-refresh-queries.md and README for context).

Design choices, and why:
- Orders/design-codes are re-pulled on a fixed lookback window (LOOKBACK_DAYS), not an
  exact "since last successful run" pointer. A lookback window is self-healing: if a
  run fails or a day is skipped, the next run still catches everything once it's back
  up, whereas an exact pointer can silently miss data if a failed run left it stale.
  Overlap with existing rows is harmless -- everything is deduped on its natural key.
- order-tracking is re-pulled in FULL every run (cheap: ~30s, ~85k rows, ~3MB) because
  an order's status/invoice/ship fields can change long after its received date --
  a lookback window on received_date would miss those updates.
- design-library.parquet changes slowly and isn't order-scoped -- not refreshed by
  this script. Re-run manually/rarely if needed. customers.parquet was in this bucket
  until 2026-10-04, when it turned out its rep assignments (`Agent #1`) had drifted on
  613 accounts in 5 weeks -- now a nightly full replace, along with agents.parquet.
- sku.parquet WAS in that "changes slowly" bucket, but its on_hand/available/
  on_order fields (SKUSIZE_QUANTITY/QTYAVAILABLE/QTYONORDER) don't change slowly at
  all -- added to the nightly refresh 2026-09-18 (Josh's call, after finding the
  inventory columns existed but were ~2 weeks stale) as a full replace_full() each
  run, not append+dedupe -- the query already returns every current row (all SKUs
  referenced by an order, ever), so a full overwrite is simplest and always correct.
  Folded in the color_desc GCOLOR join (previously a separate one-time manual pull,
  see docs/color-codes.md) since it's free to include on every pull.
"""
import csv
import io
import os
import socket
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
LOOKBACK_DAYS = 60  # generous overlap; dedup handles it
# PO comments (PORDERC) are pulled back to this date, not just LOOKBACK_DAYS, so PO
# Team Check's product-color check has older POs to test against (2026-10-08).
PO_COMMENTS_FLOOR = "2026-01-01"

# Backfilled + verified live 2026-09-10 (73,809 POs / 153,237 lines, 2004-03-03 to
# present). See docs/impress-refresh-queries.md section 6 for the verified column
# semantics (PORDER_COMPLETE is ' '/'X', not Y/N; PORDERD_ID is the real surrogate key).
ENABLE_PURCHASE_ORDERS = True

# Backfilled + verified live 2026-09-15 (1,805,248 events, 2018-01-04 to present,
# ORDERTRACKINGEVENT filtered to SORDER_CODE LIKE 'S%'). See docs/impress-refresh-
# queries.md section 7 -- OTEVENT_ID is the real surrogate key. Unlike PORDER, events
# are append-only (a logged event doesn't change after the fact), so a lookback
# window alone is sufficient -- no "still open" style OR condition needed.
ENABLE_ORDER_TRACKING_EVENTS = True

# Backfilled + verified live 2026-09-15 (20,695 rows, 2018-01-05 to present).
# See docs/impress-refresh-queries.md section 8 -- SKUTRANS_ID is the real
# surrogate key, stored as a plain string (same reasoning as po_line_id/event_id
# above). Append-only like order-tracking-events, so a lookback window suffices.
ENABLE_MISPRINTS_DAMAGED_GOODS = True

# Added 2026-09-18 (Josh's call) -- sku.parquet's on_hand/available/on_order
# (SKUSIZE_QUANTITY/QTYAVAILABLE/QTYONORDER) are live inventory figures, not slow-
# changing catalog attributes, so they need the same nightly cadence as everything
# else. Full re-pull every run (no since_date, no append+dedupe) -- see
# docs/impress-refresh-queries.md section 1 and data/sales/README.md's "Refresh
# cadence". Also folds in the GCOLOR color_desc join that was previously a separate
# manual pull (docs/color-codes.md).
ENABLE_SKU_REFRESH = True

# Added 2026-09-23 (Josh's call) -- SORDERC internal order notes, for the PO Team
# Check reconciliation tool's vendor-match and net-cost checks. Real content is
# semi-structured (consistent labeled template most of the time -- "GARMENT NETS:",
# "PROVIDE NET PRICING FOR PRODUCT & SETUPS...") but with real variance even in a
# 20-order sample: a second header phrasing for the same concept ("IF QUOTED,
# PROVIDE PRICING AND CONTACT NAME:"), inconsistent number formats, and cases with
# no parseable price at all ("GARMENT NETS: DA" for house-brand SKUs, free-text
# exceptions). reconcile.py's parser is deterministic with a graceful "couldn't
# parse, read manually" fallback -- not 100% coverage by design, see
# docs/impress-refresh-queries.md section 10.
ENABLE_ORDER_NOTES = True

# Added 2026-10-12 (Josh's call) -- order-notes-all.parquet: EVERY SORDERC note, customer-
# facing (C) and internal (I), every order (S# and A#), full history, so freight approvals,
# rush terms and other order context can be answered locally instead of by live query
# (e.g. CIR S0084653's "Matt gave permission to charge the customer 3000.00" freight note).
# Kept separate from order-notes.parquet, whose internal-only shape the PO/art team checks
# and the hourly refresh depend on. Backfilled once by backfill_order_notes_all.py; nightly
# re-pulls notes on orders received in the last LOOKBACK_DAYS and append+dedupes on
# (sales_order, kind, seq), keeping the newest text. A note edited on an order older than
# LOOKBACK_DAYS keeps its last-pulled text until the next backfill.
ENABLE_ORDER_NOTES_ALL = True

# Added 2026-10-19 (Josh's call, Claire's S0088084 report) -- so-charge-lines.parquet:
# the SO's misc / service lines (SORDERD_FORMAT 'M': PP-SETUP, PP-CPYCHG, PP-RUSH, SHIP,
# DIGI...). orders.parquet only keeps lines with a size quantity, so these never reached
# PO Team Check, which compares every PO charge line to a charge billed on the SO.
ENABLE_SO_CHARGE_LINES = True

# Added 2026-10-04 (Josh's call) -- customers.parquet (account + address-level rep
# assignments) and the new agents.parquet (rep master) were frozen at a 2026-08-29
# one-time export. Both are now fully re-pulled every night (replace_full); see
# customers_query()/agents_query() and docs/impress-refresh-queries.md section 13.
ENABLE_CUSTOMERS_REFRESH = True
# Added 2026-10-20 (Josh's call) -- so-shipments.parquet (Impress SORDERS boxes with
# tracking numbers) and shipments.parquet (every UPS package: direction + sales order,
# built by scripts/ups_shipments.py from the UPS APIs, the billing archive, purchasing's
# Third Party workbooks and ShipStation). Needs the UPS_API Windows credential on the
# machine that runs this.
ENABLE_SHIPMENTS = True
# Added 2026-10-26 (Josh's call) -- credit-memos.parquet: every credit (C) and debit (D)
# memo with totals and its INVOICEC comment text. order-tracking's invoiced_amount is
# type-I only, so credits had no nightly source (only the frozen 2026-09-25 CSVs in docs/).
ENABLE_CREDIT_MEMOS = True
# Added 2026-10-30 (Traci's 10/29 request) -- open-order-lines.parquet and
# sku-warehouse-stock.parquet, for the automated LOG OPEN and Due Dates reports.
ENABLE_OPEN_ORDER_LINES = True

# Added 2026-10-30 (Traci's 10/30 request) -- sales-order-lines.parquet for the
# automated Daily Sales Journal: every line of this year's S and A orders, with the
# misc charge lines and per-line SKU cost (Josh's DSJ cost exception, CLAUDE.md).
ENABLE_SALES_ORDER_LINES = True

# Added 2026-11-02 (Josh, KPI dashboard) -- ar-aging-daily.parquet: one row per night with the company-wide Impress
# Invoice Aging totals by bucket (cookbook recipe R19, the same report Finance emails monthly). Totals only, no
# customer detail. Feeds the manager-meeting KPI page's past-due A/R panel (scripts/kpi/pull_kpi.py).
ENABLE_AR_AGING = True

# Per-machine local bridge folder, per the run-impress-queries skill.
BRIDGE_FOLDERS = {
    "XIA_WU": r"C:\Users\joshr\Dropbox\Claude\SQL database\_query_bridge",
    "CHI-TOWN": r"C:\Users\joshr\_query_bridge",
}

POLL_INTERVAL_SEC = 15
# 30 min. All 13 queries are submitted at once and the bridge runs them one at a time,
# so the first wait (orders) absorbs the whole queue. 600s failed on 2026-10-09: SQL
# Server pre-login timeouts held the queue until 02:08 and every result was back by
# 02:11, about a minute too late. Nobody waits on this run, and 30 min still finishes
# hours before the 7am intraday refresh starts.
POLL_TIMEOUT_SEC = 1800

# Shared with gdot_hourly_refresh.py so the two never merge into the same parquet at once.
INTRADAY_LOCK_PATH = LOG_DIR / "intraday_refresh.lock"


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


def orders_query(since_date):
    """PO Team Check reconciliation fields added 2026-09-22 (due_date, ship-to,
    ship-via, line_description, gcolor_code) -- see docs/impress-refresh-queries.md
    section 9 for the full verification writeup and ImagePointe-OS's
    po-team-check-automation project for why. All live-confirmed against the three
    worked-example orders before adding here:
    - due_date is SORDER_REQUEST, NOT SORDER_DUEDATE (confirmed empty on every order
      checked -- would have silently produced nothing for the in-hands buffer check).
    - ship_name/ship_address1/ship_city/ship_state/ship_zip (SORDER_SNAME/SADDRESS1/
      SCITY/SSTATE/SZIP) confirmed populated and exact-matching real paperwork on all
      three orders.
    - line_description (SORDERD_PURCHASEDESC) is inconsistently populated (blank on
      2 of 3 worked examples, no system rule found governing when) -- treat blanks as
      "unverifiable," not a mismatch; PORDERD_VENDORDESC on the PO side is the more
      reliable half of the description-match check, see purchase_order_lines_query().
    - gcolor_code (SORDERD_GCOLOR_CODE) confirmed populated, matches the SKU's
      embedded color suffix.

    latest_ship_date (SORDER_NOTBEFORE) added 2026-09-23, for IH/due-date orders
    only. Identified via the purchasing lead's own "Date Calculator Tool" (workbook
    formulas, confirmed against Impress by Josh directly 2026-09-23): Latest Ship
    Date = WORKDAY(due_date, -(transit_time + 1), holidays) -- the same formula
    already implemented for the ASI buffer check, just computed upstream (by
    Order Entry/Purchasing, using a manual UPS transit-time lookup) and stored here
    rather than recomputed from a transit-time field we don't have for the outbound
    leg. Confirmed live against 679 real garment/IHS orders (since 2026-08-01):
    populated on 322/368 (87.5%) of orders that have a due_date, and on only 3/679
    orders that don't (noise, not a real pattern) -- use for IH/due-date orders
    only, and expect it to sometimes be legitimately absent even when a due_date
    exists (added later in the flow than order entry, per Josh). Every value
    checked (14 across two samples) fell on a weekday, consistent with the
    WORKDAY()-based formula. NOT the same as "Ship By Date," a different field
    added later still (at art confirmation) -- deliberately not pulled here, per
    Josh 2026-09-23: unreliable at PO Team Check time, same reasoning as why
    same-day POs need the GDOT hourly refresh instead of the nightly snapshot.

    Open orders added 2026-10-30: every order that isn't complete is re-pulled
    too, however old. Before, an open order received before the lookback window
    kept whatever it had when it last fell inside it, so due_date was null on old
    open orders (it was only added 2026-09-22). Traci's Due Dates report dropped 5
    open orders on 10/30 because of it. About 780 open orders, so the extra rows
    are small; merge_replace_by_group() swaps each re-pulled order in whole.

    size_qty_wip (SORDERD_QTYWIP1-6, per size) added 2026-10-30 for Traci's LOG
    OPEN Picking notes: what's already pulled into WIP, so the notes list only the
    sizes still needed. gdot_hourly_refresh.py's orders_query_scoped() has it too.
    Older closed orders are null until they're re-pulled.

    size_qty_shipped (SORDERD_QTYSHIPPED1-6, per size) added 2026-11-03 for
    Hamilton's DA Iowa backorder scheduler (docs/refresh-requests/
    2026-11-03-open-qty-per-size.md). Still-needed qty per line =
    max(0, size_qty_ordered - size_qty_shipped - size_qty_wip). Verified live
    2026-11-03 against SKUSIZE_QTYUNSCHED (Impress's own "Qty Ordered") on all DA
    SKU-sizes: 95.1% exact; every remaining gap is open A-prefixed orders, which
    this query doesn't pull (S% only). Clamp per line -- WIP can exceed ordered
    (S0088029). ASI lines read 0 shipped even when invoiced (cookbook), so don't
    use this on ASI orders. gdot_hourly_refresh.py's orders_query_scoped() has it too.
    """
    return f"""
WITH orda AS (
    SELECT OTEVENT_SORDER_CODE AS c, MIN(OTEVENT_TIME) AS ord_approved
    FROM ORDERTRACKINGEVENT
    WHERE OTEVENT_PRODACT_CODE = 'ORDA'
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
WHERE o.SORDER_CODE LIKE 'S%' AND sz.Qty > 0
  AND (o.SORDER_RECDATE >= '{since_date}' OR ISNULL(o.SORDER_COMPLETE, '') <> 'X')
ORDER BY o.SORDER_CODE, d.SORDERD_LINE;
""".strip()


def order_tracking_query():
    """Every S order, in full, every night.

    Order-level Due Dates report fields added 2026-10-30 for Traci's
    scripts/due_dates/due_date_report.py, which uses these column names as-is.
    Checked against the 10/30 "due date report" export:
    - request_date = SORDER_REQUEST, the in-hands date (orders.parquet calls it
      due_date). Here for every order, not just the orders.parquet window.
    - bo = SORDER_USERSTRING5; apb = SORDER_USERBOOL1 as 'Yes'/'No', the way the
      Impress reports print it.
    - apb_ord_apr / apb_art_apr / apb_goods = SORDER_USERDATE1 / 2 / 3.
    - assigned_artist / art_due_date come from the design header (SORDERPH),
      not SORDER: SORDER_USERS_LOGINARTIST is blank. An order with several
      design lines gets the earliest art date and the first design line's artist
      that has one.
    """
    return """
WITH art AS (
    SELECT SORDERPH_SORDER_CODE AS c,
           MIN(SORDERPH_ARTDATE) AS art_due_date,
           MAX(CASE WHEN rn = 1 THEN NULLIF(SORDERPH_USERS_LOGINARTIST, '') END) AS assigned_artist
    FROM (SELECT SORDERPH_SORDER_CODE, SORDERPH_ARTDATE, SORDERPH_USERS_LOGINARTIST,
                 ROW_NUMBER() OVER (PARTITION BY SORDERPH_SORDER_CODE
                     ORDER BY CASE WHEN ISNULL(SORDERPH_USERS_LOGINARTIST, '') = '' THEN 1 ELSE 0 END,
                              SORDERPH_DESIGNLINE) AS rn
          FROM SORDERPH WHERE SORDERPH_SORDER_CODE LIKE 'S%') x
    GROUP BY SORDERPH_SORDER_CODE
),
ev AS (
    SELECT OTEVENT_SORDER_CODE AS c,
           MIN(CASE WHEN OTEVENT_PRODACT_CODE='ORDA' THEN OTEVENT_TIME END) AS orda,
           MIN(CASE WHEN OTEVENT_PRODACT_CODE='ARTA' THEN OTEVENT_TIME END) AS arta
    FROM ORDERTRACKINGEVENT
    WHERE OTEVENT_PRODACT_CODE IN ('ORDA','ARTA')
    GROUP BY OTEVENT_SORDER_CODE
),
inv AS (
    SELECT id.INVOICED_SORDER_CODE AS c,
           SUM(id.INVOICED_TOTALPRICE) AS invoiced,
           MAX(ih.INVOICE_INVDATE) AS invoice_date
    FROM INVOICED id
    JOIN INVOICE ih ON ih.INVOICE_INVNBR = id.INVOICED_INVOICE_INVNBR
    WHERE ih.INVOICE_TYPE = 'I'
    GROUP BY id.INVOICED_SORDER_CODE
),
ship AS (
    SELECT SORDERS_SORDER_CODE AS c, MAX(SORDERS_SHIPDATE) AS last_ship_date
    FROM SORDERS GROUP BY SORDERS_SORDER_CODE
)
SELECT o.SORDER_CODE AS sales_order, o.SORDER_RECDATE AS received_date,
       o.SORDER_SHORTED AS shorted,
       CAST(o.SORDER_PRODUCTTOTAL AS decimal(18,2)) AS order_sub_total,
       CAST(o.SORDER_TOTAL AS decimal(18,2)) AS order_total,
       COALESCE(i.invoiced, 0) AS invoiced_amount,
       o.SORDER_COMPLETE AS order_status_complete,
       o.SORDER_HOLD AS on_hold, o.SORDER_CANCEL AS cancel_date,
       e.orda AS order_approved, e.arta AS art_approved,
       s.last_ship_date, i.invoice_date,
       o.SORDER_CUSTOMER_CODE AS customer_number,
       o.SORDER_AGENT_CODE1 AS order_agent_code, oa.AGENT_NAME AS order_agent_name,
       c.CUSTOMER_AGENT_CODE1 AS account_agent_code, ca.AGENT_NAME AS account_agent_name,
       o.SORDER_REQUEST AS request_date,
       o.SORDER_USERSTRING5 AS bo,
       CASE WHEN o.SORDER_USERBOOL1 = 1 THEN 'Yes' ELSE 'No' END AS apb,
       o.SORDER_USERDATE1 AS apb_ord_apr, o.SORDER_USERDATE2 AS apb_art_apr,
       o.SORDER_USERDATE3 AS apb_goods,
       a.assigned_artist, a.art_due_date
FROM SORDER o
LEFT JOIN CUSTOMER c ON c.CUSTOMER_CODE = o.SORDER_CUSTOMER_CODE
LEFT JOIN AGENT oa ON oa.AGENT_CODE = o.SORDER_AGENT_CODE1
LEFT JOIN AGENT ca ON ca.AGENT_CODE = c.CUSTOMER_AGENT_CODE1
LEFT JOIN ev e   ON e.c = o.SORDER_CODE
LEFT JOIN inv i  ON i.c = o.SORDER_CODE
LEFT JOIN ship s ON s.c = o.SORDER_CODE
LEFT JOIN art a  ON a.c = o.SORDER_CODE
WHERE o.SORDER_CODE LIKE 'S%'
ORDER BY o.SORDER_CODE;
""".strip()


def open_order_lines_query():
    """Added 2026-10-30 for Traci's automated LOG OPEN report (her 10/29 request),
    also used by the Due Dates report. One row per line of every order that isn't
    complete, on hold or not. The LOG OPEN export leaves out orders on hold, so
    filter on_hold there.

    Fields mapped and checked against the 10/20 LOG OPEN report on 2026-10-29:
    - bo = SORDER_USERSTRING5, apb = SORDER_USERBOOL1. Both are on the order header,
      so they repeat on every line.
    - quantity_wip = SORDERD_TOTALQTYWIP.
    - line_status = SORDERD_STATUSCODE1-10, the exact per-line string Impress
      shows, not a rebuild from scans.
    - sku_rcpt = latest PORDERD_EXPECTEDRECEIVEDATE on the order's PO lines for
      that SKU. SORDERD_CUSTSTOCKRCPTDATE is blank on every line.
    Artist and art due date are per line here (SORDERD_SORDERPH_ID -> SORDERPH).
    Full replace every night: only open orders, so it stays small.

    purchaser and preferred_vendor added 2026-10-30 for Traci's repo Purchasing
    Daily Report. Checked against that morning's "pur daily" export:
    - purchaser = user of the order's GDOS scan (latest one), the same on every
      line. It matched 588 of 590 orders, blanks included, counting scans up to the
      export time. It is NOT the PO's Entered By, and SORDERD_USERS_LOGIN is blank.
    - preferred_vendor = the line's SORDERD_PREFERREDVENDOR (a vendor code) as a
      name, else the SKU's preferred vendor (SKU_VENDOR_CODEPREF, as in sku.parquet).
      It matched 573 of 573 orders (the export cuts names to 16 characters).

    da_10 / da_15 added 2026-11-02 for Traci's repo production schedule: the DA 10 / DA 15
    order flags are SORDER_USERBOOL2 / SORDER_USERBOOL3 (APB is USERBOOL1), Yes/No like apb.
    """
    status = "+".join(f"ISNULL(d.SORDERD_STATUSCODE{i}, ' ')" for i in range(1, 11))
    return f"""
WITH open_so AS (
    SELECT SORDER_CODE FROM SORDER
    WHERE SORDER_CODE LIKE 'S%' AND ISNULL(SORDER_COMPLETE, '') <> 'X'
),
orda AS (
    SELECT OTEVENT_SORDER_CODE AS c, MIN(OTEVENT_TIME) AS ord_approved
    FROM ORDERTRACKINGEVENT
    WHERE OTEVENT_PRODACT_CODE = 'ORDA'
      AND OTEVENT_SORDER_CODE IN (SELECT SORDER_CODE FROM open_so)
    GROUP BY OTEVENT_SORDER_CODE
),
rcpt AS (
    SELECT p.PORDER_SORDER_CODE AS c, pd.PORDERD_RAWSKU_SKUNBR AS sku,
           MAX(pd.PORDERD_EXPECTEDRECEIVEDATE) AS sku_rcpt
    FROM PORDERD pd
    JOIN PORDER p ON p.PORDER_CODE = pd.PORDERD_PORDER_CODE
    WHERE p.PORDER_SORDER_CODE IN (SELECT SORDER_CODE FROM open_so)
    GROUP BY p.PORDER_SORDER_CODE, pd.PORDERD_RAWSKU_SKUNBR
),
gdos AS (
    SELECT c, user_login FROM (
        SELECT OTEVENT_SORDER_CODE AS c, OTEVENT_USERS_LOGIN AS user_login,
               ROW_NUMBER() OVER (PARTITION BY OTEVENT_SORDER_CODE
                                  ORDER BY OTEVENT_TIME DESC, OTEVENT_ID DESC) AS rn
        FROM ORDERTRACKINGEVENT
        WHERE OTEVENT_PRODACT_CODE = 'GDOS'
          AND OTEVENT_SORDER_CODE IN (SELECT SORDER_CODE FROM open_so)
    ) x WHERE rn = 1
),
skuv AS (
    SELECT SKU_SKUNBR AS sku, MIN(SKU_VENDOR_CODEPREF) AS vendor_code
    FROM SKU GROUP BY SKU_SKUNBR
)
SELECT o.SORDER_CODE AS sales_order, d.SORDERD_LINE AS line, d.SORDERD_TYPE AS line_type,
       d.SORDERD_RAWSKU_SKUNBR AS sku, d.SORDERD_DESC AS sku_description,
       o.SORDER_USERSTRING5 AS bo,
       CASE WHEN o.SORDER_USERBOOL1 = 1 THEN 'Yes' ELSE 'No' END AS apb,
       CASE WHEN o.SORDER_USERBOOL2 = 1 THEN 'Yes' ELSE 'No' END AS da_10,
       CASE WHEN o.SORDER_USERBOOL3 = 1 THEN 'Yes' ELSE 'No' END AS da_15,
       o.SORDER_AGENT_CODE1 AS agent_1, c.CUSTOMER_NAME AS customer_name,
       o.SORDER_RECDATE AS received_date, o.SORDER_REQUEST AS request_date,
       o.SORDER_CANCEL AS ship_by, o.SORDER_NOTBEFORE AS latest_ship_date,
       oa.ord_approved, ot.OETYPE_DESC AS sales_order_type, o.SORDER_HOLD AS on_hold,
       {status} AS line_status,
       d.SORDERD_TOTALQTYORDERED AS quantity_ordered,
       d.SORDERD_TOTALQTYWIP AS quantity_wip,
       r.sku_rcpt,
       NULLIF(ph.SORDERPH_USERS_LOGINARTIST, '') AS assigned_artist,
       ph.SORDERPH_ARTDATE AS art_due_date,
       o.SORDER_USERDATE1 AS apb_ord_apr, o.SORDER_USERDATE2 AS apb_art_apr,
       o.SORDER_USERDATE3 AS apb_goods,
       g.user_login AS purchaser,
       COALESCE(NULLIF(pv.VENDOR_NAME, ''), sv.VENDOR_NAME) AS preferred_vendor
FROM SORDER o
JOIN open_so os      ON os.SORDER_CODE = o.SORDER_CODE
JOIN SORDERD d       ON d.SORDERD_SORDER_CODE = o.SORDER_CODE
LEFT JOIN SORDERPH ph ON ph.SORDERPH_ID = d.SORDERD_SORDERPH_ID
LEFT JOIN CUSTOMER c ON c.CUSTOMER_CODE = o.SORDER_CUSTOMER_CODE
LEFT JOIN OETYPES ot ON ot.OETYPE_ID = o.SORDER_OETYPE_ID
LEFT JOIN orda oa    ON oa.c = o.SORDER_CODE
LEFT JOIN rcpt r     ON r.c = o.SORDER_CODE AND r.sku = d.SORDERD_RAWSKU_SKUNBR
LEFT JOIN gdos g     ON g.c = o.SORDER_CODE
LEFT JOIN VENDOR pv  ON pv.VENDOR_CODE = d.SORDERD_PREFERREDVENDOR
LEFT JOIN skuv k     ON k.sku = d.SORDERD_RAWSKU_SKUNBR
LEFT JOIN VENDOR sv  ON sv.VENDOR_CODE = k.vendor_code
ORDER BY o.SORDER_CODE, d.SORDERD_LINE;
""".strip()


def sales_order_lines_query(year_start):
    """Added 2026-10-30 for Traci's automated Daily Sales Journal (DSJ). It replaces
    the Impress "Daily Sales Journal" memorized-report export, which covers orders
    received Jan 1 - Dec 31 of the current year. Same range here: one row per line
    (SKU lines and misc charge lines) of every S and A order received since
    year_start. The order-level columns repeat on every line. Full replace nightly
    (about 19,000 rows in late 2026).

    Checked against the 10/30 export (DSJ Report 10.30.26.csv, 2,181 orders):
    - sku_ordered_amount = SORDERD_TOTALPRICE on every line; misc lines included,
      as on the export. 2,173 orders.
    - misc_charge = TOTALPRICE on misc lines (SORDERD_FORMAT 'M'), item_code =
      SORDERD_ITEM_CODE. 2,179 orders.
    - sku_cost = SORDERD_STDTOTALCOST, the line's total standard cost. It's 0 on misc
      lines. 2,169 orders. SORDERD_TOTALCOST is close but differs on edited lines
      (e.g. 618.00 vs the export's 301.86); STDTOTALCOST is the one that matches.
    - order_sub_total = SORDER_PRODUCTTOTAL + SORDER_MISCTOTAL (2,171).
    - quantity_ordered = the order's total SKU qty (2,177).
    - ordw_first / ordw_last: first and last ORDW (order confirmation sent) scan.
      The export's Confirmation Date matched the first on 641 of 655 orders and the
      last on 631. Re-confirmed orders go either way.
    Every miss traced to an edit or scan after the 8 a.m. export.
    The order's 10-character status isn't stored on SORDER (CUSTOMSTATUS1-16 are
    Yes/No flags). Replaying the order's scans reproduces the export's Order Status
    on 754 of 758 S orders; line_status here is Impress's per-line string.
    Cost is in scope here only for the DSJ margin flags (see CLAUDE.md).
    """
    status = "+".join(f"ISNULL(d.SORDERD_STATUSCODE{i}, ' ')" for i in range(1, 11))
    return f"""
WITH so AS (
    SELECT SORDER_CODE FROM SORDER
    WHERE SORDER_RECDATE >= '{year_start}' AND (SORDER_CODE LIKE 'S%' OR SORDER_CODE LIKE 'A%')
),
ordw AS (
    SELECT OTEVENT_SORDER_CODE AS c, MIN(OTEVENT_TIME) AS ordw_first, MAX(OTEVENT_TIME) AS ordw_last
    FROM ORDERTRACKINGEVENT
    WHERE OTEVENT_PRODACT_CODE = 'ORDW' AND OTEVENT_SORDER_CODE IN (SELECT SORDER_CODE FROM so)
    GROUP BY OTEVENT_SORDER_CODE
)
SELECT o.SORDER_CODE AS sales_order, o.SORDER_RECDATE AS received_date,
       ot.OETYPE_DESC AS sales_order_type,
       o.SORDER_AGENT_CODE1 AS agent_1, a.AGENT_NAME AS agent_name_1,
       o.SORDER_CUSTOMER_CODE AS customer_number, c.CUSTOMER_NAME AS customer_name,
       CAST(ISNULL(o.SORDER_PRODUCTTOTAL, 0) + ISNULL(o.SORDER_MISCTOTAL, 0) AS decimal(18,2)) AS order_sub_total,
       SUM(d.SORDERD_TOTALQTYORDERED) OVER (PARTITION BY o.SORDER_CODE) AS quantity_ordered,
       o.SORDER_COMPLETE AS order_complete, o.SORDER_HOLD AS on_hold,
       w.ordw_first, w.ordw_last,
       d.SORDERD_LINE AS line, d.SORDERD_FORMAT AS line_format, d.SORDERD_TYPE AS line_type,
       NULLIF(d.SORDERD_ITEM_CODE, '') AS item_code, d.SORDERD_RAWSKU_SKUNBR AS sku,
       d.SORDERD_TOTALQTYORDERED AS sku_qty_ordered,
       CAST(d.SORDERD_TOTALPRICE AS decimal(18,2)) AS sku_ordered_amount,
       CAST(CASE WHEN d.SORDERD_FORMAT = 'M' THEN d.SORDERD_TOTALPRICE ELSE 0 END AS decimal(18,2)) AS misc_charge,
       CAST(d.SORDERD_STDTOTALCOST AS decimal(18,2)) AS sku_cost,
       {status} AS line_status
FROM SORDER o
JOIN so ON so.SORDER_CODE = o.SORDER_CODE
JOIN SORDERD d ON d.SORDERD_SORDER_CODE = o.SORDER_CODE
LEFT JOIN CUSTOMER c ON c.CUSTOMER_CODE = o.SORDER_CUSTOMER_CODE
LEFT JOIN AGENT a    ON a.AGENT_CODE = o.SORDER_AGENT_CODE1
LEFT JOIN OETYPES ot ON ot.OETYPE_ID = o.SORDER_OETYPE_ID
LEFT JOIN ordw w     ON w.c = o.SORDER_CODE
ORDER BY o.SORDER_CODE, d.SORDERD_LINE;
""".strip()


def sku_warehouse_stock_query():
    """Added 2026-10-30 for Traci's LOG OPEN Picking tab: is the stock at IP or DA?
    Available by SKU, size and warehouse = SUM(SKUWHSE_QUANTITY) on type 'A' rows
    (SKUWHSE is a cost-layer table, several rows per size and warehouse). Matched
    SKUSIZE_QTYAVAILABLE on all 1,868 sizes with stock on 2026-10-29.

    Every SKU with stock anywhere, including the U- unfinished SKUs. That's why it
    isn't folded into sku_query(): that one only covers SKUs that have been on a
    sales order, which misses all but 5 of the U- SKUs. Warehouse M = Main (IP),
    D = Dignity Apparel (DA), HOLD = DA defects, SAMPLE, SUB = at vendor, plus the
    customer stock warehouses. Type 'W' (committed to orders) is left out.
    """
    return """
SELECT s.SKUSIZE_SKU_SKUNBR AS sku, s.SKUSIZE_SKU_DESIGN_CODE AS design_code,
       s.SKUSIZE_SIZE AS size, s.SKUSIZE_SIZESEQ AS size_seq,
       w.SKUWHSE_WHSE_CODE AS warehouse, wh.WHSE_DESC AS warehouse_desc,
       SUM(w.SKUWHSE_QUANTITY) AS available,
       k.SKU_MANUALDESC AS descr, k.SKU_STATUS AS status_code
FROM SKUWHSE w
JOIN SKUSIZE s   ON s.SKUSIZE_ID = w.SKUWHSE_SKUSIZE_ID
LEFT JOIN SKU k  ON k.SKU_SKUNBR = s.SKUSIZE_SKU_SKUNBR
                AND k.SKU_DESIGN_CODE = s.SKUSIZE_SKU_DESIGN_CODE
LEFT JOIN WHSE wh ON wh.WHSE_CODE = w.SKUWHSE_WHSE_CODE
WHERE w.SKUWHSE_TYPE = 'A'
GROUP BY s.SKUSIZE_SKU_SKUNBR, s.SKUSIZE_SKU_DESIGN_CODE, s.SKUSIZE_SIZE, s.SKUSIZE_SIZESEQ,
         w.SKUWHSE_WHSE_CODE, wh.WHSE_DESC, k.SKU_MANUALDESC, k.SKU_STATUS
HAVING SUM(w.SKUWHSE_QUANTITY) <> 0
ORDER BY s.SKUSIZE_SKU_SKUNBR, s.SKUSIZE_SIZESEQ, w.SKUWHSE_WHSE_CODE;
""".strip()


def ar_aging_totals_query():
    """Added 2026-11-02 for the KPI dashboard's past-due A/R panel: company-wide Impress Invoice Aging totals by
    bucket, one row. Same rules as the impress-database-cookbook's R19 (validated against the Impress Invoice Aging
    Summary Report): invoices dated before today, prepayments (type P) never aged, aged by days past the due date,
    customers with a zero net balance left out. Totals only -- no customer detail leaves Impress."""
    return """
WITH base AS (
    SELECT i.INVOICE_CUSTOMER_CODE AS cust, i.INVOICE_TYPE AS typ, i.INVOICE_BALANCE AS bal,
           DATEDIFF(day, i.INVOICE_DUEDATE, CAST(GETDATE() AS date)) AS dpd
    FROM INVOICE i
    WHERE i.INVOICE_BALANCE <> 0
      AND i.INVOICE_INVDATE < CAST(GETDATE() AS date)
),
agg AS (
    SELECT cust,
           SUM(CASE WHEN typ='P' OR dpd <= 0                THEN bal ELSE 0 END) AS b_current,
           SUM(CASE WHEN typ<>'P' AND dpd BETWEEN  1 AND 30 THEN bal ELSE 0 END) AS b_1_30,
           SUM(CASE WHEN typ<>'P' AND dpd BETWEEN 31 AND 60 THEN bal ELSE 0 END) AS b_31_60,
           SUM(CASE WHEN typ<>'P' AND dpd BETWEEN 61 AND 90 THEN bal ELSE 0 END) AS b_61_90,
           SUM(CASE WHEN typ<>'P' AND dpd > 90              THEN bal ELSE 0 END) AS b_over90,
           SUM(bal) AS total_bal
    FROM base GROUP BY cust
)
SELECT CONVERT(varchar(19), GETDATE(), 120) AS as_of,
       COUNT(*) AS customers,
       CAST(SUM(b_current) AS decimal(18,2)) AS bal_current,
       CAST(SUM(b_1_30) AS decimal(18,2)) AS bal_1_30,
       CAST(SUM(b_31_60) AS decimal(18,2)) AS bal_31_60,
       CAST(SUM(b_61_90) AS decimal(18,2)) AS bal_61_90,
       CAST(SUM(b_over90) AS decimal(18,2)) AS bal_over_90,
       CAST(SUM(total_bal) AS decimal(18,2)) AS total_balance
FROM agg
WHERE total_bal <> 0
""".strip()


def append_ar_aging(path, csv_path, fh):
    """ar-aging-daily.parquet: add tonight's one-row aging totals, one row per day (a rerun the same day replaces it)."""
    new = pd.read_csv(csv_path, encoding="utf-8-sig")
    if new.empty:
        raise RuntimeError("empty A/R aging result")
    new["as_of"] = pd.to_datetime(new["as_of"])
    for c in ["customers", "bal_current", "bal_1_30", "bal_31_60", "bal_61_90", "bal_over_90", "total_balance"]:
        new[c] = pd.to_numeric(new[c], errors="coerce")
    old = pd.read_parquet(path) if path.exists() else new.iloc[0:0]
    old = old[old["as_of"].dt.date != new["as_of"].iloc[0].date()]
    out = pd.concat([old, new], ignore_index=True).sort_values("as_of")
    out.to_parquet(path, index=False)
    log(f"{path.name}: {len(out)} nights, latest {new['as_of'].iloc[0]} total {new['total_balance'].iloc[0]:,.2f}", fh)


def still_open_pos():
    """PO codes purchase-orders.parquet still has as open (complete_flag not X),
    for purchase_orders_query()'s still_open re-pull."""
    path = DATA_DIR / "purchase-orders.parquet"
    if not path.exists():
        return []
    po = pd.read_parquet(path, columns=["po", "complete_flag"])
    is_open = po["complete_flag"].fillna("").astype(str).str.strip() != "X"
    return sorted(po.loc[is_open, "po"].dropna().astype(str).unique())


def drop_deleted_pos(still_open, po_csv, fh):
    """still_open POs are re-pulled by code, so one Impress didn't return has been
    deleted there. Drop it from purchase-orders.parquet, or it shows as open
    forever. On 2026-10-30 there were 6, e.g. P0072096 and P0072128. Skips an empty
    result, and skips more than 50 gone at once as more likely a bad pull."""
    if not still_open:
        return
    try:
        got = set(pd.read_csv(po_csv, dtype=str, usecols=["po"], encoding="utf-8-sig")["po"].dropna())
    except (pd.errors.EmptyDataError, ValueError):
        return
    gone = set(still_open) - got
    if not got or not gone:
        return
    if len(gone) > 50:
        log(f"purchase-orders.parquet: {len(gone)} still-open POs not returned; not dropping (check the pull)", fh)
        return
    path = DATA_DIR / "purchase-orders.parquet"
    df = pd.read_parquet(path)
    df[~df["po"].isin(gone)].to_parquet(path, index=False)
    log(f"purchase-orders.parquet: dropped {len(gone)} PO(s) no longer in Impress: {', '.join(sorted(gone))}", fh)


def purchase_orders_query(since_date, still_open=()):
    """Verified live 2026-09-10 against Impress (backfill: 73,809 rows, 2004-03-03 to
    2026-09-10, see docs/impress-refresh-queries.md section 6). `PORDER_COMPLETE` is
    ' ' (open, 450 of 73,809 rows) / 'X' (complete) -- NOT Y/N as originally assumed
    when this was drafted.

    Filters to "ordered recently OR still open" rather than a lookback window alone,
    so a PO's receive status can update long after its order date without requiring
    a full-table re-pull (PORDER full-table pulls have stalled the query bridge
    before -- see docs/impress-refresh-queries.md). "Still open" is
    qty_received < qty_ordered OR complete_flag <> 'X' (checked against the live
    backfill: the qty condition alone misses 2 of 450 open POs where qty_received
    already >= qty_ordered but complete_flag is still blank -- both conditions
    needed).

    PO Team Check reconciliation fields added 2026-09-22 (ship-to, ship-via) -- see
    docs/impress-refresh-queries.md section 9. Uses PORDER_SHIPNAME/SHIPADDRESS1/etc
    ("SHIP*"), NOT the separate PORDER_SNAME/SADDRESS1 block ("S*") -- live-verified
    both against all three worked-example POs: SHIP* returns Image Pointe's own
    warehouse address (correct for garment/IHS orders' leg-1 vendor->warehouse
    destination), while S* came back blank on all three. Neither field holds an
    independent ASI drop-ship destination -- whatever generates the printed PO must
    pull that from the linked SORDER dynamically. So this column is only meaningful
    for garment/IHS orders (compare to a fixed known-warehouse value); for ASI orders
    there's nothing here to reconcile against the SO's ship-to.

    ship_days/exclude_weekends added 2026-09-22, wired into the recurring query
    2026-09-22 -- these columns existed in purchase-orders.parquet from a one-off
    backfill before this fix, so POs older than that backfill (and any PO ordered/
    updated between then and this fix landing) will show nulls here until they age
    back into this query's "ordered recently OR still open" window. PORDER_SHIPDAYS/
    PORDER_EXCLUDEWEEKENDS is the actual UPS transit-time answer for this project --
    no UPS zone map exists or is needed; a purchaser looks up transit days on UPS's
    site per shipment and types the result onto the PO itself (see data/sales/
    README.md's "Schema extension" section for the live-verification detail).

    whse_code (PORDER_WHSE_CODE, added 2026-10-08): the PO's receiving warehouse
    ('M' = Image Pointe IHS, 'SUB' = ASI drop-ship on Union Wear POs, per
    ImagePointe-OS union-wear-shipping-pattern-investigation.md). Pulled so PO Team
    Check can tell a garment PO set up as a drop shipment (S0087850 / P0072047) once
    the codes are confirmed against PO Entry's Drop Shipment / Blank Stock Order
    setting. Not scored yet.

    po_type (PORDER_TYPE, added 2026-10-09): PO Entry's order-type dropdown. 'D' =
    Drop Shipment, blank = Blank Stock Order (P0072047 vs P0072031, live-checked).
    Since 2026-08-01: 691 of 698 ASI POs are 'D'; 18 of ~540 decorated-garment POs are
    'D', some legitimately (dye-house / contractor routing). Scored as a Question.

    Purchasing follow-up fields added 2026-10-30 for Traci's repo Purchasing Daily
    Report. All seven matched that morning's "pur po daily" export on 437 of 437 POs.
    The only differences were 3 POs edited after the export.
    - entered_by = PORDER_USERS_LOGIN (the PO FU tabs split on it)
    - division_code = PORDER_DIVISION_CODE (IHS / DAP / ASI)
    - vendor_oc_date = USERDATE3, in_hands_date = USERDATE4
    - fu_1wk = USERDATE1, fu_2day = USERDATE2
    - no_fu_needed = USERBOOL1 as Yes/No
    gdot_hourly_refresh.py's purchase_orders_query_scoped() pulls them too, so its
    merges don't blank them.

    still_open (added the same day): POs the parquet still has as open get re-pulled
    by code. Before this, a PO that closed after aging out of the lookback was never
    pulled again, so it stayed "open" forever. On 2026-10-30, 39 POs were open in the
    parquet and closed in Impress, 34 of them from before September."""
    reopen = ""
    if still_open:
        codes = ",".join("'" + str(c).replace("'", "") + "'" for c in still_open)
        reopen = f"\n   OR p.PORDER_CODE IN ({codes})"
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
WHERE p.PORDER_ORDERDATE >= '{since_date}'
   OR p.PORDER_TOTALQTYREC < p.PORDER_TOTALQTY
   OR p.PORDER_COMPLETE <> 'X'{reopen}
ORDER BY p.PORDER_ORDERDATE;
""".strip()


def purchase_order_lines_query(since_date):
    """Verified live 2026-09-10 against Impress (backfill: 153,237 rows, see
    docs/impress-refresh-queries.md section 6). `PORDERD_ID` is a real bigint
    surrogate key (confirmed via INFORMATION_SCHEMA) -- used directly as the dedup
    key, no composite-key guessing needed (unlike the SORDERPD_ID situation this
    was originally worried about mirroring). `unit_cost` is derived (total/qty),
    not a native Impress column -- PORDERD has no direct per-unit-cost field.

    IMPORTANT: po_line_id must stay a plain string in purchase-order-lines.parquet,
    matching design-codes.parquet's decoration_line_id convention (this repo's "all
    fields are typed as strings except date/numeric columns" rule) -- NOT
    numeric_cols, NOT cast to Int64. An initial build that cast it to Int64 caused
    every nightly merge_append_dedupe() to crash silently at the to_parquet() step
    (mixed Int64/str dtype after concat with the CSV-read new rows) from 2026-09-11
    through 2026-09-15 with nobody noticing until the next feature's backfill
    surfaced it -- fixed 2026-09-15 by recasting to str. Same rule applies to
    order_tracking_events_query()'s event_id below.

    `description` (PORDERD_VENDORDESC) added 2026-09-22 for the PO Team Check
    description-match check -- see docs/impress-refresh-queries.md section 9. This is
    the reliable half of that check: confirmed populated on every PO line checked
    (unlike orders.parquet's line_description/SORDERD_PURCHASEDESC, which is blank
    more often than not) -- Impress falls back to something (likely the catalog
    description) when nobody overrides it on the SO side. Ranges from a full
    catalog-matching description down to a generic category label ("KOOZIES",
    "Setup Charges") -- comparison logic needs to tolerate that."""
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
WHERE p.PORDER_ORDERDATE >= '{since_date}'
   OR p.PORDER_TOTALQTYREC < p.PORDER_TOTALQTY
   OR p.PORDER_COMPLETE <> 'X'
ORDER BY pd.PORDERD_PORDER_CODE;
""".strip()


def purchase_order_line_sizes_query(since_date):
    """PO size runs, one row per PO line x size with a non-zero qty -- added
    2026-10-01 for po-so-reconcile (size-level qty check, base-size net cost).
    PORDERD carries the same inline size arrays as SORDERD (SIZE1-6 /
    QTYORDERED1-6 / PRICE1-6, PRICE = per-unit cost for that size), confirmed by
    bridge query 2026-10-01: e.g. P0071927 MD/LG/XL @ 6.89, 2XL @ 8.81, summing
    exactly to PORDERD_TOTALPRICE. Setup/charge lines have qty 0 in every slot and
    so produce no rows here (their totals stay in purchase-order-lines). Written
    to its own file, purchase-order-line-sizes.parquet, and run in its own
    try/except in main() so a problem here can't block the rest of the refresh."""
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
WHERE (p.PORDER_ORDERDATE >= '{since_date}'
   OR p.PORDER_TOTALQTYREC < p.PORDER_TOTALQTY
   OR p.PORDER_COMPLETE <> 'X')
  AND sz.size_qty_ordered <> 0
ORDER BY pd.PORDERD_PORDER_CODE, pd.PORDERD_ID, sz.size_seq;
""".strip()


def purchase_order_comments_query(since_date):
    """PO comments (PORDERC) -- added 2026-10-02 for po-so-reconcile's ASI art-proof
    check. One row per PO comment block. line = PORDERD_LINE it belongs to (0 = PO
    header); before_after 'B' header / 'A' after the line; type 'C' = printed on the
    vendor PO (PRODUCT COLOR / PRINT / ART / reorder / art-source instructions, one
    per decorated line), 'I' = the SO's internal notes copied in. Confirmed on 40 ASI
    POs 2026-10-02 (ImagePointe-OS asi-proof-vs-po-test-2026-10-02.md). po_line_id /
    sku tie a comment to its PORDERD line (purchase-order-lines.parquet has no line
    number); blank for line 0. Same PO window as purchase_order_lines_query()."""
    return f"""
SELECT c.PORDERC_PORDER_CODE AS po, c.PORDERC_LINE AS line,
       c.PORDERC_BEFOREAFTER AS before_after, c.PORDERC_TYPE AS type,
       c.PORDERC_TEXT AS text,
       d.PORDERD_ID AS po_line_id, d.PORDERD_RAWSKU_SKUNBR AS sku
FROM PORDERC c
JOIN PORDER p ON p.PORDER_CODE = c.PORDERC_PORDER_CODE
LEFT JOIN PORDERD d ON d.PORDERD_PORDER_CODE = c.PORDERC_PORDER_CODE AND d.PORDERD_LINE = c.PORDERC_LINE
WHERE p.PORDER_ORDERDATE >= '{since_date}'
   OR p.PORDER_TOTALQTYREC < p.PORDER_TOTALQTY
   OR p.PORDER_COMPLETE <> 'X'
ORDER BY c.PORDERC_PORDER_CODE, c.PORDERC_LINE, c.PORDERC_BEFOREAFTER, c.PORDERC_TYPE;
""".strip()


def order_tracking_events_query(since_date):
    """Verified live 2026-09-15 against Impress (backfill: 1,805,248 rows, 2018-01-04
    to present, see docs/impress-refresh-queries.md section 7). `OTEVENT_ID` is a
    real bigint surrogate key -- used directly as the dedup key, but stored as a
    plain string in order-tracking-events.parquet, NOT Int64 -- see
    purchase_order_lines_query()'s docstring above for why that distinction matters
    (an Int64 surrogate key crashes the nightly merge silently). Filtered to
    `SORDER_CODE LIKE 'S%'` -- unfiltered, 6.8% of events are on Art Request ('A%')
    and other non-sales-order codes, the same contamination the cookbook already
    warns about for booked-orders queries.

    ~66,704 rows (3.7% of the full backfill) have a code with no match in PRODACT --
    left as null code_desc rather than guessed at. Mostly a handful of legacy codes
    (BXR, GDSX, GDSP, PICKX, PICKP, PICS) that all stop appearing right around
    2021-03-03, consistent with Impress renaming them to their current R-prefixed
    equivalents (RBXR, RGDSX, etc.) around then -- see
    order_tracking_codes_for_review.xlsx (ImagePointe-OS/projects/Completed
    Projects/order-tracking-codes-review/, not this repo) for the full per-code
    breakdown Josh reviewed before this was approved."""
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
WHERE e.OTEVENT_SORDER_CODE LIKE 'S%' AND e.OTEVENT_TIME >= '{since_date}'
ORDER BY e.OTEVENT_TIME;
""".strip()


def misprints_damaged_goods_query(since_date):
    """Verified live 2026-09-15 against Impress (backfill: 20,695 rows, 2018-01-05
    to present, see docs/impress-refresh-queries.md section 8 and the
    impress-database-cookbook's R20). `SKUTRANS_ID` is a real bigint surrogate key
    -- used as the dedup key, stored as a plain string (see purchase_order_lines_
    query()'s docstring above for why Int64 breaks the nightly merge).

    FIXED 2026-09-17 (was: KNOWN UNRESOLVED ISSUE 2026-09-15). The "full-history
    doesn't reconcile to POSTINGGL" finding was a query defect in THIS function --
    `ext_cost`/`quantity` were wrapped in `ABS()`, which erases the sign that
    distinguishes a normal write-off (negative) from a rare reversal/reclass entry
    (positive). Preserving the true sign reconciles full-history to POSTINGGL
    within ~2% in every year, 2023/2025 included -- there was never a GL-side
    reclassification. Drilling further found the 2023 ($72,882.08, SKU DAZT003BLK/
    BLKX, 2023-05-17) and 2025 ($52,800.90, SKU VST39LIM/LIMX, 2025-01-29)
    "anomalies" are two large unattributed single-SKU batch entries (no order/
    customer/comment, one user `kay`) -- not vendor credits, not customer
    misprints. Excluding them gives a clean 2023 total ($67,559.24) matching
    Finance's independently GP-reconciled "$67k" figure almost exactly. Added
    `row_type` (below) so downstream consumers don't have to re-derive this
    classification. Full writeup: impress-database-cookbook R20 (rewritten
    2026-09-17), ImagePointe-OS/projects/In process/
    misprints-damaged-goods-gl-discrepancy/, and this file's docs/lessons-learned.md.

    NOTE ext_cost/quantity are now SIGNED (negative = real write-off/outbound,
    positive = reversal/reclass/inbound) -- this is a breaking change from the
    original backfill's always-positive values. The existing parquet was
    regenerated in full on 2026-09-17 to apply the corrected sign to all 20,7xx
    existing rows, not just new ones going forward."""
    return f"""
SELECT
    t.SKUTRANS_ID AS trans_id,
    t.SKUTRANS_GLDATE AS gl_date,
    CASE WHEN t.SKUTRANS_GLACCT_CODEOFFSET LIKE '5900%' THEN 'Misprint' ELSE 'Damaged Goods' END AS category,
    CASE t.SKUTRANS_GLACCT_CODEOFFSET
        WHEN '5900.00.30.30.00' THEN 'Screen Print'
        WHEN '5900.00.30.31.00' THEN 'Embroidery'
        WHEN '5900.00.30.32.00' THEN 'Dye Sub'
        WHEN '5900.00.30.33.00' THEN 'PIP'
        WHEN '5905.00.30.30.00' THEN 'Screen Print'
        WHEN '5905.00.30.31.00' THEN 'Embroidery'
        WHEN '5905.00.30.32.00' THEN 'Dye Sub'
        ELSE 'General / Other'
    END AS decoration_method,
    t.SKUTRANS_GLACCT_CODEOFFSET AS glacct_code,
    a.GLACCT_LONGDESC AS glacct_desc,
    t.SKUTRANS_ORDERNBR AS order_nbr,
    t.SKUTRANS_CUSTVEND AS customer_code,
    COALESCE(c.CUSTOMER_NAME, '(warehouse adjustment - no order/customer)') AS customer_name,
    t.SKUTRANS_SKU_SKUNBR AS sku,
    t.SKUTRANS_SIZE AS size,
    t.SKUTRANS_QUANTITY AS quantity,
    t.SKUTRANS_AVERAGECOST AS ext_cost,
    CASE
      WHEN t.SKUTRANS_AVERAGECOST > 0
           AND COALESCE(NULLIF(LTRIM(RTRIM(t.SKUTRANS_ORDERNBR)),''),
                        NULLIF(LTRIM(RTRIM(t.SKUTRANS_CUSTVEND)),''),
                        NULLIF(LTRIM(RTRIM(t.SKUTRANS_COMMENT)),'')) IS NULL
        THEN 'unattributed_batch_reclass'
      WHEN t.SKUTRANS_COMMENT LIKE '%sample%' THEN 'sample_return'
      WHEN t.SKUTRANS_AVERAGECOST < 0 AND (t.SKUTRANS_COMMENT LIKE '%vendor%' OR t.SKUTRANS_COMMENT LIKE '%credit%')
        THEN 'vendor_return_flagged_in_comment'
      ELSE 'normal_writeoff'
    END AS row_type,
    t.SKUTRANS_SOURCE AS trans_source,
    t.SKUTRANS_COMMENT AS comment,
    t.SKUTRANS_USERS_LOGIN AS entered_by
FROM SKUTRANS t
LEFT JOIN GLACCT a ON a.GLACCT_CODE = t.SKUTRANS_GLACCT_CODEOFFSET
LEFT JOIN CUSTOMER c ON c.CUSTOMER_CODE = t.SKUTRANS_CUSTVEND
WHERE (t.SKUTRANS_GLACCT_CODEOFFSET LIKE '5900%' OR t.SKUTRANS_GLACCT_CODEOFFSET LIKE '5905%')
  AND t.SKUTRANS_GLDATE >= '{since_date}'
ORDER BY t.SKUTRANS_GLDATE;
""".strip()


# master_count (added 2026-10-19) is the design's own stitch/screen count. The order-line count
# (stitches_or_screens) is blank on about 30% of embroidery lines; master_count fills 80% of those
# and matches the line count within 10% on 95% of lines where both exist. Used by the Production
# Dashboard's capacity model (ImagePointe-OS projects/In process/order-flow-scan).
#
# 2026-11-02 (Traci's repo production schedule):
# - work_center / work_center_code: the decoration's press or machine, SORDERPD_WORKCENT_CODE ->
#   WORKCENT_DESC ("Nick", "Rod", "HEAT PRESS"...), the production report's W/C column.
# - Every open order is re-pulled, not just orders received in the window (the same scope as
#   orders_query). Open orders older than the window were never refreshed, so their screen counts
#   went stale and decorations Impress had deleted on an art revision stayed in the file
#   (S0087148: Impress has 2 decoration lines, the parquet had 4).
def design_codes_query(since_date):
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
       NULLIF(pd.SORDERPD_DECOCOUNT,0) AS stitches_or_screens,
       NULLIF(COALESCE(e.EMBRO_DECOCOUNT, pr.PRINTS_DECOCOUNT, dc.DECORATION_DECOCOUNT),0) AS master_count,
       NULLIF(pd.SORDERPD_WORKCENT_CODE,'') AS work_center_code,
       w.WORKCENT_DESC         AS work_center
FROM SORDERPD pd
JOIN SORDER o          ON o.SORDER_CODE = pd.SORDERPD_SORDER_CODE
JOIN CUSTOMER c        ON c.CUSTOMER_CODE = o.SORDER_CUSTOMER_CODE
LEFT JOIN DECOTYPE dt  ON dt.DECOTYPE_CODE = pd.SORDERPD_DECOTYPE
LEFT JOIN LOC l        ON l.LOC_CODE = pd.SORDERPD_LOC_CODE
LEFT JOIN EMBRO e      ON pd.SORDERPD_DECOCLASS = 'E' AND e.EMBRO_ID  = pd.SORDERPD_DECOID
LEFT JOIN PRINTS pr    ON pd.SORDERPD_DECOCLASS = 'S' AND pr.PRINTS_ID = pd.SORDERPD_DECOID
LEFT JOIN DECORATION dc ON pd.SORDERPD_DECOCLASS = 'O' AND dc.DECORATION_ID = pd.SORDERPD_DECOID
LEFT JOIN WORKCENT w   ON w.WORKCENT_CODE = pd.SORDERPD_WORKCENT_CODE
WHERE o.SORDER_CODE LIKE 'S%'
  AND (o.SORDER_RECDATE >= '{since_date}' OR ISNULL(o.SORDER_COMPLETE, '') <> 'X')
ORDER BY pd.SORDERPD_SORDER_CODE, pd.SORDERPD_SEQUENCE;
""".strip()


def sku_query():
    """Added 2026-09-18. Same SKU-master query validated 2026-08-30 (see
    docs/impress-refresh-queries.md section 1) -- scoped to SKUs referenced by at
    least one order line, ever (~70k rows), active + inactive. Now also joins
    GCOLOR directly for color_desc (previously a separate one-time manual pull --
    see docs/color-codes.md) since it costs nothing extra on a query that's
    already re-pulling every row.

    No since_date param: this always re-pulls the full qualifying set, not an
    incremental window. Written with replace_full(), not merge_append_dedupe(),
    for the same reason -- see data/sales/README.md's "Refresh cadence" point 3.

    qty_ordered_open + available_net added 2026-11-03 (Hamilton's refresh request,
    part B). qty_ordered_open = SKUSIZE_QTYUNSCHED, Impress's own open-demand figure
    (the inventory report's "Qty Ordered"; 23,900 on DA SKUs 2026-11-03 vs. the
    report's 23,897 at 09:09). It includes open A-prefixed orders, which
    orders.parquet doesn't. available_net = available - qty_ordered_open = the
    report's "Qty Avail" and CAN go negative. `available` itself is unchanged."""
    return """
SELECT s.SKU_SKUNBR AS sku, s.SKU_MANUALDESC AS descr,
       g.GLABEL_DESC AS brand, sc.SKUCAT_DESC AS garment_type,
       s.SKU_GSTYLE_CODE AS style, s.SKU_GCOLOR_CODE AS color,
       gc.GCOLOR_DESC AS color_desc,
       v.VENDOR_NAME AS vendor, ss.SKUSIZE_SIZE AS size,
       ss.SKUSIZE_QUANTITY AS on_hand, ss.SKUSIZE_QTYAVAILABLE AS available,
       ss.SKUSIZE_QTYONORDER AS on_order,
       ss.SKUSIZE_QTYUNSCHED AS qty_ordered_open,
       ss.SKUSIZE_QTYAVAILABLE - ss.SKUSIZE_QTYUNSCHED AS available_net,
       CAST(ss.SKUSIZE_AVGCOST AS decimal(18,4)) AS avg_cost,
       CAST(ss.SKUSIZE_LASTCOST AS decimal(18,4)) AS last_cost,
       ss.SKUSIZE_LASTINVOICEDATE AS last_invoiced,
       s.SKU_STATUS AS status_code,
       CASE WHEN s.SKU_STATUS = 'A' THEN 'active' ELSE 'inactive' END AS status
FROM SKU s
JOIN SKUSIZE ss     ON ss.SKUSIZE_SKU_SKUNBR = s.SKU_SKUNBR
                   AND ss.SKUSIZE_SKU_DESIGN_CODE = s.SKU_DESIGN_CODE
LEFT JOIN GLABEL g  ON g.GLABEL_CODE = s.SKU_GLABEL_CODE
LEFT JOIN SKUCAT sc ON sc.SKUCAT_CODE = s.SKU_SKUCAT_CODE
LEFT JOIN VENDOR v  ON v.VENDOR_CODE = s.SKU_VENDOR_CODEPREF
LEFT JOIN GCOLOR gc ON gc.GCOLOR_CODE = s.SKU_GCOLOR_CODE
WHERE EXISTS (SELECT 1 FROM SORDERD d WHERE d.SORDERD_RAWSKU_SKUNBR = s.SKU_SKUNBR)
ORDER BY s.SKU_SKUNBR, ss.SKUSIZE_SIZESEQ;
""".strip()


def customers_query():
    """Added 2026-10-04 (Josh's call). customers.parquet was a one-time Impress
    "Contacts" export pulled 2026-08-29 and never refreshed, so every "who is this
    account assigned to" answer was frozen at 8/29 (613 accounts had been reassigned
    by 10/04). Now rebuilt in full every night from CUSTOMER LEFT JOIN CUSTSHIP --
    one row per address, plus one row for each customer with no address (the export
    had the same shape). Legacy export column names are kept so existing recipes
    still work; `Agent #1` = CUSTOMER_AGENT_CODE1, the account-level rep (matched the
    8/29 export on 100% of accounts not reassigned since).

    TWO levels of rep assignment exist in Impress:
      - account_agent_code (CUSTOMER_AGENT_CODE1) -- the account owner
      - address_agent_code (CUSTSHIP_AGENT_CODE1) -- set on ~1.5% of addresses and
        often different from the account owner. Example: rep 114 (Julie Carey) owned 0
        accounts at the account level but sat on 146 addresses across 84 accounts.
    Neither one decides the order's rep. SORDER_AGENT_CODE1 (order-tracking.parquet's
    order_agent_code) matched the current account rep on only ~74% of 2025+ orders,
    and putting address-level reps first didn't raise that. See docs/impress-refresh-
    queries.md section 13."""
    return """
SELECT c.CUSTOMER_AGENT_CODE1 AS [Agent #1],
       c.CUSTOMER_CODE AS [Customer Number], c.CUSTOMER_NAME AS [Customer Name],
       cs.CUSTSHIP_CODE AS [Address Code],
       CASE WHEN cs.CUSTSHIP_BILLTO = 1 THEN 'Yes' ELSE 'No' END AS [Bill-To],
       CASE WHEN cs.CUSTSHIP_SHIPTO = 1 THEN 'Yes' ELSE 'No' END AS [Ship-To],
       CASE WHEN cs.CUSTSHIP_BUYER = 1 THEN 'Yes' ELSE 'No' END AS [Buyer],
       cs.CUSTSHIP_NAME AS [Name],
       cs.CUSTSHIP_ADDRESS1 AS [Address Line 1], cs.CUSTSHIP_ADDRESS2 AS [Address Line 2],
       cs.CUSTSHIP_ADDRESS3 AS [Address Line 3],
       cs.CUSTSHIP_CITY AS [City], cs.CUSTSHIP_STATE AS [State], cs.CUSTSHIP_ZIP AS [Zip Code],
       CASE cs.CUSTSHIP_SHIPCR WHEN 'C' THEN 'Commercial' WHEN 'R' THEN 'Residential' END AS [Comm/Res],
       cs.CUSTSHIP_PHONE AS [Phone#], cs.CUSTSHIP_FAX AS [Fax#], cs.CUSTSHIP_EMAIL AS [Email],
       cs.CUSTSHIP_TERMS_CODE AS [Terms Code], cs.CUSTSHIP_STCAT_CODENT AS [Non-Tax Category],
       cs.CUSTSHIP_SALESTAX_CODE AS [Tax Code], cs.CUSTSHIP_SHIPVIA_CODE1 AS [Ship Via],
       cs.CUSTSHIP_USERSTRING1 AS [User Defined String1],
       c.CUSTOMER_AGENT_CODE1 AS account_agent_code, ca.AGENT_NAME AS account_agent_name,
       c.CUSTOMER_AGENT_CODE2 AS account_agent_code_2, c.CUSTOMER_AGENT_CODE3 AS account_agent_code_3,
       cs.CUSTSHIP_AGENT_CODE1 AS address_agent_code, sa.AGENT_NAME AS address_agent_name,
       c.CUSTOMER_STATUS AS customer_status,
       c.CUSTOMER_FIRSTORDER AS first_order, c.CUSTOMER_LASTORDER AS last_order
FROM CUSTOMER c
LEFT JOIN CUSTSHIP cs ON cs.CUSTSHIP_CUSTOMER_CODE = c.CUSTOMER_CODE
LEFT JOIN AGENT ca ON ca.AGENT_CODE = c.CUSTOMER_AGENT_CODE1
LEFT JOIN AGENT sa ON sa.AGENT_CODE = cs.CUSTSHIP_AGENT_CODE1
ORDER BY c.CUSTOMER_CODE, cs.CUSTSHIP_CODE;
""".strip()


def agents_query():
    """Added 2026-10-04 -- the full AGENT (sales rep) master, so code -> name and
    active/inactive lookups don't depend on whatever names happen to appear on
    orders. Several codes are placeholder buckets, not people (022 DORMANT ACCT,
    030 INACTIVE, 118 NEW DORMANT ACCOUNTS, 147 CUSTOMER SERVICE
    SUPPORT)."""
    return """
SELECT AGENT_CODE AS agent_code, AGENT_NAME AS agent_name, AGENT_EMAIL AS agent_email,
       CASE WHEN AGENT_INACTIVE = 1 THEN 'inactive' ELSE 'active' END AS status,
       AGENT_CREATED AS created, AGENT_MODIFIED AS modified
FROM AGENT
ORDER BY AGENT_CODE;
""".strip()


def order_notes_query(since_date):
    """Added 2026-09-23 for the PO Team Check reconciliation tool's vendor-match
    and net-cost checks -- see docs/impress-refresh-queries.md section 10 for the
    full writeup of what's actually in these notes and why the parser is
    deterministic-with-fallback rather than regex-only. Internal notes only
    (SORDERC_TYPE = 'I') -- customer-facing notes (type 'C') don't carry
    pricing/vendor detail. One row per SORDERC_LINE (a note can span several)."""
    return f"""
SELECT sc.SORDERC_SORDER_CODE AS sales_order, sc.SORDERC_LINE AS seq,
       sc.SORDERC_TEXT AS note_text
FROM SORDERC sc
JOIN SORDER o ON o.SORDER_CODE = sc.SORDERC_SORDER_CODE
WHERE sc.SORDERC_TYPE = 'I' AND o.SORDER_CODE LIKE 'S%'
  AND o.SORDER_RECDATE >= '{since_date}'
ORDER BY sc.SORDERC_SORDER_CODE, sc.SORDERC_LINE;
""".strip()


def so_charge_lines_query(since_date):
    """Added 2026-10-19 -- the SO's misc / service lines (SORDERD_FORMAT = 'M'), which
    orders_query() drops because they have no size quantity. Item code (PP-SETUP,
    PP-CPYCHG, PP-RUSH, SHIP, DIGI...), description, quantity (SORDERD_ITEMQTY) and the
    billed amount. One row per SO line."""
    return f"""
SELECT d.SORDERD_SORDER_CODE AS sales_order, d.SORDERD_LINE AS line,
       d.SORDERD_ITEM_CODE AS item_code, d.SORDERD_DESC AS description,
       d.SORDERD_ITEMQTY AS qty, CAST(d.SORDERD_TOTALPRICE AS decimal(18,2)) AS amount,
       d.SORDERD_DIVISION_CODE AS division
FROM SORDERD d
JOIN SORDER o ON o.SORDER_CODE = d.SORDERD_SORDER_CODE
WHERE d.SORDERD_FORMAT = 'M' AND o.SORDER_CODE LIKE 'S%'
  AND o.SORDER_RECDATE >= '{since_date}'
ORDER BY d.SORDERD_SORDER_CODE, d.SORDERD_LINE;
""".strip()


def so_shipments_query(since_date):
    """Added 2026-10-20 -- SORDERS, one row per shipped box with its tracking number
    (our own WorldShip labels, 1Z300EW1...). Feeds so-shipments.parquet and lets
    scripts/ups_shipments.py tie our outbound UPS packages to their sales order. Pulled
    by ship date, not order received date, so late shipments on old orders still land."""
    return f"""
SELECT s.SORDERS_SORDER_CODE AS sales_order, CONVERT(char(10), s.SORDERS_SHIPDATE, 120) AS ship_date,
       s.SORDERS_SHIPVIA_CODE AS ship_via_code, sv.SHIPVIA_DESC AS ship_via_desc,
       s.SORDERS_TRACKINGNBR AS tracking, s.SORDERS_BOX AS box, s.SORDERS_BOXES AS boxes,
       s.SORDERS_TOTALQTYSHIPPED AS qty_shipped, CAST(s.SORDERS_WEIGHT AS decimal(18,2)) AS weight,
       CAST(s.SORDERS_FREIGHT AS decimal(18,2)) AS freight, s.SORDERS_INVOICE_INVNBR AS invoice
FROM SORDERS s
LEFT JOIN SHIPVIA sv ON sv.SHIPVIA_CODE = s.SORDERS_SHIPVIA_CODE
WHERE s.SORDERS_SHIPDATE >= '{since_date}'
ORDER BY s.SORDERS_SORDER_CODE, s.SORDERS_SHIPDATE, s.SORDERS_BOX;
""".strip()


def so_ship_to_query(since_date):
    """Added 2026-10-20 -- every order's ship-to (SORDER_S*) plus each split-ship
    destination (SORDERDST -> CUSTSHIP, joined on customer + address code). Feeds
    so-ship-to.parquet, which scripts/ups_shipments.py uses to match UPS packages with
    no usable reference to an order by address. orders.parquet carries ship-to only on
    recently pulled orders."""
    return f"""
SELECT o.SORDER_CODE AS sales_order, CONVERT(char(10), o.SORDER_RECDATE, 120) AS received_date,
       o.SORDER_CUSTOMER_CODE AS customer_number, 'order' AS address_kind, CAST(NULL AS varchar(20)) AS custship_code,
       o.SORDER_SNAME AS ship_name, o.SORDER_SADDRESS1 AS ship_address1, o.SORDER_SADDRESS2 AS ship_address2,
       o.SORDER_SCITY AS ship_city, o.SORDER_SSTATE AS ship_state, o.SORDER_SZIP AS ship_zip
FROM SORDER o
WHERE o.SORDER_CODE LIKE 'S%' AND o.SORDER_RECDATE >= '{since_date}'
UNION ALL
SELECT DISTINCT o.SORDER_CODE, CONVERT(char(10), o.SORDER_RECDATE, 120), o.SORDER_CUSTOMER_CODE, 'split',
       cs.CUSTSHIP_CODE, cs.CUSTSHIP_NAME, cs.CUSTSHIP_ADDRESS1, cs.CUSTSHIP_ADDRESS2,
       cs.CUSTSHIP_CITY, cs.CUSTSHIP_STATE, cs.CUSTSHIP_ZIP
FROM SORDERDST d
JOIN SORDER o ON o.SORDER_CODE = d.SORDERDST_SORDER_CODE
JOIN CUSTSHIP cs ON cs.CUSTSHIP_CODE = d.SORDERDST_CUSTSHIP_CODE AND cs.CUSTSHIP_CUSTOMER_CODE = o.SORDER_CUSTOMER_CODE
WHERE o.SORDER_CODE LIKE 'S%' AND o.SORDER_RECDATE >= '{since_date}';
""".strip()


def order_notes_all_query(since_date=None, until_date=None, include_undated=False):
    """Added 2026-10-12 -- every SORDERC note (types C and I), with the order's received
    date and customer. since_date/until_date bound SORDER_RECDATE (until exclusive);
    include_undated also returns notes whose order has no SORDER row or no received date
    (backfill only). One row per SORDERC_LINE."""
    conds = []
    if since_date:
        conds.append(f"o.SORDER_RECDATE >= '{since_date}'")
    if until_date:
        conds.append(f"o.SORDER_RECDATE < '{until_date}'")
    where = " AND ".join(conds) if conds else "1=1"
    if include_undated:
        where = f"(({where}) OR o.SORDER_RECDATE IS NULL)"
    return f"""
SELECT sc.SORDERC_SORDER_CODE AS sales_order, sc.SORDERC_TYPE AS kind, sc.SORDERC_LINE AS seq,
       sc.SORDERC_TEXT AS note_text, CONVERT(char(10), o.SORDER_RECDATE, 120) AS received_date,
       o.SORDER_CUSTOMER_CODE AS customer_number
FROM SORDERC sc
LEFT JOIN SORDER o ON o.SORDER_CODE = sc.SORDERC_SORDER_CODE
WHERE {where}
ORDER BY sc.SORDERC_SORDER_CODE, sc.SORDERC_TYPE, sc.SORDERC_LINE;
""".strip()


def credit_memos_query(since_date):
    """Added 2026-10-26 -- one row per credit (C) or debit (D) memo dated on or after
    since_date. Filtered on INVOICE_TYPE, not the invoice number: a few D memos carry
    C-sequence numbers (query-recipes R17). apply_to_invoice is the invoice the memo
    applies to (INVOICE_INVOICE_INVNBRAPPLYTO -- note the doubled prefix). Memos have no
    SO-linked INVOICED lines of their own, so sales_order comes from the apply-to
    invoice's lines (INVOICE_SORDER_CODEREF is mostly blank, cookbook gotcha #15).
    Totals are negative on credits."""
    return f"""
WITH so AS (
    SELECT INVOICED_INVOICE_INVNBR AS inv, MIN(NULLIF(INVOICED_SORDER_CODE, '')) AS sales_order,
           COUNT(DISTINCT NULLIF(INVOICED_SORDER_CODE, '')) AS sales_order_count
    FROM INVOICED GROUP BY INVOICED_INVOICE_INVNBR
)
SELECT i.INVOICE_INVNBR AS invoice_nbr, i.INVOICE_TYPE AS invoice_type,
       CONVERT(char(10), i.INVOICE_INVDATE, 120) AS invoice_date,
       CONVERT(char(10), i.INVOICE_DATEENTERED, 120) AS date_entered,
       i.INVOICE_CUSTOMER_CODE AS customer_code, c.CUSTOMER_NAME AS customer_name,
       i.INVOICE_AGENT_CODE1 AS agent_code, a.AGENT_NAME AS agent_name,
       i.INVOICE_DIVISION_CODE AS division, i.INVOICE_USERS_LOGINENT AS entered_by,
       i.INVOICE_INVOICE_INVNBRAPPLYTO AS apply_to_invoice, i.INVOICE_SORDER_CODEREF AS order_ref,
       so.sales_order, so.sales_order_count,
       CAST(i.INVOICE_PRODUCTTOTAL AS decimal(18,2)) AS product_total,
       CAST(i.INVOICE_MISCTOTAL AS decimal(18,2)) AS misc_total,
       CAST(i.INVOICE_SHIPPINGCOST AS decimal(18,2)) AS shipping,
       CAST(i.INVOICE_TAXTOTAL AS decimal(18,2)) AS tax,
       CAST(i.INVOICE_INVTOTAL AS decimal(18,2)) AS inv_total,
       CAST(i.INVOICE_TOTALCOST AS decimal(18,2)) AS total_cost
FROM INVOICE i
LEFT JOIN CUSTOMER c ON c.CUSTOMER_CODE = i.INVOICE_CUSTOMER_CODE
LEFT JOIN AGENT a ON a.AGENT_CODE = i.INVOICE_AGENT_CODE1
LEFT JOIN so ON so.inv = i.INVOICE_INVOICE_INVNBRAPPLYTO
WHERE i.INVOICE_TYPE IN ('C','D') AND i.INVOICE_INVDATE >= '{since_date}'
ORDER BY i.INVOICE_INVDATE, i.INVOICE_INVNBR;
""".strip()


def credit_memo_comments_query(since_date):
    """INVOICEC lines for the same memos as credit_memos_query(); merged into that file's
    `comments` column by merge_credit_memos()."""
    return f"""
SELECT ic.INVOICEC_INVOICE_INVNBR AS invoice_nbr, ic.INVOICEC_LINE AS line,
       ic.INVOICEC_TEXT AS text
FROM INVOICEC ic
JOIN INVOICE i ON i.INVOICE_INVNBR = ic.INVOICEC_INVOICE_INVNBR
WHERE i.INVOICE_TYPE IN ('C','D') AND i.INVOICE_INVDATE >= '{since_date}'
ORDER BY ic.INVOICEC_INVOICE_INVNBR, ic.INVOICEC_LINE;
""".strip()


def submit_query(tag, bridge_dir, name, sql, fh):
    """Write + push a query and return immediately (don't wait for the result).
    Submitting every query up front, before polling any of them, lets the server
    process them in the same cycle -- per the run-impress-queries skill, 4 queries
    submitted together take about as long as 1. Waiting for each one sequentially
    (submit -> poll -> submit -> poll -> ...) was the single biggest reason a 3-query
    refresh took ~3 minutes instead of ~1."""
    inbox = bridge_dir / "query_inbox"
    sql_path = inbox / f"{tag}__{name}.sql"
    sql_path.write_text(sql, encoding="utf-8")
    log(f"wrote {sql_path.name}", fh)

    # Belt-and-suspenders explicit push -- harmless if a local daemon already
    # synced it (that just fails with "directory not found", which we ignore).
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
        # Explicit pull -- on machines where the local bridge folder lives outside
        # Dropbox's synced tree (e.g. CHI-TOWN, to avoid the Dropbox-app-vs-rclone
        # conflict), nothing else brings results down from Dropbox for an unattended
        # run. Harmless no-op on machines where Dropbox already syncs it locally.
        subprocess.run(
            [RCLONE, "copy", f"{DROPBOX_REMOTE_ROOT}/query_results/", str(results)],
            capture_output=True, text=True,
        )
        if status_path.exists():
            status = status_path.read_text(encoding="utf-8-sig").strip()
            if status.startswith("OK"):
                # status.txt can appear (or be visible via rclone's directory listing)
                # before the matching CSV has actually finished syncing down from
                # Dropbox -- returning on "OK" alone raced and crashed
                # merge_append_dedupe() with FileNotFoundError on 2026-09-20. Keep
                # polling for the CSV itself, not just its status file.
                if csv_path.exists():
                    log(f"{name}: {status}", fh)
                    return csv_path
            else:
                raise RuntimeError(f"{name} query failed: {status}")
        time.sleep(POLL_INTERVAL_SEC)
    if status_path.exists() and status_path.read_text(encoding="utf-8-sig").strip().startswith("OK"):
        raise TimeoutError(
            f"{name}: status said OK but {csv_path.name} never synced down within {POLL_TIMEOUT_SEC}s"
        )
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
        # zero rows -- same fix as gdot_hourly_refresh.py's copy of this function.
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


def merge_replace_by_group(existing_path, new_csv_path, date_cols, numeric_cols, group_col, fh,
                           extra_groups=None):
    """Like merge_append_dedupe(), but for a source where a ROW can disappear
    upstream entirely (an SO line or PO line deleted/edited away in Impress
    after already being pulled once) -- key-based dedup alone can't detect
    that. drop_duplicates(keep='last') only overwrites a key that reappears in
    a new pull; a key that's simply absent from every pull since its source
    row was deleted just sits in the parquet forever as a phantom row, causing
    a real false "on SO but not on PO" mismatch (found live 2026-09-23:
    S0087649's AS2775BLK/ORA/PUR lines were deleted from the order in Impress
    at some point, but merge_append_dedupe() never noticed and kept surfacing
    them as a false SKU/qty mismatch every reconciliation run since).

    Fix: for every `group_col` value present in THIS pull (e.g. every SO
    actually returned by this refresh's since_date/SO-scoped query), drop ALL
    of that group's existing cached rows first, then add the freshly-pulled
    set -- so a row genuinely gone from Impress actually disappears from the
    parquet too. Groups NOT present in this pull (outside the lookback
    window/scope) are left untouched, same incremental-freshness model as
    merge_append_dedupe() everywhere else in this pipeline."""
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
        refreshed_groups = set(new_df[group_col].dropna().unique()) | set(extra_groups or ())
        stale = old_df[group_col].astype(str).isin({str(g) for g in refreshed_groups})
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


def merge_credit_memos(existing_path, memos_csv, comments_csv, fh):
    """credit-memos.parquet: joins each memo's INVOICEC lines (in line order) into one
    `comments` string, adds comment_sales_order (first S-number in that text), then
    appends and dedupes on invoice_nbr so a re-pulled memo replaces its old row."""
    try:
        new_df = pd.read_csv(memos_csv, dtype=str, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        log(f"{existing_path.name}: 0 rows returned, nothing to merge", fh)
        return len(pd.read_parquet(existing_path)) if existing_path.exists() else 0
    try:
        cm = pd.read_csv(comments_csv, dtype=str, encoding="utf-8-sig", keep_default_na=False)
        cm["line"] = pd.to_numeric(cm["line"], errors="coerce")
        cm = cm.sort_values(["invoice_nbr", "line"])
        text = (cm[cm["text"].str.strip() != ""].groupby("invoice_nbr")["text"]
                .agg(lambda s: "\n".join(t.replace("\r\n", "\n").rstrip() for t in s)))
    except pd.errors.EmptyDataError:
        text = pd.Series(dtype=str)
    new_df["comments"] = new_df["invoice_nbr"].map(text)
    # Staff usually end the memo comment with the order number ("Incorrect Art ... S0085758 td").
    new_df["comment_sales_order"] = new_df["comments"].str.extract(r"(S\d{7})", expand=False)
    new_df = parse_mixed_dates(new_df, ["invoice_date", "date_entered"])
    for col in ["sales_order_count", "product_total", "misc_total", "shipping", "tax",
                "inv_total", "total_cost"]:
        new_df[col] = pd.to_numeric(new_df[col], errors="coerce")

    combined = new_df
    if existing_path.exists():
        combined = pd.concat([pd.read_parquet(existing_path), new_df], ignore_index=True)
    before = len(combined)
    combined = (combined.drop_duplicates(subset=["invoice_nbr"], keep="last")
                .sort_values(["invoice_date", "invoice_nbr"]).reset_index(drop=True))
    log(f"{existing_path.name}: {before} -> {len(combined)} rows after dedup on invoice_nbr "
        f"({len(new_df)} pulled, {new_df['comments'].notna().sum()} with comments)", fh)
    combined.to_parquet(existing_path, index=False)
    return len(combined)


def replace_full(existing_path, new_csv_path, date_cols, numeric_cols, fh):
    new_df = pd.read_csv(new_csv_path, dtype=str, encoding="utf-8-sig")
    new_df = parse_mixed_dates(new_df, date_cols)
    for col in numeric_cols:
        if col in new_df.columns:
            new_df[col] = pd.to_numeric(new_df[col], errors="coerce")
    new_df.to_parquet(existing_path, index=False)
    log(f"{existing_path.name}: replaced in full, {len(new_df)} rows", fh)
    return len(new_df)


def replace_customers(existing_path, new_csv_path, fh):
    """customers.parquet: full replace, plus the export's two composite address
    columns (`Name/Address`, `Address`) rebuilt in pandas so the old file's shape is
    kept. The 8/29 export's four `Contact Soack/Pack/Invoice/Stmt` flags had no
    confirmed source column and nothing used them, so they were dropped."""
    df = pd.read_csv(new_csv_path, dtype=str, encoding="utf-8-sig")
    df = parse_mixed_dates(df, ["first_order", "last_order"])

    def join(row, cols):
        parts = [row[c] for c in cols if isinstance(row[c], str) and row[c].strip()]
        csz = " ".join(p for p in [
            f"{row['City']}," if isinstance(row["City"], str) and row["City"].strip() else "",
            row["State"] if isinstance(row["State"], str) else "",
            row["Zip Code"] if isinstance(row["Zip Code"], str) else "",
        ] if p).strip(" ,")
        if csz:
            parts.append(csz)
        return "\n".join(parts) if parts else None

    lines = ["Address Line 1", "Address Line 2", "Address Line 3"]
    df.insert(df.columns.get_loc("Address Line 1"), "Name/Address",
              df.apply(lambda r: join(r, ["Name"] + lines), axis=1))
    df.insert(df.columns.get_loc("Address Line 1"), "Address",
              df.apply(lambda r: join(r, lines), axis=1))
    df.to_parquet(existing_path, index=False)
    log(f"{existing_path.name}: replaced in full, {len(df)} rows, "
        f"{df['Customer Number'].nunique()} customers", fh)
    return len(df)


def write_last_refresh(fh):
    """Small marker file the agent checks to answer 'how current is this data' /
    'is this stale' questions without having to inspect every Parquet file's max
    date itself."""
    info = {
        "last_refresh_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "files": {},
    }
    import pyarrow.parquet as pq
    for name in ["orders", "order-tracking", "design-codes", "sku", "design-library", "customers", "agents",
                 "purchase-orders", "purchase-order-lines", "purchase-order-line-sizes", "purchase-order-comments", "order-tracking-events",
                 "misprints-damaged-goods", "order-notes", "order-notes-all", "so-charge-lines",
                 "so-shipments", "so-ship-to", "shipments", "credit-memos",
                 "open-order-lines", "sku-warehouse-stock", "sales-order-lines", "ar-aging-daily"]:
        path = DATA_DIR / f"{name}.parquet"
        if path.exists():
            info["files"][name] = {"rows": pq.ParquetFile(path).metadata.num_rows}
    import json
    out_path = DATA_DIR / "LAST_REFRESH.json"
    out_path.write_text(json.dumps(info, indent=2), encoding="utf-8")
    log(f"wrote {out_path.name}: {info['last_refresh_utc']}", fh)


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"refresh_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log"
    with open(log_path, "w", encoding="utf-8") as fh:
        try:
            tag, bridge_dir = get_tag_and_bridge_dir()
            log(f"running on {tag}, bridge dir {bridge_dir}", fh)

            since_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
            # --reuse-run-id <id>: merge a failed run's results that are already in
            # query_results instead of sending the queries to Impress again.
            reuse = "--reuse-run-id" in sys.argv
            run_id = sys.argv[sys.argv.index("--reuse-run-id") + 1] if reuse else uuid.uuid4().hex[:8]
            if reuse:
                log(f"reusing results from run {run_id}; no queries submitted", fh)
                submit = lambda *a: None
            else:
                submit = submit_query

            # Wait out an intraday run already in progress (its lock goes stale at 45 min).
            for _ in range(60):
                if not INTRADAY_LOCK_PATH.exists() or time.time() - INTRADAY_LOCK_PATH.stat().st_mtime > 45 * 60:
                    break
                time.sleep(15)
            INTRADAY_LOCK_PATH.write_text(str(os.getpid()), encoding="utf-8")

            orders_name = f"daily_orders_{run_id}"
            dc_name = f"daily_design_codes_{run_id}"
            ot_name = f"daily_order_tracking_{run_id}"
            po_name = f"daily_purchase_orders_{run_id}"
            pol_name = f"daily_purchase_order_lines_{run_id}"
            pols_name = f"daily_purchase_order_line_sizes_{run_id}"
            poc_name = f"daily_purchase_order_comments_{run_id}"
            ote_name = f"daily_order_tracking_events_{run_id}"
            mdg_name = f"daily_misprints_damaged_goods_{run_id}"
            sku_name = f"daily_sku_{run_id}"
            notes_name = f"daily_order_notes_{run_id}"
            notes_all_name = f"daily_order_notes_all_{run_id}"
            charges_name = f"daily_so_charge_lines_{run_id}"
            shipments_name = f"daily_so_shipments_{run_id}"
            ship_to_name = f"daily_so_ship_to_{run_id}"
            cust_name = f"daily_customers_{run_id}"
            agents_name = f"daily_agents_{run_id}"
            memos_name = f"daily_credit_memos_{run_id}"
            memo_comments_name = f"daily_credit_memo_comments_{run_id}"
            ool_name = f"daily_open_order_lines_{run_id}"
            whse_name = f"daily_sku_warehouse_stock_{run_id}"
            sol_name = f"daily_sales_order_lines_{run_id}"
            year_start = f"{datetime.now().year}-01-01"

            # Submit all queries before waiting on any of them -- see submit_query's
            # docstring for why this matters for wall-clock time.
            submit(tag, bridge_dir, orders_name, orders_query(since_date), fh)
            submit(tag, bridge_dir, dc_name, design_codes_query(since_date), fh)
            submit(tag, bridge_dir, ot_name, order_tracking_query(), fh)
            if ENABLE_PURCHASE_ORDERS:
                still_open = still_open_pos()
                submit(tag, bridge_dir, po_name,
                       purchase_orders_query(since_date, still_open), fh)
                submit(tag, bridge_dir, pol_name, purchase_order_lines_query(since_date), fh)
                submit(tag, bridge_dir, pols_name, purchase_order_line_sizes_query(since_date), fh)
                # PO comments reach back to PO_COMMENTS_FLOOR (2026-10-08) so PO Team
                # Check can test against older POs (S0078577 / P0069799, the black totes).
                submit(tag, bridge_dir, poc_name,
                             purchase_order_comments_query(min(since_date, PO_COMMENTS_FLOOR)), fh)
            if ENABLE_ORDER_TRACKING_EVENTS:
                submit(tag, bridge_dir, ote_name, order_tracking_events_query(since_date), fh)
            if ENABLE_MISPRINTS_DAMAGED_GOODS:
                submit(tag, bridge_dir, mdg_name, misprints_damaged_goods_query(since_date), fh)
            if ENABLE_SKU_REFRESH:
                submit(tag, bridge_dir, sku_name, sku_query(), fh)
            if ENABLE_ORDER_NOTES:
                submit(tag, bridge_dir, notes_name, order_notes_query(since_date), fh)
            if ENABLE_ORDER_NOTES_ALL:
                submit(tag, bridge_dir, notes_all_name, order_notes_all_query(since_date), fh)
            if ENABLE_SO_CHARGE_LINES:
                submit(tag, bridge_dir, charges_name, so_charge_lines_query(since_date), fh)
            if ENABLE_SHIPMENTS:
                submit(tag, bridge_dir, shipments_name, so_shipments_query(since_date), fh)
                submit(tag, bridge_dir, ship_to_name, so_ship_to_query(since_date), fh)
            if ENABLE_CUSTOMERS_REFRESH:
                submit(tag, bridge_dir, cust_name, customers_query(), fh)
                submit(tag, bridge_dir, agents_name, agents_query(), fh)
            if ENABLE_CREDIT_MEMOS:
                submit(tag, bridge_dir, memos_name, credit_memos_query(since_date), fh)
                submit(tag, bridge_dir, memo_comments_name, credit_memo_comments_query(since_date), fh)
            if ENABLE_OPEN_ORDER_LINES:
                submit(tag, bridge_dir, ool_name, open_order_lines_query(), fh)
                submit(tag, bridge_dir, whse_name, sku_warehouse_stock_query(), fh)
            if ENABLE_SALES_ORDER_LINES:
                submit(tag, bridge_dir, sol_name, sales_order_lines_query(year_start), fh)
            ar_name = f"daily_ar_aging_{run_id}"
            if ENABLE_AR_AGING:
                submit(tag, bridge_dir, ar_name, ar_aging_totals_query(), fh)

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

            dc_csv = wait_for_result(tag, bridge_dir, dc_name, fh)
            # 2026-11-02 (R46, Claire's S0088391): replace each re-pulled order's design codes
            # as a group, so a code deleted in Impress drops out (merge_append_dedupe kept it
            # forever). Orders the query covers that now have NO design codes are cleared too.
            # Since 2026-11-02 both queries cover the window plus every open order, so every order
            # in the orders pull is a group to replace.
            try:
                win = pd.read_csv(orders_csv, dtype=str, encoding="utf-8-sig", usecols=["sales_order"])
                window_orders = set(win["sales_order"].dropna())
            except (pd.errors.EmptyDataError, ValueError):
                window_orders = set()
            merge_replace_by_group(
                DATA_DIR / "design-codes.parquet", dc_csv,
                date_cols=["received"],
                numeric_cols=["stitches_or_screens", "master_count"],
                group_col="ord",
                fh=fh,
                extra_groups=window_orders,
            )

            ot_csv = wait_for_result(tag, bridge_dir, ot_name, fh)
            replace_full(
                DATA_DIR / "order-tracking.parquet", ot_csv,
                date_cols=["received_date", "order_approved", "art_approved", "last_ship_date", "invoice_date",
                           "cancel_date", "request_date", "apb_ord_apr", "apb_art_apr", "apb_goods",
                           "art_due_date"],
                numeric_cols=["order_sub_total", "order_total", "invoiced_amount"],
                fh=fh,
            )

            if ENABLE_PURCHASE_ORDERS:
                po_csv = wait_for_result(tag, bridge_dir, po_name, fh)
                merge_append_dedupe(
                    DATA_DIR / "purchase-orders.parquet", po_csv,
                    date_cols=["ordered_date", "expected_date", "received_date",
                               "vendor_oc_date", "in_hands_date", "fu_1wk", "fu_2day"],
                    numeric_cols=["qty_ordered", "qty_received", "po_total", "ship_days"],
                    key_cols=["po"],
                    fh=fh,
                )
                drop_deleted_pos(still_open, po_csv, fh)

                pol_csv = wait_for_result(tag, bridge_dir, pol_name, fh)
                merge_replace_by_group(
                    DATA_DIR / "purchase-order-lines.parquet", pol_csv,
                    date_cols=[],
                    numeric_cols=["qty_ordered", "qty_received", "line_total_cost", "unit_cost"],
                    group_col="po",
                    fh=fh,
                )

                # Size runs (2026-10-01) -- isolated: a failure here is logged and
                # skipped, never allowed to stop the rest of the nightly refresh.
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

            if ENABLE_ORDER_TRACKING_EVENTS:
                ote_csv = wait_for_result(tag, bridge_dir, ote_name, fh)
                merge_append_dedupe(
                    DATA_DIR / "order-tracking-events.parquet", ote_csv,
                    date_cols=["event_time"],
                    numeric_cols=[],
                    key_cols=["event_id"],
                    fh=fh,
                )

            if ENABLE_MISPRINTS_DAMAGED_GOODS:
                mdg_csv = wait_for_result(tag, bridge_dir, mdg_name, fh)
                merge_append_dedupe(
                    DATA_DIR / "misprints-damaged-goods.parquet", mdg_csv,
                    date_cols=["gl_date"],
                    numeric_cols=["quantity", "ext_cost"],
                    key_cols=["trans_id"],
                    fh=fh,
                )

            if ENABLE_SKU_REFRESH:
                sku_csv = wait_for_result(tag, bridge_dir, sku_name, fh)
                replace_full(
                    DATA_DIR / "sku.parquet", sku_csv,
                    date_cols=["last_invoiced"],
                    numeric_cols=["on_hand", "available", "on_order", "qty_ordered_open",
                                  "available_net", "avg_cost", "last_cost"],
                    fh=fh,
                )

            if ENABLE_ORDER_NOTES:
                notes_csv = wait_for_result(tag, bridge_dir, notes_name, fh)
                merge_append_dedupe(
                    DATA_DIR / "order-notes.parquet", notes_csv,
                    date_cols=[],
                    numeric_cols=[],
                    key_cols=["sales_order", "seq"],
                    fh=fh,
                )

            if ENABLE_ORDER_NOTES_ALL:
                # Isolated: a failure here is logged and skipped, never allowed to stop
                # the rest of the refresh.
                try:
                    notes_all_csv = wait_for_result(tag, bridge_dir, notes_all_name, fh)
                    merge_append_dedupe(
                        DATA_DIR / "order-notes-all.parquet", notes_all_csv,
                        date_cols=[],
                        numeric_cols=[],
                        key_cols=["sales_order", "kind", "seq"],
                        fh=fh,
                    )
                except Exception as e:
                    log(f"order-notes-all: SKIPPED after error, rest of refresh continues: {e}", fh)

            if ENABLE_SO_CHARGE_LINES:
                # Isolated: a failure here is logged and skipped. Replaced by SO so a
                # charge line deleted in Impress disappears here too.
                try:
                    charges_csv = wait_for_result(tag, bridge_dir, charges_name, fh)
                    merge_replace_by_group(
                        DATA_DIR / "so-charge-lines.parquet", charges_csv,
                        date_cols=[],
                        numeric_cols=["line", "qty", "amount"],
                        group_col="sales_order",
                        fh=fh,
                    )
                except Exception as e:
                    log(f"so-charge-lines: SKIPPED after error, rest of refresh continues: {e}", fh)

            if ENABLE_CUSTOMERS_REFRESH:
                # Isolated like the PO size-run/comment pulls: a failure here is
                # logged and skipped, never allowed to stop the rest of the refresh.
                try:
                    cust_csv = wait_for_result(tag, bridge_dir, cust_name, fh)
                    replace_customers(DATA_DIR / "customers.parquet", cust_csv, fh)
                except Exception as e:
                    log(f"customers: SKIPPED after error, rest of refresh continues: {e}", fh)
                try:
                    agents_csv = wait_for_result(tag, bridge_dir, agents_name, fh)
                    replace_full(DATA_DIR / "agents.parquet", agents_csv,
                                 date_cols=["created", "modified"], numeric_cols=[], fh=fh)
                except Exception as e:
                    log(f"agents: SKIPPED after error, rest of refresh continues: {e}", fh)

            if ENABLE_CREDIT_MEMOS:
                # Isolated: a failure here is logged and skipped.
                try:
                    memos_csv = wait_for_result(tag, bridge_dir, memos_name, fh)
                    memo_comments_csv = wait_for_result(tag, bridge_dir, memo_comments_name, fh)
                    merge_credit_memos(DATA_DIR / "credit-memos.parquet", memos_csv,
                                       memo_comments_csv, fh)
                except Exception as e:
                    log(f"credit-memos: SKIPPED after error, rest of refresh continues: {e}", fh)

            if ENABLE_OPEN_ORDER_LINES:
                # Isolated: a failure here is logged and skipped. Both are full replaces.
                try:
                    ool_csv = wait_for_result(tag, bridge_dir, ool_name, fh)
                    replace_full(
                        DATA_DIR / "open-order-lines.parquet", ool_csv,
                        date_cols=["received_date", "request_date", "ship_by", "latest_ship_date",
                                   "ord_approved", "sku_rcpt", "art_due_date", "apb_ord_apr",
                                   "apb_art_apr", "apb_goods"],
                        numeric_cols=["line", "quantity_ordered", "quantity_wip"],
                        fh=fh,
                    )
                except Exception as e:
                    log(f"open-order-lines: SKIPPED after error, rest of refresh continues: {e}", fh)
                try:
                    whse_csv = wait_for_result(tag, bridge_dir, whse_name, fh)
                    replace_full(
                        DATA_DIR / "sku-warehouse-stock.parquet", whse_csv,
                        date_cols=[], numeric_cols=["size_seq", "available"], fh=fh,
                    )
                except Exception as e:
                    log(f"sku-warehouse-stock: SKIPPED after error, rest of refresh continues: {e}", fh)

            if ENABLE_SALES_ORDER_LINES:
                try:
                    sol_csv = wait_for_result(tag, bridge_dir, sol_name, fh)
                    replace_full(
                        DATA_DIR / "sales-order-lines.parquet", sol_csv,
                        date_cols=["received_date", "ordw_first", "ordw_last"],
                        numeric_cols=["order_sub_total", "quantity_ordered", "line", "sku_qty_ordered",
                                      "sku_ordered_amount", "misc_charge", "sku_cost"],
                        fh=fh,
                    )
                except Exception as e:
                    log(f"sales-order-lines: SKIPPED after error, rest of refresh continues: {e}", fh)

            if ENABLE_AR_AGING:
                # Isolated: a failure here is logged and skipped; the file keeps its earlier nights.
                try:
                    ar_csv = wait_for_result(tag, bridge_dir, ar_name, fh)
                    append_ar_aging(DATA_DIR / "ar-aging-daily.parquet", ar_csv, fh)
                except Exception as e:
                    log(f"ar-aging-daily: SKIPPED after error, rest of refresh continues: {e}", fh)

            if ENABLE_SHIPMENTS:
                # Isolated: so-shipments (Impress) then shipments.parquet (UPS Quantum
                # View + Tracking APIs, billing archive, rematch). A failure in either is
                # logged and skipped.
                try:
                    shipments_csv = wait_for_result(tag, bridge_dir, shipments_name, fh)
                    merge_replace_by_group(
                        DATA_DIR / "so-shipments.parquet", shipments_csv,
                        date_cols=["ship_date"],
                        numeric_cols=["boxes", "qty_shipped", "weight", "freight"],
                        group_col="sales_order",
                        fh=fh,
                    )
                except Exception as e:
                    log(f"so-shipments: SKIPPED after error, rest of refresh continues: {e}", fh)
                try:
                    ship_to_csv = wait_for_result(tag, bridge_dir, ship_to_name, fh)
                    merge_replace_by_group(
                        DATA_DIR / "so-ship-to.parquet", ship_to_csv,
                        date_cols=["received_date"], numeric_cols=[],
                        group_col="sales_order", fh=fh,
                    )
                except Exception as e:
                    log(f"so-ship-to: SKIPPED after error, rest of refresh continues: {e}", fh)
                try:
                    r = subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "ups_shipments.py")],
                                       cwd=REPO_ROOT, capture_output=True, text=True, timeout=1800)
                    log(f"ups_shipments.py exit {r.returncode}\n{r.stdout[-3000:]}{r.stderr[-2000:]}", fh)
                except Exception as e:
                    log(f"shipments: SKIPPED after error, rest of refresh continues: {e}", fh)

            write_last_refresh(fh)
            # No git commit: the data folder isn't a repo since 2026-11-18 (readers use it in place)
            log("done", fh)
        except Exception as e:
            log(f"FAILED: {e}", fh)
            raise
        finally:
            if INTRADAY_LOCK_PATH.exists() and INTRADAY_LOCK_PATH.read_text(encoding="utf-8").strip() == str(os.getpid()):
                INTRADAY_LOCK_PATH.unlink()


if __name__ == "__main__":
    main()
