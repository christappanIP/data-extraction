"""
Early-morning refresh of open-order-lines.parquet and design-codes.parquet (Traci's 2026-11-05 request,
Josh's go-ahead the same day).

Why: second shift scans until about midnight, after the 11 p.m. nightly (daily_refresh.py) has pulled, and
the intraday refresh (gdot_hourly_refresh.py) only runs 7 a.m.-7 p.m. So the 5 a.m. Ops reports (Due Dates
5:00, purchasing 5:02, LOG OPEN 5:04, DSJ 5:06, production 5:08) missed second shift's late work. This
re-pulls the two files they take line status and decorations from, weekdays at 4:45 a.m.
(task ImagePointe-EarlyRefresh -> scripts/early_task_launcher.ps1).

How:
- The queries and merge helpers are imported from daily_refresh.py, so they stay the nightly's own.
  open-order-lines is a full replace. design-codes replaces each re-pulled order's rows as a group (every
  open order plus the LOOKBACK_DAYS window), and every open order counts as re-pulled, so an order whose
  decorations were all deleted in Impress is cleared. The column type lists below copy the nightly's
  merge calls; keep them in step.
- Deadline (--deadline HH:MM; the task passes 04:56): a result that isn't back by then is skipped and that
  file keeps the nightly's data, so the 5 a.m. reports never read a file mid-write. A run that starts after
  the deadline (a late catch-up start) does nothing. A hand run without --deadline gets 10 minutes.
- Writes data/sales/EARLY_REFRESH.json (time, row counts, anything skipped). LAST_REFRESH.json is left
  alone: it stays the nightly's marker, which the reports' stale-data check reads.
- Shares the intraday lock with the nightly and hourly refreshes. Since 2026-11-18 the data folder isn't a git
  repo, so nothing is committed: readers use the files in place.

Usage:  python scripts/early_refresh.py [--deadline 04:56]
"""
import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import daily_refresh as dr  # noqa: E402

DATA_DIR = dr.DATA_DIR
LOG_DIR = dr.LOG_DIR
LOCK_PATH = dr.INTRADAY_LOCK_PATH
LOCK_STALE_MIN = 45
MARKER = DATA_DIR / "EARLY_REFRESH.json"
HAND_RUN_SEC = 600

OOL_DATE_COLS = ["received_date", "request_date", "ship_by", "latest_ship_date", "ord_approved", "sku_rcpt",
                 "art_due_date", "apb_ord_apr", "apb_art_apr", "apb_goods"]
OOL_NUMERIC_COLS = ["line", "quantity_ordered", "quantity_wip"]
DC_DATE_COLS = ["received"]
DC_NUMERIC_COLS = ["stitches_or_screens", "master_count"]


def deadline_from_args():
    if "--deadline" not in sys.argv:
        return time.time() + HAND_RUN_SEC
    h, m = (int(x) for x in sys.argv[sys.argv.index("--deadline") + 1].split(":"))
    return datetime.now().replace(hour=h, minute=m, second=0, microsecond=0).timestamp()


def take_lock(deadline, fh):
    """Wait for a nightly/hourly run holding the shared lock, up to a minute before the deadline."""
    while LOCK_PATH.exists():
        age_min = (time.time() - LOCK_PATH.stat().st_mtime) / 60
        if age_min > LOCK_STALE_MIN:
            dr.log(f"removing stale intraday lock ({age_min:.0f} min old)", fh)
            break
        if time.time() > deadline - 60:
            return False
        time.sleep(15)
    LOCK_PATH.write_text(str(os.getpid()), encoding="utf-8")
    return True


def result_by(deadline, tag, bridge_dir, name, fh):
    """The result CSV, or None if it failed or isn't back by the deadline."""
    left = int(deadline - time.time())
    if left <= 0:
        dr.log(f"{name}: past the deadline, skipped", fh)
        return None
    dr.POLL_TIMEOUT_SEC = left   # read by dr.wait_for_result on each call
    try:
        return dr.wait_for_result(tag, bridge_dir, name, fh)
    except Exception as e:
        dr.log(f"{name}: skipped, {e}", fh)
        return None


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"early_refresh_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log"
    deadline = deadline_from_args()
    with open(log_path, "w", encoding="utf-8") as fh:
        if time.time() >= deadline:
            dr.log("started after the deadline; nothing pulled (the reports keep the nightly's data)", fh)
            return 0
        if not take_lock(deadline, fh):
            dr.log("another refresh still holds the lock at the deadline; nothing pulled", fh)
            return 1
        refreshed, skipped = {}, []
        try:
            tag, bridge_dir = dr.get_tag_and_bridge_dir()
            run_id = uuid.uuid4().hex[:8]
            since_date = (datetime.now() - timedelta(days=dr.LOOKBACK_DAYS)).strftime("%Y-%m-%d")
            dr.log(f"running on {tag}; deadline {datetime.fromtimestamp(deadline):%H:%M:%S}", fh)
            ool_name = f"early_open_order_lines_{run_id}"
            dc_name = f"early_design_codes_{run_id}"
            dr.submit_query(tag, bridge_dir, ool_name, dr.open_order_lines_query(), fh)
            dr.submit_query(tag, bridge_dir, dc_name, dr.design_codes_query(since_date), fh)

            open_orders = set()
            ool_csv = result_by(deadline, tag, bridge_dir, ool_name, fh)
            if ool_csv:
                try:
                    rows = dr.replace_full(DATA_DIR / "open-order-lines.parquet", ool_csv,
                                           date_cols=OOL_DATE_COLS, numeric_cols=OOL_NUMERIC_COLS, fh=fh)
                    open_orders = set(pd.read_csv(ool_csv, dtype=str, encoding="utf-8-sig",
                                                  usecols=["sales_order"])["sales_order"].dropna())
                    refreshed["open-order-lines"] = {"rows": rows, "open_orders": len(open_orders)}
                except Exception as e:
                    dr.log(f"open-order-lines: merge failed, kept the nightly's file: {e}", fh)
            if "open-order-lines" not in refreshed:
                skipped.append("open-order-lines")

            dc_csv = result_by(deadline, tag, bridge_dir, dc_name, fh)
            if dc_csv:
                try:
                    rows = dr.merge_replace_by_group(DATA_DIR / "design-codes.parquet", dc_csv,
                                                     date_cols=DC_DATE_COLS, numeric_cols=DC_NUMERIC_COLS,
                                                     group_col="ord", fh=fh, extra_groups=open_orders)
                    refreshed["design-codes"] = {"rows": rows}
                except Exception as e:
                    dr.log(f"design-codes: merge failed, kept the nightly's file: {e}", fh)
            if "design-codes" not in refreshed:
                skipped.append("design-codes")

            now = datetime.now(timezone.utc)
            MARKER.write_text(json.dumps({
                "early_refresh_utc": now.isoformat(timespec="seconds"),
                "early_refresh_local": now.astimezone().isoformat(timespec="seconds"),
                "files": refreshed,
                "skipped": skipped,
            }, indent=2), encoding="utf-8")
            dr.log(f"wrote {MARKER.name}: refreshed {sorted(refreshed) or 'nothing'}"
                   + (f", skipped {skipped}" if skipped else ""), fh)
        finally:
            if LOCK_PATH.exists() and LOCK_PATH.read_text(encoding="utf-8").strip() == str(os.getpid()):
                LOCK_PATH.unlink()
        dr.log("done", fh)
        return 0 if not skipped else 1


if __name__ == "__main__":
    sys.exit(main())
