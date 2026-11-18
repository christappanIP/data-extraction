"""One-time (re-runnable) full-history backfill of data/sales/order-notes-all.parquet.

Pulls every SORDERC note (customer C + internal I, all orders) through the query bridge in
received-date chunks, so no single result CSV is huge, then append+dedupes into the parquet
on (sales_order, kind, seq) exactly like the nightly refresh does. Safe to re-run: it
refreshes the text of every note, which also picks up notes edited on orders older than the
nightly LOOKBACK_DAYS window.

Usage (on CHI-TOWN or another machine in daily_refresh.BRIDGE_FOLDERS):
    python scripts/backfill_order_notes_all.py
"""
import sys
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import daily_refresh as dr  # noqa: E402

# [since, until) on SORDER_RECDATE; the first chunk also takes notes with no order date.
CHUNKS = [(None, "2016-01-01"), ("2016-01-01", "2019-01-01"), ("2019-01-01", "2021-01-01"),
          ("2021-01-01", "2023-01-01"), ("2023-01-01", "2025-01-01"), ("2025-01-01", None)]


def main():
    log_path = dr.LOG_DIR / f"backfill_order_notes_all_{datetime.now():%Y-%m-%d_%H%M%S}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w", encoding="utf-8") as fh:
        tag, bridge_dir = dr.get_tag_and_bridge_dir()
        run_id = uuid.uuid4().hex[:8]
        # The first query in a bridge batch often times out connecting to SQL (cold
        # connection), so lead with a throwaway query and ignore its result. The bridge
        # runs files in name order, so the warm-up name must sort first.
        dr.submit_query(tag, bridge_dir, f"aaa_warmup_{run_id}",
                        "SELECT TOP 1 SORDERC_SORDER_CODE FROM SORDERC;", fh)
        names = []
        for i, (since, until) in enumerate(CHUNKS):
            name = f"backfill_order_notes_all_{run_id}_{i}"
            sql = dr.order_notes_all_query(since, until, include_undated=(i == 0))
            dr.submit_query(tag, bridge_dir, name, sql, fh)
            names.append(name)
        out = dr.DATA_DIR / "order-notes-all.parquet"
        failed = []
        for i, name in enumerate(names):
            csv_path = None
            for attempt in (1, 2):
                try:
                    csv_path = dr.wait_for_result(tag, bridge_dir, name, fh)
                    break
                except RuntimeError as e:
                    # The bridge's SQL connection times out intermittently; resubmit once.
                    dr.log(f"chunk {i} attempt {attempt} failed: {e}", fh)
                    if attempt == 1:
                        name = f"{name}_r"
                        since, until = CHUNKS[i]
                        dr.submit_query(tag, bridge_dir, name,
                                        dr.order_notes_all_query(since, until, include_undated=(i == 0)), fh)
            if csv_path is None:
                failed.append(i)
                continue
            dr.merge_append_dedupe(out, csv_path, date_cols=[], numeric_cols=[],
                                   key_cols=["sales_order", "kind", "seq"], fh=fh)
        dr.log(f"backfill done -> {out}; failed chunks: {failed or 'none'}", fh)
        if failed:
            sys.exit(1)


if __name__ == "__main__":
    main()
