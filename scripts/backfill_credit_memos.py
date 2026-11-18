"""Full re-pull of credit-memos.parquet: every credit (C) and debit (D) memo since
Impress began (2016-09-16 is the first), with comments. The nightly refresh only
re-pulls the last LOOKBACK_DAYS, so run this if old memos or their comments were
edited, or to rebuild the file from scratch. Safe to re-run: it merges on invoice_nbr.
Read-only against Impress, via the same query bridge as daily_refresh.py."""
import sys
import uuid

import daily_refresh as d


def main():
    tag, bridge_dir = d.get_tag_and_bridge_dir()
    run_id = uuid.uuid4().hex[:8]
    memos_name = f"backfill_credit_memos_{run_id}"
    comments_name = f"backfill_credit_memo_comments_{run_id}"
    d.submit_query(tag, bridge_dir, memos_name, d.credit_memos_query("2000-01-01"), sys.stdout)
    d.submit_query(tag, bridge_dir, comments_name, d.credit_memo_comments_query("2000-01-01"), sys.stdout)
    memos_csv = d.wait_for_result(tag, bridge_dir, memos_name, sys.stdout)
    comments_csv = d.wait_for_result(tag, bridge_dir, comments_name, sys.stdout)
    d.merge_credit_memos(d.DATA_DIR / "credit-memos.parquet", memos_csv, comments_csv, sys.stdout)


if __name__ == "__main__":
    main()
