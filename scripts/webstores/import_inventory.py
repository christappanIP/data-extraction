"""Copy the union webstores' inventory purchases from Image Pointe into data/webstores/ (Josh, 2026-10-13).

Union stores stock their merchandise by buying it from Image Pointe on their own Impress accounts ("UA 669 WEBSTORE
INVENTORY", "IBEW 26 WEBSTORE INVENTORY", ...). Those orders ARE Image Pointe sales and the sales dashboard already
counts them; the webstores page shows them only so a store's inventory buying sits beside what its members bought.
This is the one place the webstore data reads data/sales/, and it copies only these accounts' confirmed orders.

Accounts: every Impress customer whose name contains WEBSTORE / WEB STORE / ONLINE STORE, plus the numbers listed in
fee-stores.csv's inventory_customers column. Confirmed = order approved and not cancelled (shorted with nothing
invoiced); dated at the later of order and art approval, like the sales dashboard. Amount = booked merchandise.

Run: python import_inventory.py   (nightly, after the Impress refresh; fail-open)
"""
import os, sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.environ.get('SALES_DATA_DIR') or os.path.join(HERE, '..', '..', '..', 'data')  # the data folder (not a repo since 2026-11-18): SALES_DATA_DIR, or 'data' next to the repos
SALES, OUT = os.path.join(DATA_ROOT, 'sales'), os.path.join(DATA_ROOT, 'webstores')

try:
    ot = pd.read_parquet(os.path.join(SALES, 'order-tracking.parquet'),
                         columns=['sales_order', 'customer_number', 'order_sub_total', 'invoiced_amount', 'shorted', 'order_approved', 'art_approved'])
    cu = pd.read_parquet(os.path.join(SALES, 'customers.parquet'), columns=['Customer Number', 'Customer Name'])
except Exception as e:
    print(f'inventory purchases: SAA sales data not readable ({e}); keeping the last copy')
    sys.exit(0)
name = cu.drop_duplicates('Customer Number').set_index('Customer Number')['Customer Name'].astype(str).str.strip()
xw_p = os.path.join(OUT, 'fee-stores.csv')
extra = set()
if os.path.exists(xw_p):
    xw = pd.read_csv(xw_p, dtype=str, keep_default_na=False)
    if 'inventory_customers' in xw.columns:
        extra = {c.zfill(6) for v in xw.inventory_customers for c in v.split(';') if c}
ot['cust'] = ot.customer_number.map(name).fillna('')
keep = ot.cust.str.contains('WEBSTORE|WEB STORE|ONLINE STORE', case=False) | ot.customer_number.astype(str).str.zfill(6).isin(extra)
x = ot[keep & ot.order_approved.notna()].copy()
inv = pd.to_numeric(x.invoiced_amount, errors='coerce').fillna(0)
x = x[~((x.shorted == 'X') & (inv <= 0))]
x['confirmed'] = pd.to_datetime(x[['order_approved', 'art_approved']].max(axis=1), errors='coerce').dt.strftime('%Y-%m-%d')
d = pd.DataFrame(dict(cust_no=x.customer_number.astype(str).str.lstrip('0'), cust=x.cust, order=x.sales_order, confirmed=x.confirmed,
                      product=pd.to_numeric(x.order_sub_total, errors='coerce').fillna(0).round(2), invoiced=inv[x.index].round(2)))
d = d[d.confirmed >= '2024-01-01'].sort_values(['confirmed', 'order'])
d.to_parquet(os.path.join(OUT, 'inventory-purchases.parquet'), index=False)
print(f'inventory purchases: {len(d)} orders, {d.cust_no.nunique()} accounts, {d.confirmed.min()} to {d.confirmed.max()}, ${d["product"].sum():,.0f}')
