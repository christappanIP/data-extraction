"""Import the two by-hand inputs for the customer-webstore shipping check into data/webstores/.

  1. ShipStation label costs: Tim's ShipStation export (CSV with Order #, Store, Ship Date, Service, Carrier Fee (IP Paid),
     Insurance Fee (IP Paid), Shipping Paid At Checkout, Postal Code). Recipient names are dropped.
  2. Union webstore bills: the parsed bill reports from ImagePointe-OS
     (projects/In process/budget-vs-actual-2026/scripts/parse_webstore_reports.py output in
     knowledge/reference files/shipping/derived/): webstore-reports-<year>-by-order.csv, -by-report.csv and
     -orders-vs-shipstation.csv (which bill store is which ShipStation store). Member names are dropped.

Run (either part on its own):
  python import_shipping.py --shipstation "<export.csv>" [--bills-dir "<derived folder>" --year 2026]
Writes shipstation-labels-<year>.parquet, union-bill-lines-<year>.parquet, union-bill-reports-<year>.parquet and
bill-stores.csv, and records each source and date in SOURCES.json. Until a ShipStation API key exists, re-run this when
Tim sends a new export or new union bills are parsed; the nightly build uses whatever was imported last.
"""
import json, os, sys
from datetime import datetime

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.environ.get('SALES_DATA_DIR') or os.path.join(HERE, '..', '..', '..', 'data')  # the data folder (not a repo since 2026-11-18): SALES_DATA_DIR, or 'data' next to the repos
OUT = os.path.abspath(os.path.join(DATA_ROOT, 'webstores'))
SRC = os.path.join(OUT, 'SOURCES.json')


def arg(name):
    a = sys.argv[1:]
    return a[a.index(name) + 1] if name in a else None


def note(key, **kw):
    s = json.load(open(SRC)) if os.path.exists(SRC) else {}
    s[key] = dict(kw, imported=datetime.now().strftime('%Y-%m-%d %H:%M'))
    json.dump(s, open(SRC, 'w'), indent=1)


def shipstation(path):
    s = pd.read_csv(path, low_memory=False, dtype={'Order #': str, 'Postal Code': str}, encoding_errors='replace')
    num = lambda c: pd.to_numeric(s[c], errors='coerce').fillna(0)
    d = pd.DataFrame(dict(ss_store=s['Store'].astype(str).str.strip(), order=s['Order #'].astype(str).str.strip(),
                          zip5=s['Postal Code'].astype(str).str[:5], ship_date=pd.to_datetime(s['Ship Date'], errors='coerce').dt.strftime('%Y-%m-%d'),
                          service=s['Service'], label=num('Carrier Fee (IP Paid)'), insurance=num('Insurance Fee (IP Paid)'),
                          paid_checkout=num('Shipping Paid At Checkout')))
    d = d[d.ship_date.notna()]
    years = sorted(d.ship_date.str[:4].unique())
    for y in years:
        d[d.ship_date.str[:4] == y].to_parquet(os.path.join(OUT, f'shipstation-labels-{y}.parquet'), index=False)
    note('shipstation', file=os.path.basename(path), ship_dates=[d.ship_date.min(), d.ship_date.max()], shipments=int(len(d)))
    print(f'labels: {len(d)} shipments {d.ship_date.min()} to {d.ship_date.max()} -> shipstation-labels-{",".join(years)}.parquet')


def bills(folder, year):
    p = lambda n: os.path.join(folder, f'webstore-reports-{year}-{n}.csv')
    b = pd.read_csv(p('by-order'), dtype={'order_number': str})
    v = pd.read_csv(p('orders-vs-shipstation'))
    r = pd.read_csv(p('by-report'))
    m = v.dropna(subset=['ss']).groupby('store').ss.agg(lambda x: x.mode().iat[0]).rename('ss_store').reset_index() \
         .rename(columns={'store': 'bill_store'})
    m.to_csv(os.path.join(OUT, 'bill-stores.csv'), index=False)
    lines = pd.DataFrame(dict(bill_store=b.store, period=b.period.astype(str), file_name=b.file_name,
                              order=b.order_number.astype(str).str.replace(r'\.0$', '', regex=True).str.strip(),
                              handling=b.handling, shipping=b.shipping, amount=b.amount, discounts=b.discounts, due=b.due))
    lines.to_parquet(os.path.join(OUT, f'union-bill-lines-{year}.parquet'), index=False)
    keep = ['sent_date', 'store', 'period', 'quarter', 'file_name', 'n_order_lines', 'n_orders', 'sum_amount', 'sum_discounts',
            'sum_handling', 'sum_shipping', 'processing_fees', 'sales_tax', 'net_due', 'net_direction', 'notes']
    r[[c for c in keep if c in r]].rename(columns={'store': 'bill_store'}).to_parquet(os.path.join(OUT, f'union-bill-reports-{year}.parquet'), index=False)
    note(f'union_bills_{year}', folder=os.path.basename(folder.rstrip('/\\')), reports=int(len(r)), lines=int(len(lines)),
         last_sent=str(r.sent_date.max()), periods=sorted(r.period.astype(str).unique().tolist())[-3:])
    print(f'bills {year}: {len(r)} reports, {len(lines)} order lines, {len(m)} bill stores mapped to ShipStation')


def add_da_units(d):
    """Garment units on each split-ship order, and how many are Dignity Apparel, from SAA's Impress order lines
    (data/sales/orders.parquet; read here at import, never by the page build). DA = SKU starting "DA", plus DA garments
    from customer-owned stock under a customer-prefixed SKU (e.g. SEIUDASSTRI003BP), counted separately. Used for the
    free-shipping rule: until 2026-10-12 DA orders over 144 units shipped free even when split; since then free
    shipping is only for orders going to one address (Josh, 2026-10-13)."""
    p = os.path.join(OUT, '..', 'sales', 'orders.parquet')
    if not os.path.exists(p):
        return d
    o = pd.read_parquet(p, columns=['sales_order', 'sku', 'size_qty_ordered', 'size_ordered_amount'])
    o = o[o.sales_order.isin(d.ord)].copy()
    o['q'] = pd.to_numeric(o.size_qty_ordered, errors='coerce').fillna(0)
    o['a'] = pd.to_numeric(o.size_ordered_amount, errors='coerce').fillna(0)
    sku = o.sku.fillna('').astype(str)
    o['da'] = sku.str.startswith('DA')
    o['da_cs'] = ~o.da & sku.str.match(r'^[A-Z0-9]{2,8}DA(?:SS|ST|HL|HT|ZT|LS|P0)')
    g = o.groupby('sales_order').agg(units=('q', 'sum'))
    g['da_units'] = o[o.da].groupby('sales_order').q.sum()
    g['da_cs_units'] = o[o.da_cs].groupby('sales_order').q.sum()
    g['da_product'] = o[o.da].groupby('sales_order').a.sum()
    d = d.drop(columns=[c for c in g.columns if c in d.columns]).merge(g.fillna(0).reset_index().rename(columns={'sales_order': 'ord'}), on='ord', how='left')
    return d


def split_ship(path):
    """Impress split-ship billing check (ImagePointe-OS impress_split_ship_orders.sql + split_ship_billed_vs_ups.py output):
    one row per Impress order whose ship-to says SPLIT SHIP, with what was billed and the matched UPS cost. A billing
    snapshot for the gaps panel; it is Impress order data, refreshed only when that check is re-run."""
    d = pd.read_csv(path)
    keep = ['ord', 'recd', 'cust_name', 'grp', 'product', 'billed', 'split_chg', 'other_chg', 'ship_hdr', 'ups_cost', 'has_cost',
            'dests', 'shipments', 'inv', 'first_inv', 'inv_shipvia']
    d = d[d.cancelled.astype(str).str.strip() != 'X'][keep]
    d = add_da_units(d)
    d.to_parquet(os.path.join(OUT, 'impress-split-ship-orders.parquet'), index=False)
    note('split_ship', file=os.path.basename(path), orders=int(len(d)), received=[str(d.recd.min()), str(d.recd.max())])
    print(f'split ship: {len(d)} orders received {d.recd.min()} to {d.recd.max()}')


if __name__ == '__main__':
    os.makedirs(OUT, exist_ok=True)
    if arg('--shipstation'):
        shipstation(arg('--shipstation'))
    if arg('--bills-dir'):
        bills(arg('--bills-dir'), arg('--year') or str(datetime.now().year))
    if arg('--split-ship'):
        split_ship(arg('--split-ship'))
    if '--split-da' in sys.argv:          # add DA units to the existing split-ship snapshot without a new export
        f_ = os.path.join(OUT, 'impress-split-ship-orders.parquet')
        add_da_units(pd.read_parquet(f_)).to_parquet(f_, index=False)
        print('split ship: DA units added')
    if not (arg('--shipstation') or arg('--bills-dir') or arg('--split-ship') or '--split-da' in sys.argv):
        print(__doc__)
