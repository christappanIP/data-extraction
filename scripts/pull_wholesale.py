"""Pull DA Wholesale orders from the WooCommerce wholesale store (wholesale.dignityapparel.com) into
data/dashboard/wholesale-orders.csv for the sales dashboard. Read-only: GET requests only.

Wholesale orders aren't entered in Impress (since mid-2025), so the dashboard adds these to the DA Wholesale
rep (Josh, 2026-10-12: every completed order counts). One row per order: completed or processing orders
created from Jan 1 two years back through today, with item sales after discounts (no shipping or tax) and
units by tee / fleece / polo (Impress SKU garment type, same rules as the DA units section).

Credentials stay outside the repo: %USERPROFILE%\\.woocommerce\\sites.json, a list of
{"name": "DA Wholesale", "url": "https://wholesale.dignityapparel.com/", "key": "ck_...", "secret": "cs_..."}
(a read-only WooCommerce REST key). Fail-open: no config or an API error leaves the committed CSV as-is.
Usage: python pull_wholesale.py
"""
import base64, csv, json, os, sys, time, urllib.error, urllib.parse, urllib.request
from datetime import date, timedelta

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)   # data-extraction (moved from the sales-dashboard skill 2026-11-18)
DATA_ROOT = os.environ.get('SALES_DATA_DIR') or os.path.join(os.path.dirname(REPO), 'data')  # the data folder (not a repo since 2026-11-18): SALES_DATA_DIR, or 'data' next to the repos
OUT = os.path.join(DATA_ROOT, 'dashboard', 'wholesale-orders.csv')
CFG = os.path.join(os.path.expanduser('~'), '.woocommerce', 'sites.json')
SITE = 'DA Wholesale'
COLS = ['number', 'created', 'status', 'cust_key', 'cust_name', 'state', 'sales', 'tee', 'fleece', 'polo', 'other']


def get(site, params):
    url = f"{site['url'].rstrip('/')}/wp-json/wc/v3/orders?{urllib.parse.urlencode(params)}"
    auth = base64.b64encode(f"{site['key']}:{site['secret']}".encode()).decode()
    req = urllib.request.Request(url, headers={'Authorization': f'Basic {auth}', 'User-Agent': 'SAA read-only pull'})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r), int(r.headers.get('X-WP-TotalPages', '1'))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < 3:
                time.sleep(5 * (attempt + 1)); continue
            raise


def money(x):
    try:
        return round(float(x or 0), 2)
    except ValueError:
        return 0.0


def main():
    try:
        site = next(s for s in json.load(open(CFG, encoding='utf-8-sig')) if s.get('name') == SITE)
    except (OSError, StopIteration, ValueError) as e:
        print(f'wholesale: no "{SITE}" entry in {CFG} ({type(e).__name__}); keeping the committed CSV'); return
    sku = pd.read_parquet(os.path.join(DATA_ROOT, 'sales', 'sku.parquet'), columns=['sku', 'garment_type']).drop_duplicates('sku')
    gt = dict(zip(sku.sku, sku.garment_type.fillna('').str.lower()))
    codes = sorted(gt, key=len, reverse=True)
    cache = {}

    def fam(w):                          # store SKUs are "W" + Impress style/color + size: WDASST001RED2X
        w = str(w or ''); w = w[1:] if w.startswith('W') else w
        if w not in cache:
            f = 'other'
            if w.startswith(('DAHLSTRI003', 'DAHTRI003', 'DAZTRI003')):
                f = 'polo'                   # triblend hoods count as polos (Josh, 2026-10-10)
            else:
                c = next((c for c in codes if w.startswith(c)), None)
                g = gt.get(c, '')
                f = 'tee' if 'tee' in g else 'fleece' if g == 'sweatshirt' else 'polo' if g == 'polo' else 'other'
            cache[w] = f
        return cache[w]

    start = date(date.today().year - 2, 1, 1); end = date.today() + timedelta(days=1)
    rows, page, pages = [], 1, 1
    try:
        while page <= pages:
            data, pages = get(site, {'after': f'{start}T00:00:00', 'before': f'{end}T00:00:00', 'per_page': 100,
                                     'page': page, 'status': 'any', 'orderby': 'date', 'order': 'asc'})
            for o in data:
                if o['status'] not in ('completed', 'processing'):
                    continue
                b, s = o.get('billing') or {}, o.get('shipping') or {}
                name = (b.get('company') or s.get('company') or f"{b.get('first_name', '')} {b.get('last_name', '')}").strip() or 'Wholesale customer'
                cid = o.get('customer_id') or 0
                u = {'tee': 0, 'fleece': 0, 'polo': 0, 'other': 0}
                for i in o.get('line_items', []):
                    u[fam(i.get('sku'))] += int(i.get('quantity') or 0)
                rows.append([o['number'], o['date_created'][:16].replace('T', ' '), o['status'],
                             f'W{cid}' if cid else 'W:' + name.upper()[:40], name[:60], (s.get('state') or b.get('state') or '')[:2],
                             round(sum(money(i.get('total')) for i in o.get('line_items', [])), 2),
                             u['tee'], u['fleece'], u['polo'], u['other']])
            page += 1
    except Exception as e:                   # fail-open: yesterday's CSV stays
        print(f'wholesale: WARNING pull failed ({type(e).__name__}: {str(e)[:200]}); keeping the committed CSV'); return
    if not rows:
        print('wholesale: WARNING no orders returned; keeping the committed CSV'); return
    with open(OUT, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f); w.writerow(COLS); w.writerows(sorted(rows, key=lambda r: r[1]))
    df = pd.DataFrame(rows, columns=COLS)
    print(f"wholesale: {len(df)} orders {start}..{end - timedelta(days=1)}, this year ${df[df.created.str[:4] == str(date.today().year)].sales.sum():,.0f}")


if __name__ == '__main__':
    sys.exit(main())
