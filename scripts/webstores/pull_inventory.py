"""Pull current product stock from every union webstore (WooCommerce, read-only) into data/webstores/ (Josh, 2026-10-13).

One row per sellable item: a simple product, or one variation (size / color) of a variable product, with its SKU, its
stock setting and the quantity on hand the store shows. Uses the same "Union webstores" login and store list as
pull_webstores.py. Only GET requests are made. Stores whose products don't track stock show manage_stock = False and no
quantity (WooCommerce then only knows "in stock" / "out of stock").

Run: python pull_inventory.py        (nightly after the order pull; fail-open: a store that errors keeps its last rows)
Writes webstore-inventory.parquet and INVENTORY_PULL.json.
"""
import base64, json, os, sys, time, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timezone

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pull_webstores import OUT, load_sites  # noqa: E402

TEST = ('clonewebstore.', 'shippingteststore.', 'sandbox.', 'blankwebstore.', 'betabilitarian.')
COLS = ['store', 'product_id', 'variation_id', 'sku', 'name', 'options', 'product_type', 'status', 'manage_stock',
        'stock_quantity', 'stock_status', 'price', 'backorders']


def get(site, path, params):
    url = f"{site['url'].rstrip('/')}/wp-json/wc/v3/{path}?{urllib.parse.urlencode(params)}"
    auth = base64.b64encode(f"{site['user']}:{site['app_password']}".encode()).decode()
    req = urllib.request.Request(url, headers={'Authorization': f'Basic {auth}', 'User-Agent': 'SAA read-only pull'})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r), int(r.headers.get('X-WP-TotalPages', '1'))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < 3:
                time.sleep(3 * (attempt + 1)); continue
            raise
        except urllib.error.URLError:
            if attempt < 3:
                time.sleep(3 * (attempt + 1)); continue
            raise


def pages(site, path, params):
    p, out = 1, []
    while True:
        rows, total = get(site, path, dict(params, per_page=100, page=p))
        out += rows
        if p >= total or not rows:
            return out
        p += 1


def num(v):
    try:
        return float(v) if v not in (None, '') else None
    except (TypeError, ValueError):
        return None


def store_rows(site):
    rows = []
    for pr in pages(site, 'products', dict(status='any')):
        base = dict(store=site['store'], product_id=pr['id'], name=pr.get('name', ''), product_type=pr.get('type', ''), status=pr.get('status', ''))
        if pr.get('type') == 'variable':
            for v in pages(site, f"products/{pr['id']}/variations", {}):
                opts = ' / '.join(f"{a.get('option')}" for a in v.get('attributes', []) if a.get('option'))
                ms = v.get('manage_stock')
                rows.append(dict(base, variation_id=v['id'], sku=v.get('sku') or pr.get('sku') or '', options=opts,
                                 status=v.get('status') or base['status'], manage_stock=bool(ms) if ms != 'parent' else bool(pr.get('manage_stock')),
                                 stock_quantity=num(v.get('stock_quantity') if ms != 'parent' else pr.get('stock_quantity')),
                                 stock_status=v.get('stock_status', ''), price=num(v.get('price')), backorders=v.get('backorders', '')))
        else:
            rows.append(dict(base, variation_id=0, sku=pr.get('sku') or '', options='', manage_stock=bool(pr.get('manage_stock')),
                             stock_quantity=num(pr.get('stock_quantity')), stock_status=pr.get('stock_status', ''),
                             price=num(pr.get('price')), backorders=pr.get('backorders', '')))
    return rows


def main():
    sites, err = load_sites()
    if not sites:
        print(f'inventory: {err}; keeping the last pull'); return
    path = os.path.join(OUT, 'webstore-inventory.parquet')
    old = pd.read_parquet(path) if os.path.exists(path) else pd.DataFrame(columns=COLS)
    frames, failed, done = [], [], []
    for site in sites:
        if site['store'].startswith(TEST):
            continue
        try:
            r = store_rows(site)
            frames.append(pd.DataFrame(r, columns=COLS)); done.append(site['store'])
            print(f"  {site['store']}: {len(r)} items")
        except Exception as e:
            failed.append(dict(store=site['store'], error=f'{type(e).__name__}: {e}'[:200]))
            frames.append(old[old.store == site['store']])
            print(f"  {site['store']}: FAILED ({e})")
    d = pd.concat(frames, ignore_index=True)[COLS] if frames else old
    d.to_parquet(path, index=False)
    json.dump(dict(pulled_utc=datetime.now(timezone.utc).isoformat(timespec='seconds'), stores=len(done), items=int(len(d)),
                   stores_failed=failed), open(os.path.join(OUT, 'INVENTORY_PULL.json'), 'w'), indent=1)
    print(f'inventory: {len(d)} items from {len(done)} stores, {len(failed)} failed')


if __name__ == '__main__':
    main()
