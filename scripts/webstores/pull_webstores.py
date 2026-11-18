"""Pull union / customer webstore orders (WooCommerce, read-only) into data/webstores/.

CUSTOMER WEBSTORE DATA IS NOT IMAGE POINTE SALES. Members pay the store at checkout; the union is billed (or paid) through
the monthly / quarterly webstore reports, and that revenue reaches Impress through those bills. Nothing here is ever read
by the sales dashboard or counted in sales, goals or rep figures (Josh, 2026-10-12). See data/webstores/README.md.

Credentials stay outside the repo: %USERPROFILE%\\.woocommerce\\sites.json, entry "Union webstores":
    {"name": "Union webstores", "user": "ip-reporting", "app_password": "...", "urls_file": "~/.woocommerce/webstore-urls.txt"}
A network super admin's WordPress application password works on every store; urls_file lists one store per line.
The DA Wholesale store has its own entry and pipeline (sales-dashboard/scripts/pull_wholesale.py) and is skipped here.

Run:
  python pull_webstores.py                  nightly: orders created or modified in the last 45 days, merged into the year files
  python pull_webstores.py --since 2025-01-01   full re-pull from a date (about 15 minutes for every store)
  python pull_webstores.py --seed <dir>     one-time load from woo_pull.py CSVs (ImagePointe-OS reference files)
Fail-open: no credentials, or a store that errors, keeps what is already committed for that store.
Only GET requests are made. Since 2026-10-28 (Josh: customer data is fine in SAA) each order also keeps the member's
billing name, e-mail and phone, the full ship-to address, and the customer note (batch imports like "IUPAT BUP kit
import 9-21-26" are labelled there). None of that goes on the page.
"""
import base64, json, os, re, sys, time, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.environ.get('SALES_DATA_DIR') or os.path.join(HERE, '..', '..', '..', 'data')  # the data folder (not a repo since 2026-11-18): SALES_DATA_DIR, or 'data' next to the repos
OUT = os.path.abspath(os.path.join(DATA_ROOT, 'webstores'))
CFG = os.path.join(os.path.expanduser('~'), '.woocommerce', 'sites.json')
ENTRY = 'Union webstores'
SKIP_HOSTS = {'wholesale.dignityapparel.com'}        # DA Wholesale: separate pipeline, separate key
DAYS = 45
FLOOR = '2025-01-01'     # the data starts here; older orders that come back as recently modified are left out
KEY = ['store', 'order_id']
OCOLS = ['store', 'order_id', 'number', 'status', 'created', 'paid', 'completed', 'modified', 'payment_method',
         'billing_company', 'ship_state', 'ship_zip5', 'coupons', 'item_subtotal', 'discount', 'item_sales', 'units',
         'checkout_shipping', 'shipping_method', 'handling_fee', 'insurance_fee', 'other_fees', 'hidden_handling_fee',
         'tax', 'total', 'refunded', 'customer_note', 'bill_name', 'bill_email', 'bill_phone', 'ship_name', 'ship_company',
         'ship_address1', 'ship_address2', 'ship_city', 'ship_zip', 'ship_country', 'ship_phone']
ICOLS = ['store', 'order_id', 'number', 'created', 'sku', 'name', 'quantity', 'subtotal', 'total']


def money(x):
    try:
        return round(float(x or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def fee_split(fees):
    """fees: list of (name, total) -> handling, insurance, other."""
    h = sum(t for n, t in fees if 'handling' in str(n).lower())
    i = sum(t for n, t in fees if 'insur' in str(n).lower())
    return round(h, 2), round(i, 2), round(sum(t for _, t in fees) - h - i, 2)


def load_sites():
    try:
        entry = next(s for s in json.load(open(CFG, encoding='utf-8-sig')) if s.get('name') == ENTRY)
    except (OSError, StopIteration, ValueError) as e:
        return None, f'no "{ENTRY}" entry in {CFG} ({type(e).__name__})'
    urls = list(entry.get('urls') or [])
    if entry.get('urls_file'):
        with open(os.path.expanduser(entry['urls_file']), encoding='utf-8-sig') as f:
            for line in f:   # accepts a raw copy-paste of Network Admin > Sites (skips e-mail addresses)
                urls += [f'https://{h}/' for h in re.findall(r'(?<![@\w.-])((?:[a-z0-9-]+\.)+[a-z]{2,})(?![\w@-])', line.lower())]
    sites = []
    for u in dict.fromkeys(urls):
        host = (urllib.parse.urlparse(u).netloc + urllib.parse.urlparse(u).path).rstrip('/')
        if host not in SKIP_HOSTS:
            sites.append(dict(entry, url=u, store=host))
    return sites, None


def get(site, params):
    url = f"{site['url'].rstrip('/')}/wp-json/wc/v3/orders?{urllib.parse.urlencode(params)}"
    auth = base64.b64encode(f"{site['user']}:{site['app_password']}".encode()).decode()
    req = urllib.request.Request(url, headers={'Authorization': f'Basic {auth}', 'User-Agent': 'SAA read-only pull'})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r), int(r.headers.get('X-WP-TotalPages', '1'))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < 3:
                time.sleep(5 * (attempt + 1)); continue
            raise


def rows_from_api(store, o):
    items = o.get('line_items', [])
    meta = {m['key']: m['value'] for m in o.get('meta_data', []) if isinstance(m.get('value'), (str, int, float))}
    h, ins, oth = fee_split([(f.get('name'), money(f.get('total'))) for f in o.get('fee_lines', [])])
    ship, bill = o.get('shipping', {}) or {}, o.get('billing', {}) or {}
    name = lambda a: f"{a.get('first_name') or ''} {a.get('last_name') or ''}".strip() or None
    order = dict(store=store, order_id=int(o['id']), number=str(o.get('number')), status=o['status'],
                 created=o.get('date_created'), paid=o.get('date_paid'), completed=o.get('date_completed'), modified=o.get('date_modified'),
                 payment_method=o.get('payment_method'), billing_company=bill.get('company') or None,
                 ship_state=ship.get('state') or None, ship_zip5=(ship.get('postcode') or '')[:5] or None,
                 coupons=';'.join(c['code'] for c in o.get('coupon_lines', [])) or None,
                 item_subtotal=round(sum(money(i['subtotal']) for i in items), 2), discount=money(o.get('discount_total')),
                 item_sales=round(sum(money(i['total']) for i in items), 2), units=int(sum(i.get('quantity') or 0 for i in items)),
                 checkout_shipping=money(o.get('shipping_total')),
                 shipping_method='; '.join(s.get('method_title') or '' for s in o.get('shipping_lines', [])) or None,
                 handling_fee=h, insurance_fee=ins, other_fees=oth, hidden_handling_fee=money(meta.get('hidden_handling_fee_amount')),
                 tax=money(o.get('total_tax')), total=money(o.get('total')),
                 refunded=round(-sum(money(r.get('total')) for r in o.get('refunds', [])), 2),
                 customer_note=(o.get('customer_note') or '').strip() or None,
                 bill_name=name(bill), bill_email=bill.get('email') or None, bill_phone=bill.get('phone') or None,
                 ship_name=name(ship), ship_company=ship.get('company') or None, ship_address1=ship.get('address_1') or None,
                 ship_address2=ship.get('address_2') or None, ship_city=ship.get('city') or None, ship_zip=ship.get('postcode') or None,
                 ship_country=ship.get('country') or None, ship_phone=ship.get('phone') or None)
    its = [dict(store=store, order_id=int(o['id']), number=str(o.get('number')), created=o.get('date_created'), sku=i.get('sku') or None,
                name=i.get('name'), quantity=int(i.get('quantity') or 0), subtotal=money(i.get('subtotal')), total=money(i.get('total')))
           for i in items]
    return order, its


def pull_store(site, since):
    """Orders created OR modified since `since` (a status change or refund on an older order is picked up too)."""
    got = {}
    for field in ('after', 'modified_after'):
        page, pages = 1, 1
        while page <= pages:
            data, pages = get(site, {field: f'{since}T00:00:00', 'per_page': 100, 'page': page, 'status': 'any', 'orderby': 'date', 'order': 'asc'})
            for o in data:
                got[o['id']] = o
            page += 1
    return [rows_from_api(site['store'], o) for o in got.values()]


def load_existing(kind):
    fs = sorted(f for f in os.listdir(OUT) if re.fullmatch(rf'{kind}-\d{{4}}\.parquet', f))
    return pd.concat([pd.read_parquet(os.path.join(OUT, f)) for f in fs], ignore_index=True) if fs else None


def write_years(df, kind, cols):
    df = df.reindex(columns=cols)
    yr = df.created.astype(str).str[:4]
    for y in sorted(yr.unique()):
        part = df[yr == y].sort_values(['store', 'created', 'order_id']).reset_index(drop=True)
        part.to_parquet(os.path.join(OUT, f'{kind}-{y}.parquet'), index=False)


def merge(old, new, cols, since, stores_ok):
    """Replace each successfully pulled store's window (created >= since) and upsert re-pulled orders by key."""
    if old is None or old.empty:
        return new[cols]
    old = old.reindex(columns=cols)     # files written before a column was added (customer_note, 2026-10-28)
    if new.empty:
        return old
    drop = old.store.isin(stores_ok) & (old.created.astype(str) >= since)
    drop |= old.set_index(KEY).index.isin(new.set_index(KEY).index)
    return pd.concat([old[~drop], new], ignore_index=True)[cols]


def seed(src):
    o = pd.read_csv(os.path.join(src, 'woo_orders.csv'), dtype={'number': str, 'ship_postcode': str, 'billing_company': str, 'coupons': str})
    it = pd.read_csv(os.path.join(src, 'woo_items.csv'), dtype={'number': str, 'sku': str})
    sh = pd.read_csv(os.path.join(src, 'woo_shipping.csv'), dtype={'number': str})
    fe = pd.read_csv(os.path.join(src, 'woo_fees.csv'), dtype={'number': str})
    o = o[~o.site.isin(SKIP_HOSTS)]
    fees = fe.groupby(['site', 'order_id']).apply(lambda g: fee_split(list(zip(g['name'], g['total']))), include_groups=False)
    ships = sh.groupby(['site', 'order_id']).method_title.agg(lambda s: '; '.join(s.fillna('')))
    agg_i = it.groupby(['site', 'order_id']).agg(units=('quantity', 'sum'), item_sales=('total', 'sum'))
    hh = o.store_meta.fillna('').str.extract(r'"hidden_handling_fee_amount": "([\d.]+)"')[0]
    x = o.set_index(['site', 'order_id'])
    f3 = fees.reindex(x.index)
    orders = pd.DataFrame(dict(
        store=o.site.values, order_id=o.order_id.astype(int).values, number=o.number.values, status=o.status.values,
        created=o.created.values, paid=None, completed=o.completed.values, modified=None, payment_method=o.payment_method.values,
        billing_company=o.billing_company.values, ship_state=o.ship_state.values, ship_zip5=o.ship_postcode.fillna('').str[:5].replace('', None).values,
        coupons=o.coupons.values, item_subtotal=o.subtotal_items.values, discount=o.discount_total.values,
        item_sales=agg_i.item_sales.reindex(x.index).fillna(0).round(2).values, units=agg_i.units.reindex(x.index).fillna(0).astype(int).values,
        checkout_shipping=o.shipping_total.values, shipping_method=ships.reindex(x.index).values,
        handling_fee=[v[0] if isinstance(v, tuple) else 0.0 for v in f3], insurance_fee=[v[1] if isinstance(v, tuple) else 0.0 for v in f3],
        other_fees=[v[2] if isinstance(v, tuple) else 0.0 for v in f3], hidden_handling_fee=pd.to_numeric(hh, errors='coerce').fillna(0).values,
        tax=o.total_tax.values, total=o.total.values, refunded=None, customer_note=None))
    created = o.set_index(['site', 'order_id']).created
    it = it[~it.site.isin(SKIP_HOSTS)]
    items = pd.DataFrame(dict(store=it.site, order_id=it.order_id.astype(int), number=it.number,
                              created=[created.get((s, i)) for s, i in zip(it.site, it.order_id)], sku=it.sku, name=it.name,
                              quantity=it.quantity.astype(int), subtotal=it.subtotal, total=it.total))
    return orders, items


def main():
    os.makedirs(OUT, exist_ok=True)
    now = datetime.now(timezone.utc)
    args = sys.argv[1:]
    if args[:1] == ['--seed']:
        orders, items = seed(args[1])
        write_years(orders, 'orders', OCOLS); write_years(items, 'items', ICOLS)
        info = dict(pulled_utc=now.isoformat(timespec='seconds'), mode='seed', source=os.path.basename(args[1].rstrip('/\\')),
                    window_from=str(orders.created.min())[:10], stores_ok=int(orders.store.nunique()), stores_failed=[],
                    orders=int(len(orders)), items=int(len(items)))
        json.dump(info, open(os.path.join(OUT, 'LAST_PULL.json'), 'w'), indent=1)
        print(f"webstores: seeded {len(orders)} orders / {len(items)} items from {args[1]}"); return
    since = args[args.index('--since') + 1] if '--since' in args else (now - timedelta(days=DAYS)).strftime('%Y-%m-%d')
    sites, err = load_sites()
    if err:
        print(f'webstores: {err}; keeping the committed files'); return
    new_o, new_i, ok, failed = [], [], [], []
    for s in sites:
        try:
            rows = pull_store(s, since)
            new_o += [r[0] for r in rows]; new_i += [i for r in rows for i in r[1]]; ok.append(s['store'])
        except Exception as e:      # one bad store must not stop the others
            failed.append(dict(store=s['store'], error=f'{type(e).__name__}: {str(e)[:150]}'))
    if not ok:
        print(f'webstores: every store failed ({failed[:1]}); keeping the committed files'); return
    no, ni = pd.DataFrame(new_o, columns=OCOLS), pd.DataFrame(new_i, columns=ICOLS)
    no, ni = no[no.created.astype(str) >= FLOOR], ni[ni.created.astype(str) >= FLOOR]
    orders = merge(load_existing('orders'), no, OCOLS, since, ok)
    items = merge(load_existing('items'), ni, ICOLS, since, ok)
    write_years(orders, 'orders', OCOLS); write_years(items, 'items', ICOLS)
    info = dict(pulled_utc=now.isoformat(timespec='seconds'), mode='since', window_from=since, stores_ok=len(ok), stores_failed=failed,
                orders_in_window=len(no), orders=int(len(orders)), items=int(len(items)))
    json.dump(info, open(os.path.join(OUT, 'LAST_PULL.json'), 'w'), indent=1)
    print(f'webstores: {len(no)} orders since {since} from {len(ok)} stores ({len(failed)} failed); {len(orders)} orders on file')


if __name__ == '__main__':
    main()
