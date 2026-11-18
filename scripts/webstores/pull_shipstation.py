"""Pull ShipStation label costs (read-only API) into data/webstores/shipstation-labels-<year>.parquet.

Replaces Tim's by-hand export for the shipping check: shipments shipped in the last 45 days replace that window in the
year file; older rows (including the first by-hand import) are kept. Voided labels are left out.
Each label keeps its tracking number and ship-to name and address (Josh, 2026-10-28: customer data is fine in SAA).
Credentials stay outside the repo: %USERPROFILE%\\.shipstation\\credentials.json = {"api_key": "...", "api_secret": "..."}
(ShipStation > Settings > Account > API Settings). Only GET requests are made. Fail-open: no key or an API error keeps
the committed file.

Run:  python pull_shipstation.py                 nightly (last 45 days)
      python pull_shipstation.py --since 2026-01-01
"""
import base64, json, os, sys, time, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timedelta

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.environ.get('SALES_DATA_DIR') or os.path.join(HERE, '..', '..', '..', 'data')  # the data folder (not a repo since 2026-11-18): SALES_DATA_DIR, or 'data' next to the repos
OUT = os.path.abspath(os.path.join(DATA_ROOT, 'webstores'))
CFG = os.path.join(os.path.expanduser('~'), '.shipstation', 'credentials.json')
DAYS = 45
COLS = ['ss_store', 'order', 'zip5', 'ship_date', 'service', 'label', 'insurance', 'paid_checkout', 'tracking',
        'ship_name', 'ship_company', 'ship_address1', 'ship_address2', 'ship_city', 'ship_state', 'ship_zip', 'ship_country']


def get(auth, path, params=None):
    url = 'https://ssapi.shipstation.com' + path + ('?' + urllib.parse.urlencode(params) if params else '')
    req = urllib.request.Request(url, headers={'Authorization': auth, 'User-Agent': 'SAA read-only pull'})
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                if int(r.headers.get('X-Rate-Limit-Remaining', '40') or 40) < 2:     # 40 calls a minute
                    time.sleep(int(r.headers.get('X-Rate-Limit-Reset', '30') or 30) + 1)
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < 4:
                time.sleep(int(e.headers.get('X-Rate-Limit-Reset', '0') or 0) + 5 * (attempt + 1)); continue
            raise


def pull(auth, since, until):
    stores = {s['storeId']: s['storeName'] for s in get(auth, '/stores', {'showInactive': 'true'})}
    rows, page, pages = [], 1, 1
    while page <= pages:
        d = get(auth, '/shipments', {'shipDateStart': since, 'shipDateEnd': until, 'includeShipmentItems': 'false',
                                     'pageSize': 500, 'page': page, 'sortBy': 'ShipDate', 'sortDir': 'ASC'})
        pages = d.get('pages') or 1
        for s in d['shipments']:
            if s.get('voided'):
                continue
            to = s.get('shipTo') or {}
            rows.append(dict(ss_store=stores.get((s.get('advancedOptions') or {}).get('storeId'), ''), order=str(s.get('orderNumber') or '').strip(),
                             zip5=(to.get('postalCode') or '')[:5], ship_date=(s.get('shipDate') or '')[:10],
                             service=f"{s.get('carrierCode') or ''} {s.get('serviceCode') or ''}".strip(),
                             label=float(s.get('shipmentCost') or 0), insurance=float(s.get('insuranceCost') or 0), paid_checkout=0.0,
                             tracking=s.get('trackingNumber') or None, ship_name=to.get('name') or None, ship_company=to.get('company') or None,
                             ship_address1=to.get('street1') or None, ship_address2=' '.join(filter(None, [to.get('street2'), to.get('street3')])) or None,
                             ship_city=to.get('city') or None, ship_state=to.get('state') or None, ship_zip=to.get('postalCode') or None,
                             ship_country=to.get('country') or None))
        page += 1
    return pd.DataFrame(rows, columns=COLS)


def main():
    a = sys.argv[1:]
    since = a[a.index('--since') + 1] if '--since' in a else (datetime.now() - timedelta(days=DAYS)).strftime('%Y-%m-%d')
    until = a[a.index('--until') + 1] if '--until' in a else datetime.now().strftime('%Y-%m-%d')
    try:
        c = json.load(open(CFG, encoding='utf-8-sig'))
        auth = 'Basic ' + base64.b64encode(f"{c['api_key'].strip()}:{c['api_secret'].strip()}".encode()).decode()
    except (OSError, KeyError, ValueError) as e:
        print(f'shipstation: no key in {CFG} ({type(e).__name__}); keeping the committed labels'); return
    try:
        new = pull(auth, since, until)
    except Exception as e:
        print(f'shipstation: API failed ({type(e).__name__}: {str(e)[:150]}); keeping the committed labels'); return
    if '--dry-run' in a:
        new.to_csv(a[a.index('--dry-run') + 1], index=False); print(f'shipstation: {len(new)} shipments -> {a[a.index("--dry-run") + 1]}'); return
    for y in sorted(set(new.ship_date.str[:4]) | {since[:4]}):
        p = os.path.join(OUT, f'shipstation-labels-{y}.parquet')
        old = pd.read_parquet(p).reindex(columns=COLS) if os.path.exists(p) else pd.DataFrame(columns=COLS)
        keep = old[(old.ship_date < since) | (old.ship_date > until)]
        part = pd.concat([keep, new[new.ship_date.str[:4] == y]], ignore_index=True)[COLS].sort_values(['ship_date', 'ss_store', 'order'])
        part.reset_index(drop=True).to_parquet(p, index=False)
    s = json.load(open(os.path.join(OUT, 'SOURCES.json'))) if os.path.exists(os.path.join(OUT, 'SOURCES.json')) else {}
    first = s.get('shipstation', {}).get('ship_dates', [since])[0]
    s['shipstation'] = dict(file='ShipStation API (pull_shipstation.py)', ship_dates=[min(first, since), until], window_from=since,
                            shipments_in_window=int(len(new)), imported=datetime.now().strftime('%Y-%m-%d %H:%M'))
    json.dump(s, open(os.path.join(OUT, 'SOURCES.json'), 'w'), indent=1)
    print(f'shipstation: {len(new)} shipments {since} to {until} merged')


if __name__ == '__main__':
    main()
