"""Addresses for every merch payee in data/dol/merch-vendors.parquet, from the raw DOL payer/payee files.

Unions spell the same vendor many ways ("K & R Industries", "K&R Branding Solutions", "K AND R BRANDING"); the payee
address is what ties them together. Writes data/dol/payee-addresses.parquet: one row per payee name as filed, with
street, city, state, ZIP and `addr_key` (ZIP5 + street number + first 4 letters of the street name). Payees that share an addr_key are the same company.
Run after import_dol.py:  python scripts/dol_payee_addresses.py ["<DOL data folder with <date> refresh/YYYY.zip>"]
"""
import collections, csv, glob, io, os, re, sys, zipfile

import pandas as pd

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
DATA_ROOT = os.environ.get('SALES_DATA_DIR') or os.path.join(os.path.dirname(REPO), 'data')  # the data folder (not a repo since 2026-11-18): SALES_DATA_DIR, or 'data' next to the repos
OUT = os.path.join(DATA_ROOT, 'dol')
B = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.expanduser('~'), 'Dropbox', 'Claude', 'Enriched Sales Dataset', 'Sales - Master Data', 'DOL data')
Z = sorted(glob.glob(os.path.join(B, '* refresh')))[-1]
csv.field_size_limit(10 ** 9)
names = set(pd.read_parquet(os.path.join(OUT, 'merch-vendors.parquet')).vendor.str.strip().str.upper())
cnt = collections.Counter()
for y in ['2023', '2024', '2025', '2026']:
    p = os.path.join(Z, f'{y}.zip')
    if not os.path.exists(p):
        continue
    z = zipfile.ZipFile(p)
    f = io.TextIOWrapper(z.open(f'ar_payer_payee_data_{y}.txt'), encoding='utf-8', errors='replace')
    r = csv.reader(f, delimiter='|', quoting=csv.QUOTE_NONE)
    h = next(r); ix = {k: i for i, k in enumerate(h)}
    for row in r:
        if len(row) < len(h):
            continue
        n = row[ix['NAME']].strip().upper()
        if n in names:
            cnt[(n, row[ix['STREET']].strip().upper(), row[ix['CITY']].strip().upper(), row[ix['STATE']].strip().upper(), row[ix['ZIP']].strip()[:5])] += 1
rows = []
for (n, st, city, state, zp), c in cnt.items():
    num = re.match(r'\s*(\d+)\s+(?:[NSEW]\.?\s+)?([A-Z0-9]+)', st)
    po = re.search(r'P\.?\s*O\.?\s*BOX\s*(\d+)', st)
    key = f'{zp}|{num.group(1)}|{num.group(2)[:4]}' if (zp and num) else f'{zp}|PO{po.group(1)}' if (zp and po) else ''
    rows.append(dict(vendor_upper=n, street=st, city=city, state=state, zip5=zp, addr_key=key, times=c))
df = pd.DataFrame(rows).sort_values(['vendor_upper', 'times'], ascending=[True, False])
df.to_parquet(os.path.join(OUT, 'payee-addresses.parquet'), index=False)
print(f'payee-addresses.parquet: {len(df):,} rows, {df.vendor_upper.nunique():,} payee names, {df[df.addr_key != ""].addr_key.nunique():,} addresses')
