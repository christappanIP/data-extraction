"""Import the processed DOL (OLMS union filings) data into data/dol/ so every SAA user can query it.

Sources (both built from the raw DOL zips in Dropbox/Claude/Enriched Sales Dataset/Sales - Master Data/DOL data/):
  - ImagePointe-OS knowledge/03_customers/data/dol-*.csv            (dol_scripts/1_extract + 2_aggregate: per-union totals,
                                                                      prospects, Image Pointe payees, customer matches)
  - ip-data-foundation reference files/ip-sales-model/<date>/aim/   (ip-sales-statistical-model p1/p4d scripts: every filer,
                                                                      itemized merch vendors and payments by fiscal year)
Run on a machine with both folders after a DOL refresh:
  python scripts/import_dol.py [--os "<ImagePointe-OS path>"] [--aim "<.../aim path>"]
Writes data/dol/*.parquet + LAST_IMPORT.json. Read-only on the sources; public DOL data plus our own match tables.
Since 2026-11-02 it also copies the payee -> company lookup (ImagePointe-OS dol_scripts/market_share/vendor_company_map.csv)
to data/dol/vendor-companies.csv and adds a `company` column to the vendor and payment files (scripts/dol_company.py).
"""
import glob, json, os, shutil, sys
from datetime import datetime

import pandas as pd

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
DATA_ROOT = os.environ.get('SALES_DATA_DIR') or os.path.join(os.path.dirname(REPO), 'data')  # the data folder (not a repo since 2026-11-18): SALES_DATA_DIR, or 'data' next to the repos
OUT = os.path.join(DATA_ROOT, 'dol')
HOME = os.path.expanduser('~')
a = sys.argv[1:]
OS = a[a.index('--os') + 1] if '--os' in a else os.path.join(HOME, 'OneDrive - Image Pointe', 'ImagePointe-OS')
AIM = a[a.index('--aim') + 1] if '--aim' in a else sorted(glob.glob(os.path.join(
    HOME, 'OneDrive - Image Pointe', 'IP Agents', 'ip-data-foundation', 'reference files', 'ip-sales-model', '*', 'aim')))[-1]
KD = os.path.join(OS, 'knowledge', '03_customers', 'data')
os.makedirs(OUT, exist_ok=True)
info = dict(imported=datetime.now().strftime('%Y-%m-%d %H:%M'), sources=dict(knowledge=KD, aim=AIM), files={})


def save(df, name):
    df.to_parquet(os.path.join(OUT, name), index=False)
    info['files'][name] = int(len(df))
    print(f'{name}: {len(df):,} rows')


# every filer, latest filing (name, affiliation, city/state, members, form type, receipts)
F = json.load(open(os.path.join(AIM, 'dol_filers.json'), encoding='utf-8'))
fl = pd.DataFrame(F.values())
fl.columns = [c.lower() for c in fl.columns]
for c in ('members', 'ttl_receipts'):
    fl[c] = pd.to_numeric(fl[c], errors='coerce')
save(fl, 'filers.parquet')

# per-union four-year totals (merch, Image Pointe, known competitors, other category vendors) + members
save(pd.read_csv(os.path.join(KD, 'dol-union-master-2023-2026.csv'), dtype={'f_num': str}), 'union-totals.parquet')
save(pd.read_csv(os.path.join(KD, 'dol-prospects-2023-2026.csv'), dtype={'f_num': str}), 'prospects.parquet')
save(pd.read_csv(os.path.join(KD, 'dol-image-pointe-confirmed-payees-2023-2026.csv'), dtype={'f_num': str}), 'image-pointe-payees.parquet')
save(pd.read_csv(os.path.join(KD, 'dol-matched-current-customers-2023-2026.csv'), dtype={'customer_code': str}), 'customer-matches-v0.parquet')

# itemized merch vendors by fiscal year, itemized payments and hoodie payments
V = json.load(open(os.path.join(AIM, 'dol_vendors.json'), encoding='utf-8'))
vend, items, hood = [], [], []
for f, e in V.items():
    for fy, lst in e.get('vendors', {}).items():
        for name, amt, ip, *t in lst:
            vend.append(dict(f_num=f, fy=fy, vendor=name, amount=float(amt), image_pointe=bool(ip), types='; '.join(t[0] if t else [])))
    for k, dest in (('items', items), ('hood', hood)):
        for it in e.get(k, []):
            dest.append(dict(f_num=f, fy=str(it.get('fy')), date=it.get('date'), vendor=it.get('vendor'), purpose=it.get('purpose'),
                             amount=float(it.get('amt') or 0), image_pointe=bool(it.get('ip')), types='; '.join(it.get('types') or [])))
shutil.copyfile(os.path.join(KD, 'dol_scripts', 'market_share', 'vendor_company_map.csv'), os.path.join(OUT, 'vendor-companies.csv'))
info['files']['vendor-companies.csv'] = sum(1 for _ in open(os.path.join(OUT, 'vendor-companies.csv'), encoding='utf-8')) - 1
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dol_company import company
for rows in (vend, items, hood):
    for r in rows:
        r['company'] = company(r['vendor'], r['image_pointe'])
save(pd.DataFrame(vend), 'merch-vendors.parquet')
save(pd.DataFrame(items), 'merch-payments.parquet')
save(pd.DataFrame(hood), 'hoodie-payments.parquet')
el = os.path.join(AIM, 'dol_elections.csv')
if os.path.exists(el):
    save(pd.read_csv(el, dtype={'f': str, 'groups': str}).rename(columns={'f': 'f_num'}), 'officer-elections.parquet')
json.dump(info, open(os.path.join(OUT, 'LAST_IMPORT.json'), 'w'), indent=1)
