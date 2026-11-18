"""Import the two by-hand webstore fee exports into data/webstores/ (Josh, 2026-10-13).

  1. Webstore fees billed (Impress): an impr0008 invoice export filtered to the "Webstore Fee" misc charge, one row per
     invoice (Customer Number, Customer Name, Invoice#, Invoice Date, Misc Chgs, Invoice Balance). These are the
     monthly store fees ($99 / $249 / $499 tiers), billed early in the month.
  2. Card processing fee income (GP): account transactions for 4300.20.10 "Credit Card Fee Income", one row per
     journal entry. Finance posts one entry per store per month at month end, labelled like "IBEW 26 JULY WEBSTORE";
     it equals 4% of what members paid by card that month.

Copies of the exports live in ImagePointe-OS knowledge/reference files/webstores/fees/.

Run:
  python import_fees.py --invoices "<Webstore Storage Fees.xlsx>" --gl "<Webstore Processing Fee income.xlsx>"
Writes webstore-fee-invoices.parquet and card-fee-income-gl.parquet and records each source in SOURCES.json. Store
matching (fee-stores.csv) happens in build.py, so fixing the crosswalk doesn't need a re-import.
"""
import json, os, re, sys
from datetime import datetime

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.environ.get('SALES_DATA_DIR') or os.path.join(HERE, '..', '..', '..', 'data')  # the data folder (not a repo since 2026-11-18): SALES_DATA_DIR, or 'data' next to the repos
OUT = os.path.abspath(os.path.join(DATA_ROOT, 'webstores'))
SRC = os.path.join(OUT, 'SOURCES.json')
MON = {m: i + 1 for i, m in enumerate(['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'])}


def arg(name):
    a = sys.argv[1:]
    return a[a.index(name) + 1] if name in a else None


def note(key, **kw):
    s = json.load(open(SRC)) if os.path.exists(SRC) else {}
    s[key] = dict(kw, imported=datetime.now().strftime('%Y-%m-%d %H:%M'))
    json.dump(s, open(SRC, 'w'), indent=1)


def sheet_with(path, col):
    for name, df in pd.read_excel(path, sheet_name=None).items():
        if col in df.columns:
            return df
    raise SystemExit(f'{path}: no sheet with a "{col}" column')


def invoices(path):
    f = sheet_with(path, 'Invoice#')
    f = f[f['Item Description'].astype(str).str.strip().str.lower() == 'webstore fee']
    d = pd.DataFrame(dict(cust_no=f['Customer Number'].astype(str).str.strip().str.lstrip('0'), cust=f['Customer Name'].astype(str).str.strip(),
                          invoice=f['Invoice#'].astype(str).str.strip(), date=pd.to_datetime(f['Invoice Date']).dt.strftime('%Y-%m-%d'),
                          amount=pd.to_numeric(f['Misc Chgs'], errors='coerce').fillna(0).round(2),
                          balance=pd.to_numeric(f['Invoice Balance'], errors='coerce').fillna(0).round(2)))
    d.to_parquet(os.path.join(OUT, 'webstore-fee-invoices.parquet'), index=False)
    note('webstore_fees', file=os.path.basename(path), invoices=len(d), dates=[d.date.min(), d.date.max()], open_balance=round(float(d.balance.sum()), 2))
    print(f'webstore fee invoices: {len(d)} ({d.date.min()} to {d.date.max()}), open balance ${d.balance.sum():,.2f}')


def period(label, d):
    """The month the fee is for: the month named in the label, else the posting month. A month named after the
    posting month belongs to the year before (e.g. "32BJ OCT WEBSTORE" posted 12/31 is October)."""
    m = re.search(r'\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[A-Z]*\b', label.upper())
    if not m or 'WRITE OFF' in label.upper():
        return d.strftime('%Y-%m')
    mm = MON[m.group(1)]
    return f'{d.year - (1 if mm > d.month + 1 else 0)}-{mm:02d}'


def gl(path):
    f = sheet_with(path, 'Journal Entry')
    ref = f.Reference.fillna('').astype(str).str.strip()
    master = f['Originating Master Name'].fillna('').astype(str).str.strip()
    label = ref.where(f.Series == 'Financial', (master + ' ' + ref).str.strip())
    d = pd.DataFrame(dict(je=f['Journal Entry'].astype(str), series=f.Series.astype(str), date=pd.to_datetime(f['TRX Date']),
                          label=label, amount=pd.to_numeric(f.Amount, errors='coerce').fillna(0).round(2)))
    d['period'] = [period(l, x) for l, x in zip(d.label, d.date)]
    d['writeoff'] = d.label.str.upper().str.contains('WRITE OFF|WRITE-OFF')
    d['date'] = d.date.dt.strftime('%Y-%m-%d')
    d.to_parquet(os.path.join(OUT, 'card-fee-income-gl.parquet'), index=False)
    note('card_fee_gl', file=os.path.basename(path), account='4300.20.10 Credit Card Fee Income', entries=len(d),
         dates=[d.date.min(), d.date.max()], total=round(float(d.amount.sum()), 2))
    print(f'card fee GL: {len(d)} entries ({d.date.min()} to {d.date.max()}), ${d.amount.sum():,.2f}')


if __name__ == '__main__':
    if arg('--invoices'):
        invoices(arg('--invoices'))
    if arg('--gl'):
        gl(arg('--gl'))
    if not (arg('--invoices') or arg('--gl')):
        print(__doc__)
