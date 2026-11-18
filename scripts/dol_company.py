"""DOL payee name -> company, the default for every DOL report on us or competitors (Josh, 2026-11-02).

Unions type vendor names by hand, so one company shows up under many spellings (San Fran Stitch Co: 47; K&R: 34).
ImagePointe-OS knowledge/03_customers/data/dol_scripts/market_share/cluster.py merges them by name, street address and
city plus checked hand merges, and writes vendor_company_map.csv. import_dol.py copies it here as
data/dol/vendor-companies.csv. Rules: ImagePointe-OS lessons/2026-11-02-dol-competitor-numbers-merge-vendors-first.md.

  company(name, paid_to_us=False) -> display name: 'Image Pointe (us)' for us, the merged company's name if the map has
                                     the spelling, else the name as filed
  merge_filers(filers, vendor_types) -> the same {f_num: {fy: [[payee, $, paid_to_us], ...]}} shape with one row per
                                     company per fiscal year (amounts summed, paid_to_us if any row was)

python scripts/dol_company.py   merges data/dashboard/dol-merch-vendors.json in place (safe to run twice)
"""
import csv, json, os, re

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
DATA_ROOT = os.environ.get('SALES_DATA_DIR') or os.path.join(os.path.dirname(REPO), 'data')  # the data folder (not a repo since 2026-11-18): SALES_DATA_DIR, or 'data' next to the repos
MAP = os.path.join(DATA_ROOT, 'dol', 'vendor-companies.csv')
US = 'Image Pointe (us)'
_key = lambda s: re.sub(r'\s+', ' ', str(s).upper()).strip()
_M = None


def _load():
    global _M
    if _M is None:
        _M = {}
        if os.path.exists(MAP):
            for r in csv.DictReader(open(MAP, encoding='utf-8')):
                _M[_key(r['vendor'])] = US if r['is_us'] == 'true' else r['label']
        for v in set(_M.values()):                              # a merged name maps to itself
            _M.setdefault(_key(v), v)
    return _M


def company(name, paid_to_us=False):
    if paid_to_us:
        return US
    return _load().get(_key(name), name)


def merge_filers(filers, vendor_types):
    F, VT = {}, {}
    for f, years in filers.items():
        F[f] = {}
        for fy, rows in years.items():
            acc = {}
            for n, amt, ip in rows:
                c = company(n, ip)
                a = acc.setdefault(c, [c, 0, False])
                a[1] += amt
                a[2] = a[2] or bool(ip)
            F[f][fy] = sorted(acc.values(), key=lambda r: -r[1])
    for n, types in vendor_types.items():
        VT.setdefault(company(n), set()).update(types)
    return F, {k: sorted(v) for k, v in VT.items()}


if __name__ == '__main__':
    P = os.path.join(DATA_ROOT, 'dashboard', 'dol-merch-vendors.json')
    D = json.load(open(P, encoding='utf-8'))
    n0 = sum(len(r) for v in D['filers'].values() for r in v.values())
    t0 = sum(x[1] for v in D['filers'].values() for r in v.values() for x in r)
    D['filers'], D['vendor_types'] = merge_filers(D['filers'], D['vendor_types'])
    n1 = sum(len(r) for v in D['filers'].values() for r in v.values())
    t1 = sum(x[1] for v in D['filers'].values() for r in v.values() for x in r)
    assert abs(t0 - t1) < 1, (t0, t1)
    D['merged'] = 'payee spellings merged by company (scripts/dol_company.py, data/dol/vendor-companies.csv)'
    json.dump(D, open(P, 'w', encoding='utf-8'), ensure_ascii=False)
    print(f'rows {n0:,} -> {n1:,}; dollars unchanged {t1:,.0f}')
