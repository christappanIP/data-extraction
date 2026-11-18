"""Write data/dashboard/dol-merch-vendors.json: the sales model's dol_vendors.json (p4d_dol_vendors_all.py output, then
p29_merch_rule.py since 2026-11-09, so it is already merch-only under the shared rule in ImagePointe-OS
knowledge/03_customers/data/dol_scripts/merch_rule.py), trimmed to the DOL filers in data/dashboard/crosswalk-groups.csv.
Until 2026-11-09 it was trimmed before p28 and build.py applied its own merch-name test.

  filers:       {f_num: {fy: [[payee, $, paid_to_us], ...]}} for crosswalk filers with any payee rows
  vendor_types: {payee: [product types]}, from every filer that paid that payee

usage: python scripts/trim_dol_vendors.py <dol_vendors.json> [--keep-types]
  --keep-types  keep the current file's vendor_types and add types only for payee names that are new. Used for the
                2026-10-30 rebuild (payee-name fix), so that rebuild changed nothing but who counts as us.
Checked 2026-10-30: without --keep-types, the 10-10 p4d output reproduces the 10-10 file's filers exactly; its
vendor_types came from an earlier p4c run, so they differ."""
import json, sys, csv, os, collections, datetime

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DASH = os.path.join(os.environ.get('SALES_DATA_DIR') or os.path.join(os.path.dirname(HERE), 'data'), 'dashboard')  # the data folder (not a repo since 2026-11-18): SALES_DATA_DIR, or 'data' next to the repos
OUT = os.path.join(DASH, 'dol-merch-vendors.json')
# crosswalk-groups.csv moved to tools/dashboards/data/dashboard (2026-11-17); tools sits next to this repo unless TOOLS_REPO says otherwise
TOOLS_DASH = os.path.join(os.environ.get('TOOLS_REPO') or os.path.join(os.path.dirname(HERE), 'tools'), 'dashboards', 'data', 'dashboard')
src = sys.argv[1]
V = json.load(open(src, encoding='utf-8'))
xf = {r['dol_f_num'] for r in csv.DictReader(open(os.path.join(TOOLS_DASH, 'crosswalk-groups.csv'), encoding='utf-8')) if r.get('dol_f_num')}
F, vt = {}, collections.defaultdict(set)
for f, v in V.items():
    if f in xf and v['vendors']:
        F[f] = {fy: [[r[0], r[1], r[2]] for r in rows] for fy, rows in v['vendors'].items()}
for v in V.values():
    for rows in v['vendors'].values():
        for r in rows:
            vt[r[0]].update(r[3] if len(r) > 3 else [])
names = {r[0] for v in F.values() for rows in v.values() for r in rows}
if '--keep-types' in sys.argv:
    old = json.load(open(OUT, encoding='utf-8'))
    VT = old['vendor_types']
    old_names = {r[0] for v in old['filers'].values() for rows in v.values() for r in rows}
    for k in sorted(names - old_names - set(VT)):
        if vt.get(k):
            VT[k] = sorted(vt[k])
else:
    VT = {k: sorted(s) for k, s in vt.items() if s and k in names}
# one row per company per fiscal year, not per spelling (DOL default since 2026-11-02; scripts/dol_company.py). Without it a
# vendor filed under a new spelling looks like new business moved elsewhere (IUOE 49 TC showed a $529K "gain").
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dol_company import merge_filers
F, VT = merge_filers(F, VT)
json.dump(dict(source="ImagePointe-OS projects/In process/ip-sales-statistical-model/scripts/p4d_dol_vendors_all.py -> dol_vendors.json (DOL OLMS FY2023-26, Oct 2026 refresh), trimmed to crosswalk filers",
               built=datetime.date.today().isoformat(), filers=F, vendor_types=VT), open(OUT, 'w', encoding='utf-8'), ensure_ascii=False)
print('filers', len(F), 'payee names', len(names), 'with types', len(VT))
