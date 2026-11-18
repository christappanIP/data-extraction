"""Extract the numbers the sales dashboard needs from the three source workbooks into small
JSON files under data/dashboard/. Only these JSONs are committed; the workbooks themselves stay
on SharePoint.

Sources (newest file in each folder is used):
  - Sales Dashboard MMDDYYYY.xlsx          (Sales site > Dashboard)                 -> sales-workbook-<year>.json
  - Conslidated Management Dashboard IP DA (Josh Meyer, KPI Dashboard/<year>)        -> management-dashboard-<year>.json
  - Customer Health Metrics v*.xlsx       (Sales site > WIGS & Tools > Customer Health Metrics) -> health-snapshot.json

Usage:
  python extract_inputs.py --scan                 # look in the folders listed in config/sources.json
  python extract_inputs.py --sales-workbook X.xlsx --management Y.xlsx --health Z.xlsx
Prints what it wrote; writes nothing for a source it can't find. Exit 0 either way (fail-open,
so the nightly launcher never breaks on a missing shortcut).
"""
import argparse, glob, json, os, re, sys
from collections import Counter
from datetime import datetime

import openpyxl

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))); import paths  # noqa: E402
OUT = paths.DATA_DASH   # the data folder's dashboard/
MON = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
# Rep tab name in the sales workbook -> Impress agent code
REP_TABS = {'JS': '104', 'Jim': '102', 'Mel': '135', 'Ryan': '136', 'Meredith': '156', 'Dave': '154',
            'DA Wholesale': '131', 'Matt': '152', 'Beth Anna': '139', 'Tatum': '144', 'Kenney': '120',
            'Mike': '125', 'Jess': '137', 'CSS (Julie)': '147'}


def num(v):
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.replace(',', ''))
        except ValueError:
            return None
    return None


def newest(folder, pattern):
    files = [f for f in glob.glob(os.path.join(folder, pattern)) if not os.path.basename(f).startswith('~$')]
    return max(files, key=os.path.getmtime) if files else None


def dump(name, obj):
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, name)
    old = open(path, encoding='utf-8').read() if os.path.exists(path) else None
    new = json.dumps(obj, indent=1, default=str)
    if old != new:
        open(path, 'w', encoding='utf-8').write(new)
        print('wrote', path)
    else:
        print('unchanged', path)


# ---------------------------------------------------------------- sales workbook
def sales_workbook(path):
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)

    def tab_goals(name):
        rows = list(wb[name].iter_rows(min_row=1, max_row=24, max_col=22, values_only=True))
        i = [k for k, v in enumerate(rows[6]) if v == 'Annual'][2]   # third "Annual" = the current-year block
        g = {}
        for r in rows[6:23]:
            lab = str(r[i]).strip() if r[i] is not None else ''
            if lab in MON or lab == 'Annual':
                f = lambda k: num(r[k]) or 0.0
                g[lab] = dict(goal=f(i + 1), prog=f(i + 2), off_goal=f(i + 6), off_prog=f(i + 7))
        return g

    # DA unit goals per rep tab (Josh, 2026-10-29: rep dashboards get DA unit goals): the annual goal in column V of the
    # Fleece # / Polo # / Tee # rows; the tab's monthly and YTD goals are that times the lookup tab's monthly share (Z:AC)
    DA_ROWS = {'Fleece #': 'fleece', 'Polo #': 'polo', 'Tee #': 'tee'}

    def tab_da(name):
        g = {}
        for r in wb[name].iter_rows(min_row=3, max_row=14, min_col=17, max_col=22, values_only=True):
            lab = str(r[0]).strip() if r[0] is not None else ''
            if lab in DA_ROWS and num(r[5]):
                g[DA_ROWS[lab]] = num(r[5])
        return g

    goals = {t: tab_goals(t) for t in REP_TABS if t in wb.sheetnames}
    goals['IP Consolidated'] = tab_goals('IP Consolidated')
    da_goals = {t: g for t in REP_TABS if t in wb.sheetnames for g in [tab_da(t)] if g}
    lk = list(wb['lookup'].iter_rows(min_row=2, max_row=13, min_col=26, max_col=29, values_only=True))
    da_share = dict(fleece=[num(r[1]) for r in lk], tee=[num(r[2]) for r in lk], polo=[num(r[3]) for r in lk])
    s = list(wb['Sales'].iter_rows(min_row=1, max_row=93, max_col=4, values_only=True))
    col = lambda a, b, c: [num(s[k][c]) for k in range(a - 1, b)]
    top = list(wb['IP Consolidated'].iter_rows(min_row=1, max_row=4, max_col=30, values_only=True))
    asof = next((v for v in top[0] if hasattr(v, 'strftime')), None)          # "as of date" cell in row 1
    m = re.search(r'(\d{2})(\d{2})(\d{4})', os.path.basename(path))
    year = int(m.group(3)) if m else datetime.now().year
    r4 = [num(v) for v in top[3] if num(v) is not None]                        # row 4: retained, beginning accounts
    retention = dict(retained=r4[0] if len(r4) > 0 else None, beg_accounts=r4[1] if len(r4) > 1 else None)
    return year, dict(
        source=os.path.basename(path), extracted=datetime.now().strftime('%Y-%m-%d %H:%M'),
        data_as_of=asof.strftime('%Y-%m-%d') if hasattr(asof, 'strftime') else None,
        rep_codes=REP_TABS, goals=goals,
        sales_tab=dict(tee=col(20, 31, 1), fleece=col(35, 46, 1), polo=col(50, 61, 1), offense=col(66, 77, 1)),
        da_units_cum=dict(tee=col(20, 31, 2), fleece=col(35, 46, 2), polo=col(50, 61, 2)),
        da_goals=da_goals, da_share=da_share,
        retention=retention)


# ---------------------------------------------------------------- management dashboard (Josh Meyer)
def management(path):
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    mg = [list(r) for r in wb['Monthly Goals'].iter_rows(min_row=1, max_row=87, max_col=16, values_only=True)]
    row = lambda r, c0=2: [num(v) for v in mg[r - 1][c0:c0 + 12]]
    label = lambda r: str(mg[r - 1][1] or '').strip()
    # locate rows by label so a reshuffled sheet still extracts (fall back to the 10/06/2026 layout)
    def find(prefix, default):
        for i, r in enumerate(mg):
            if str(r[1] or '').strip().lower().startswith(prefix.lower()):
                return i + 1
        return default
    da = {}
    for fam, gr, cr in [('tee', 2, 8), ('fleece', 3, 10), ('polo', 4, 12)]:
        da[fam] = dict(team_goal=num(mg[gr - 1][1]), wholesale_goal=num(mg[gr - 1][2]), curve_cum=row(cr))
    da['tee'].update(monthly=row(find('DA Monthly Tee', 17)), wholesale=row(find('Wholesale Tee', 18)))
    da['fleece'].update(monthly=row(find('DA Monthly Fleece', 22)), wholesale=row(find('Wholesale Fleece', 23)))
    da['polo'].update(monthly=row(find('DA Monthly Polo', 27)), wholesale=row(find('Wholesale Polo', 28)))
    cs_row = None
    for i, r in enumerate(mg):
        if str(r[2] or '').strip() == str(datetime.now().year) or (i > 70 and num(r[2]) == datetime.now().year):
            cs_row = i + 1
    cs_cum = row(cs_row, 3) if cs_row else [None] * 12
    cs_month = [None if v is None else v - ((cs_cum[i - 1] or 0) if i else 0) for i, v in enumerate(cs_cum)]
    ys = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', os.path.basename(path))
    year = int(ys.group(3)) if ys else datetime.now().year
    return year, dict(
        source=os.path.basename(path), extracted=datetime.now().strftime('%Y-%m-%d %H:%M'),
        budget=row(find(f'{year} Goal', 46)), actual=row(find('IP Sales Actual', 49)),
        actual_prior={str(year - 1): row(find(f'{year - 1} Actual', 55)), str(year - 2): row(find(f'{year - 2} Actual', 51)),
                      str(year - 3): row(find(f'{year - 3} Actual', 53))},
        open_orders=row(find('Total Open Orders', 48)), da=da,
        cs_tickets_month=cs_month, cs_tickets_cum=cs_cum)


# ---------------------------------------------------------------- Hamilton's customer health workbook
def health(path):
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    d = list(wb['Dashboard'].iter_rows(min_row=1, max_row=24, max_col=16, values_only=True))
    month = f"{d[1][1]}-{int(d[2][1]):02d}"
    reps = [dict(who=r[1], active=[num(r[3]), num(r[4]), num(r[5])], lapse=num(r[7]), lapse_grade=r[8],
                 tenured=[num(r[10]), num(r[11]), num(r[12])], grow=num(r[14]), grow_grade=r[15]) for r in d[5:24] if r[1]]
    t = list(wb['Trend'].iter_rows(values_only=True))
    dates = [(j, h.strftime('%Y-%m')) for j, h in enumerate(t[2]) if hasattr(h, 'year')]
    trend = {}
    for r in t[3:]:
        if r[1] and r[2] and r[1] != 'Check':
            trend.setdefault(r[1], {})[r[2]] = {m: num(r[j]) for j, m in dates if num(r[j]) is not None}
    lists = {n: dict(Counter(r[3] for r in wb[n].iter_rows(min_row=6, values_only=True) if r[1]))
             for n in ('Already Lapsed', 'Lapsing Soon', 'Declining') if n in wb.sheetnames}
    return dict(source=os.path.basename(path), extracted=datetime.now().strftime('%Y-%m-%d %H:%M'), month=month,
                reps=reps, trend=trend, lists=lists)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--scan', action='store_true')
    ap.add_argument('--sales-workbook'); ap.add_argument('--management'); ap.add_argument('--health')
    a = ap.parse_args()
    src = dict(sales_workbook=a.sales_workbook, management=a.management, health=a.health)
    if a.scan:
        cfg = json.load(open(os.path.join(paths.CONFIG, 'sources.json'), encoding='utf-8'))
        for key, spec in cfg['sources'].items():
            for folder in spec['folders']:
                f = newest(os.path.expandvars(folder), spec['pattern'])
                if f:
                    src[key] = src[key] or f
                    break
    for key, fn in (('sales_workbook', sales_workbook), ('management', management)):
        if src[key]:
            try:
                year, obj = fn(src[key])
                dump(f"{key.replace('_', '-')}-{year}.json" if key == 'sales_workbook' else f'management-dashboard-{year}.json', obj)
            except Exception as e:   # fail-open: a reshaped workbook shouldn't stop the nightly run
                print('FAILED', key, src[key], repr(e))
        else:
            print('not found', key)
    if src['health']:
        try:
            dump('health-snapshot.json', health(src['health']))
        except Exception as e:
            print('FAILED health', src['health'], repr(e))
    else:
        print('not found health')


if __name__ == '__main__':
    main()
