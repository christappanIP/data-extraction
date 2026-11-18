"""Pull the sales model's season opportunity lists into data/dashboard/opportunities.json.

Usage: python extract_opportunities.py <folder with rep5-<code>.json> [--season-start 2026-10-01]

The sales model (ImagePointe-OS projects/In process/ip-sales-statistical-model) writes one review file per
rep, rep5-<code>.json, with four ranked lists: current accounts (expected spend this season plus disclosed
merch spend going elsewhere), prospects (DOL merch spend with other vendors), hoodie opportunities at current
accounts, and hoodie prospects. This keeps the numbers, union names, DOL vendors and itemized purchases (public
OLMS data) and drops every person-level field (officers, buyers, HubSpot contacts, rep notes).
Season = the six months the model forecast: Oct-Mar (Q4/Q1) or Apr-Sep (Q2/Q3)."""
import argparse, glob, json, os, sys
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))); import paths  # noqa: E402
OUT = os.path.join(paths.DATA_DASH, 'opportunities.json')
ap = argparse.ArgumentParser()
ap.add_argument('folder')
ap.add_argument('--season-start', default='2026-10-01')
ap.add_argument('--dol', help="the model's dol_vendors.json (default: next to the review folder)")
ap.add_argument('--run-date', help='date of the model run (default: the date in the review files)')
ap.add_argument('--pattern', default='rep5-*.json', help='review files to read (rep4-*.json = list build only, before the finish step)')
a = ap.parse_args()

# Merch only, but all of it: spend with other vendors is recomputed from every merch payee in the union's filing
# (not just the model's top three), and non-merch payees (mailers, billboards, travel, clubs) are dropped. Same test
# as build.py's buying-elsewhere check.
import re
IPRX = re.compile(r'IMAGE ?POINTE?|BACK ?ALLEY PRINT|DIGNITY APPAREL', re.I)   # Back Alley Printers = our legal name
APP = {'Tees', 'Hoodies & sweatshirts', 'Polos', 'Hats', 'Jackets & vests', 'Apparel', 'Bags', 'Drinkware', 'Pins, patches & decals', 'Promo items'}
OK = re.compile(r'PRINT|SCREEN|EMBROID|APPAREL|WEAR|SHIRT|\bTEES?\b|PROMO|SPORT|UNIFORM|BRAND|IMPRINT|SILK|AWARD|TROPHY|THREAD|STITCH|CLOTH|GEAR|\bINK|SPECIALT|NOVELT|BADGE|PIN|BUTTON|LOGO', re.I)
BAD = re.compile(r'BUS WRAP|LAMAR|OUTFRONT|CLEAR CHANNEL|BILLBOARD|MEDIA|RADIO|TELEVISION|BROADCAST|AIRLINE|HOTEL|TRAVEL|CATER|STAPLES|OFFICE DEPOT|COUNTRY CLUB|GOLF|RESORT|RESTAURANT|MAIL|POSTAGE|POSTMASTER|PIVOT GROUP|CONSULT', re.I)
# Since 2026-11-09 the model's dol_vendors.json is merch-only under the shared DOL merch rule (ImagePointe-OS
# knowledge/03_customers/data/dol_scripts/merch_rule.py, applied by p29_merch_rule.py), so this name test is off: it
# dropped real merch sellers with no merch word in the name (K&R Industries) and kept season tickets. OK/BAD stay for
# reading old run folders that were built before p29.
merch = lambda n, types: True
isip = lambda n, f: bool(f) or bool(IPRX.search(n or ''))
dolp = a.dol or os.path.join(os.path.dirname(os.path.normpath(a.folder)), 'dol_vendors.json')
DV = json.load(open(dolp, encoding='utf-8')) if os.path.exists(dolp) else {}


def merch_fy(f, fy_hint):
    """Merch spend for one filer in one fiscal year: (fy, total, ours, [[vendor, amt], ...] outside, merch items)."""
    v = DV.get(f)
    if not v:
        return None
    fys = [y for y in ('2026', '2025', '2024') if v['vendors'].get(y)]
    fy = fy_hint if fy_hint in fys else (fys[0] if fys else None)
    if not fy:
        return None
    rows = [r for r in v['vendors'][fy] if merch(r[0], r[3] if len(r) > 3 else [])]
    ours = sum(r[1] for r in rows if isip(r[0], r[2]))
    out = sorted(([r[0], round(r[1])] for r in rows if not isip(r[0], r[2])), key=lambda x: -x[1])
    its = [i for i in v.get('items', []) if merch(i.get('vendor'), i.get('types'))]
    return fy, round(sum(r[1] for r in rows)), round(ours), out, its

s0 = date.fromisoformat(a.season_start)
s1 = date(s0.year + (1 if s0.month >= 10 else 0), (s0.month + 5 - 1) % 12 + 1, 1)
lab = lambda d: d.strftime('%b %Y')
end_label = lab(s1)
season = dict(start=s0.isoformat(), end_month=s1.strftime('%Y-%m'), label=f'{lab(s0)} - {end_label}',
              quarters='Q4/Q1' if s0.month == 10 else 'Q2/Q3')


def top(lst, n=3):
    return [[v[0], round(v[1])] for v in (lst or [])[:n]]


def items(lst, n=4):
    return [dict(vendor=i.get('vendor'), what=i.get('purpose'), date=i.get('date'), amt=round(i.get('amt') or 0)) for i in (lst or [])[:n]]


def dol(d):
    if not d:
        return None
    base = dict(f=d.get('f'), name=d.get('name'), city=d.get('city'), st=d.get('st'), members=d.get('members'))
    m = merch_fy(d.get('f'), d.get('runfy'))
    if not m:      # filer not in the vendor file: keep the model's figures, flagged
        return dict(base, run=round(d.get('run') or 0), fy=d.get('runfy'), ip=round(d.get('ip') or 0), share=d.get('share'),
                    out=round((d.get('run') or 0) - (d.get('ip') or 0)), top=top(d.get('top'), 15), items=items(d.get('items')), merch_only=False)
    fy, tot, ours, out, its = m
    return dict(base, run=tot, fy=fy, ip=ours, share=round(ours / tot * 100) if tot else None, out=tot - ours,
                top=out[:15], n_vendors=len(out), items=items(sorted(its, key=lambda i: i.get('date') or '', reverse=True), 6), merch_only=True)


def hood(h):
    if not h:
        return None
    return dict(fy=h.get('fy'), other=round(h.get('other') or 0), ip=round(h.get('ip') or 0), vendors=top(h.get('vendors'), 15),
                items=items(h.get('rows')))


def hist(lst):
    return [dict(acct=x.get('cn'), name=x.get('name'), total=round(x.get('tot') or 0), last=x.get('last')) for x in (lst or [])]


out = dict(source="sales model review files (rep5-<code>.json); outside spend recomputed merch-only from the model's dol_vendors.json",
           folder=os.path.basename(os.path.normpath(a.folder)), season=season, built=date.today().isoformat(), reviews={})
for f in sorted(x for x in glob.glob(os.path.join(a.folder, a.pattern)) if '.prev.' not in x):
    d = json.load(open(f, encoding='utf-8'))
    code = str(d['rep'])
    cur = []
    for c in d['cur']:
        dd = dol(c.get('dol')); el = dd['out'] if dd else 0; ch = c.get('like') or 0
        # the model's ranking (expected + 10% of outside spend x chance), on merch-only outside spend
        cur.append(dict(g=c['g'], name=c['name'], city=c.get('city'), st=c.get('st'), accts=c.get('n'), last_season=round(c.get('W2') or 0),
                        prior_season=round(c.get('W1') or 0), expected=round(c.get('expected') or 0), chance=c.get('like'),
                        elsewhere=el, opp=round((c.get('expected') or 0) + 0.1 * el * ch), last=c.get('last'),
                        cats=[x[0] for x in (c.get('cats') or [])[:3]], dol=dd, in_terr=c.get('in_terr')))
    pros = []
    for p in d['pros']:
        dd = dol(p['d']); el = dd['out'] if dd else 0
        if el < 10000:          # the model's prospect floor, applied to merch-only spend
            continue
        pros.append(dict(f=p['d']['f'], name=p['d']['name'], city=p['d'].get('city'), st=p['d'].get('st'), elsewhere=el,
                         door=p.get('door'), chance=p.get('prob'), score=round(el * (p.get('prob') or 0)), dol=dd, hood=hood(p['d'].get('hood')),
                         hist=hist(p.get('hist'))))
    hcur = [dict(g=h['g'], name=h['name'], city=h.get('city'), st=h.get('st'), kind=h.get('kind'), ours=round(h.get('hours') or 0),
                 elsewhere=round(h.get('hother') or 0), cross=round(h.get('hcross') or 0), potential=round(h.get('hpot') or 0),
                 score=round(h.get('hscore') or 0), hood_last2=[round(h.get('H2') or 0), round(h.get('H1') or 0)],
                 tees_last2=[round(h.get('T2') or 0), round(h.get('T1') or 0)], last=h.get('last'), hood=hood(h.get('hd')))
            for h in d['hcur']]
    hpro = [dict(f=p['d']['f'], name=p['d']['name'], city=p['d'].get('city'), st=p['d'].get('st'), door=p.get('door'),
                 chance=p.get('prob'), score=round(p.get('hscore') or 0), hood=hood(p.get('hd')), hist=hist(p.get('hist')))
            for p in d['hpro']]
    out['reviews'][code] = dict(label=d.get('label'), territory=d.get('region'), refreshed=d.get('refreshed'), hood_min=d.get('hood_min'),
                                cur=cur, pros=pros, hcur=hcur, hpro=hpro)
    out['model_refreshed'] = d.get('refreshed')
if a.run_date:
    out['model_refreshed'] = a.run_date
out['run_folder'] = os.path.basename(os.path.dirname(os.path.dirname(os.path.normpath(a.folder))))
json.dump(out, open(OUT, 'w', encoding='utf-8'), indent=0)
print(OUT, {k: [len(v[x]) for x in ('cur', 'pros', 'hcur', 'hpro')] for k, v in out['reviews'].items()}, season['label'])
