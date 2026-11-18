"""Nightly pull for the Quality Report page (Josh, 2026-11-03).

Rebuilds Traci's "Image Pointe Quality Report.xlsx" (her OneDrive, folder "Quality Report") as data, every night,
from the same four sources the workbook uses:

1. Customer Service: every HubSpot Support Pipeline ticket (pipeline 0), read-only private-app token on CHI-TOWN
   (%USERPROFILE%\\.hubspot\\credentials.json, the KPI dashboard's "Josh_R_API" key). Same columns and rules as the
   workbook's tab 1 (checked ticket by ticket against her 2026-11-03 copy):
   - Date / Closed Date = the UTC calendar date of createdate / closed_date (the workbook and the KPI page both do this).
   - Description = Description of incident, else Ticket description, else Ticket name (whitespace collapsed).
   - Responsible Dept: HubSpot stores two options by odd internal values, "true" = CSS and "false" = DA - IA. The
     workbook showed the raw values and called them data-entry errors; here they get their labels.
   - Customer Name = the Impress customer on the first order number in the ticket, else HubSpot's Customer Name.
   - Credit Memo Amt Lost: HubSpot's amount, cross-checked against Impress quality credit memos (type C, dated 2024
     or later) on the ticket's order numbers (the memo's own order field, its comment order, or an S-number in its
     comments; the workbook only used the comments, so it missed memos whose comment names the remake order).
     Blank in HubSpot -> filled in from Impress; HubSpot more than $1 BELOW Impress -> raised to Impress, old amount
     kept; HubSpot above Impress -> HubSpot stands (a ticket can cover a sibling order it doesn't list). Memos through
     2026-09-25 use the hand classification in docs/credit-memos-classified-2024-2026ytd-2026-09-25.csv; later memos
     use the keyword rule in memo_bucket().
   - CS issue close-out (Josh 2026-11-03): each ticket gets the list of fields Traci's CS & Quality SOP (v1.0,
     effective 2026-10-12) requires before close that are still blank on it (close_gaps()), and closeout() works out
     each month's status under a one-month lag: a month's tickets must be Closed with every required field by the last
     day of the next month. See scripts/quality/README.md.
2. Misprint / Damaged Goods: data/sales/misprints-damaged-goods.parquet (Impress GL 5900/5905, nightly). Ext cost is
   signed; only row_type normal_writeoff counts toward totals; sums are signed first, then the absolute value.
3-6. Manual tabs (Internal Cost Issues, Suggestions, Approval Table, Corrective Action Log): read from Traci's
   workbook through read-only Microsoft Graph (scripts/kpi/kpi_graph.py's sign-in). Traci keeps typing into the
   workbook; this only reads it. "Example row" rows are skipped.
   Plus the Internal Quality Issue and Suggestion for Improvement JotForms (2026-11-03, Traci's ask): their emails
   in quality@imagepointe.com are read read-only (form_submissions()); a submission already on tab 3 or 4 is matched
   and the workbook row wins; the rest show on the page marked "from the form" until Traci adds them.

Writes data/quality/quality-data.json (git-ignored: ticket text and the manual tabs name customers and employees).
Every source fails open: if one can't be read tonight, the last good copy in that file is kept and marked stale.

Usage: python scripts/quality/pull_quality.py [--no-graph] [--no-hubspot]
"""
import datetime as dt
import io
import json
import os
import re
import sys
import traceback
import urllib.request

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))); import paths  # noqa: E402  (data-extraction)
SALES = paths.SALES
OUT_DIR = paths.QUALITY
OUT = os.path.join(OUT_DIR, 'quality-data.json')
CLASSIFIED = os.path.join(paths.DOCS, 'credit-memos-classified-2024-2026ytd-2026-09-25.csv')
sys.path.insert(0, paths.GRAPH)   # kpi_graph.py, in this repo

# Traci's workbook (her OneDrive, folder "Quality Report"); found through her sharing link 2026-11-03.
QR_DRIVE = 'b!vseqtz1g7U2Yfao4T4C4LUn3QpZrX4FJs1kYj-jZE9wZJHkef6NcRI1ANEQ754mb'
QR_ITEM = '01CW7I7HG3XUX4DY3IXRBY374TVF2TQDEH'
QR_WEB = ('https://imagepointe-my.sharepoint.com/personal/traci_imagepointe_com/_layouts/15/Doc.aspx?sourcedoc='
          '%7BC12FBDDB-68E3-43BC-8DFF-93A975380C87%7D&file=Image%20Pointe%20Quality%20Report.xlsx&action=default')

PORTAL = '5530129'
STAGE = {'1': 'New', '149808057': 'Customer Acknowledged', '2': 'Waiting on contact', '3': 'Waiting on us',
         '3032664': 'Waiting on Mfg/Vendor', '4': 'Closed'}
DEPT = {'true': 'CSS', 'false': 'DA - IA'}
MEMO_START = '2024-01-01'   # the workbook's memo cross-check covers memos from 2024 on
HOLIDAYS = os.path.join(paths.TOOLS_DASH, 'holidays.json')   # the sales dashboard's company holidays

# CS issue close-out (Josh 2026-11-03). A month's tickets (by HubSpot create date, UTC, as everywhere on the page) must
# be Closed, with every field the CS & Quality SOP requires before close, by the last day of the NEXT month: at the end
# of October only September has to be closed out. September 2026 is the first month held to it (due 10/31, the first
# deadline after the SOP took effect on 10/12); open tickets from earlier months show as backlog. A closed-out month
# goes to the monthly department quality reviews.
CLOSE_START = '2026-09'
# The SOP's "Required ticket fields" that HubSpot has today (key, label on the page, the HubSpot field it's read from).
# "Description, with photos": the photos can't be checked from here, only that there's a description.
CLOSE_CHECKS = [
    ('order', 'Order number', 'Order number'),
    ('desc', 'Description', 'Description of incident, or the ticket description'),
    ('owner', 'Ticket owner', 'Ticket owner'),
    ('dept', 'Responsible dept', 'Responsible Dept'),
    ('lost', '$ lost (0 if none)', 'Credit Memo/$ Amount Lost'),
    ('at_risk', 'Customer at risk', 'Customer at Risk'),
    ('resolution', 'Resolution notes', 'Brief Notes on Resolution'),
    ('vendor', 'Vendor name (Vendor tickets)', 'Vendor, or PO # to the Vendor/Vendor Name'),
]
# SOP fields HubSpot doesn't have yet (Traci's "HubSpot changes in progress", SOP v1.0). Each night the ticket
# property list is read; a custom property whose label matches is checked from then on with no code change, and the
# page names the property it matched, so a wrong match shows. Credit memo # is only required when money was lost.
SOP_PENDING = [
    ('issue_type', 'Issue type', r'\bissue type\b|\btype of issue\b'),
    ('root_cause', 'Root cause', r'\broot cause'),
    ('memo_no', 'Credit memo #', r'credit memo\s*(#|no\b|num)'),
    ('vendor_recoverable', 'Vendor-recoverable', r'vendor.{0,3}recover'),
    ('confirmed', 'Customer confirmed resolved', r'customer confirm|confirmed (resolved|date|on)|resolution confirm'),
]
TODAY = dt.date.today()
A = sys.argv[1:]


def log(*a):
    print(*a, flush=True)


def clean(s):
    return re.sub(r'\s+', ' ', str(s or '')).strip()


def money(v):
    t = re.sub(r'[^0-9.\-]', '', str(v or ''))
    try:
        return float(t) if t not in ('', '-', '.') else None
    except ValueError:
        return None


def orders_in(s):
    # exactly S + 7 digits: an 8-digit typo like "S00756431" must not match S0075643, a different order
    return list(dict.fromkeys(re.findall(r'(?<![0-9A-Z])S\d{7}(?!\d)', str(s or '').upper())))


def iso(v):
    if v is None or (isinstance(v, float) and pd.isna(v)) or v is pd.NaT:
        return None
    if isinstance(v, (dt.datetime, pd.Timestamp)):
        return v.strftime('%Y-%m-%d')
    if isinstance(v, dt.date):
        return v.isoformat()
    return str(v)[:10] or None


# ------------------------------------------------------------------------------------------------------ HubSpot
TICKET_PROPS = ['subject', 'content', 'description_of_incident', 'createdate', 'order_number', 'type_of_order',
                'customer_name', 'sales_agent', 'responsible_dept', 'subtotal', 'shipping_charge',
                'credit_memo_amount_lost', 'customer_at_risk', 'commission_lost', 'brief_notes_on_resolution',
                'hs_pipeline_stage', 'closed_date', 'vendor', 'po___to_the_vendor_vendor_name', 'hubspot_owner_id']


def _hs(path, body=None):
    """One read-only HubSpot call (GET, or a POST search) with the private-app token."""
    p = os.path.join(os.path.expanduser('~'), '.hubspot', 'credentials.json')
    if not os.path.exists(p):
        raise RuntimeError('no HubSpot read-only token on this PC (%USERPROFILE%\\.hubspot\\credentials.json)')
    token = json.load(open(p, encoding='utf-8'))['token']
    req = urllib.request.Request('https://api.hubapi.com' + path, method='POST' if body else 'GET',
                                 data=json.dumps(body).encode() if body else None,
                                 headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def hubspot_tickets(props):
    out, after = [], None
    while True:
        body = {'filterGroups': [{'filters': [{'propertyName': 'hs_pipeline', 'operator': 'EQ', 'value': '0'}]}],
                'properties': props, 'limit': 100,
                'sorts': [{'propertyName': 'createdate', 'direction': 'ASCENDING'}]}
        if after:
            body['after'] = after
        d = _hs('/crm/v3/objects/tickets/search', body)
        out += [dict(x['properties'], id=x['id']) for x in d['results']]
        after = (d.get('paging') or {}).get('next', {}).get('after')
        if not after:
            return out


def sop_properties():
    """{SOP key: {name, label}} for the SOP fields in SOP_PENDING that now exist as custom ticket properties. If two
    properties match one field, the newest wins."""
    found = {}
    props = sorted((p for p in _hs('/crm/v3/properties/tickets')['results'] if not p.get('hubspotDefined')),
                   key=lambda p: p.get('createdAt') or '')
    for key, _, pat in SOP_PENDING:
        for p in props:
            if re.search(pat, p.get('label') or '', re.I) and p['name'] not in TICKET_PROPS:
                found[key] = dict(name=p['name'], label=p.get('label'))
    return found


def hubspot_owners():
    """{owner id: name}, archived owners too (old tickets can point at someone who has left)."""
    names = {}
    for arch in ('false', 'true'):
        after = None
        while True:
            d = _hs(f'/crm/v3/owners?limit=100&archived={arch}' + (f'&after={after}' if after else ''))
            for o in d['results']:
                n = clean(f'{o.get("firstName") or ""} {o.get("lastName") or ""}').title() or o.get('email') or ''
                names.setdefault(str(o['id']), n)
            after = (d.get('paging') or {}).get('next', {}).get('after')
            if not after:
                break
    return names


# ------------------------------------------------------------------------------------------------ credit memos
def memo_bucket(comments, agent):
    """Keyword rule for memos newer than the hand classification. Only 'quality' memos are matched to tickets.
    Checked against the 760 hand-classified memos: of the memos that share an order with a ticket, 331 of 343 are
    quality credits, so the rule mainly has to drop donations, tax fixes, webstore revenue and re-invoicing."""
    t, ag = str(comments or '').lower(), str(agent or '').lower()
    if 'in kind' in t or 'in-kind' in t or 'donat' in t or 'contribution' in t or 'charit' in ag or 'contribution' in ag:
        return 'in_kind_donation'
    if re.search(r'\btax', t) or 'avalara' in t:
        return 'billing_tax_paperwork'
    if 'webstore' in t and re.search(r'revenue|payout|payments|monthly fee|\d(st|nd|rd|th) q', t):
        return 'webstore_revenue_credit'
    if re.search(r'rebill|re-bill|new invoice|separate (order|invoice)|one invoice|same invoice|wrong (price|pricing|'
                 r'customer|account)|duplicate|double (bill|charge)|prepay|balance adjust|move shipping', t):
        return 'billing_tax_paperwork'
    return 'quality_concession'


def quality_memos():
    """{sales order: [(memo #, net credit as a positive $)]} for quality credit memos dated 2024 on."""
    m = pd.read_parquet(os.path.join(SALES, 'credit-memos.parquet'))
    m = m[(m.invoice_type == 'C') & (m.invoice_date >= MEMO_START)]
    hand = pd.read_csv(CLASSIFIED, dtype=str) if os.path.exists(CLASSIFIED) else pd.DataFrame(columns=['invoice_nbr'])
    bucket = dict(zip(hand.invoice_nbr, hand.get('analysis_bucket', [])))
    net = dict(zip(hand.invoice_nbr, pd.to_numeric(hand.get('net_credit_total', []), errors='coerce')))
    by_order, n_hand, n_rule = {}, 0, 0
    for r in m.itertuples():
        if r.invoice_nbr in bucket:
            b, amt = bucket[r.invoice_nbr], net.get(r.invoice_nbr)
            n_hand += 1
        else:
            b, amt = memo_bucket(r.comments, r.agent_name), r.inv_total
            n_rule += 1
        if b != 'quality_concession' or amt is None or pd.isna(amt):
            continue
        for o in set(orders_in(r.sales_order)) | set(orders_in(r.comment_sales_order)) | set(orders_in(r.comments)):
            by_order.setdefault(o, []).append((r.invoice_nbr, -float(amt)))
    last = m.invoice_date.max()
    return by_order, dict(hand=n_hand, rule=n_rule, through=iso(last))


def order_customers():
    o = pd.read_parquet(os.path.join(SALES, 'orders.parquet'), columns=['sales_order', 'customer_name'])
    o = o.dropna().drop_duplicates('sales_order')
    return dict(zip(o.sales_order.str.upper().str.strip(), o.customer_name.str.strip()))


def close_gaps(t, depts, hs_raw, hs_amt, memo_found, sop, sop_vals):
    """The SOP's before-close fields still blank on this ticket (keys of CLOSE_CHECKS / SOP_PENDING). Read from
    HubSpot's own fields: an amount the page fills in from Impress still counts as missing on the ticket."""
    miss = []
    if not clean(t.get('order_number')):
        miss.append('order')
    if not (clean(t.get('description_of_incident')) or clean(t.get('content'))):
        miss.append('desc')
    if not t.get('hubspot_owner_id'):
        miss.append('owner')
    if not depts:
        miss.append('dept')
    if not hs_raw:
        miss.append('lost')
    if not t.get('customer_at_risk'):
        miss.append('at_risk')
    if not clean(t.get('brief_notes_on_resolution')):
        miss.append('resolution')
    vendorish = 'Vendor' in depts or str(sop_vals.get('vendor_recoverable') or '').lower() in ('yes', 'true')
    if vendorish and not (clean(t.get('vendor')) or clean(t.get('po___to_the_vendor_vendor_name'))):
        miss.append('vendor')
    for key, _, _ in SOP_PENDING:
        if key not in sop:
            continue
        if key == 'memo_no' and not ((hs_amt or 0) > 0 or memo_found):
            continue
        if not clean(sop_vals.get(key)):
            miss.append(key)
    return miss


def build_tickets(raw, sop=None, owners=None):
    memos, memo_info = quality_memos()
    cust = order_customers()
    sop, owners = sop or {}, owners or {}
    rows = []
    for t in raw:
        ords = orders_in(t.get('order_number'))
        desc = clean(t.get('description_of_incident')) or clean(t.get('content')) or clean(t.get('subject'))
        depts = [DEPT.get(x, x) for x in (t.get('responsible_dept') or '').split(';') if x]
        hs_raw = clean(t.get('credit_memo_amount_lost'))
        hs = money(hs_raw)
        if hs is None and hs_raw:      # "n/a", "na", "none": the rep says nothing was lost
            hs = 0.0
        found = {}
        for o in ords:
            for nbr, amt in memos.get(o, []):
                found[nbr] = amt
        # Impress memos on the ticket's own orders are a floor, not the whole amount: a ticket can cover a sibling
        # order it doesn't list (ticket 253417315048: $5,359.79 = memos on S0081137 and S0081138, only the first is
        # on the ticket). So Impress fills a blank and raises a lower HubSpot amount, but never lowers HubSpot.
        amt, before, source, imp = hs, None, None, None
        if found:
            imp = round(sum(found.values()), 2)
            if hs is None:
                amt, source = imp, 'Impress - filled in'
            elif imp - hs > 1:
                amt, before, source = imp, hs, 'Impress - corrected'
            elif hs - imp > 1:
                source = 'HubSpot - more than Impress memos'
            else:
                source = 'HubSpot - matches Impress'
        elif hs is not None:
            source = 'HubSpot'
        name = next((cust[o] for o in ords if o in cust), None) or clean(t.get('customer_name')) or '(unassigned)'
        sop_vals = {k: clean(t.get(p['name'])) or None for k, p in sop.items()}
        oid = t.get('hubspot_owner_id')
        rows.append(dict(
            owner=(owners.get(str(oid)) or f'Owner {oid}') if oid else None,
            missing=close_gaps(t, depts, hs_raw, hs, bool(found), sop, sop_vals),
            vendor=clean(t.get('vendor')) or clean(t.get('po___to_the_vendor_vendor_name')) or None,
            **{k: v for k, v in sop_vals.items() if v},
            id=t['id'], date=(t.get('createdate') or '')[:10] or None, order=clean(t.get('order_number')) or None,
            type=t.get('type_of_order') or None, customer=name, rep=t.get('sales_agent') or None, desc=desc,
            dept=';'.join(depts) or None, subtotal=money(t.get('subtotal')), shipping=money(t.get('shipping_charge')),
            lost=None if amt is None else round(amt, 2), at_risk=t.get('customer_at_risk') or None,
            comm_lost=t.get('commission_lost') or None, resolution=clean(t.get('brief_notes_on_resolution')) or None,
            status=STAGE.get(t.get('hs_pipeline_stage'), t.get('hs_pipeline_stage')),
            closed=(t.get('closed_date') or '')[:10] or None, memos=', '.join(sorted(found)) or None,
            impress=imp, hs_before=before, src=source))
    rows.sort(key=lambda r: (r['date'] or '', r['id']))
    return rows, memo_info


def _month_end(ym):
    y, m = map(int, ym.split('-'))
    return dt.date(y + m // 12, m % 12 + 1, 1) - dt.timedelta(days=1)


def _next_month(ym):
    y, m = map(int, ym.split('-'))
    return f'{y + m // 12}-{m % 12 + 1:02d}'


def closeout(tickets, sop, prev):
    """Month-by-month close-out status from CLOSE_START to this month. A month is closed out when it has ended and
    every ticket opened in it is Closed with no required field blank. Status: closed | overdue (past the last day of
    the next month) | due (ended, inside its close-out month) | current (still running). closed_on = the first
    nightly run that saw it closed out (kept from the last run's file; dropped if a ticket reopens it)."""
    was = {m['month']: m for m in (prev or {}).get('months') or []}
    blocks = lambda t: t['status'] != 'Closed' or bool(t['missing'])
    months, ym, cur = [], CLOSE_START, TODAY.strftime('%Y-%m')
    while ym <= cur:
        ts = [t for t in tickets if (t['date'] or '')[:7] == ym]
        bad = [t for t in ts if blocks(t)]
        due = _month_end(_next_month(ym))
        ended = _month_end(ym) < TODAY
        status = 'closed' if ended and not bad else 'overdue' if TODAY > due else 'due' if ended else 'current'
        miss = {}
        for t in ts:
            for k in t['missing']:
                miss[k] = miss.get(k, 0) + 1
        months.append(dict(
            month=ym, due=due.isoformat(), status=status, n=len(ts), blocking=len(bad),
            open=sum(t['status'] != 'Closed' for t in ts), closed_missing=sum(t['status'] == 'Closed' and bool(t['missing']) for t in ts),
            missing=miss, lost=round(sum(t['lost'] or 0 for t in ts), 2),
            closed_on=((was.get(ym) or {}).get('closed_on') or TODAY.isoformat()) if status == 'closed' else None))
        ym = _next_month(ym)
    # Backlog: every ticket still open from before CLOSE_START, plus closed ones missing data from this calendar year
    # only. Most of the SOP fields were added to HubSpot on 2026-03-27, so older closed tickets never had them.
    old = [t for t in tickets if t['date'] and t['date'][:7] < CLOSE_START]
    since = f'{CLOSE_START[:4]}-01-01'
    backlog = dict(open=sum(t['status'] != 'Closed' for t in old),
                   oldest_open=min((t['date'] for t in old if t['status'] != 'Closed'), default=None),
                   closed_missing=sum(t['status'] == 'Closed' and bool(t['missing']) and t['date'] >= since for t in old),
                   closed_missing_since=since)
    checks = [dict(key=k, label=lab, hubspot=hs) for k, lab, hs in CLOSE_CHECKS]
    pending = [dict(key=k, label=lab, hubspot=(f'{sop[k]["label"]} ({sop[k]["name"]})' if k in sop else None))
               for k, lab, _ in SOP_PENDING]
    return dict(start=CLOSE_START, rule='one-month lag', months=months, backlog=backlog, checks=checks, pending=pending)


def holidays():
    """Company holidays (the sales dashboard's list) so the page counts business days the way the SOP's 10-day target
    means them."""
    try:
        d = json.load(open(HOLIDAYS, encoding='utf-8'))
        return sorted(day for y, v in d.items() if y[:1].isdigit() for day in (v.get('dates') or {}))
    except Exception as e:
        log(f'holidays: could not read {HOLIDAYS} ({e}); business days = Mon-Fri')
        return []


# ------------------------------------------------------------------------------------------ misprints / damage
def misprints():
    m = pd.read_parquet(os.path.join(SALES, 'misprints-damaged-goods.parquet'))
    m['year'] = m.gl_date.dt.year
    m['month'] = m.gl_date.dt.month
    m['inc'] = m.row_type == 'normal_writeoff'
    inc = m[m.inc]
    # $ lost = minus the signed sum (write-offs are negative), so a rare reversal in a group nets out instead of
    # adding to the loss -- the workbook's "sum signed first, then take the absolute value" rule
    agg = {}
    for key, col in (('category', 'category'), ('method', 'decoration_method')):
        g = inc.groupby(['year', col]).agg(n=('trans_id', 'size'), cost=('ext_cost', 'sum')).reset_index()
        agg[key] = [dict(year=int(r.year), k=r[col], n=int(r.n), lost=round(-r.cost, 2)) for _, r in g.iterrows()]
    g = inc.groupby(['year', 'month', 'category']).agg(n=('trans_id', 'size'), cost=('ext_cost', 'sum')).reset_index()
    agg['month'] = [dict(year=int(r.year), m=int(r.month), k=r.category, n=int(r.n), lost=round(-r.cost, 2))
                    for _, r in g.iterrows()]
    g = inc.groupby('year').agg(n=('trans_id', 'size'), cost=('ext_cost', 'sum')).reset_index()
    agg['year'] = [dict(year=int(r.year), n=int(r.n), lost=round(-r.cost, 2)) for _, r in g.iterrows()]
    # detail rows: this year and last year, newest first
    d = m[m.year >= TODAY.year - 1].sort_values(['gl_date', 'trans_id'], ascending=False)
    rows = [dict(date=iso(r.gl_date), cat=r.category, method=r.decoration_method,
                 order=clean(r.order_nbr if isinstance(r.order_nbr, str) else '') or None,
                 customer=r.customer_name, sku=r.sku, size=r.size,
                 qty=int(r.quantity), cost=round(float(r.ext_cost), 2), inc=bool(r.inc), row_type=r.row_type,
                 gl=r.glacct_code, comment=clean(r.comment if isinstance(r.comment, str) else '') or None,
                 by=r.entered_by, src=r.trans_source,
                 tid=r.trans_id) for r in d.itertuples()]
    return dict(agg=agg, rows=rows, through=iso(m.gl_date.max()), total_rows=int(len(m)))


# ----------------------------------------------------------------------------------------- Traci's manual tabs
MANUAL = {  # table name in the workbook -> (key, kind)
    'tblInternalCostCY': ('internal', 'cy'), 'tblInternalCostALL': ('internal', 'archive'),
    'tblSuggestionsCY': ('suggestions', 'cy'), 'tblSuggestionsALL': ('suggestions', 'archive'),
    'tblApprovalTable': ('approval', 'ref'), 'tblCorrectiveActions': ('actions', 'ref'),
}
FIELD = {  # workbook header -> JSON field
    'Date': 'date', 'Department': 'dept', 'Reported By': 'by', 'Order Number (if any)': 'order',
    'Customer Name (if any)': 'customer', 'Cost Type': 'cost_type', 'Description': 'desc', '$ Lost': 'lost',
    'Preventable (Y/N)': 'preventable', 'Resolution / Corrective Action': 'resolution',
    'Date Submitted': 'date', 'Category': 'category', 'Submitted By': 'by', 'Suggestion / Feedback': 'desc',
    'Priority': 'priority', 'Status': 'status', 'Assigned To': 'assigned', 'Resolution / Action Taken': 'resolution',
    'Date Closed': 'closed', 'Issue Type / Remedy': 'remedy', 'Approval Limit ($)': 'limit',
    'Approver (up to limit)': 'approver', 'Approver (above limit)': 'approver_above',
    'Policy Reference / Notes': 'notes', 'Date Opened': 'date', 'Report Month': 'report_month', 'Source': 'source',
    'Ticket / Ref #': 'ref', 'Issue / Pattern': 'issue', 'Root Cause': 'root_cause', 'Proposed Fix': 'fix',
    'Fix Type': 'fix_type', 'Owner': 'owner', 'Due Date': 'due', 'Approved By': 'approved_by',
    'Date Approved': 'approved', 'Verified in Report (Month)': 'verified_month', 'Notes': 'notes',
}


def manual_tabs():
    import openpyxl
    import kpi_graph as g
    item = g.get(f'/drives/{QR_DRIVE}/items/{QR_ITEM}')
    data = g.download(item)
    wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True)
    out = {'internal': [], 'suggestions': [], 'approval': [], 'actions': []}
    seen = set()
    for ws in wb.worksheets:
        for name, ref in ws.tables.items():   # openpyxl gives (table name, "A1:J202")
            if name not in MANUAL:
                continue
            seen.add(name)
            key, kind = MANUAL[name]
            cells = ws[ref]
            head = [str(c.value or '').strip() for c in cells[0]]
            for row in cells[1:]:
                rec = {}
                for h, c in zip(head, row):
                    v = c.value
                    if isinstance(v, (dt.datetime, dt.date)):
                        v = iso(v)
                    elif isinstance(v, str):
                        v = clean(v) or None
                    rec[FIELD.get(h, h)] = v
                if not any(v not in (None, '') for v in rec.values()):
                    continue
                if str(rec.get('desc') or '').lower().startswith('example row'):
                    continue
                if key == 'internal' and rec.get('lost') is not None:
                    rec['lost'] = money(rec['lost'])
                if key == 'approval' and rec.get('limit') is not None:
                    rec['limit'] = money(rec['limit'])
                rec['tab'] = kind
                out[key].append(rec)
    missing = sorted(set(MANUAL) - seen)
    return out, dict(modified=item.get('lastModifiedDateTime'),
                     modified_by=((item.get('lastModifiedBy') or {}).get('user') or {}).get('displayName'),
                     missing_tables=missing)


# ------------------------------------------------------------- JotForm submissions in quality@imagepointe.com
# Traci asked 2026-11-03 that the page keep reading the form submissions. The Internal Quality Issue and Suggestion
# for Improvement JotForms email quality@imagepointe.com; Josh's sign-in (kpi_graph's cache, Mail.Read.Shared) reads
# that mailbox read-only: nothing is moved, flagged or marked read. Every folder counts except Deleted Items, Junk,
# Drafts and Outbox, so deleting a test submission's email drops it here. A submission already typed into the
# workbook (tab 3 or 4) is matched and the workbook row wins; the rest show on the page marked "from the form".
QUALITY_BOX = 'quality@imagepointe.com'
FORMS = {'internal quality issue': 'internal', 'suggestion for improvement': 'suggestions'}
# form question (start of the label, lower case) -> JSON field, the same fields as the workbook tabs
FORM_FIELD = [('name', 'by'), ('date', 'date'), ('department', 'dept'), ('order', 'order'), ('customer', 'customer'),
              ('cost type', 'cost_type'), ('what happened', 'desc'), ('your suggestion', 'desc'),
              ('suggestion', 'desc'), ('feedback', 'desc'), ('estimated', 'lost'), ('$', 'lost'),
              ('was this preventable', 'preventable'), ('preventable', 'preventable'), ('resolution', 'resolution'),
              ('category', 'category'), ('priority', 'priority')]
SKIP_FOLDERS = ('deleteditems', 'junkemail', 'drafts', 'outbox')


def _form_field(label):
    lab = clean(label).lower()
    return next((f for k, f in FORM_FIELD if lab.startswith(k)), None)


def _form_date(s):
    s = clean(s)
    for fmt in ('%m-%d-%Y', '%m/%d/%Y', '%Y-%m-%d', '%m-%d-%y', '%m/%d/%y', '%B %d, %Y', '%b %d, %Y'):
        try:
            return dt.datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _central_date(utc_iso):
    from zoneinfo import ZoneInfo
    t = dt.datetime.fromisoformat(utc_iso.replace('Z', '+00:00'))
    return t.astimezone(ZoneInfo('America/Chicago')).date().isoformat()


def parse_form(html, subject=''):
    """One JotForm notification email -> (form key, {label: value}). Multi-pick answers are joined with ';' like the
    workbook ("Sales;Logistics"). A plain forward loses JotForm's markup, so its label/value lines are read instead."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html or '', 'html.parser')
    h3 = soup.find('h3')
    title = clean(h3.get_text(' ')) if h3 else re.sub(r'^((re|fw|fwd):\s*)+', '', clean(subject), flags=re.I).split(' - ')[0]
    form = FORMS.get(title.lower())
    if not form:
        return None, {}
    vals = {}
    for tr in soup.find_all('tr', id=re.compile(r'^row_\d+$')):
        q, v = tr.find(id=re.compile(r'^question_')), tr.find(id=re.compile(r'^value_'))
        if not q or not v:
            continue
        chips = [clean(c.get_text(' ')) for c in v.find_all('td', style=re.compile('border-radius'))]
        for br in v.find_all('br'):
            br.replace_with('\n')
        vals[clean(q.get_text(' '))] = ';'.join(c for c in chips if c) if chips else '\n'.join(
            clean(x) for x in v.get_text('').split('\n') if clean(x))
    if not vals:   # forwarded: "Label\nValue\nLabel\nValue ..." after the form title, up to JotForm's footer
        lines = [clean(x) for x in soup.get_text('\n').split('\n') if clean(x)]
        try:
            lines = lines[len(lines) - 1 - lines[::-1].index(title):][1:]
        except ValueError:
            lines = []
        cur = None
        for ln in lines:
            if ln.lower().startswith('you can'):
                break
            if _form_field(ln) and (cur is None or vals.get(cur)):
                cur = ln
                vals[cur] = ''
            elif cur:
                vals[cur] = (vals[cur] + '\n' + ln).strip()
    return form, vals


def form_submissions():
    import requests
    import kpi_graph as g
    app = g._app()
    accounts = app.get_accounts()
    res = app.acquire_token_silent(['Mail.Read.Shared'], account=accounts[0]) if accounts else None
    if not res or 'access_token' not in res:
        raise g.NotSignedIn('No Mail.Read.Shared token in the KPI Graph sign-in. Run: python scripts/kpi/kpi_graph.py --login')
    s = requests.Session()
    s.headers['Authorization'] = f'Bearer {res["access_token"]}'
    base = f'https://graph.microsoft.com/v1.0/users/{QUALITY_BOX}'

    def get(url, params=None):
        r = s.get(url if url.startswith('http') else base + url, params=params, timeout=120)
        r.raise_for_status()
        return r.json()

    def pages(url, params):
        while url:
            d = get(url, params)
            params = None
            yield from d.get('value', [])
            url = d.get('@odata.nextLink')

    # well-known folder ids come back in the same form as a folder listing (not as a message's parentFolderId)
    skip = {get(f'/mailFolders/{f}', {'$select': 'id'})['id'] for f in SKIP_FOLDERS}
    folders, todo = [], list(pages('/mailFolders', {'$top': 100, '$select': 'id,displayName,childFolderCount'}))
    while todo:
        f = todo.pop()
        if f['id'] in skip:
            continue
        folders.append(f)
        if f.get('childFolderCount'):
            todo += list(pages(f'/mailFolders/{f["id"]}/childFolders',
                               {'$top': 100, '$select': 'id,displayName,childFolderCount'}))
    subs, unknown = {}, set()
    for f in folders:
        for m in pages(f'/mailFolders/{f["id"]}/messages',
                       {'$top': 50, '$select': 'subject,from,receivedDateTime,body'}):
            body = (m.get('body') or {}).get('content') or ''
            if 'jotform' not in body.lower():
                continue
            form, vals = parse_form(body, m.get('subject'))
            if not form or not vals:
                continue
            sid = re.search(r'jotform\.com/edit/(\d+)', body)
            sender = (((m.get('from') or {}).get('emailAddress') or {}).get('address') or '').lower()
            rec = {'form': form, 'sub_id': sid.group(1) if sid else None, 'received': _central_date(m['receivedDateTime']),
                   'folder': f['displayName'], 'original': sender == 'noreply@jotform.com'}
            for lab, v in vals.items():
                k = _form_field(lab)
                if not k:
                    unknown.add(lab)
                elif v and not rec.get(k):
                    rec[k] = v
            rec['date'] = _form_date(rec.get('date')) if form == 'internal' and rec.get('date') else None
            rec['date'] = rec['date'] or rec['received']
            rec['lost'] = money(rec.get('lost')) if rec.get('lost') else None
            rec['order'] = clean(rec.get('order')).upper() or None
            if form == 'suggestions':
                rec['by'] = rec.get('by') or '(anonymous)'
                rec['status'] = 'New'
            key = rec['sub_id'] or (form, rec.get('by'), rec['date'], clean(rec.get('desc'))[:200])
            # the JotForm email itself beats a forward of it
            if key not in subs or (rec['original'] and not subs[key]['original']):
                subs[key] = rec
    if unknown:
        log(f'forms: questions not mapped to a column (ignored): {sorted(unknown)}')
    return sorted(subs.values(), key=lambda r: (r['received'], r['date'] or ''))


def match_forms(forms, manual):
    """Mark each submission already on the workbook (in_workbook). Same day plus the same person, order or similar
    text; or within two weeks with similar text; or near-identical text. One workbook row per submission."""
    import difflib

    def norm(s):
        return re.sub(r'[^a-z0-9]', '', str(s or '').lower())

    def day(s):
        try:
            return dt.date.fromisoformat(str(s)[:10])
        except ValueError:
            return None
    for kind in ('internal', 'suggestions'):
        book, used = list((manual or {}).get(kind) or []), set()
        for f in (x for x in forms if x['form'] == kind):
            best, best_score = None, 0
            for i, r in enumerate(book):
                if i in used:
                    continue
                d1, d2 = day(r.get('date')), day(f.get('date'))
                gap = abs((d1 - d2).days) if d1 and d2 else 999
                who = norm(r.get('by')) and norm(r.get('by')) == norm(f.get('by'))
                order = f.get('order') and norm(r.get('order')) == norm(f.get('order'))
                text = difflib.SequenceMatcher(None, norm(r.get('desc'))[:400], norm(f.get('desc'))[:400]).ratio()
                if (gap == 0 and (who or order or text >= .5)) or (gap <= 14 and text >= .6) or text >= .85:
                    score = (2 if gap == 0 else 0) + who + bool(order) + 2 * text
                    if score > best_score:
                        best, best_score = i, score
            f['in_workbook'] = best is not None
            if best is not None:
                used.add(best)
    return forms


# ------------------------------------------------------------------------------------------------------- main
def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    prev = json.load(open(OUT, encoding='utf-8')) if os.path.exists(OUT) else {}
    res = dict(built=dt.datetime.now().strftime('%Y-%m-%d %H:%M'), year=TODAY.year, workbook_url=QR_WEB,
               portal=PORTAL, sources={})

    def keep(key, why):
        res[key] = prev.get(key)
        s = dict((prev.get('sources') or {}).get(key) or {})
        s.update(ok=False, error=why, stale_since=s.get('stale_since') or s.get('as_of'))
        res['sources'][key] = s
        log(f'{key}: FAILED ({why}); keeping the copy from {s.get("as_of")}')

    # 1. tickets
    if '--no-hubspot' in A:
        keep('tickets', 'skipped (--no-hubspot)')
    else:
        try:
            try:
                sop = sop_properties()
            except Exception as e:     # the property list is a nice-to-have; the tickets still come in without it
                sop = {}
                log(f'tickets: could not read the ticket property list ({type(e).__name__}: {e}); SOP fields not checked')
            try:
                owners = hubspot_owners()
            except Exception as e:
                owners = {}
                log(f'tickets: could not read HubSpot owners ({type(e).__name__}: {e}); owners show by id')
            rows, memo_info = build_tickets(hubspot_tickets(TICKET_PROPS + [p['name'] for p in sop.values()]), sop, owners)
            res['tickets'] = rows
            res['sources']['tickets'] = dict(ok=True, as_of=TODAY.isoformat(), n=len(rows), memos=memo_info)
            log(f'tickets: {len(rows)} (memos through {memo_info["through"]}: {memo_info["hand"]} hand-classified, '
                f'{memo_info["rule"]} by rule)')
            res['closeout'] = closeout(rows, sop, prev.get('closeout'))
            log('close-out: ' + '; '.join(f'{m["month"]} {m["status"]} ({m["open"]} open, {m["closed_missing"]} closed '
                                          f'missing data)' for m in res['closeout']['months']) +
                f'; SOP fields found in HubSpot: {sorted(sop) or "none yet"}')
        except Exception as e:
            traceback.print_exc()
            keep('tickets', f'{type(e).__name__}: {e}')
    if 'closeout' not in res:
        res['closeout'] = prev.get('closeout')
    res['holidays'] = holidays()
    # 2. misprints / damage
    try:
        mp = misprints()
        res['misprints'] = mp
        res['sources']['misprints'] = dict(ok=True, as_of=TODAY.isoformat(), through=mp['through'], n=mp['total_rows'])
        log(f'misprints: {mp["total_rows"]} rows through {mp["through"]}')
    except Exception as e:
        traceback.print_exc()
        keep('misprints', f'{type(e).__name__}: {e}')
    # 3-6. manual tabs
    if '--no-graph' in A:
        keep('manual', 'skipped (--no-graph)')
    else:
        try:
            man, info = manual_tabs()
            res['manual'] = man
            res['sources']['manual'] = dict(ok=True, as_of=TODAY.isoformat(), **info,
                                            n={k: len(v) for k, v in man.items()})
            log(f'manual tabs: {res["sources"]["manual"]["n"]}; workbook saved {info["modified"]} by '
                f'{info["modified_by"]}' + (f'; MISSING tables {info["missing_tables"]}' if info['missing_tables'] else ''))
        except Exception as e:
            traceback.print_exc()
            keep('manual', f'{type(e).__name__}: {e}')
    # 3-4 also: JotForm submissions in quality@imagepointe.com, matched against tonight's (or the kept) workbook rows
    if '--no-graph' in A:
        keep('forms', 'skipped (--no-graph)')
    else:
        try:
            res['forms'] = form_submissions()
            res['sources']['forms'] = dict(ok=True, as_of=TODAY.isoformat(), mailbox=QUALITY_BOX, n=len(res['forms']))
        except Exception as e:
            traceback.print_exc()
            keep('forms', f'{type(e).__name__}: {e}')
    if res.get('forms'):
        match_forms(res['forms'], res.get('manual'))
        new = [f for f in res['forms'] if not f.get('in_workbook')]
        log(f'forms: {len(res["forms"])} submissions in {QUALITY_BOX}, {len(new)} not on the workbook yet '
            f'({sum(f["form"] == "internal" for f in new)} internal, {sum(f["form"] == "suggestions" for f in new)} suggestions)')
    json.dump(res, open(OUT, 'w', encoding='utf-8'), ensure_ascii=False, separators=(',', ':'), default=str)
    log('wrote', OUT, f'({os.path.getsize(OUT) / 1e6:.2f} MB)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
