"""Nightly read-only HubSpot pull for the dashboard's contact activity (Josh, 2026-10-17).

Josh: show, for each current customer, the contacts who placed orders from 2023 on, each with their last call, last
email and last visit. READ-ONLY, always (CLAUDE.md "HubSpot integration"): every request here is a GET or a search /
batch-read POST; nothing is created, updated or deleted in HubSpot.

Token: a HubSpot private app with read scopes only, saved in %USERPROFILE%\\.hubspot\\credentials.json as
{"token": "pat-..."} (never in the repo). No token or an API error = print why and exit 0, so the nightly run keeps
yesterday's files (fail-open, like pull_wholesale.py).

Writes (data/sales/; Josh OK'd contact names, titles, emails and phones in the
private repo, 2026-10-17):
  hubspot-engagements.parquet  one row per call / email / meeting since 2023-01-01: id, type, ts, direction, owner,
                               meeting location / outcome / activity type, associated contact ids. No subjects or bodies.
  hubspot-contacts.parquet     the HubSpot contacts whose email is on an Impress order since 2023: id, email, name,
                               title, phone, mobile.
  hubspot-owners.json          owner id -> name
  hubspot-notes.parquet        notes since 2026-01-01: a 600-character excerpt of the text, owner, contacts, companies
  hubspot-deals.parquet        deals created since 2026-01-01: name (carries the SO number), stage, amount, dates, owner,
                               next activity date, companies
  hubspot-tasks.parquet        tasks created since 2026-01-01 (Josh, 2026-11-02): title, status, priority, type, due,
                               completed, owner, a 600-character excerpt of the task notes (WIG commitments: what the rep
                               says they did), contacts, companies
  hubspot-companies.parquet    every company: name, Impress customer number, contact count, last contacted, owner
  hubspot-contact-accounts.parquet  (Josh, 2026-11-02) every contact on a company that has an Impress customer number:
                               contact id, customer number (the rep pages count activity with a business manager who never
                               ordered toward that account)
  hubspot-activity-state.json  last successful pull (the next run only fetches engagements modified since then)

First run backfills every engagement since 2023-01-01 (a few thousand API calls, several minutes); later runs fetch
what changed in the last day or two.
"""
import json, os, re, sys, time
from datetime import datetime, timedelta, timezone
import urllib.request, urllib.error
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)   # data-extraction (moved from the sales-dashboard skill 2026-11-18)
SALES = os.path.join(os.environ.get('SALES_DATA_DIR') or os.path.join(os.path.dirname(REPO), 'data'), 'sales')  # the data folder (not a repo since 2026-11-18): SALES_DATA_DIR, or 'data' next to the repos
CRED = os.path.join(os.path.expanduser('~'), '.hubspot', 'credentials.json')
ENG = os.path.join(SALES, 'hubspot-engagements.parquet')
CON = os.path.join(SALES, 'hubspot-contacts.parquet')
OWN = os.path.join(SALES, 'hubspot-owners.json')
STATE = os.path.join(SALES, 'hubspot-activity-state.json')
SINCE = '2023-01-01'
API = 'https://api.hubapi.com'
TYPES = {   # object -> properties read (no subjects, bodies or notes)
    'calls': ['hs_timestamp', 'hs_call_direction', 'hs_call_status', 'hs_call_disposition', 'hubspot_owner_id', 'hs_lastmodifieddate'],
    'emails': ['hs_timestamp', 'hs_email_direction', 'hubspot_owner_id', 'hs_lastmodifieddate', 'hs_email_from_email', 'hs_email_subject'],
    'meetings': ['hs_timestamp', 'hs_meeting_location', 'hs_meeting_outcome', 'hs_activity_type', 'hubspot_owner_id', 'hs_lastmodifieddate'],
}
TOKEN = None
NOISE_DROPPED = [0]
# Call outcome = the call's disposition label (2026-11-02). The rep pages count a Connected or inbound call as "reached";
# the hand MCP pulls carried it, so the nightly must too. Labels come from HubSpot each night; this is the fallback.
CALL_OUTCOMES = {'f240bbac-87c9-4f6e-bf70-924b57d47db7': 'Connected', 'b2cf5968-551e-4856-9783-52b3da59a7d0': 'Left voicemail',
                 '73a0d17f-1163-4015-bdd5-ec830791da20': 'No answer', 'a4c4c377-d246-4b32-a13b-75a56a4cd0ff': 'Left live message',
                 '9d9162e7-6cf3-4944-bf63-4dff82258764': 'Busy', '2e7360c1-6b71-40e9-ab2b-30ae98a4678c': 'Meeting booked',
                 '17b47fee-58de-441e-a44c-c6300d46f273': 'Wrong number'}
OUTCOME_VERSION = 1   # bump to force one full engagement backfill when the saved rows lack a newer field
# Mail that isn't a conversation with the customer (Josh, 2026-10-18), dropped before anything is saved. The sender and
# subject are read only to make this call; neither is stored. Automated senders (HubSpot notifications, Microsoft
# quarantine, no-reply / mailer-daemon), auto-replies, bounces and calendar responses ("Accepted:"), and "incoming" mail
# that came from our own staff. HubSpot marketing email is a separate object and never reaches this pull.
NOISE_FROM = re.compile(r'no-?reply|notifications?@|quarantine@|mailer-daemon|postmaster|bounces?@|@notifications\.hubspot\.com$', re.I)
NOISE_SUBJ = re.compile(r'^\s*(automatic reply|auto[- ]?reply|autoreply|out of (the )?office|undeliverable|delivery status notification|'
                        r'mail delivery (failed|subsystem)|returned mail|accepted:|declined:|tentative:|read:)', re.I)
OUR_DOMAINS = re.compile(r'@(imagepointe|dignityapparel)\.com$', re.I)


def is_noise(p):
    frm, subj = (p.get('hs_email_from_email') or '').strip(), p.get('hs_email_subject') or ''
    return bool(NOISE_FROM.search(frm) or NOISE_SUBJ.search(subj) or
                (p.get('hs_email_direction') == 'INCOMING_EMAIL' and OUR_DOMAINS.search(frm)))


def req(method, path, body=None, tries=6):
    """One read-only API call with backoff on 429 / 5xx. Only GET and read POSTs (search, batch/read) are used."""
    assert method == 'GET' or path.endswith('/search') or '/batch/read' in path, f'not a read call: {method} {path}'
    data = json.dumps(body).encode() if body is not None else None
    for i in range(tries):
        r = urllib.request.Request(API + path, data=data, method=method,
                                   headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(r, timeout=60) as resp:
                return json.loads(resp.read() or b'{}')
        except urllib.error.HTTPError as e:
            msg = e.read().decode(errors='replace')[:400]
            if e.code in (429, 500, 502, 503, 504) and i < tries - 1:
                time.sleep(2 ** i); continue
            raise RuntimeError(f'HubSpot {e.code} on {method} {path}: {msg}')
        except urllib.error.URLError as e:
            if i < tries - 1:
                time.sleep(2 ** i); continue
            raise RuntimeError(f'HubSpot unreachable: {e}')


def ms(dt):
    return str(int(dt.timestamp() * 1000))


def search_window(obj, prop, a, b):
    """Every object with a <= prop < b. Search returns at most 10,000 per query, so a full window is split in half."""
    out, after = [], None
    while True:
        body = dict(filterGroups=[dict(filters=[dict(propertyName=prop, operator='GTE', value=ms(a)),
                                                dict(propertyName=prop, operator='LT', value=ms(b))])],
                    properties=(TYPES.get(obj) or EXTRA[obj][1]), limit=200, sorts=[dict(propertyName=prop, direction='ASCENDING')])
        if after: body['after'] = after
        r = req('POST', f'/crm/v3/objects/{obj}/search', body)
        if not after and r.get('total', 0) > 9800 and (b - a) > timedelta(hours=2):
            mid = a + (b - a) / 2
            return search_window(obj, prop, a, mid) + search_window(obj, prop, mid, b)
        out += r.get('results', [])
        after = (r.get('paging') or {}).get('next', {}).get('after')
        time.sleep(0.22)                    # search API: about 5 requests a second
        if not after: return out


def contacts_of(obj, ids, to='contacts'):
    """object id -> [associated ids] (contacts by default), v4 batch associations (1,000 per call)."""
    m = {}
    for i in range(0, len(ids), 1000):
        r = req('POST', f'/crm/v4/associations/{obj}/{to}/batch/read', dict(inputs=[dict(id=x) for x in ids[i:i + 1000]]))
        for x in r.get('results', []):
            fid = str(x['from']['id'])
            got = [str(t['toObjectId']) for t in x.get('to', [])]
            after = ((x.get('paging') or {}).get('next') or {}).get('after')
            while after:                     # a record with more associations than one page (a big company's contacts)
                p = req('GET', f'/crm/v4/objects/{obj}/{fid}/associations/{to}?limit=500&after={after}')
                got += [str(t['toObjectId']) for t in p.get('results', [])]
                after = ((p.get('paging') or {}).get('next') or {}).get('after')
            m[fid] = got
        time.sleep(0.12)
    return m


def call_outcomes():
    try:
        got = {d['id']: d['label'] for d in req('GET', '/calling/v1/dispositions')}
        return {**CALL_OUTCOMES, **got}
    except Exception as e:
        print(f'  call outcome labels: using the saved list ({str(e)[:120]})', flush=True)
        return CALL_OUTCOMES


def pull_engagements(since):
    rows = []
    now = datetime.now(timezone.utc)
    disp = call_outcomes()
    for obj in TYPES:
        prop = 'hs_timestamp' if since is None else 'hs_lastmodifieddate'
        start = datetime.fromisoformat(SINCE).replace(tzinfo=timezone.utc) if since is None else since
        got = []
        a = start
        while a < now:                       # month windows keep each search well under the 10,000 cap
            b = min(now + timedelta(minutes=5), (a.replace(day=1) + timedelta(days=32)).replace(day=1, hour=0, minute=0, second=0, microsecond=0))
            got += search_window(obj, prop, a, b)
            a = b
        if obj == 'emails':
            n0 = len(got)
            got = [x for x in got if not is_noise(x.get('properties', {}))]
            NOISE_DROPPED[0] += n0 - len(got)
        assoc = contacts_of(obj, [x['id'] for x in got])
        for x in got:
            p = x.get('properties', {})
            rows.append(dict(id=f"{obj}:{x['id']}", type=obj, ts=p.get('hs_timestamp'),
                             direction=p.get('hs_call_direction') or p.get('hs_email_direction'), status=p.get('hs_call_status'),
                             owner=p.get('hubspot_owner_id'), location=p.get('hs_meeting_location'),
                             outcome=(disp.get(p.get('hs_call_disposition') or '', p.get('hs_call_disposition')) if obj == 'calls'
                                      else p.get('hs_meeting_outcome')),
                             activity_type=p.get('hs_activity_type'), contacts=','.join(assoc.get(str(x['id']), []))))
        print(f'  {obj}: {len(got)} {"since " + since.isoformat() if since else "since " + SINCE}', flush=True)
    return pd.DataFrame(rows)


# Notes, deals and companies (Josh, 2026-10-18), for the reps' two-week pages: notes reps log on the accounts they're
# working (a short excerpt of the text, tags stripped), deals created (a deal for every order, early deals), and each
# company's contact count and last-contacted date. Backfill from 2026-01-01; later runs fetch what changed.
# Tasks (Josh, 2026-11-02): the reps' HubSpot to-dos, incl. the "MM.DD.YY WIG Commitment #n" tasks and the ones Mai enters
# from the two-week plans, with the rep's task notes, so the two-week review reads them from here instead of by hand.
EXTRA = {  # object -> (date property for the backfill, properties read)
    'notes': ('hs_timestamp', ['hs_timestamp', 'hs_note_body', 'hubspot_owner_id', 'hs_lastmodifieddate']),
    'deals': ('createdate', ['dealname', 'dealstage', 'amount', 'createdate', 'closedate', 'hubspot_owner_id', 'hs_lastmodifieddate',
                             'notes_next_activity_date']),
    'tasks': ('hs_createdate', ['hs_createdate', 'hs_timestamp', 'hs_task_completion_date', 'hs_task_status', 'hs_task_priority',
                                'hs_task_type', 'hs_task_subject', 'hs_task_body', 'hubspot_owner_id', 'hs_lastmodifieddate']),
}
# columns each saved file must have; a file without one (written before the column was added) is backfilled once
COLS = {'notes': ['id', 'ts', 'owner', 'body', 'contacts', 'companies'],
        'deals': ['id', 'name', 'stage', 'amount', 'created', 'closed', 'owner', 'next_activity', 'companies'],
        'tasks': ['id', 'created', 'due', 'completed', 'status', 'priority', 'type', 'owner', 'subject', 'body', 'contacts', 'companies']}
NOTES, DEALS, TASKS, COMPANIES, CONTACT_ACCOUNTS = (os.path.join(SALES, f'hubspot-{x}.parquet')
                                                   for x in ('notes', 'deals', 'tasks', 'companies', 'contact-accounts'))
EXTRA_SINCE = '2026-01-01'
TAGS = re.compile(r'<[^>]+>')


def excerpt(s, n=600):
    return ' '.join(TAGS.sub(' ', s or '').replace('&nbsp;', ' ').split())[:n]


def windows(start, now):
    a = start
    while a < now:
        b = min(now + timedelta(minutes=5), (a.replace(day=1) + timedelta(days=32)).replace(day=1, hour=0, minute=0, second=0, microsecond=0))
        yield a, b
        a = b


def pull_extra(obj, since, path):
    """notes, deals or tasks since EXTRA_SINCE (or changed since the last pull), merged into the saved file."""
    now = datetime.now(timezone.utc)
    fresh = since is None or not os.path.exists(path) or not set(COLS[obj]) <= set(pd.read_parquet(path).columns)
    prop = EXTRA[obj][0] if fresh else 'hs_lastmodifieddate'
    start = datetime.fromisoformat(EXTRA_SINCE).replace(tzinfo=timezone.utc) if fresh else since
    got = [x for a, b in windows(start, now) for x in search_window(obj, prop, a, b)]
    ids = [x['id'] for x in got]
    cos = contacts_of(obj, ids, 'companies')
    con = contacts_of(obj, ids) if obj in ('notes', 'tasks') else {}
    rows = []
    for x in got:
        p, i = x.get('properties', {}), str(x['id'])
        links = dict(contacts=','.join(con.get(i, [])), companies=','.join(cos.get(i, [])))
        if obj == 'notes':
            rows.append(dict(id=i, ts=p.get('hs_timestamp'), owner=p.get('hubspot_owner_id'), body=excerpt(p.get('hs_note_body')), **links))
        elif obj == 'tasks':
            rows.append(dict(id=i, created=p.get('hs_createdate'), due=p.get('hs_timestamp'), completed=p.get('hs_task_completion_date'),
                             status=p.get('hs_task_status'), priority=p.get('hs_task_priority'), type=p.get('hs_task_type'),
                             owner=p.get('hubspot_owner_id'), subject=excerpt(p.get('hs_task_subject'), 200),
                             body=excerpt(p.get('hs_task_body')), **links))
        else:
            rows.append(dict(id=i, name=p.get('dealname'), stage=p.get('dealstage'), amount=p.get('amount'),
                             created=p.get('createdate'), closed=p.get('closedate'), owner=p.get('hubspot_owner_id'),
                             next_activity=p.get('notes_next_activity_date'), companies=links['companies']))
    new = pd.DataFrame(rows, columns=COLS[obj])
    if not fresh:
        old = pd.read_parquet(path)
        new = pd.concat([old[~old.id.isin(new.id)] if len(new) else old, new], ignore_index=True)
    new.to_parquet(path, index=False)
    print(f'  {obj}: {len(got)} fetched, {len(new)} saved', flush=True)
    return len(new)


def pull_companies():
    """Every company: name, Impress customer number, contact count, last contacted, owner (a full list each night)."""
    rows, after = [], None
    props = 'name,customer_number,num_associated_contacts,notes_last_contacted,hubspot_owner_id'
    while True:
        r = req('GET', f'/crm/v3/objects/companies?limit=100&properties={props}' + (f'&after={after}' if after else ''))
        for x in r.get('results', []):
            p = x.get('properties', {})
            rows.append(dict(id=str(x['id']), name=p.get('name'), customer_number=(p.get('customer_number') or '').strip(),
                             contacts_n=int(p.get('num_associated_contacts') or 0), last_contacted=p.get('notes_last_contacted'),
                             owner=p.get('hubspot_owner_id')))
        after = (r.get('paging') or {}).get('next', {}).get('after')
        time.sleep(0.11)
        if not after: break
    pd.DataFrame(rows).to_parquet(COMPANIES, index=False)
    print(f'  companies: {len(rows)}', flush=True)
    return len(rows)


def pull_contact_accounts():
    """Every contact on a company with an Impress customer number (Josh, 2026-11-02; a hand MCP pull until then). Runs after
    pull_companies, from the file it just wrote."""
    cos = pd.read_parquet(COMPANIES)
    ck = {str(i): k for i, k in zip(cos.id, cos.customer_number.fillna('').astype(str).str.strip()) if k}
    assoc = contacts_of('companies', list(ck))
    ca = pd.DataFrame([dict(id=c, customer_number=ck[co]) for co, cs in assoc.items() if co in ck for c in cs],
                      columns=['id', 'customer_number']).drop_duplicates()
    ca.to_parquet(CONTACT_ACCOUNTS, index=False)
    print(f'  contact-accounts: {len(ca)} contacts on {ca.customer_number.nunique()} of {len(ck)} customer-numbered companies', flush=True)
    return len(ca)


def ordering_emails():
    o = pd.read_parquet(os.path.join(SALES, 'orders.parquet'), columns=['sales_order', 'received_date', 'buyer_email'])
    o = o[pd.to_datetime(o.received_date, errors='coerce') >= SINCE]
    e = o.buyer_email.dropna().astype(str).str.lower().str.extract(r"([a-z0-9._%+'-]+@[a-z0-9-]+(?:\.[a-z0-9-]+)+)", expand=False)
    return sorted(set(e.dropna().str.strip('.')))


def pull_contacts(emails):
    rows = []
    props = ['email', 'firstname', 'lastname', 'jobtitle', 'phone', 'mobilephone']
    for i in range(0, len(emails), 100):
        body = dict(filterGroups=[dict(filters=[dict(propertyName='email', operator='IN', values=emails[i:i + 100])])],
                    properties=props, limit=200)
        r = req('POST', '/crm/v3/objects/contacts/search', body)
        for x in r.get('results', []):
            p = x.get('properties', {})
            rows.append(dict(id=str(x['id']), email=(p.get('email') or '').lower(), first=p.get('firstname'), last=p.get('lastname'),
                             title=p.get('jobtitle'), phone=p.get('phone'), mobile=p.get('mobilephone')))
        time.sleep(0.22)
    return pd.DataFrame(rows)


def pull_owners():
    out, after = {}, None
    while True:
        r = req('GET', '/crm/v3/owners?limit=500' + (f'&after={after}' if after else ''))
        for x in r.get('results', []):
            out[str(x['id'])] = ' '.join(filter(None, [x.get('firstName'), x.get('lastName')])) or x.get('email') or str(x['id'])
        after = (r.get('paging') or {}).get('next', {}).get('after')
        if not after: return out


def main():
    global TOKEN
    if not os.path.exists(CRED):
        print(f'HubSpot activity: no token at {CRED}; keeping the existing files'); return 0
    TOKEN = json.load(open(CRED)).get('token')
    if not TOKEN:
        print('HubSpot activity: credentials.json has no "token"; keeping the existing files'); return 0
    state = json.load(open(STATE)) if os.path.exists(STATE) else {}
    started = datetime.now(timezone.utc)
    try:
        owners = pull_owners()
        last = state.get('last_pull')
        since = ((datetime.fromisoformat(last) - timedelta(days=2))
                 if (last and os.path.exists(ENG) and state.get('outcome_version', 0) >= OUTCOME_VERSION) else None)
        print('HubSpot activity: ' + ('backfill since ' + SINCE if since is None else 'changes since ' + since.isoformat()), flush=True)
        new = pull_engagements(since)
        emails = ordering_emails()
        con = pull_contacts(emails)
    except Exception as e:                     # fail-open: yesterday's files stay
        print(f'HubSpot activity: FAILED, keeping the existing files: {e}'); return 0
    eng = new
    if since is not None and os.path.exists(ENG):
        old = pd.read_parquet(ENG)
        eng = pd.concat([old[~old.id.isin(new.id)], new], ignore_index=True)
    eng = eng[pd.to_datetime(eng.ts, errors='coerce', utc=True, format='ISO8601') >= pd.Timestamp(SINCE, tz='UTC')]
    eng.to_parquet(ENG, index=False)
    con.to_parquet(CON, index=False)
    json.dump(owners, open(OWN, 'w'), indent=0)
    extra = {}
    for name, fn in (('notes', lambda: pull_extra('notes', since, NOTES)), ('deals', lambda: pull_extra('deals', since, DEALS)),
                     ('tasks', lambda: pull_extra('tasks', since, TASKS)), ('companies', pull_companies),
                     ('contact_accounts', pull_contact_accounts)):
        try:                                   # each on its own: a missing scope or error keeps that file as it was
            extra[name] = fn()
        except Exception as e:
            extra[name] = f'failed: {str(e)[:200]}'; print(f'  {name}: FAILED, keeping the existing file: {e}', flush=True)
    json.dump(dict(last_pull=started.isoformat(), outcome_version=OUTCOME_VERSION, engagements=int(len(eng)), contacts=int(len(con)),
                   ordering_emails=len(emails),
                   noise_emails_dropped=NOISE_DROPPED[0], **extra),
              open(STATE, 'w'), indent=1)
    print(f'HubSpot activity: {len(eng)} engagements, {len(con)} of {len(emails)} ordering emails found in HubSpot')
    return 0


if __name__ == '__main__':
    sys.exit(main())
