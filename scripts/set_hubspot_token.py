"""Save the HubSpot read-only private app token on this PC and test it (Josh, 2026-11-02).

The nightly pulls that read HubSpot (sales dashboard contact activity, the KPI page's CS panels, the rep pages) look for
%USERPROFILE%\\.hubspot\\credentials.json = {"token": "pat-..."}. That file is never in the repo.

How to use, on CHI-TOWN:
  1. In HubSpot, create the private app (read scopes only) and copy its access token. HubSpot now calls this a
     "Service Key"; the one in use is "Josh_R_API" (made 2026-11-02, read scopes only, automatic rotation off).
     If rotation is ever turned on, rerun this script within 7 days of each rotation.
  2. Run:  python scripts/set_hubspot_token.py
     It reads the token from the clipboard, clears the clipboard, saves the file, and runs one read-only test call
     per object the scripts use. The token is never printed or logged.
     With --wait it polls the clipboard for up to 20 minutes, so it can be started before the token is copied.

Every test call is a GET or a search POST (READ-ONLY, ALWAYS: CLAUDE.md "HubSpot integration").
"""
import json, os, subprocess, sys, time, urllib.request, urllib.error

CRED_DIR = os.path.join(os.path.expanduser('~'), '.hubspot')
CRED = os.path.join(CRED_DIR, 'credentials.json')
API = 'https://api.hubapi.com'
SEARCH1 = {'limit': 1, 'properties': ['hs_object_id']}
# what each nightly script reads -> one read-only call that proves the scope
TESTS = [
    ('tickets (KPI page CS panels)', 'POST', '/crm/v3/objects/tickets/search',
     {'limit': 1, 'filterGroups': [{'filters': [{'propertyName': 'hs_pipeline', 'operator': 'EQ', 'value': '0'}]}],
      'properties': ['createdate', 'responsible_dept', 'credit_memo_amount_lost']}),
    ('contacts', 'POST', '/crm/v3/objects/contacts/search', SEARCH1),
    ('companies', 'GET', '/crm/v3/objects/companies?limit=1', None),
    ('owners', 'GET', '/crm/v3/owners?limit=1', None),
    ('calls', 'POST', '/crm/v3/objects/calls/search', SEARCH1),
    ('meetings', 'POST', '/crm/v3/objects/meetings/search', SEARCH1),
    ('emails (sales email read)', 'POST', '/crm/v3/objects/emails/search', SEARCH1),
    ('notes', 'POST', '/crm/v3/objects/notes/search', SEARCH1),
    ('deals (rep pages: deals by SO number)', 'POST', '/crm/v3/objects/deals/search', SEARCH1),   # scope added 2026-11-02
    ('tasks', 'POST', '/crm/v3/objects/tasks/search', SEARCH1),
]


def clipboard(read=True):
    cmd = 'Get-Clipboard -Raw' if read else 'Set-Clipboard -Value " "'
    r = subprocess.run(['powershell', '-NoProfile', '-Command', cmd], capture_output=True, text=True, timeout=30)
    return (r.stdout or '').strip()


def looks_like_token(t):
    return t.startswith('pat-') and len(t) >= 30 and not any(c.isspace() for c in t)


def call(token, method, path, body):
    assert method == 'GET' or path.endswith('/search'), 'read-only calls only'
    req = urllib.request.Request(API + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            d = json.loads(r.read() or b'{}')
            return True, f"ok ({d.get('total', len(d.get('results', [])))} found)"
    except urllib.error.HTTPError as e:
        try:
            msg = json.loads(e.read()).get('message', '')
        except Exception:
            msg = ''
        return False, f'HTTP {e.code} {msg[:160]}'


def main():
    if '--test' in sys.argv:            # re-test the saved token without touching the clipboard
        token = json.load(open(CRED, encoding='utf-8'))['token']
    else:
        token = clipboard()
        if '--wait' in sys.argv:        # poll the clipboard (up to 20 min) until a token is copied; only the prefix is checked
            print('Waiting for a HubSpot token on the clipboard (20 minutes)...', flush=True)
            end = time.time() + 1200
            while not looks_like_token(token) and time.time() < end:
                time.sleep(2)
                token = clipboard()
        if not looks_like_token(token):
            print('The clipboard does not hold a HubSpot private app token (it should start with "pat-"). Nothing saved.')
            return 1
        clipboard(read=False)
        print('Token read from the clipboard; clipboard cleared.')
    ok_all = True
    for name, method, path, body in TESTS:
        ok, msg = call(token, method, path, body)
        ok_all &= ok
        print(f"  {'OK  ' if ok else 'FAIL'} {name}: {msg}")
    if '--test' in sys.argv:
        return 0 if ok_all else 1
    if not call(token, *TESTS[0][1:])[0]:
        print('The token could not read tickets, so it was NOT saved. Check the token and its scopes.')
        return 1
    os.makedirs(CRED_DIR, exist_ok=True)
    tmp = CRED + '.tmp'
    json.dump({'token': token}, open(tmp, 'w', encoding='utf-8'))
    os.replace(tmp, CRED)
    print(f'Saved to {CRED}.' + ('' if ok_all else ' Some scopes failed above: add them to the private app, then run this again with --test.'))
    return 0


if __name__ == '__main__':
    sys.exit(main())
