"""Read-only Microsoft Graph access for the KPI dashboard pulls (Josh, 2026-11-02).

Used by scripts/kpi/pull_kpi.py to read three SharePoint workbooks (DA TN line production, DA Iowa pipeline, the
commitment tracker) and Emily Nolting's weekly "Money $$$" email in Josh's mailbox. Every call is a GET: nothing here
writes, moves, flags or deletes anything in SharePoint or Outlook.

Same pattern as .claude/skills/po-so-reconcile/vendor_mail.py (Microsoft Graph Command Line Tools app, delegated,
device-code sign-in as Josh on CHI-TOWN), but with its OWN token cache and its own scopes, so signing in or out of one
never touches the other. msal keeps the cache DPAPI-encrypted for this Windows user, so the nightly run never prompts.

    python scripts/kpi/kpi_graph.py --login    # once; prints a code for microsoft.com/devicelogin (sign in as josh@)
    python scripts/kpi/kpi_graph.py --check    # signed in? can it reach the three sites and the mailbox?
"""
import os
import sys
import time
import urllib.parse
from pathlib import Path

import requests

CLIENT_ID = "14d82eec-204b-4c2f-b7e8-296a70dab67e"   # Microsoft Graph Command Line Tools
AUTHORITY = "https://login.microsoftonline.com/organizations"
SCOPES = ["Files.Read.All", "Sites.Read.All", "Mail.Read"]
GRAPH = "https://graph.microsoft.com/v1.0"
HOME = Path(os.environ.get("USERPROFILE", str(Path.home())))
TOKEN_CACHE = Path(os.environ.get("KPI_GRAPH_CACHE", Path(os.environ.get("LOCALAPPDATA", HOME / "AppData" / "Local"))
                                  / "ip-kpi-dashboard" / "graph_token.bin"))
HOST = "imagepointe.sharepoint.com"


class NotSignedIn(RuntimeError):
    pass


def _app():
    import msal
    from msal_extensions import FilePersistenceWithDataProtection, PersistedTokenCache
    TOKEN_CACHE.parent.mkdir(parents=True, exist_ok=True)
    cache = PersistedTokenCache(FilePersistenceWithDataProtection(str(TOKEN_CACHE)))
    return msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)


def token():
    app = _app()
    accounts = app.get_accounts()
    if not accounts:
        raise NotSignedIn(f'No KPI Graph sign-in on this PC. Run: python "{Path(__file__)}" --login')
    res = app.acquire_token_silent(SCOPES, account=accounts[0])
    if not res or "access_token" not in res:
        raise NotSignedIn(f'The KPI Graph sign-in expired or was revoked. Run: python "{Path(__file__)}" --login')
    return res["access_token"]


def login():
    app = _app()
    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise RuntimeError(f"Couldn't start the sign-in: {flow}")
    print(flow["message"], flush=True)
    res = app.acquire_token_by_device_flow(flow)
    if "access_token" not in res:
        raise RuntimeError(f"Sign-in failed: {res.get('error_description') or res}")
    print(f"Signed in as {res.get('id_token_claims', {}).get('preferred_username')}; token cached at {TOKEN_CACHE}",
          flush=True)


_S = {}


def get(url, params=None, raw=False):
    """GET with the cached token; retries throttling (429) and transient 5xx. Read-only by construction."""
    url = url if url.startswith("http") else GRAPH + url
    s = _S.setdefault("s", requests.Session())
    for attempt in range(6):
        r = s.get(url, params=params, headers={"Authorization": f"Bearer {token()}"}, timeout=120)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(60, int(r.headers.get("Retry-After", 2 ** attempt))))
            continue
        r.raise_for_status()
        return r.content if raw else r.json()
    r.raise_for_status()


def site_id(site_path):
    """'sites/DignityApparelTN' -> Graph site id."""
    return get(f"/sites/{HOST}:/{site_path.strip('/')}")["id"]


def drive_item(site_path, item_path, drive_name=None):
    """A file's driveItem by its path inside the site's document library (default library unless drive_name)."""
    sid = site_id(site_path)
    if drive_name:
        drives = get(f"/sites/{sid}/drives")["value"]
        did = next(d["id"] for d in drives if d["name"].lower() == drive_name.lower())
        base = f"/drives/{did}"
    else:
        base = f"/sites/{sid}/drive"
    return get(f"{base}/root:/{urllib.parse.quote(item_path.strip('/'))}")


def download(item):
    """File bytes for a driveItem (kept in memory only; callers never write it into the repo)."""
    return get(f"/drives/{item['parentReference']['driveId']}/items/{item['id']}/content", raw=True)


def search_mail(query, top=25, select="id,subject,from,receivedDateTime"):
    """Josh's own mailbox, newest first."""
    return get("/me/messages", params={"$search": f'"{query}"', "$top": top, "$select": select})["value"]


def message_text(msg_id):
    m = get(f"/me/messages/{msg_id}", params={"$select": "subject,receivedDateTime,body,from"})
    return m


def main():
    if "--login" in sys.argv:
        login()
        return 0
    if "--check" in sys.argv:
        token()
        print("signed in as", _app().get_accounts()[0].get("username"))
        for sp in ("sites/DignityApparelTN", "sites/DignityApparel", "sites/ImagePointeEmployees"):
            try:
                print(sp, "ok" if site_id(sp) else "?")
            except Exception as e:
                print(sp, "ERROR", e)
        print("mail:", len(search_mail("Money", top=3)), "recent 'Money' messages visible")
        return 0
    print(__doc__)
    return 0


if __name__ == "__main__":
    sys.exit(main())
