"""Build and refresh data/sales/shipments.parquet: one row per UPS package (plus webstore
ShipStation labels), with its direction and the sales order it belongs to.

Added 2026-10-20 (Josh's call). Read-only against every source: nothing is written to
UPS, Impress, or the purchasing workbooks.

Direction (see ImagePointe-OS knowledge/08_operations/ups-package-direction-and-order-matching-2026-10.md):
  inbound_to_us       ship-to is Image Pointe or Dignity Apparel, shipper isn't us
  outbound_from_us    our label (1Z300EW1 / 1ZW682C8 / 1ZX31J95) or shipper IP/DA in Waterloo
  vendor_to_customer  neither side is us (vendor drop-ship, usually billed third party to 537162)
  internal            both sides are us
The UPS account alone does NOT decide direction: 537162 carries inbound packages and
E44W64 carries drop-ships, and Unionwear ships to us under the IMAGE POINTE name from NJ.

Order-match rubric (2026-10-21; ImagePointe-OS knowledge/08_operations/ups-shipment-order-matching-rubric.md).
First rule that fires wins. match_tier says how far down the rubric the package got.
  Tier 1, exact key (confidence high):
    impress_sorders  tracking number is on the SO's shipment in Impress (so-shipments.parquet)
    shipstation      webstore label (W682C8/X31J95) found in the ShipStation export -> webstore order
    purch_sheet      purchasing typed the SO#/PO# on its Third Party tracking sheet
    so_ref           an S0xxxxxx in a UPS reference field
    po_ref           an explicit PO (P0071591, PO0071591) in a reference field -> purchase-orders.for_order
    po_bare          any other spelling of a real PO number ("43445", "PO#: 058816", "PO.071787",
                     "0072114", "IP-37269"), accepted only when the shipper is that PO's vendor AND the
                     PO was placed 7 days after to 240 days before the ship date (100% right, held out)
    A PO with no sales order (stock, supplies) matches the PO alone: sales_order empty, po set.
  Tier 2, two independent signals agree (confidence medium). Held-out accuracy, 2026:
    vendor_zip       drop-ship: shipper is the vendor on one of our POs placed 0-150 days before
                     (and not received 7+ days before the box shipped), and the package ZIP is that
                     PO's sales-order ship-to. Only one order may fit. 98.2%.
    ship_zip_date    our label: the package ZIP is the ship-to of exactly one order that Impress
                     shows shipping the same day. 92.2% (98.6% right customer).
    address          ship-to name/street matches an order received 0-150 days before (address_match).
                     Our labels 93.3%; drop-ships 97.5% (must also pass vendor_product_check).
  Tier 3, no order expected (no_order_reason): internal, return_to_vendor, zero_charge, supplies
  Tier 4, queue: nobody could match it; `candidates` lists the likely orders/POs for a person to pick.

Sources:
  billing    UPS weekly billing CSVs (ImagePointe-OS reference files copy of K:\\UPS Statements):
             every package billed to our accounts, with cost. Re-scanned in full each run (~1 min).
  purch_3p   purchasing's daily Third Party Quantum View workbooks, 2020-2026
             (OneDrive "Purchasing - General/Tracking Spreadsheets"). Backfill only (--backfill).
  qv_api     Quantum View API (subscriptions IPAPI3P / IPAPIIB / IPAPIOB on user joshruyleIP),
             pulled nightly by date range (never "unread", so purchasing's own downloads are untouched).
  tracking   UPS Tracking API, status refresh for recent undelivered packages (capped per run).
  shipstation  ShipStation export (webstore labels).

Credentials: Windows Credential Manager generic credential UPS_API (user = client ID,
password = secret). Each machine that runs this needs its own copy (cmdkey).

Usage:
  python scripts/ups_shipments.py --backfill      # full rebuild incl. purchasing workbooks
  python scripts/ups_shipments.py                 # nightly: QV pull + tracking refresh + billing rescan + rematch
  python scripts/ups_shipments.py --report        # print match-rate report only
"""
import base64, csv, ctypes, glob, json, os, re, sys, time, uuid, urllib.error, urllib.request
import ctypes.wintypes as wt
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = Path(os.environ.get("SALES_DATA_DIR") or REPO_ROOT.parent / "data")  # the data folder (not a repo since 2026-11-18): SALES_DATA_DIR, or 'data' next to the repos
DATA_DIR = DATA_ROOT / "sales"
LOG_DIR = REPO_ROOT / "scripts" / "logs"
OUT_PATH = DATA_DIR / "shipments.parquet"
QUEUE_PATH = DATA_DIR / "shipments-queue.csv"
PURCH_CACHE = DATA_DIR / "shipments-purch-3p-history.parquet"
ONEDRIVE = REPO_ROOT.parents[1]
BILLING_DIR = ONEDRIVE / "ImagePointe-OS" / "knowledge" / "reference files" / "shipping" / "UPS Shipping Reports"
SERVER_BILLING_DIRS = [r"K:\UPS Statements", r"\\10.0.0.14\Sys2\UPS Statements"]
PURCH_DIR = ONEDRIVE / "Purchasing - General" / "Tracking Spreadsheets"
SHIPSTATION_DIR = ONEDRIVE / "ImagePointe-OS" / "knowledge" / "reference files" / "shipping" / "ShipStation"

UPS_BASE = "https://onlinetools.ups.com"
QV_DAYS = 3                  # nightly Quantum View window (API max is 7)
TRACKING_REFRESH_CAP = 400   # Tracking API calls per run
TRACKING_REFRESH_DAYS = 21   # only refresh packages shipped in the last N days

COLUMNS = ["tracking", "direction", "sales_order", "po", "match_via", "match_confidence", "match_tier",
           "no_order_reason", "candidates", "vendor_check",
           "webstore_order", "webstore_store", "ship_date", "status", "delivered_date",
           "scheduled_delivery", "shipper", "ship_to", "ship_to_city", "ship_to_state", "ship_to_zip",
           "ship_to_company", "ship_to_attn", "ship_to_address1", "ship_to_address2", "ship_to_country", "service",
           "billed_account", "ref1", "ref2", "net_charge", "sources", "updated_at"]

# ---------------------------------------------------------------- helpers

def log(msg, fh=None):
    line = f"{datetime.now():%Y-%m-%d %H:%M:%S} {msg}"
    print(line, flush=True)
    if fh:
        fh.write(line + "\n"); fh.flush()


SO_RE = re.compile(r"(?<![A-Z0-9])S0?(\d{6,7})(?!\d)", re.I)
PO_RE = re.compile(r"(?<![A-Z0-9])P[O0]?(\d{5,7})(?!\d)", re.I)
BARE_RE = re.compile(r"(?<![\dA-Z])(?:IP-?|PO\s*[#:]?\s*)?(\d{5})(?!\d)", re.I)
IP_RE = re.compile(r"IMAGE\s*POINTE|1224\s*LA\s*PORTE|LAPORTE", re.I)
DA_RE = re.compile(r"DIGNITY\s*APPAREL|SHEFFIELD|AIRLINE\s*CIR", re.I)
LOCAL_RE = re.compile(r"WATERLOO|CEDAR FALLS", re.I)
OUR_LABELS = ("1Z300EW1", "1ZW682C8", "1ZX31J95")


def s(x):
    return x.strip() if isinstance(x, str) else ""


def direction(shipper, ship_to, trk):
    to_us = bool(IP_RE.search(ship_to) or DA_RE.search(ship_to))
    from_us = trk.startswith(OUR_LABELS) or bool((IP_RE.search(shipper) or DA_RE.search(shipper)) and LOCAL_RE.search(shipper))
    if to_us and from_us:
        return "internal"
    if to_us:
        return "inbound_to_us"
    if from_us:
        return "outbound_from_us"
    return "vendor_to_customer" if (shipper or ship_to) else None


def aslist(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


# ---------------------------------------------------------------- credentials / API

class _CRED(ctypes.Structure):
    _fields_ = [("Flags", wt.DWORD), ("Type", wt.DWORD), ("TargetName", wt.LPWSTR), ("Comment", wt.LPWSTR),
                ("LastWritten", wt.FILETIME), ("CredentialBlobSize", wt.DWORD), ("CredentialBlob", ctypes.POINTER(ctypes.c_byte)),
                ("Persist", wt.DWORD), ("AttributeCount", wt.DWORD), ("Attributes", ctypes.c_void_p),
                ("TargetAlias", wt.LPWSTR), ("UserName", wt.LPWSTR)]


def ups_credentials():
    p = ctypes.POINTER(_CRED)()
    if not ctypes.windll.advapi32.CredReadW("UPS_API", 1, 0, ctypes.byref(p)):
        raise RuntimeError("Windows credential UPS_API not found on this machine (add it with cmdkey)")
    c = p.contents
    secret = ctypes.string_at(c.CredentialBlob, c.CredentialBlobSize).decode("utf-16-le")
    user = c.UserName
    ctypes.windll.advapi32.CredFree(p)
    return user, secret


def _http(req):
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(body)
        except ValueError:
            return e.code, {"raw": body[:500]}


def ups_token():
    cid, secret = ups_credentials()
    auth = base64.b64encode(f"{cid}:{secret}".encode()).decode()
    req = urllib.request.Request(UPS_BASE + "/security/v1/oauth/token", data=b"grant_type=client_credentials",
                                 method="POST", headers={"Authorization": "Basic " + auth,
                                                         "Content-Type": "application/x-www-form-urlencoded",
                                                         "x-merchant-id": "537162"})
    st, res = _http(req)
    if st != 200:
        raise RuntimeError(f"UPS token request failed: HTTP {st} {res}")
    return res["access_token"]


def _hdr(token, json_body=False):
    h = {"Authorization": "Bearer " + token, "transId": uuid.uuid4().hex[:32], "transactionSrc": "IP_UPS_API"}
    if json_body:
        h["Content-Type"] = "application/json"
    return h


def _ref_values(*objs):
    vals = []
    for o in objs:
        for r in aslist(o):
            if isinstance(r, dict) and s(r.get("Value")):
                vals.append(s(r["Value"]))
    return vals


def _addr(party):
    if not isinstance(party, dict):
        return "", ""
    a = party.get("Address") or {}
    name = " ".join(x for x in [s(party.get("CompanyName")), s(party.get("Name")), s(a.get("ConsigneeName")),
                                s(party.get("AttentionName"))] if x)
    lines = " ".join(s(x) for x in aslist(a.get("AddressLine")) + [a.get("AddressLine1"), a.get("AddressLine2")] if s(x))
    full = " ".join(x for x in [name, lines, s(a.get("City")), s(a.get("StateProvinceCode")), s(a.get("PostalCode")),
                                s(a.get("CountryCode"))] if x)
    return full, s(a.get("StateProvinceCode")), s(a.get("City")), s(a.get("PostalCode"))[:5]


def _addr_parts(party):
    """The ship-to as separate fields, for the Shipped Orders workbook's address columns."""
    if not isinstance(party, dict):
        return {}
    a = party.get("Address") or {}
    lines = [s(x) for x in aslist(a.get("AddressLine")) + [a.get("AddressLine1"), a.get("AddressLine2")] if s(x)]
    return dict(ship_to_company=s(a.get("ConsigneeName")) or s(party.get("CompanyName")) or s(party.get("Name")),
                ship_to_attn=s(party.get("AttentionName")),
                ship_to_address1=lines[0] if lines else "", ship_to_address2=" ".join(lines[1:]),
                ship_to_country=s(a.get("CountryCode")))


def _ymd(d):
    d = s(d)
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}" if re.fullmatch(r"\d{8}", d) else (d or None)


def quantum_view_pull(token, days, fh, raw_dir):
    """Pull every subscription's events for the last `days` days by date range and return
    package-level rows. Date-range requests don't mark files read."""
    end = datetime.now(); begin = end - timedelta(days=days)
    body = {"QuantumViewRequest": {"Request": {"RequestAction": "QVEvents"},
            "SubscriptionRequest": {"DateTimeRange": {"BeginDateTime": begin.strftime("%Y%m%d%H%M%S"),
                                                      "EndDateTime": end.strftime("%Y%m%d%H%M%S")}}}}
    rows, pages, bookmark = [], 0, None
    raw_dir.mkdir(parents=True, exist_ok=True)
    while True:
        if bookmark:
            body["QuantumViewRequest"]["Bookmark"] = bookmark
        req = urllib.request.Request(f"{UPS_BASE}/api/quantumview/v3/events", data=json.dumps(body).encode(),
                                     method="POST", headers=_hdr(token, True))
        st, res = _http(req)
        pages += 1
        (raw_dir / f"qv_{datetime.now():%Y%m%d_%H%M%S}_{pages}.json").write_text(json.dumps(res), encoding="utf-8")
        if st != 200:
            errs = json.dumps(res)
            if "330023" in errs:          # no files for the window: normal on a quiet day
                log("quantum view: no files for the window", fh)
                break
            raise RuntimeError(f"Quantum View pull failed: HTTP {st} {errs[:400]}")
        ev = res.get("QuantumViewResponse", {}).get("QuantumViewEvents", {})
        for sub in aslist(ev.get("SubscriptionEvents")):
            sub_name = s(sub.get("Name"))
            for f in aslist(sub.get("SubscriptionFile")):
                for m in aslist(f.get("Manifest")):
                    shipper = _addr(m.get("Shipper"))[0]
                    ship_to, st_state, st_city, st_zip = _addr(m.get("ShipTo"))
                    shipment_refs = _ref_values(m.get("ReferenceNumber"))
                    for pkg in aslist(m.get("Package")):
                        refs = _ref_values(pkg.get("ReferenceNumber"), pkg.get("PackageReferenceNumber")) + shipment_refs
                        rows.append(dict(tracking=s(pkg.get("TrackingNumber")).upper(), src="qv_api", qv_subscription=sub_name,
                                         shipper=shipper, ship_to=ship_to, ship_to_state=st_state,
                                         ship_to_city=st_city, ship_to_zip=st_zip,
                                         service=s((m.get("Service") or {}).get("Description")) or s((m.get("Service") or {}).get("Code")),
                                         ship_date=_ymd(m.get("PickupDate")), scheduled_delivery=_ymd(m.get("ScheduledDeliveryDate")),
                                         billed_account=s((m.get("BillToAccount") or {}).get("Number")),
                                         ref1=refs[0] if refs else "", ref2=" | ".join(refs[1:]), status="Manifested",
                                         **_addr_parts(m.get("ShipTo"))))
                for kind, status in (("Origin", "In Transit"), ("Exception", None), ("Delivery", "Delivered"), ("Generic", None)):
                    for e in aslist(f.get(kind)):
                        refs = _ref_values(e.get("PackageReferenceNumber"), e.get("ShipmentReferenceNumber"))
                        st_desc = status or s(e.get("StatusDescription")) or s(e.get("ActivityType")) or kind
                        when = _ymd(e.get("Date") or (e.get("Activity") or {}).get("Date"))
                        rows.append(dict(tracking=s(e.get("TrackingNumber")).upper(), src="qv_api", qv_subscription=sub_name,
                                         status=st_desc, status_date=when,
                                         delivered_date=when if kind == "Delivery" else None,
                                         ship_date=when if kind == "Origin" else None,
                                         billed_account=s((e.get("BillToAccount") or {}).get("Number")),
                                         ref1=refs[0] if refs else "", ref2=" | ".join(refs[1:])))
        bookmark = ev.get("Bookmark") or res.get("QuantumViewResponse", {}).get("Bookmark")
        if not bookmark or pages >= 50:
            break
    rows = [r for r in rows if r["tracking"].startswith("1Z")]
    log(f"quantum view: {pages} page(s), {len(rows)} package events", fh)
    return pd.DataFrame(rows)


def tracking_refresh(token, trackings, fh):
    rows = []
    for t in trackings:
        req = urllib.request.Request(f"{UPS_BASE}/api/track/v1/details/{t}", headers=_hdr(token))
        st, res = _http(req)
        if st != 200:
            continue
        try:
            pkg = res["trackResponse"]["shipment"][0]["package"][0]
        except (KeyError, IndexError, TypeError):
            continue
        desc = s((pkg.get("currentStatus") or {}).get("description"))
        delivered = None
        for d in aslist(pkg.get("deliveryDate")):
            if s(d.get("type")) == "DEL":
                delivered = _ymd(d.get("date"))
        sched = None
        for d in aslist(pkg.get("deliveryDate")):
            if s(d.get("type")) in ("SDD", "RDD"):
                sched = _ymd(d.get("date"))
        rows.append(dict(tracking=t, src="tracking_api", status=desc, delivered_date=delivered, scheduled_delivery=sched))
        time.sleep(0.1)
    log(f"tracking api: refreshed {len(rows)} of {len(trackings)} packages", fh)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- file sources

def billing_packages(fh):
    def acct_of(path):
        for part in reversed(Path(path).parts[:-1]):
            m = re.match(r"^0?([0-9A-Z]{6})\b", part.upper())
            if m:
                return m.group(1)
        return ""
    pk = {}
    files = glob.glob(str(BILLING_DIR / "**" / "*.csv"), recursive=True)
    # The server copy gets new weekly statements before the repo copy does. Read this
    # year's folder straight from it when the share is reachable (office network / VPN).
    seen = {os.path.basename(f).lower() for f in files}
    for srv in SERVER_BILLING_DIRS:
        try:
            if os.path.isdir(srv):
                yr = os.path.join(srv, str(datetime.now().year))
                extra = [f for f in glob.glob(os.path.join(yr, "**", "*.csv"), recursive=True)
                         if os.path.basename(f).lower() not in seen]
                files += extra
                log(f"billing: {len(extra)} newer CSVs read from {srv}", fh)
                break
        except OSError:
            continue
    for f in files:
        acct = acct_of(f)
        with open(f, newline="", encoding="utf-8-sig", errors="replace") as fh_csv:
            for r in csv.reader(fh_csv):
                if len(r) < 82 or not r[13].upper().startswith("1Z"):
                    continue
                t = r[13].upper()
                d = pk.get(t)
                if d is None:
                    # col 11 = pickup date; col 4 is the invoice date (always the week-ending Saturday)
                    d = pk[t] = dict(tracking=t, src="billing", billed_account=acct, ship_date=s(r[11]) or r[4], ref1=s(r[15]), ref2=s(r[16]),
                                     shipper=" ".join(s(x) for x in r[66:73] if s(x)), ship_to=" ".join(s(x) for x in r[74:82] if s(x)),
                                     ship_to_city=s(r[78]), ship_to_state=s(r[79]), ship_to_zip=s(r[80])[:5],
                                     ship_to_attn=s(r[74]), ship_to_company=s(r[75]), ship_to_address1=s(r[76]),
                                     ship_to_address2=s(r[77]), ship_to_country=s(r[81]),
                                     net_charge=0.0)
                try:
                    d["net_charge"] += float(r[52] or 0)
                except ValueError:
                    pass
    df = pd.DataFrame(pk.values())
    if len(df):
        df["ship_date"] = pd.to_datetime(df.ship_date, errors="coerce", format="mixed").dt.strftime("%Y-%m-%d")
    log(f"billing: {len(files)} CSVs, {len(df)} packages", fh)
    return df


def purchasing_workbooks(fh):
    import openpyxl
    rows = []
    files = glob.glob(str(PURCH_DIR / "UPS Tracking 20*" / "**" / "*.xls*"), recursive=True)
    for i, f in enumerate(files):
        try:
            wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
        except Exception as e:
            log(f"  purchasing workbook skipped {os.path.basename(f)}: {e}", fh)
            continue
        for ws in wb.worksheets:
            hdr = None
            for r in ws.iter_rows(values_only=True):
                cells = ["" if c is None else str(c).strip() for c in r]
                if hdr is None:
                    if "Tracking Number" in cells:
                        hdr = {c: j for j, c in enumerate(cells) if c}
                    continue
                g = lambda k: cells[hdr[k]] if k in hdr and hdr[k] < len(cells) else ""
                t = g("Tracking Number").upper()
                if not t.startswith("1Z"):
                    continue
                refs = g("Reference Number(s)")
                parts = [p.strip() for p in g("Ship To").split(",")]   # "NAME , CITY , ST , US"
                rows.append(dict(tracking=t, src="purch_3p", ref1=refs, sheet_so=g("Sales Order#"), sheet_po=g("PO#"),
                                 ship_date=g("Manifest Date")[:10], shipper=g("Shipper"), ship_to=g("Ship To"),
                                 ship_to_city=parts[-3] if len(parts) >= 4 else None,
                                 ship_to_state=parts[-2] if len(parts) >= 4 else None,
                                 service=g("Service"), scheduled_delivery=g("Scheduled Delivery")[:10] or None,
                                 status=g("Status") or None))
        wb.close()
        if i % 100 == 0:
            log(f"  purchasing workbooks {i}/{len(files)}", fh)
    df = pd.DataFrame(rows)
    if len(df):
        df["ship_date"] = pd.to_datetime(df.ship_date, errors="coerce", format="mixed").dt.strftime("%Y-%m-%d")
        # the same package shows on several daily sheets; keep the one with the most filled-in fields
        df["_filled"] = df.notna().sum(axis=1) + (df.sheet_so.str.len() > 0)
        df = df.sort_values("_filled").drop_duplicates("tracking", keep="last").drop(columns="_filled")
    log(f"purchasing workbooks: {len(files)} files, {len(df)} packages", fh)
    return df


def impress_shipments():
    p = DATA_DIR / "so-shipments.parquet"
    if not p.exists():
        return pd.DataFrame(columns=["tracking", "sales_order"])
    d = pd.read_parquet(p)
    d["tracking"] = d.tracking.fillna("").str.upper().str.replace(r"\s", "", regex=True)
    d = d[d.tracking.str.startswith("1Z")]
    return d.drop_duplicates("tracking")[["tracking", "sales_order", "ship_date"]].rename(columns={"ship_date": "impress_ship_date"})


def impress_packages(fh):
    """Our own labels as packages in their own right: the billing CSVs for 300EW1 only
    start in 2022, but Impress has the WorldShip tracking number on every box since 2018."""
    p = DATA_DIR / "so-shipments.parquet"
    if not p.exists():
        return pd.DataFrame()
    d = pd.read_parquet(p)
    d["tracking"] = d.tracking.fillna("").str.upper().str.replace(r"\s", "", regex=True)
    d = d[d.tracking.str.startswith("1Z") & (d.tracking.str.len() == 18)].drop_duplicates("tracking")
    out = pd.DataFrame(dict(tracking=d.tracking, src="impress",
                            ship_date=pd.to_datetime(d.ship_date, errors="coerce").dt.strftime("%Y-%m-%d"),
                            service=d.ship_via_desc))
    log(f"impress: {len(out)} packages with a UPS tracking number", fh)
    return out


def shipstation(fh):
    rows = []
    # Nightly ShipStation API labels (data/webstores, tracking # kept since 2026-10-28) come first; the one-time
    # export below (2026-01-01 to 10-05) only fills anything the API files don't have.
    for f in sorted((DATA_ROOT / "webstores").glob("shipstation-labels-*.parquet")):
        d = pd.read_parquet(f)
        if "tracking" not in d:
            continue
        d = d[d.tracking.fillna("").str.upper().str.startswith("1Z")]
        rows.append(pd.DataFrame(dict(tracking=d.tracking.str.upper(), src="shipstation", webstore_order=d.order,
                                      webstore_store=d.ss_store, ship_date=d.ship_date, service=d.service,
                                      ship_to_state=d.ship_state)))
    for f in glob.glob(str(SHIPSTATION_DIR / "*.csv")):
        d = pd.read_csv(f, dtype=str)
        d = d[d["Tracking #"].fillna("").str.upper().str.startswith("1Z")]
        rows.append(pd.DataFrame(dict(tracking=d["Tracking #"].str.upper(), src="shipstation",
                                      webstore_order=d["Order #"], webstore_store=d["Store"],
                                      ship_date=pd.to_datetime(d["Ship Date"], errors="coerce", format="mixed").dt.strftime("%Y-%m-%d"),
                                      service=d["Service"], ship_to_state=d["State"])))
    df = pd.concat(rows, ignore_index=True).drop_duplicates("tracking") if rows else pd.DataFrame()
    log(f"shipstation: {len(df)} UPS labels", fh)
    return df


# ---------------------------------------------------------------- combine + match

# Which source wins per field, first listed = preferred.
FIELD_PRIORITY = {
    "status": ["tracking_api", "qv_api", "purch_3p"],
    "delivered_date": ["tracking_api", "qv_api"],
    "scheduled_delivery": ["tracking_api", "qv_api", "purch_3p"],
    "net_charge": ["billing"],
    "billed_account": ["billing", "qv_api"],
    "_default": ["billing", "qv_api", "purch_3p", "shipstation", "impress", "existing", "tracking_api"],
}


def combine(frames):
    df = pd.concat([f for f in frames if f is not None and len(f)], ignore_index=True)
    df = df[df.tracking.fillna("").str.startswith("1Z")]
    out = pd.DataFrame({"tracking": sorted(df.tracking.unique())}).set_index("tracking")
    cols = set(df.columns) - {"tracking", "src"}
    for col in cols:
        order = FIELD_PRIORITY.get(col, FIELD_PRIORITY["_default"])
        rank = {src: i for i, src in enumerate(order)}
        sub = df[["tracking", "src", col]]
        sub = sub[sub[col].notna() & (sub[col].astype(str).str.strip() != "")]
        if col in ("status", "delivered_date") and "status_date" in df.columns:
            sub = sub.assign(_r=sub.src.map(rank).fillna(99))
        else:
            sub = sub.assign(_r=sub.src.map(rank).fillna(99))
        sub = sub.sort_values(["_r"]).drop_duplicates("tracking")
        out[col] = sub.set_index("tracking")[col]
    # carried-over rows bring their earlier sources, so a --no-api run doesn't strip the
    # qv_api tag and let the next run drop packages only Quantum View knew about
    prior = df[df.src == "existing"][["tracking", "prior_sources"]] if "prior_sources" in df else df.iloc[0:0][["tracking"]]
    tags = pd.concat([df[df.src != "existing"][["tracking", "src"]],
                      prior.assign(src=prior.get("prior_sources", pd.Series(dtype=str)).fillna("").str.split("|"))
                      .explode("src")[["tracking", "src"]]])
    srcs = tags[tags.src.fillna("") != ""].groupby("tracking").src.agg(lambda x: "|".join(sorted(set(x))))
    out["sources"] = srcs
    out = out.drop(columns=["prior_sources"], errors="ignore")
    return out.reset_index()


def match_orders(df):
    po = pd.read_parquet(DATA_DIR / "purchase-orders.parquet", columns=["po", "for_order", "ordered_date"])
    po = po[po.po.str.match(r"^P\d{7}$", na=False)]
    po_so = dict(zip(po.po, po.for_order))
    po_date = dict(zip(po.po, pd.to_datetime(po.ordered_date, errors="coerce")))
    orders = set(pd.read_parquet(DATA_DIR / "order-tracking.parquet", columns=["sales_order"]).sales_order) | \
             set(pd.read_parquet(DATA_DIR / "orders.parquet", columns=["sales_order"]).sales_order)
    imp = dict(zip(*impress_shipments()[["tracking", "sales_order"]].values.T)) if (DATA_DIR / "so-shipments.parquet").exists() else {}

    def one(r):
        so = imp.get(r["tracking"])
        if isinstance(so, str) and so:
            return so, None, "impress_sorders", "high"
        sheet = " ".join(s(r.get(c)) for c in ("sheet_so", "sheet_po"))
        for cand in SO_RE.findall(sheet):
            c = f"S{int(cand):07d}"
            if c in orders:
                return c, None, "purch_sheet", "high"
        for cand in PO_RE.findall(sheet):
            p = f"P{int(cand):07d}"
            if isinstance(po_so.get(p), str):
                return po_so[p], p, "purch_sheet", "high"
        txt = " ".join(s(r.get(c)) for c in ("ref1", "ref2"))
        for cand in SO_RE.findall(txt):
            c = f"S{int(cand):07d}"
            if c in orders:
                return c, None, "so_ref", "high"
        for cand in PO_RE.findall(txt):
            p = f"P{int(cand):07d}"
            if isinstance(po_so.get(p), str):
                return po_so[p], p, "po_ref", "high"
        # bare PO numbers are read by any_po_number, which also checks the vendor
        return None, None, None, None

    res = [one(r) for r in df.to_dict("records")]
    df["sales_order"], df["po"], df["match_via"], df["match_confidence"] = zip(*res) if res else ([], [], [], [])
    if "webstore_order" in df:
        df.loc[df.webstore_order.notna() & df.match_via.isna(), ["match_via", "match_confidence"]] = ["shipstation", "high"]
    df = any_po_number(df)
    df = vendor_zip_match(df)
    df = ship_zip_date_match(df)
    df = address_match(df, po)
    df = vendor_product_check(df)
    return no_order_and_queue(df)


# ---------------------------------------------------------------- rubric helpers (tiers 1b-4)

ANY_PO_RE = re.compile(r"(?<![A-Z0-9])(?:P[O0]?\s*[#:.]?\s*|IP-?\s*|ORDER\s*#?\s*)?0*(\d{5,6})(?!\d)", re.I)
# Words that say nothing about which vendor a shipper is
VENDOR_NOISE = {"SHIPPING", "DEPARTMENT", "DEPT", "ATTN", "THE", "INC", "LLC", "CORP", "COMPANY", "USA", "GROUP",
                "PRODUCTS", "PRODUCT", "APPAREL", "SPORTSWEAR", "CUSTOM", "PROMOTIONS", "PROMOTIONAL", "INDUSTRIES",
                "INTERNATIONAL", "MANUFACTURING", "ENTERPRISES", "DIST", "DISTRIBUTION", "WAREHOUSE", "SALES", "AVE",
                "ROAD", "STREET", "SUITE", "BLVD", "DRIVE", "NORTH", "SOUTH", "EAST", "WEST", "AMERICA", "AMERICAN",
                "LTD", "DBA", "AND", "RECEIVING", "SHIP", "GRAPHICS", "PRINTING", "EMBROIDERY", "SCREEN", "PRINT",
                "DESIGN", "DESIGNS", "IMPRINT", "BRANDS", "MADE", "SPECIALTY", "SPECIALTIES", "WEAR", "ACTIVE",
                "TRADING", "SUPPLY", "SOLUTIONS", "SYSTEMS", "HOLDINGS", "MFG", "UNDELIVERABLE", "RETURN"}


def _vtoks(x):
    return [w for w in re.findall(r"[A-Z]{3,}", str(x).upper()) if w not in VENDOR_NOISE]


def _skey(x):
    return " ".join(_vtoks(x)[:2])


class _Vendors:
    """Which of our PO vendors a UPS shipper string is. Two ways: the shipper's first
    meaningful word equals the vendor's (AURA BADGE CO -> AURA BADGE CO W), or this
    shipper has shipped against that vendor's POs before (learned from tier-1 matches,
    so FANCY TERRY -> Agua without a hand list)."""
    def __init__(self, po, df):
        self.po = po
        first = {}
        for v, n in zip(po.vendor_code, po.vendor):
            t = _vtoks(n)
            if t:
                first.setdefault(t[0], set()).add(v)
        self.first = first
        known = df[df.match_via.isin(["po_ref", "purch_sheet", "po_bare"]) & df.po.notna()]
        vc = dict(zip(po.po, po.vendor_code))
        learned = {}
        for k, p in zip(known.shipper.map(_skey), known.po):
            if k and p in vc:
                learned.setdefault(k, {}).setdefault(vc[p], 0)
                learned[k][vc[p]] += 1
        self.learned = {k: {v for v, n in c.items() if n >= 2} for k, c in learned.items()}
        self.by_vendor = {v: g for v, g in po[po.od.notna()].groupby("vendor_code")}

    def of(self, shipper):
        # the vendor's name can sit behind a contact or prefix ("HELEN KARAPETYAN MVP SPORTSWEAR",
        # "Email info@shopwudn. WUDN HANDCRAFTED"), so any 4+ letter word may name it
        t = _vtoks(shipper)
        out = set(self.learned.get(_skey(shipper), set())) | (self.first.get(t[0], set()) if t else set())
        for w in t[1:]:
            if len(w) >= 4:
                out |= self.first.get(w, set())
        return out

    def pos(self, shipper, sd, back=150, ahead=0):
        gs = [self.by_vendor[v] for v in self.of(shipper) if v in self.by_vendor]
        if not gs:
            return self.po.iloc[0:0]
        g = pd.concat(gs)
        return g[(g.od <= sd + pd.Timedelta(days=ahead)) & (g.od >= sd - pd.Timedelta(days=back))]


def _po_table():
    po = pd.read_parquet(DATA_DIR / "purchase-orders.parquet",
                         columns=["po", "vendor_code", "vendor", "for_order", "ordered_date", "received_date"])
    po = po[po.po.str.match(r"^P\d{7}$", na=False)].copy()
    po["od"] = pd.to_datetime(po.ordered_date, errors="coerce")
    po["rd"] = pd.to_datetime(po.received_date, errors="coerce")
    return po


def _unmatched(df):
    return df.match_via.isna() & (df.webstore_order.isna() if "webstore_order" in df else True)


def _zip(r):
    z = s(r.get("ship_to_zip"))[:5]
    if re.fullmatch(r"\d{5}", z):
        return z
    m = re.search(r"\b(\d{5})(?:-?\d{4})?\s+US\s*$", s(r.get("ship_to")))
    return m.group(1) if m else ""


def any_po_number(df):
    """Tier 1b: a real PO number written any way, plus stock POs (no sales order) that the
    strict po_ref rule skipped. The number must exist and the shipper must be that PO's
    vendor (100% right in a 2026 held-out test: 5,266 of 5,268). A number whose PO is from a
    different vendor was right only 57% of the time even when the dates fit, so it becomes
    the package's first queue candidate instead of a match."""
    po = _po_table()
    info = {p: (fo, od, v) for p, fo, od, v in zip(po.po, po.for_order, po.od, po.vendor_code)}
    vend = _Vendors(po, df)
    if "_hint" not in df:
        df["_hint"] = None
    hits = 0
    # Not on our own labels: there a bare number is the customer's PO, and only 2% of
    # those "matches" agreed with the order's ship-to ZIP (tested 2026-10-21).
    for i, r in df[_unmatched(df) & df.direction.isin(["vendor_to_customer", "inbound_to_us"])].iterrows():
        sd = pd.to_datetime(r.get("ship_date"), errors="coerce")
        txt = " ".join(s(r.get(c)) for c in ("sheet_po", "ref1", "ref2"))
        for cand in ANY_PO_RE.findall(txt):
            p = f"P{int(cand):07d}"
            if p not in info:
                continue
            fo, od, v = info[p]
            v_ok = v in vend.of(r.get("shipper"))
            d_ok = pd.notna(sd) and pd.notna(od) and -7 <= (sd - od).days <= 240
            if not (v_ok and d_ok):   # vendor without the dates was right 10% of the time
                if d_ok and not s(df.at[i, "_hint"]):
                    df.at[i, "_hint"] = f"{p}->{fo} (PO number on label, other vendor)" if isinstance(fo, str) \
                        else f"{p} (PO number on label, other vendor)"
                continue
            df.at[i, "po"] = p
            df.at[i, "sales_order"] = fo if isinstance(fo, str) and fo else None
            df.at[i, "match_via"] = "po_bare" if isinstance(fo, str) and fo else "po_stock"
            df.at[i, "match_confidence"] = "high"
            hits += 1
            break
    print(f"any PO number: {hits} more packages", flush=True)
    return df


def _ship_to_index():
    st = pd.read_parquet(DATA_DIR / "so-ship-to.parquet", columns=["sales_order", "customer_number", "ship_zip"])
    st["zip5"] = st.ship_zip.fillna("").str[:5]
    st = st[st.zip5.str.fullmatch(r"\d{5}")]
    return st.groupby("sales_order").zip5.agg(set).to_dict(), st.groupby("zip5").sales_order.agg(set).to_dict(), \
        dict(zip(st.sales_order, st.customer_number))


def _received_near(c, sd):
    """Drop-ship POs are marked received about 5 days after the box ships (median; 90% within
    12). A PO received more than a week before the box shipped, or 2+ months after, is some
    other shipment. Open POs pass. Tested 2026-10-21: vendor_zip went from 1,476 hits at 97.2%
    to 1,921 at 98.1% on held-out 2026 drop-ships."""
    return c.rd.isna() | ((c.rd >= sd - pd.Timedelta(days=7)) & (c.rd <= sd + pd.Timedelta(days=60)))


def vendor_zip_match(df):
    """Tier 2: a drop-ship whose shipper is the vendor on one of our recent POs and whose
    ZIP is that PO's sales-order ship-to. Exactly one order may fit."""
    po = _po_table()
    vend = _Vendors(po, df)
    so_zips, _, _ = _ship_to_index()
    hits = 0
    for i, r in df[_unmatched(df) & (df.direction == "vendor_to_customer")].iterrows():
        sd, z = pd.to_datetime(r.get("ship_date"), errors="coerce"), _zip(r)
        if pd.isna(sd) or not z:
            continue
        c = vend.pos(r.get("shipper"), sd)
        c = c[c.for_order.notna() & _received_near(c, sd)]
        fit =[so for so in c.for_order.unique() if z in so_zips.get(so, ())]
        if len(fit) == 1:
            df.at[i, "sales_order"] = fit[0]
            df.at[i, "po"] = c[c.for_order == fit[0]].sort_values("od").po.iloc[-1]
            df.at[i, "match_via"], df.at[i, "match_confidence"] = "vendor_zip", "medium"
            hits += 1
    print(f"vendor + ZIP: {hits} drop-ships", flush=True)
    return df


def ship_zip_date_match(df):
    """Tier 2: one of our labels with no order reference, whose ZIP is the ship-to of
    exactly one order Impress shows shipping that same day. Same day beats +-1 day in a fair
    held-out test (the package's own Impress box excluded): 92.2% vs 86.1% right order,
    98.6% vs 97.2% right customer."""
    so_zips, zip_sos, _ = _ship_to_index()
    ss = pd.read_parquet(DATA_DIR / "so-shipments.parquet", columns=["sales_order", "ship_date"])
    ss["sd"] = pd.to_datetime(ss.ship_date, errors="coerce").dt.normalize()
    dates = ss.dropna(subset=["sd"]).groupby("sales_order").sd.agg(set).to_dict()
    hits = 0
    for i, r in df[_unmatched(df) & (df.direction == "outbound_from_us")].iterrows():
        sd, z = pd.to_datetime(r.get("ship_date"), errors="coerce"), _zip(r)
        if pd.isna(sd) or not z:
            continue
        sd = sd.normalize()
        fit = [so for so in zip_sos.get(z, ()) if sd in dates.get(so, ())]
        if len(fit) == 1:
            df.at[i, "sales_order"] = fit[0]
            df.at[i, "match_via"], df.at[i, "match_confidence"] = "ship_zip_date", "medium"
            hits += 1
    print(f"ZIP + Impress ship date: {hits} of our labels", flush=True)
    return df


def vendor_product_check(df):
    """Does the matched order buy from the vendor that shipped the box? For every drop-ship
    with an order, `vendor_check` is:
      po_to_vendor    the order has a PO to that vendor (the normal case: 99%+ of PO-reference matches)
      product_type    no PO to that vendor, but the order has a product type (sku.garment_type)
                      that vendor has supplied on 5%+ of its PO lines
      conflict        neither: the order's products aren't that vendor's kind
      vendor_unknown  the shipper isn't recognizable as any PO vendor
    An address match (the only drop-ship rule with no vendor in it) that comes back conflict or
    vendor_unknown goes to the queue instead, with that order as its first candidate."""
    po = _po_table()
    vend = _Vendors(po, df)
    pl = pd.read_parquet(DATA_DIR / "purchase-order-lines.parquet", columns=["po", "sku"])
    sku = pd.read_parquet(DATA_DIR / "sku.parquet", columns=["sku", "garment_type"]).dropna()
    gt = dict(zip(sku.sku, sku.garment_type))
    pl = pl.merge(po[["po", "vendor_code"]], on="po")
    pl["gt"] = pl.sku.map(gt)
    vend_types = pl.dropna(subset=["gt"]).groupby("vendor_code").gt.agg(
        lambda x: set(x.value_counts()[lambda c: c / c.sum() >= 0.05].index)).to_dict()
    so_vendors = po.dropna(subset=["for_order"]).groupby("for_order").vendor_code.agg(set).to_dict()
    o = pd.read_parquet(DATA_DIR / "orders.parquet", columns=["sales_order", "sku"])
    o["gt"] = o.sku.map(gt)
    so_types = o.dropna(subset=["gt"]).groupby("sales_order").gt.agg(set).to_dict()
    df["vendor_check"] = None
    if "_hint" not in df:
        df["_hint"] = None
    demoted = 0
    for i, r in df[(df.direction == "vendor_to_customer") & df.sales_order.notna()].iterrows():
        vs, so = vend.of(r.get("shipper")), r["sales_order"]
        if not vs:
            chk = "vendor_unknown"
        elif vs & so_vendors.get(so, set()):
            chk = "po_to_vendor"
        else:
            vt = set().union(*[vend_types.get(v, set()) for v in vs])
            chk = "product_type" if vt & so_types.get(so, set()) else "conflict"
        df.at[i, "vendor_check"] = chk
        if r.get("match_via") == "address" and chk in ("conflict", "vendor_unknown"):
            df.at[i, "_hint"] = f"{so} (address only, {chk.replace('_', ' ')})"
            df.at[i, "sales_order"] = df.at[i, "match_via"] = df.at[i, "match_confidence"] = None
            demoted += 1
    print(f"vendor/product check: {demoted} drop-ship address matches sent to the queue", flush=True)
    return df


def no_order_and_queue(df):
    """Tier 3 tags what never needs an order; tier 4 lists candidates for the rest so a
    person can pick one instead of searching."""
    po = _po_table()
    vend = _Vendors(po, df)
    stock_share = po.assign(stock=po.for_order.isna()).groupby("vendor_code").stock.mean()
    vendor_first = set(vend.first)
    so_zips, zip_sos, so_cust = _ship_to_index()
    rt = pd.read_parquet(DATA_DIR / "so-ship-to.parquet", columns=["sales_order", "received_date"])
    received = dict(zip(rt.sales_order, pd.to_datetime(rt.received_date, errors="coerce")))
    df["match_tier"] =df.match_via.map({"impress_sorders": 1, "shipstation": 1, "purch_sheet": 1, "so_ref": 1,
                                         "po_ref": 1, "po_bare": 1, "po_stock": 1, "vendor_zip": 2, "ship_zip_date": 2,
                                         "address": 2})
    df["no_order_reason"] = None
    df["candidates"] = None
    for i, r in df[df.match_tier.isna()].iterrows():
        d, refs = r.get("direction"), " ".join(s(r.get(c)) for c in ("ref1", "ref2")).upper()
        ship_to_first = (_vtoks(r.get("ship_to")) or [""])[0]
        reason = None
        if d == "internal":
            reason = "internal"
        elif d == "outbound_from_us" and (re.search(r"\bRA\b|RA#|RETURN|\bRMA\b", refs)
                                          or (ship_to_first in vendor_first and "RETURN" in s(r.get("ship_to")).upper())):
            reason = "return_to_vendor"
        elif d == "outbound_from_us" and r.get("net_charge") == 0 and s(r.get("status")).lower() not in ("delivered", "in transit"):
            reason = "zero_charge"
        elif d == "inbound_to_us" and any(stock_share.get(v, 0) >= 0.8 for v in vend.of(r.get("shipper"))):
            reason = "supplies"
        if reason:
            df.at[i, "match_tier"], df.at[i, "no_order_reason"] = 3, reason
            continue
        df.at[i, "match_tier"] = 4
        sd = pd.to_datetime(r.get("ship_date"), errors="coerce")
        if pd.isna(sd):
            continue
        if d in ("vendor_to_customer", "inbound_to_us"):
            c = vend.pos(r.get("shipper"), sd, back=120)
            if d == "inbound_to_us":
                c = c[c.rd.isna() | (c.rd >= sd - pd.Timedelta(days=1))]
            else:
                c = c[_received_near(c, sd)]
            c = c.sort_values("od", ascending=False).head(5)
            cands = ([r["_hint"]] if s(r.get("_hint")) else []) + \
                [f"{p}->{fo}" if isinstance(fo, str) else p for p, fo in zip(c.po, c.for_order)]
            df.at[i, "candidates"] = "; ".join(cands) or None
        elif d == "outbound_from_us":
            z = _zip(r)
            sos = sorted((so for so in zip_sos.get(z, ()) if pd.notna(received.get(so))
                          and 0 <= (sd - received[so]).days <= 150), reverse=True)[:5] if z else []
            df.at[i, "candidates"] = "; ".join(f"{so} (cust {so_cust.get(so)})" for so in sos) or None
    df["match_tier"] = df.match_tier.astype("Int64").astype(str).replace("<NA>", None)
    return df


STOP = {"ATTN", "LOCAL", "UNION", "INTERNATIONAL", "COUNCIL", "DISTRICT", "SUITE", "STREET", "AVENUE", "ROAD",
        "DRIVE", "NORTH", "SOUTH", "EAST", "WEST", "BLVD", "HIGHWAY", "FLOOR", "UNITED", "STATES", "WORKERS",
        "COMPANY", "PO BOX", "BOX", "INC", "LLC", "CORP"}


def _tokens(x):
    return {w for w in re.findall(r"[A-Z]{4,}", str(x).upper()) if w not in STOP}


def _street_no(x):
    m = re.search(r"(?<![\d-])(\d{1,6})\s+[A-Z]", str(x).upper())
    return m.group(1) if m else None


def address_match(df, po):
    """Second pass for packages with no usable reference: match the package's ship-to
    against order ship-to addresses (so-ship-to.parquet, incl. split-ship destinations).
    Same ZIP (or city + state when the source has no ZIP), order received 0-150 days
    before the ship date, and a street-number or name match. Accepted only when one
    order wins outright; a vendor drop-ship tie is broken by a PO to a vendor whose
    name matches the shipper. Tagged match_via='address', confidence 'medium'."""
    p = DATA_DIR / "so-ship-to.parquet"
    if not p.exists():
        return df
    st = pd.read_parquet(p)
    st["rd"] = pd.to_datetime(st.received_date, errors="coerce")
    st["zip5"] = st.ship_zip.fillna("").str[:5]
    st["citykey"] = st.ship_city.fillna("").str.upper().str.strip() + "|" + st.ship_state.fillna("").str.upper().str.strip()
    st["street"] = st.ship_address1.map(_street_no)
    st["tok"] = (st.ship_name.fillna("") + " " + st.ship_address1.fillna("")).map(_tokens)
    by_zip = {k: g for k, g in st[st.zip5 != ""].groupby("zip5")}
    by_city = {k: g for k, g in st.groupby("citykey")}
    po_vendor = po.assign(v=po.get("vendor")) if "vendor" in po else None
    vend = pd.read_parquet(DATA_DIR / "purchase-orders.parquet", columns=["for_order", "vendor"]).dropna()
    so_vendor_tokens = vend.groupby("for_order").vendor.agg(lambda x: set().union(*[_tokens(v) for v in x]))

    todo = _unmatched(df) & df.direction.isin(["vendor_to_customer", "outbound_from_us"])
    hits = 0
    for i, r in df[todo].iterrows():
        sd = pd.to_datetime(r.get("ship_date"), errors="coerce")
        if pd.isna(sd):
            continue
        z = s(r.get("ship_to_zip"))
        g = by_zip.get(z) if z else by_city.get(s(r.get("ship_to_city")).upper() + "|" + s(r.get("ship_to_state")).upper())
        if g is None:
            continue
        g = g[(g.rd <= sd) & (g.rd >= sd - pd.Timedelta(days=150))]
        if not len(g):
            continue
        num, tok = _street_no(r.get("ship_to")), _tokens(r.get("ship_to"))
        score = (g.street.eq(num) & pd.notna(num)).astype(int) * 2 + g.tok.map(lambda t: min(len(t & tok), 2))
        g = g.assign(score=score)
        g = g[g.score >= 2]
        if not len(g):
            continue
        best = g[g.score == g.score.max()].sales_order.unique()
        if len(best) > 1 and r.get("direction") == "vendor_to_customer":
            sw = _tokens(r.get("shipper"))
            best = [so for so in best if sw & so_vendor_tokens.get(so, set())] or best
        if len(best) == 1:
            df.at[i, "sales_order"], df.at[i, "match_via"], df.at[i, "match_confidence"] = best[0], "address", "medium"
            hits += 1
    print(f"address match: {hits} of {int(todo.sum())} unmatched packages", flush=True)
    return df


def finalize(df, fh):
    for c in ("shipper", "ship_to"):
        df[c] = df[c].fillna("") if c in df else ""
    df["direction"] = [direction(a, b, t) for a, b, t in zip(df.shipper, df.ship_to, df.tracking)]
    # Quantum View subscription type fills in when there's no address yet (status-only events)
    if "qv_subscription" in df:
        hint = df.qv_subscription.map({"IPAPIIB": "inbound_to_us", "IPAPIOB": "outbound_from_us"})
        df["direction"] = df.direction.fillna(hint)
    df.loc[df.tracking.str.startswith(OUR_LABELS) & df.direction.isna(), "direction"] = "outbound_from_us"
    # No addresses at all: an Impress SO shipment is ours; a package only on purchasing's
    # Third Party sheet is a vendor shipping on our account (94% of the ones with addresses are drop-ships).
    srcs = df.sources.fillna("")
    df.loc[df.direction.isna() & srcs.str.contains("impress"), "direction"] = "outbound_from_us"
    df.loc[df.direction.isna() & srcs.str.contains("purch_3p"), "direction"] = "vendor_to_customer"
    df = match_orders(df)
    df["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for c in COLUMNS:
        if c not in df:
            df[c] = None
    df = df[COLUMNS].copy()
    for c in COLUMNS:
        if c != "net_charge":
            df[c] = df[c].astype("string")
    df["net_charge"] = pd.to_numeric(df.net_charge, errors="coerce")
    df.to_parquet(OUT_PATH, index=False)
    log(f"shipments.parquet: {len(df)} packages written", fh)
    # Daily queue: tier-4 packages from the last 14 days, routed to whoever can resolve them
    q = df[(df.match_tier == "4") & (pd.to_datetime(df.ship_date, errors="coerce")
                                     >= pd.Timestamp.now().normalize() - pd.Timedelta(days=14))].copy()
    q["owner"] = q.direction.map({"outbound_from_us": "Shipping", "vendor_to_customer": "Purchasing",
                                  "inbound_to_us": "Purchasing"}).fillna("Purchasing")
    q.sort_values(["owner", "ship_date"])[["owner", "ship_date", "direction", "tracking", "shipper", "ship_to", "ref1", "ref2",
                                           "candidates", "status", "net_charge"]].to_csv(QUEUE_PATH, index=False)
    log(f"queue: {len(q)} packages from the last 14 days need a person -> {QUEUE_PATH.name}", fh)
    return df


# ---------------------------------------------------------------- report

def report(df=None):
    df = pd.read_parquet(OUT_PATH) if df is None else df
    df = df.copy()
    df["year"] = pd.to_datetime(df.ship_date, errors="coerce").dt.year
    lines = []
    def pct(x):
        return f"{x:.1%}"
    # a stock PO (no sales order) is as far as an inbound package can match
    df["any_match"] = df.sales_order.notna() | df.webstore_order.notna() | df.po.notna()
    lines.append(f"Packages: {len(df):,}")
    for label, sub in (("All packages", df),
                       ("Only packages UPS billed us for (the fair test: no Impress-only rows)",
                        df[df.sources.fillna("").str.contains("billing")])):
        lines.append(f"\n{label}:")
        g = sub.groupby("direction", dropna=False).agg(packages=("tracking", "size"),
                                                       to_sales_order=("sales_order", lambda x: x.notna().mean()),
                                                       to_webstore_order=("webstore_order", lambda x: x.notna().mean()),
                                                       matched_total=("any_match", "mean"),
                                                       ups_cost=("net_charge", "sum"))
        tot = pd.DataFrame(dict(packages=[len(sub)], to_sales_order=[sub.sales_order.notna().mean()],
                                to_webstore_order=[sub.webstore_order.notna().mean()],
                                matched_total=[sub.any_match.mean()], ups_cost=[sub.net_charge.sum()]), index=["ALL"])
        g = pd.concat([g, tot])
        lines.append(g.to_string(formatters={"to_sales_order": pct, "to_webstore_order": pct, "matched_total": pct,
                                             "ups_cost": "${:,.0f}".format}))
    lines.append("\nBy how it matched:")
    lines.append(df.groupby(["direction", "match_via"], dropna=False).size().unstack(fill_value=0).to_string())
    lines.append("\nMatch rate by year:")
    y = df[df.year >= 2018].pivot_table(index="year", columns="direction", values="any_match", aggfunc="mean")
    lines.append(y.to_string(float_format=lambda v: f"{v:.1%}"))
    if "match_tier" in df:
        recent = df[pd.to_datetime(df.ship_date, errors="coerce") >= pd.Timestamp.now().normalize() - pd.Timedelta(days=30)]
        lines.append("\nRubric tiers, packages shipped in the last 30 days (1 exact, 2 inferred, 3 no order expected, 4 queue):")
        lines.append(pd.crosstab(recent.direction.fillna("?"), recent.match_tier.fillna("?"), margins=True).to_string())
        lines.append(recent[recent.match_tier == "3"].no_order_reason.value_counts().to_string())
    txt = "\n".join(lines)
    print(txt)
    return txt


# ---------------------------------------------------------------- main

def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_DIR / f"ups_shipments_{datetime.now():%Y-%m-%d_%H%M%S}.log", "w", encoding="utf-8") as fh:
        if "--report" in sys.argv:
            report(); return
        frames = [billing_packages(fh)]
        if "--backfill" in sys.argv or not PURCH_CACHE.exists():
            purch = purchasing_workbooks(fh)
            purch.astype("string").to_parquet(PURCH_CACHE, index=False)
        frames.append(pd.read_parquet(PURCH_CACHE))
        frames.append(impress_packages(fh))
        try:
            frames.append(shipstation(fh))
        except Exception as e:
            log(f"shipstation: SKIPPED: {e}", fh)
        if OUT_PATH.exists():
            ex = pd.read_parquet(OUT_PATH)
            # keep API-sourced fields from earlier runs (QV keeps only a few days)
            keep = ex[ex.sources.fillna("").str.contains("qv_api|tracking_api")]
            frames.append(keep.rename(columns={"sources": "prior_sources"})
                          .drop(columns=["updated_at", "sales_order", "po", "match_via",
                                             "match_confidence", "direction", "match_tier", "no_order_reason",
                                             "candidates", "vendor_check"], errors="ignore").assign(src="existing"))
        if "--no-api" not in sys.argv:
            try:
                token = ups_token()
                frames.append(quantum_view_pull(token, QV_DAYS, fh, LOG_DIR / "ups_raw"))
                if "--no-tracking" not in sys.argv:    # the hourly shipped-orders run pulls Quantum View only
                    cut = (datetime.now() - timedelta(days=TRACKING_REFRESH_DAYS)).strftime("%Y-%m-%d")
                    base = combine(frames)
                    undelivered = base.delivered_date.isna() if "delivered_date" in base else pd.Series(True, index=base.index)
                    todo = base[(base.ship_date.fillna("") >= cut) & undelivered] \
                        .sort_values("ship_date", ascending=False).tracking.tolist()[:TRACKING_REFRESH_CAP]
                    frames.append(tracking_refresh(token, todo, fh))
            except Exception as e:
                log(f"UPS API step SKIPPED, file sources still processed: {e}", fh)
        df = finalize(combine(frames), fh)
        fh.write(report(df) + "\n")


if __name__ == "__main__":
    main()
