#!/usr/bin/env python3
"""DA workbooks for the inventory search page: pull (split out of tools/dashboards/inventory/build_inventory.py on
2026-11-18, phase 4 of the repo split).

Reads the newest DA Inventory Monitor (DignityApparelTN > General > Reporting & Analysis > Inventory Monitor: Stockout,
TN Planning and Lookup tabs) and the DA Iowa pipeline workbook (DignityApparel > General > Reporting & Analysis >
Pipeline: cut, tees, stock cuts and fabric status tabs), read-only with scripts/kpi/kpi_graph.py (Josh's Graph sign-in
on CHI-TOWN), into <data folder>/inventory/da-workbooks.json. A failed pull keeps the last good extract and records
the error in it ("error", "error_at"), which the page shows with the extract's date.

Usage: python scripts/inventory/pull_da_workbooks.py
"""
import datetime as dt
import io
import json
import os
import re
import sys
import traceback
import urllib.parse

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))); import paths  # noqa: E402  (data-extraction)
CACHE = os.path.join(paths.INVENTORY, "da-workbooks.json")


def n(v):
    try:
        f = float(v)
        return 0 if f != f else f
    except (TypeError, ValueError):
        return 0


def i0(v):
    return int(round(n(v)))


def d(v):
    if v is None or (isinstance(v, float) and v != v) or v is pd.NaT:
        return None
    try:
        return pd.Timestamp(v).date().isoformat()
    except (ValueError, TypeError):
        return None


def s(v):
    if v is None or (isinstance(v, float) and v != v):
        return ""
    return re.sub(r"\s+", " ", str(v)).strip()


# ------------------------------------------------------------------------------------------------ workbooks
SZ7 = ["SM", "MD", "LG", "XL", "2X", "3X", "4X"]


def rowvec(row, start, names):
    return {nm: i0(row[start + j]) for j, nm in enumerate(names) if start + j < len(row)}


def pull_workbooks():
    sys.path.insert(0, paths.GRAPH)   # kpi_graph.py, in this repo
    import kpi_graph as g
    import openpyxl

    # newest DA Inventory Monitor (same rule as scripts/kpi/pull_kpi.py monitor_file)
    sid = g.site_id("sites/DignityApparelTN")
    folder = "General/Reporting & Analysis/Inventory Monitor"
    items = g.get(f"/sites/{sid}/drive/root:/{urllib.parse.quote(folder)}:/children",
                  params={"$select": "id,name,parentReference,lastModifiedDateTime,file", "$top": 200})["value"]
    best = None
    for it in items:
        m = re.match(r"DA Inventory Monitor v[\d.]+ as of (\d{1,2})\.(\d{1,2})\.(\d{2})\.xlsx$", it["name"])
        if m and "file" in it:
            day = dt.date(2000 + int(m.group(3)), int(m.group(1)), int(m.group(2)))
            if best is None or day > best[1]:
                best = (it, day)
    if not best:
        raise RuntimeError("no DA Inventory Monitor file found")
    mit, mdate = best
    wb = openpyxl.load_workbook(io.BytesIO(g.download(mit)), data_only=True, read_only=True)

    mon = {}
    ws = wb["Stockout"]
    logs = []
    for row in ws.iter_rows(min_row=4, max_row=ws.max_row, max_col=27, values_only=True):
        row = list(row) + [None] * (27 - len(row))
        if row[0] in ("TN", "IA") and row[1] and row[2]:
            k = s(row[1]) + s(row[2])
            mon[k] = dict(style=s(row[1]), color=s(row[2]), f=row[0],
                          fin=rowvec(row, 3, ["PC"] + SZ7), ok=bool(row[13]) if row[13] is not None else None)
        st, co, added, cleared, status = row[16], row[17], row[18], row[19], row[20]
        if st and isinstance(added, dt.datetime):
            logs.append(dict(k=s(st) + s(co), added=added.date().isoformat(),
                             cleared=cleared.date().isoformat() if isinstance(cleared, dt.datetime) else None,
                             open=s(status).lower() == "open"))
    for e in logs:
        if e["open"] and e["k"] in mon:
            mon[e["k"]]["log"] = e["added"]
    tp = wb["TN Planning"]
    for row in tp.iter_rows(min_row=4, max_row=tp.max_row, max_col=96, values_only=True):
        row = list(row) + [None] * (96 - len(row))
        if not (row[0] and row[1] and str(row[0]).strip().upper().startswith("DA")):
            continue
        k = s(row[0]) + s(row[1])
        e = mon.setdefault(k, dict(style=s(row[0]), color=s(row[1]), f="TN"))
        e.update(all=rowvec(row, 2, SZ7), unc=rowvec(row, 10, SZ7), fin2=rowvec(row, 18, SZ7),
                 unf=rowvec(row, 26, SZ7), fu=rowvec(row, 34, SZ7), tgt=rowvec(row, 50, SZ7),
                 po=rowvec(row, 66, SZ7), gap=rowvec(row, 74, SZ7),
                 note=s(row[91]), tih=s(row[93]), pri=s(row[90]))
    old = {}
    lk = wb["Lookup"]
    for row in lk.iter_rows(min_row=2, max_row=lk.max_row, max_col=4, values_only=True):
        if len(row) >= 4 and row[2] and row[3]:
            old.setdefault(s(row[3]), []).append(s(row[2]))
    monitor = dict(file=mit["name"], as_of=mdate.isoformat(), modified=mit.get("lastModifiedDateTime"),
                   rows=mon, old=old)

    # DA Iowa pipeline workbook
    pit = g.drive_item("sites/DignityApparel", "General/Reporting & Analysis/Pipeline/Pipeline (DA Iowa) v4.1.xlsx")
    wb = openpyxl.load_workbook(io.BytesIO(g.download(pit)), data_only=True, read_only=True)
    ia = {}
    cut = wb["cut"]
    sz9 = SZ7 + ["5X", "6X"]
    for row in cut.iter_rows(min_row=2, max_row=cut.max_row, max_col=43, values_only=True):
        row = list(row) + [None] * (43 - len(row))
        if not row[1]:
            continue
        status = s(row[9]) or s(row[8])
        if status.lower() != "active":
            continue
        k = s(row[1]) + s(row[12])
        rem = rowvec(row, 34, sz9)
        ia.setdefault(k, {}).setdefault("cuts", []).append(dict(
            n=s(row[0]), d=d(row[3]), po=s(row[2]), q=i0(row[4]), left=i0(row[14]), rem=rem, ty=s(row[10])))
    tees = wb["tees"]
    for row in tees.iter_rows(min_row=3, max_row=tees.max_row, max_col=18, values_only=True):
        row = list(row) + [None] * (18 - len(row))
        if row[6] and i0(row[16]) > 0:
            k = s(row[6]) + s(row[7])
            ia.setdefault(k, {}).setdefault("tees", []).append(dict(
                n=s(row[0]), start=d(row[1]), end=d(row[2]), size=s(row[8]), left=i0(row[16]), note=s(row[17])))
    sc = wb["stock cuts"]
    for row in sc.iter_rows(min_row=4, max_row=sc.max_row, max_col=86, values_only=True):
        row = list(row) + [None] * (86 - len(row))
        if not (row[0] and row[1]):
            continue
        k = s(row[0]) + s(row[1])
        plan = rowvec(row, 58, SZ7)
        note = s(row[57])
        if any(plan.values()) or note:
            e = ia.setdefault(k, {})
            if any(plan.values()):
                e["plan"] = plan
            if note:
                e["note"] = note
    fs = wb["fabric status"]
    for row in fs.iter_rows(min_row=4, max_row=fs.max_row, max_col=16, values_only=True):
        row = list(row) + [None] * (16 - len(row))
        if row[0] and row[1] and row[2]:
            k = s(row[1]) + s(row[2])
            ia.setdefault(k, {})["fab"] = dict(fabric=s(row[0]), ypu=round(n(row[3]), 3), stock=round(n(row[7])),
                                               over=round(n(row[8])), pending=round(n(row[10])),
                                               to_order=round(n(row[14])), note=s(row[15]))
    iowa = dict(file=pit["name"], modified=pit.get("lastModifiedDateTime"), rows=ia)
    return dict(monitor=monitor, iowa=iowa, pulled=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"))


def save_extract(wbs):
    """Indented with sorted keys so each night's git diff shows what changed in the DA workbooks."""
    os.makedirs(os.path.dirname(CACHE), exist_ok=True)
    tmp = CACHE + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        json.dump(wbs, f, indent=1, sort_keys=True, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, CACHE)


def main():
    try:
        save_extract(pull_workbooks())
        print(f"DA workbooks saved to {CACHE}")
        return 0
    except Exception as e:  # fail open: keep the last good extract, note the error for the page
        traceback.print_exc()
        last = json.load(open(CACHE, encoding="utf-8")) if os.path.exists(CACHE) else dict(monitor=None, iowa=None, pulled=None)
        last.update(error=f"{type(e).__name__}: {e}", error_at=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"))
        save_extract(last)
        return 1


if __name__ == "__main__":
    sys.exit(main())
