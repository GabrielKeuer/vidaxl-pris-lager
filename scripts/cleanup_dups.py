"""Oprydning af duplikerede produkter: 301-redirect + slet redundante produkter.

Læser cleanup/plan_clusters.json: [{scenario, keep, deletes:[handle...]}].
For hver klynge:
  1. Resolve keep + deletes (handle -> {id, skus}).
  2. SIKKERHED: slet KUN et produkt hvis ALLE dets SKUs også findes på keeperen
     (ellers ville en SKU blive hjemløs -> skip + log). Belt-and-suspenders ovenpå
     at identical/subset er sikre per konstruktion.
  3. Opret 301-redirect /products/<delete> -> /products/<keep> (idempotent).
  4. productDelete på det redundante produkt.

--dry-run (default): resolver + sikkerhedstjek + rapport. INGEN writes.
--live: udfør redirects + sletninger.
--limit N: maks N klynger (til lille verifikations-batch).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bulk_repricing import _shop_gql

PLAN_PATH = "cleanup/plan_clusters.json"

Q_RESOLVE = """
query($h: String!) {
  productByHandle(handle: $h) {
    id handle
    variants(first: 250) { edges { node { sku } } pageInfo { hasNextPage } }
  }
}
"""
M_REDIRECT = """
mutation($r: UrlRedirectInput!) {
  urlRedirectCreate(urlRedirect: $r) { urlRedirect { id } userErrors { field message } }
}
"""
M_DELETE = """
mutation($id: ID!) {
  productDelete(input: { id: $id }) { deletedProductId userErrors { field message } }
}
"""


def resolve(handle):
    d = _shop_gql(Q_RESOLVE, {"h": handle})
    p = (d.get("data") or {}).get("productByHandle")
    if not p:
        return None
    skus = set()
    for e in p["variants"]["edges"]:
        s = (e["node"].get("sku") or "").strip()
        if s:
            skus.add(s)
    return {"id": p["id"], "handle": p["handle"], "skus": skus,
            "truncated": p["variants"]["pageInfo"]["hasNextPage"]}


def make_redirect(from_handle, keep_handle, dry):
    path = f"/products/{from_handle}"
    target = f"/products/{keep_handle}"
    if dry:
        return "would-create"
    d = _shop_gql(M_REDIRECT, {"r": {"path": path, "target": target}})
    errs = (d.get("data") or {}).get("urlRedirectCreate", {}).get("userErrors") or []
    if errs:
        msg = "; ".join(e["message"] for e in errs)
        if "already" in msg.lower() or "taget" in msg.lower() or "taken" in msg.lower():
            return "exists"
        return f"error:{msg[:120]}"
    return "created"


def delete_product(pid, dry):
    if dry:
        return "would-delete"
    d = _shop_gql(M_DELETE, {"id": pid})
    errs = (d.get("data") or {}).get("productDelete", {}).get("userErrors") or []
    if errs:
        return "error:" + "; ".join(e["message"] for e in errs)[:120]
    return "deleted"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    dry = not args.live

    plan = json.load(open(PLAN_PATH, encoding="utf-8"))
    if args.limit:
        plan = plan[:args.limit]
    print(f"🧹 cleanup_dups — {'DRY-RUN' if dry else 'LIVE'} — {len(plan)} klynger")

    st = {"redirected": 0, "deleted": 0, "skip_orphan": 0, "skip_keep_missing": 0,
          "skip_already_gone": 0, "redir_exists": 0, "errors": 0, "orphan_clusters": []}

    for i, c in enumerate(plan, 1):
        keep = resolve(c["keep"])
        if not keep:
            st["skip_keep_missing"] += 1
            print(f"  [{i}] ⚠ keeper mangler: {c['keep']} — hele klyngen sprunget over")
            continue
        if keep["truncated"]:
            print(f"  [{i}] ⚠ keeper {c['keep']} har >250 varianter — sikkerhedstjek kan være ufuldstændigt, springer over")
            st["skip_keep_missing"] += 1
            continue
        for dh in c["deletes"]:
            dp = resolve(dh)
            if not dp:
                # Allerede væk: sørg dog for redirect findes
                r = make_redirect(dh, c["keep"], dry)
                if r == "created":
                    st["redirected"] += 1
                elif r == "exists":
                    st["redir_exists"] += 1
                st["skip_already_gone"] += 1
                continue
            # SIKKERHED: alle slet-SKUs skal findes på keeperen
            orphans = dp["skus"] - keep["skus"]
            if orphans:
                st["skip_orphan"] += 1
                st["orphan_clusters"].append({"keep": c["keep"], "delete": dh,
                                              "orphans": sorted(orphans)[:10]})
                print(f"  [{i}] ⛔ SKIP {dh}: {len(orphans)} SKUs findes ikke på keeper — ville blive hjemløse")
                continue
            r = make_redirect(dh, c["keep"], dry)
            if r == "created":
                st["redirected"] += 1
            elif r == "exists":
                st["redir_exists"] += 1
            elif r.startswith("error"):
                # Slet ALDRIG hvis redirect ikke kunne oprettes -> undgå 404 uden redirect
                st["errors"] += 1
                print(f"  [{i}] redirect-fejl {dh}: {r} — SPRINGER sletning over (undgår 404)")
                continue
            dr = delete_product(dp["id"], dry)
            if dr == "deleted" or dr == "would-delete":
                st["deleted"] += 1
            else:
                st["errors"] += 1
                print(f"  [{i}] delete-fejl {dh}: {dr}")
        if i % 50 == 0:
            print(f"  …{i}/{len(plan)} klynger | slettet={st['deleted']} redirect={st['redirected']} orphan-skip={st['skip_orphan']}")

    print("\n📊 RESULTAT:")
    for k, v in st.items():
        if k == "orphan_clusters":
            continue
        print(f"   {k}: {v}")
    if st["orphan_clusters"]:
        print(f"   ⛔ {len(st['orphan_clusters'])} orphan-skip (eksempler):")
        for o in st["orphan_clusters"][:10]:
            print(f"      keep={o['keep']} slet={o['delete']} orphans={o['orphans']}")
    return 1 if st["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
