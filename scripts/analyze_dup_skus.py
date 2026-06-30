"""Diagnostik + oprydnings-grundlag: duplikerede SKUs i Shopify (vendor-filtreret).

SKUs BØR være unikke pr. variant. Dette script bulk-eksporterer alle (vendor)-
produkter+varianter (read-only) og:
  1. finder SKUs der sidder på >1 produkt,
  2. danner KLYNGER (produkter forbundet via delte SKUs, transitivt),
  3. klassificerer hver klynge efter SKU-mængde-relation:
       - identical : alle produkter har præcis samme SKU-sæt (eksakte kopier)
       - subset    : ét produkts SKU-sæt er supersæt af de andre (regruppering)
       - partial   : produkterne overlapper delvist (hver har unikke SKUs)
       - single    : kun single-variant-produkter involveret
  4. foreslår en handling pr. klynge (KEEP/REDIRECT/REVIEW) som UDGANGSPUNKT.

Skriver output/dup_clusters.json (uploades som artifact) + printer resumé.
Skriver INTET til Shopify.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bulk_repricing import _shop_gql

VENDOR = os.environ.get("DUP_VENDOR", "vidaXL")

_BULK_Q_RUN = """
mutation bulkOperationRunQuery($query: String!) {
  bulkOperationRunQuery(query: $query) { bulkOperation { id status } userErrors { field message } } }
"""
_BULK_Q_STATUS = """
query { currentBulkOperation(type: QUERY) { id status errorCode objectCount url } }
"""


def export(vendor):
    q_filter = f"vendor:'{vendor}'"
    inner = (
        "{ products(query: %s) { edges { node { id handle title createdAt status "
        "totalInventory variants { edges { node { id sku createdAt } } } } } } }" % json.dumps(q_filter)
    )
    for _ in range(60):
        s = _shop_gql(_BULK_Q_STATUS)["data"]["currentBulkOperation"]
        if not s or s["status"] not in ("CREATED", "RUNNING"):
            break
        time.sleep(10)
    res = _shop_gql(_BULK_Q_RUN, {"query": inner})["data"]["bulkOperationRunQuery"]
    if res.get("userErrors"):
        raise SystemExit(f"bulkOperationRunQuery fejl: {res['userErrors']}")
    print(f"🚀 Bulk-export: {res['bulkOperation']['id']}")
    start = time.time(); url = None; last = None
    while True:
        time.sleep(10)
        s = _shop_gql(_BULK_Q_STATUS)["data"]["currentBulkOperation"]
        if not s:
            continue
        if s["status"] != last:
            print(f"   [{int(time.time()-start):>4}s] {s['status']} objectCount={s.get('objectCount')}")
            last = s["status"]
        if s["status"] == "COMPLETED":
            url = s.get("url"); break
        if s["status"] in ("FAILED", "CANCELED", "EXPIRED"):
            raise SystemExit(f"Bulk-export {s['status']} ({s.get('errorCode')})")
        if time.time() - start > 45 * 60:
            raise SystemExit("Bulk-export timeout")

    products = {}
    prod_skus = defaultdict(set)
    if not url:
        return products, prod_skus
    with urllib.request.urlopen(urllib.request.Request(url), timeout=300) as resp:
        for raw in resp:
            line = raw.decode("utf-8").strip()
            if not line:
                continue
            o = json.loads(line)
            oid = o.get("id", "") or ""
            if "/Product/" in oid:
                products[oid] = {
                    "pid": oid,
                    "handle": o.get("handle") or "",
                    "title": o.get("title") or "",
                    "created": (o.get("createdAt") or "")[:10],
                    "status": o.get("status") or "",
                    "inventory": o.get("totalInventory"),
                    "variant_count": 0,
                }
            elif "/ProductVariant/" in oid:
                pid = o.get("__parentId")
                sku = (o.get("sku") or "").strip()
                if pid and sku:
                    prod_skus[pid].add(sku)
    for pid, skus in prod_skus.items():
        if pid in products:
            products[pid]["variant_count"] = len(skus)
    return products, prod_skus


def main():
    print(f"=== Dup-SKU oprydnings-analyse for vendor='{VENDOR}' ===")
    products, prod_skus = export(VENDOR)
    print(f"📦 {len(products)} produkter\n")

    # SKU -> produkter
    sku_products = defaultdict(set)
    for pid, skus in prod_skus.items():
        for s in skus:
            sku_products[s].add(pid)
    dup_skus = {s: ps for s, ps in sku_products.items() if len(ps) > 1}
    dup_pids = set().union(*dup_skus.values()) if dup_skus else set()

    # Union-find klynger (produkter forbundet via delte SKUs)
    parent = {p: p for p in dup_pids}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]; x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for s, ps in dup_skus.items():
        ps = list(ps)
        for p in ps[1:]:
            union(ps[0], p)

    clusters = defaultdict(list)
    for p in dup_pids:
        clusters[find(p)].append(p)

    def classify(pids):
        sets = {p: prod_skus[p] for p in pids}
        sizes = [len(s) for s in sets.values()]
        if all(s == sets[pids[0]] for s in sets.values()):
            return "identical"
        # supersæt: ét sæt indeholder ALLE andre
        for p, sp in sets.items():
            if all(sets[q] <= sp for q in pids if q != p):
                return "subset"
        if max(sizes) == 1:
            return "single"
        return "partial"

    scen = Counter()
    out_clusters = []
    for root, pids in clusters.items():
        pids = sorted(pids, key=lambda p: (products.get(p, {}).get("created", ""), p))
        sc = classify(pids)
        if max(len(prod_skus[p]) for p in pids) == 1 and sc != "identical":
            sc = "single"
        scen[sc] += 1

        # delt vs unikt pr. produkt
        shared = set.intersection(*[prod_skus[p] for p in pids])
        prods = []
        for p in pids:
            info = products.get(p, {})
            prods.append({
                "handle": info.get("handle"), "title": info.get("title"),
                "created": info.get("created"), "status": info.get("status"),
                "inventory": info.get("inventory"), "variants": len(prod_skus[p]),
                "shared": len(prod_skus[p] & shared), "unique": len(prod_skus[p] - shared),
            })

        # Forslag (UDGANGSPUNKT — hub'en finpudser keeper med GSC-trafik ved visning)
        if sc == "identical":
            keeper = prods[0]["handle"]  # ældste
            rec = f"Eksakte kopier (samme SKU-sæt). Behold ét → 301-redirect + slet de øvrige."
        elif sc == "subset":
            keeper = max(prods, key=lambda x: x["variants"])["handle"]
            rec = "Gamle del-produkter supersedet af ét samlet variant-produkt (single→variant-migration). Behold supersættet → redirect + slet del-produkterne."
        elif sc == "single":
            keeper = prods[0]["handle"]
            rec = "Single-variant-rester der deler SKU. Behold ét → redirect + slet resten."
        else:
            keeper = None
            rec = "Delvist overlap (mega-gruppering på kryds og tværs). Kræver manuel stillingtagen — produkterne er ofte blandet sammen og navnene giver ikke mening."

        out_clusters.append({
            "scenario": sc, "n_products": len(pids), "n_shared_skus": len(shared),
            "shared_skus_sample": sorted(list(shared))[:8], "keeper": keeper,
            "products": prods, "recommendation": rec,
        })

    os.makedirs("output", exist_ok=True)
    with open("output/dup_clusters.json", "w", encoding="utf-8") as f:
        json.dump({
            "vendor": VENDOR, "products": len(products),
            "dup_skus": len(dup_skus), "clusters": len(out_clusters),
            "by_scenario": dict(scen), "data": out_clusters,
        }, f, ensure_ascii=False, indent=2)

    print(f"🔁 {len(dup_skus)} dup-SKUs → {len(out_clusters)} klynger\n")
    print("— SCENARIER (antal klynger) —")
    for k, c in scen.most_common():
        print(f"   {k}: {c}")
    print()
    # 4 eksempler pr. scenarie
    for target in ["identical", "subset", "partial", "single"]:
        exs = [c for c in out_clusters if c["scenario"] == target][:4]
        if not exs:
            continue
        print(f"=== Eksempler: {target} ===")
        for c in exs:
            print(f"  [{c['n_products']} prod, {c['n_shared_skus']} delte SKUs] → {c['recommendation']}")
            for p in c["products"]:
                print(f"     - {p['handle']} | {p['variants']}var | lager={p['inventory']} | {p['created']} | {p['status']} | delt={p['shared']} unik={p['unique']}")
        print()

    # === Skriv til Supabase → fodrer hub'ens Dubletter-fane ===
    try:
        import uuid
        from datetime import datetime, timezone
        from sync_prices_v2 import get_supabase_client
        sb = get_supabase_client()
    except Exception as e:
        sb = None
        print(f"⚠ Supabase ikke tilgængelig ({e}) — springer DB-skrivning over")
    if sb is not None:
        batch = str(uuid.uuid4())
        try:
            sb.table("dup_scans").insert({
                "id": batch, "vendor": VENDOR, "status": "completed",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "total_products": len(products),
                "total_variants": sum(len(s) for s in prod_skus.values()),
                "dup_skus": len(dup_skus), "clusters": len(out_clusters),
                "by_scenario": dict(scen),
            }).execute()
            rows = [{
                "scan_batch": batch, "vendor": VENDOR, "scenario": c["scenario"],
                "n_products": c["n_products"], "n_shared_skus": c["n_shared_skus"],
                "products": c["products"], "keeper_handle": c.get("keeper"),
                "proposed_action": c["recommendation"], "shared_skus": c["shared_skus_sample"],
                "status": "pending",
            } for c in out_clusters]
            for i in range(0, len(rows), 200):
                sb.table("dup_clusters").insert(rows[i:i + 200]).execute()
            print(f"✅ Supabase: scan {batch} + {len(rows)} klynger skrevet")
        except Exception as e:
            print(f"⚠ Supabase-skrivning fejlede: {e}")


if __name__ == "__main__":
    main()
