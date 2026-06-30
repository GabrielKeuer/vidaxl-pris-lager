"""Diagnostik: find duplikerede SKUs i Shopify (vendor-filtreret).

Baggrund: bulk-repricing fejlede på ~3.289 varianter med "Duplicated input value"
fordi flere varianter deler samme SKU. SKUs BØR være unikke pr. variant. Dette
script bulk-eksporterer alle (vendor)-produkter+varianter MED oprettelsesdatoer og
analyserer:
  - hvor mange SKUs er dubletter, og hvor mange varianter det rammer
  - er dubletterne PÅ SAMME produkt (within) eller PÅ TVÆRS af produkter (cross)
  - oprettelsesdato-mønstre (produkt + variant) → peger det på en bestemt
    create-kørsel / dato der genererer dubletter?

Kør read-only via GitHub Actions (Shopify-creds i secrets). Skriver intet til Shopify.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Genbrug bulk-query-primitiver fra bulk_repricing (samme _shop_gql + poll-mønster)
from bulk_repricing import _shop_gql, _gid_num

VENDOR = os.environ.get("DUP_VENDOR", "vidaXL")

_BULK_Q_RUN = """
mutation bulkOperationRunQuery($query: String!) {
  bulkOperationRunQuery(query: $query) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
"""
_BULK_Q_STATUS = """
query { currentBulkOperation(type: QUERY) { id status errorCode objectCount url } }
"""


def export_variants(vendor):
    q_filter = f"vendor:'{vendor}'"
    inner = (
        "{ products(query: %s) { edges { node { id handle createdAt status "
        "variants { edges { node { id sku createdAt } } } } } } }" % json.dumps(q_filter)
    )
    # Vent på ledig query-slot
    for _ in range(60):
        s = _shop_gql(_BULK_Q_STATUS)["data"]["currentBulkOperation"]
        if not s or s["status"] not in ("CREATED", "RUNNING"):
            break
        time.sleep(10)
    d = _shop_gql(_BULK_Q_RUN, {"query": inner})
    res = d["data"]["bulkOperationRunQuery"]
    if res.get("userErrors"):
        raise SystemExit(f"bulkOperationRunQuery fejl: {res['userErrors']}")
    print(f"🚀 Bulk-export startet: {res['bulkOperation']['id']}")
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

    products = {}   # pid -> {handle, created, status}
    variants = []   # {vid, sku, vcreated, pid}
    if not url:
        return products, variants
    with urllib.request.urlopen(urllib.request.Request(url), timeout=300) as resp:
        for raw in resp:
            line = raw.decode("utf-8").strip()
            if not line:
                continue
            o = json.loads(line)
            oid = o.get("id", "") or ""
            if "/Product/" in oid:
                products[oid] = {
                    "handle": o.get("handle") or "",
                    "created": (o.get("createdAt") or "")[:10],
                    "status": o.get("status") or "",
                }
            elif "/ProductVariant/" in oid:
                variants.append({
                    "vid": oid,
                    "sku": (o.get("sku") or "").strip(),
                    "vcreated": (o.get("createdAt") or "")[:10],
                    "pid": o.get("__parentId"),
                })
    return products, variants


def main():
    print(f"=== Dup-SKU analyse for vendor='{VENDOR}' ===")
    products, variants = export_variants(VENDOR)
    print(f"📦 {len(products)} produkter, {len(variants)} varianter\n")

    # Gruppér pr. SKU (ignorér tomme SKUs)
    by_sku = defaultdict(list)
    empty = 0
    for v in variants:
        if not v["sku"]:
            empty += 1
            continue
        by_sku[v["sku"]].append(v)

    dup_skus = {s: vs for s, vs in by_sku.items() if len(vs) > 1}
    dup_variant_total = sum(len(vs) for vs in dup_skus.values())
    redundant = dup_variant_total - len(dup_skus)  # "ekstra" varianter ud over 1 pr. sku

    within = cross = 0
    cross_examples = []
    within_examples = []
    dup_product_ids = set()
    prod_created_hist = Counter()
    var_created_hist = Counter()
    # For "redundante" (alt efter den første) variant: hvornår blev den oprettet?
    redundant_var_created = Counter()

    for sku, vs in dup_skus.items():
        pids = {v["pid"] for v in vs}
        for v in vs:
            dup_product_ids.add(v["pid"])
            var_created_hist[v["vcreated"]] += 1
        # sortér efter variant-oprettelse; alt efter den ældste = "redundante"
        vs_sorted = sorted(vs, key=lambda x: x["vcreated"] or "9999")
        for v in vs_sorted[1:]:
            redundant_var_created[v["vcreated"]] += 1
        if len(pids) == 1:
            within += 1
            if len(within_examples) < 12:
                within_examples.append((sku, vs_sorted))
        else:
            cross += 1
            if len(cross_examples) < 12:
                cross_examples.append((sku, vs_sorted))

    for pid in dup_product_ids:
        p = products.get(pid)
        if p:
            prod_created_hist[p["created"]] += 1

    print(f"🔁 Dubletter: {len(dup_skus)} SKUs på >1 variant  ({dup_variant_total} varianter, {redundant} redundante)")
    print(f"   tomme SKUs: {empty}")
    print(f"   PÅ SAMME produkt (within):  {within} SKUs")
    print(f"   PÅ TVÆRS af produkter (cross): {cross} SKUs   <-- mest alvorligt (lager/fulfillment)\n")

    def _top(counter, n=15, label=""):
        print(f"— {label} (top {n} datoer) —")
        for k, c in sorted(counter.items(), key=lambda kv: -kv[1])[:n]:
            print(f"   {k or '(ukendt)'}: {c}")
        print()

    _top(prod_created_hist, 20, "Oprettelsesdato for DUP-PRODUKTER")
    _top(redundant_var_created, 20, "Oprettelsesdato for de REDUNDANTE varianter (den nyere af et par)")

    print("— Eksempler PÅ TVÆRS af produkter (cross) —")
    for sku, vs in cross_examples:
        parts = [f"{products.get(v['pid'],{}).get('handle','?')}@{v['vcreated']}" for v in vs]
        print(f"   SKU {sku}: " + "  |  ".join(parts))
    print()
    print("— Eksempler PÅ SAMME produkt (within) —")
    for sku, vs in within_examples:
        h = products.get(vs[0]["pid"], {}).get("handle", "?")
        print(f"   SKU {sku} @ {h}: varianter oprettet " + ", ".join(v["vcreated"] for v in vs))
    print()

    # Maskinlæsbart resumé (sidste linje) til nem opsamling
    print("RESULT_JSON=" + json.dumps({
        "vendor": VENDOR,
        "products": len(products),
        "variants": len(variants),
        "dup_skus": len(dup_skus),
        "dup_variants": dup_variant_total,
        "redundant_variants": redundant,
        "within_product": within,
        "cross_product": cross,
        "empty_skus": empty,
        "top_product_created": sorted(prod_created_hist.items(), key=lambda kv: -kv[1])[:10],
        "top_redundant_var_created": sorted(redundant_var_created.items(), key=lambda kv: -kv[1])[:10],
    }))


if __name__ == "__main__":
    main()
