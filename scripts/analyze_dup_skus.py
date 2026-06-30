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

    from datetime import datetime as _dt
    import statistics as _st

    def _parse(d):
        try:
            return _dt.strptime(d, "%Y-%m-%d")
        except Exception:
            return None

    variant_count = Counter(v["pid"] for v in variants)   # varianter pr. produkt

    within = cross = 0
    cross_examples = []
    dup_product_ids = set()
    prod_created_hist = Counter()
    redundant_var_created = Counter()
    pair_class = Counter()        # single×single / single×multi / multi×multi (cross-par m. 2 produkter)
    redundant_is_single = 0       # den redundante (nyere) variant sidder på et single-variant-produkt
    redundant_status = Counter()
    older_status = Counter()
    gap_days = []                 # dage mellem ældste og nyeste variant i en dublet

    for sku, vs in dup_skus.items():
        pids = {v["pid"] for v in vs}
        for v in vs:
            dup_product_ids.add(v["pid"])
        vs_sorted = sorted(vs, key=lambda x: x["vcreated"] or "9999")
        older_status[products.get(vs_sorted[0]["pid"], {}).get("status", "?")] += 1
        for v in vs_sorted[1:]:
            redundant_var_created[v["vcreated"]] += 1
            if variant_count.get(v["pid"], 0) == 1:
                redundant_is_single += 1
            redundant_status[products.get(v["pid"], {}).get("status", "?")] += 1
        ds = [_parse(v["vcreated"]) for v in vs if _parse(v["vcreated"])]
        if len(ds) >= 2:
            gap_days.append((max(ds) - min(ds)).days)
        if len(pids) == 1:
            within += 1
        else:
            cross += 1
            if len(pids) == 2:
                counts = sorted(variant_count.get(p, 0) for p in pids)
                cls = ("single×single" if counts[1] == 1 else
                       "single×multi" if counts[0] == 1 else "multi×multi")
                pair_class[cls] += 1
            else:
                pair_class[f"{len(pids)} produkter"] += 1
            if len(cross_examples) < 15:
                cross_examples.append((sku, vs_sorted))

    for pid in dup_product_ids:
        p = products.get(pid)
        if p:
            prod_created_hist[p["created"]] += 1

    print(f"🔁 Dubletter: {len(dup_skus)} SKUs (within={within}, cross={cross})  tomme={empty}\n")

    print("— PAR-KLASSIFIKATION (variant-antal på de involverede produkter) —")
    for k, c in pair_class.most_common():
        print(f"   {k}: {c}")
    print(f"\n— Den redundante (nyere) variant sidder på et SINGLE-variant-produkt: {redundant_is_single}/{len(dup_skus)} —")
    print(f"— Nyere-produkt status: {dict(redundant_status)}")
    print(f"— Ældre-produkt status: {dict(older_status)}")
    if gap_days:
        gap_days.sort()
        print(f"— Dato-gap mellem dubletter (dage): median={_st.median(gap_days)}, "
              f"min={gap_days[0]}, max={gap_days[-1]}, samme-dag={sum(1 for g in gap_days if g == 0)} —")
    print()

    def _top(counter, n=15, label=""):
        print(f"— {label} (top {n} datoer) —")
        for k, c in sorted(counter.items(), key=lambda kv: -kv[1])[:n]:
            print(f"   {k or '(ukendt)'}: {c}")
        print()

    _top(prod_created_hist, 15, "Oprettelsesdato for DUP-PRODUKTER")
    _top(redundant_var_created, 15, "Oprettelsesdato for de REDUNDANTE (nyere) varianter")

    print("— Eksempler (cross) m. variant-antal + status —")
    for sku, vs in cross_examples:
        parts = []
        for v in vs:
            p = products.get(v["pid"], {})
            parts.append(f"{p.get('handle','?')}[{variant_count.get(v['pid'],0)}var,{p.get('status','?')}]@{v['vcreated']}")
        print(f"   SKU {sku}: " + "  |  ".join(parts))
    print()

    print("RESULT_JSON=" + json.dumps({
        "vendor": VENDOR,
        "products": len(products),
        "variants": len(variants),
        "dup_skus": len(dup_skus),
        "within_product": within,
        "cross_product": cross,
        "pair_class": dict(pair_class),
        "redundant_on_single_product": redundant_is_single,
        "redundant_status": dict(redundant_status),
        "older_status": dict(older_status),
        "gap_days_median": _st.median(gap_days) if gap_days else None,
        "gap_same_day": sum(1 for g in gap_days if g == 0) if gap_days else 0,
        "top_product_created": sorted(prod_created_hist.items(), key=lambda kv: -kv[1])[:8],
        "top_redundant_var_created": sorted(redundant_var_created.items(), key=lambda kv: -kv[1])[:8],
    }))


if __name__ == "__main__":
    main()
