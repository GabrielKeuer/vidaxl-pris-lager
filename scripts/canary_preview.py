"""CANARY-PREVIEW (READ-ONLY): vis hvordan de første merge-grupper vil se ud EFTER item_variant.
Vælger fuldt-scrapede eksekverbare grupper (fejl-merges først) → deres options er ENDELIGE.
Struktur (keeper/sletninger/redirects) er stabil uanset scrape. Output: fil + terminal."""
import csv, json, os, re, sys
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
from rebuild_plan_options import axis_name, load_variants

SP = r"C:\Users\APC\AppData\Local\Temp\claude\C--Users-APC\c0b60326-0d7f-46aa-bec2-7289b435d558\scratchpad"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 10

def main():
    iv = load_variants()
    plans = [json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8")]
    prods = json.load(open(SP + r"\plan_data_cache.json", encoding="utf-8"))["prods"]
    varz = json.load(open(SP + r"\plan_data_cache.json", encoding="utf-8"))["vars"]
    oracle = {r["sku"]: r["approved_title"] for r in csv.DictReader(open("output/approved_titles_by_sku.csv", encoding="utf-8-sig"))}

    # eksekverbare (ikke karantæne), fejl-merges først → mindste først.
    # Options læses fra den REBUILT plan (item_variant-autoritative).
    cand = [p for p in plans
            if p["action"] in ("fix_mismerge_rest", "merge")
            and not p.get("unresolved_collisions") and not p.get("dup_sku_quarantine")
            and p["variant_creates"]]
    cand.sort(key=lambda p: (p["action"] != "fix_mismerge_rest", len(p["variant_creates"])))
    pick = cand[:N]

    out = []
    for i, p in enumerate(pick, 1):
        keeper = Counter(varz[m["sku"]]["pid"] for m in p["variant_creates"] if m["sku"] in varz).most_common(1)
        kh = p["keeper_handle"]; title = oracle.get(p["variant_creates"][0]["sku"], p["new_title"])
        # options DIREKTE fra den rebuilt plan (item_variant-autoritative)
        variants = [{"sku": m["sku"], "options": m["option_values"] or {}, "pris": varz.get(m["sku"], {}).get("price")}
                    for m in p["variant_creates"]]
        blk = {
            "nr": i, "handling": p["action"], "keeper_handle": kh,
            "ny_titel": title, "akser": sorted({k for v in variants for k in v["options"]}),
            "antal_varianter": len(variants),
            "varianter": variants,
            "produkter_der_slettes": [d["handle"] for d in p["product_deletes"]],
            "redirects": [f"{r['from']} → {r['to']}" for r in p["redirects"]],
        }
        out.append(blk)

    json.dump(out, open(r"C:\Users\APC\Desktop\canary_preview.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    # terminal-render
    print(f"\n{'='*70}\nCANARY-PREVIEW: {len(out)} grupper (fuldt scrapet = ENDELIG tilstand)\n{'='*70}")
    for b in out:
        print(f"\n[{b['nr']}] {b['handling'].upper()} → keeper: {b['keeper_handle']}")
        print(f"    TITEL: {b['ny_titel']!r}")
        print(f"    AKSER: {b['akser']}  ({b['antal_varianter']} varianter)")
        for v in b["varianter"][:6]:
            print(f"      • {v['sku']}: {v['options']}  ({v['pris']} kr)")
        if len(b["varianter"]) > 6: print(f"      … +{len(b['varianter'])-6} flere")
        if b["produkter_der_slettes"]:
            print(f"    SLETTES + 301-redirect: {b['produkter_der_slettes'][:4]}"
                  + (f" (+{len(b['produkter_der_slettes'])-4})" if len(b['produkter_der_slettes']) > 4 else ""))
    print(f"\n✅ fuld fil: C:\\Users\\APC\\Desktop\\canary_preview.json")

if __name__ == "__main__":
    main()
