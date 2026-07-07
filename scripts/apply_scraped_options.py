"""Flet scrapede autoritative options (vidaXL dwvar) ind i merge-planen (DRY-RUN).
For hver scrapet master: erstat variant option_values med vidaXL's egne (dansk display),
normalisér aksenavne, re-klassificér kollisioner, skriv opdateret plan + revalidér."""
import json, os, re, sys
from collections import defaultdict, Counter
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from build_merge_plan import _AXIS_MAP

PLAN = r"C:\Users\APC\vidaxl-pris-lager\output\merge_plan.jsonl"
SCR = r"C:\Users\APC\vidaxl-pris-lager\output\scraped_options.jsonl"

def canon(n):
    return _AXIS_MAP.get((n or "").lower().strip(), (n or "").strip())

def main():
    scraped = {}
    for l in open(SCR, encoding="utf-8"):
        try:
            d = json.loads(l)
            if d.get("variant_map"): scraped[d["master"]] = d
        except Exception: pass
    print(f"🔬 {len(scraped)} masters med scrapet variant-map")

    plans = [json.loads(l) for l in open(PLAN, encoding="utf-8")]
    upd = resolved = still = 0
    for p in plans:
        if not p.get("unresolved_collisions"): continue
        master = p["key"].split("|")[1]
        sc = scraped.get(master)
        if not sc: continue
        vm = {str(k).strip(): v for k, v in sc["variant_map"].items()}
        hit = sum(1 for m in p["variant_creates"] if m["sku"] in vm)
        if hit == 0: continue
        # erstat options for ALLE gruppens varianter der findes i map'et (autoritativ matrix)
        for m in p["variant_creates"]:
            if m["sku"] in vm:
                m["option_values"] = {canon(n): v for n, v in vm[m["sku"]].items()}
        axes = sorted({n for m in p["variant_creates"] for n in m["option_values"]})
        if len(axes) > 3:
            # Shopify hard limit 3 akser — vidaXL bruger 4+ dimensioner her → kan ikke merges rent
            p["target_axes"] = axes
            p["unresolved_collisions"] = [[m["sku"] for m in p["variant_creates"]]]
            p["warnings"].append(f"FOR_MANGE_AKSER({len(axes)}): {axes} — kræver manuel/split")
            still += 1
            continue
        p["target_axes"] = axes
        p["warnings"] = [w for w in p["warnings"] if not w.startswith("ULØST_KOLLISION")]
        p["warnings"].append(f"options_fra_vidaxl_scrape({hit}/{len(p['variant_creates'])} SKUs)")
        # re-klassificér: kollision hvor ALLE SKUs findes i vidaXL's egen matrix = ÆGTE dublet
        # (vidaXL mapper dem selv til samme kombination); ellers uløst (scrape-hul)
        rest = defaultdict(list)
        for m in p["variant_creates"]:
            rest[tuple(sorted((m["option_values"] or {}).items()))].append(m)
        true_dup, unresolved = [], []
        for ms in rest.values():
            if len(ms) <= 1: continue
            skus_set = [m["sku"] for m in ms]
            if all(s in vm for s in skus_set): true_dup.append(skus_set)
            else: unresolved.append(skus_set)
        p["unresolved_collisions"] = unresolved or None
        p["dup_sku_quarantine"] = true_dup or None   # ERSTAT gammel feed-baseret karantæne (upålidelig Color)
        if true_dup:
            p["warnings"].append(f"DUP_SKU_KARANTÆNE(vidaXL-matrix-bekræftet): {len(true_dup)} sæt")
        if p["unresolved_collisions"]: still += 1
        else:
            p.pop("unresolved_collisions", None); resolved += 1
        upd += 1
    with open(PLAN, "w", encoding="utf-8") as f:
        for p in plans: f.write(json.dumps(p, ensure_ascii=False) + "\n")
    print(f"✅ {upd} grupper opdateret m. autoritative options | fuldt løst: {resolved} | stadig uløst: {still}")

if __name__ == "__main__":
    main()
