"""FULD SAMMENHÆNGSKONTROL (READ-ONLY) af det eksekverbare sæt før go.
Tjekker struktur-invarianter A-Z: akse-grænse, kollisioner, titel-dækning, titel-renhed,
SKU-bevarelse, redirects. Rapporterer alle brud (fikses derefter)."""
import csv, json, os, re, sys
from collections import defaultdict, Counter
sys.stdout.reconfigure(encoding="utf-8")
plans = [json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8")]
oracle = {r["sku"]: r["approved_title"] for r in csv.DictReader(open("output/approved_titles_by_sku.csv", encoding="utf-8-sig"))}

EX = [p for p in plans if p["action"] in ("merge", "split", "atomize", "fix_mismerge_rest")
      and not p.get("unresolved_collisions") and not p.get("dup_sku_quarantine")
      and (p["variant_creates"] or p["product_deletes"])]   # udelad no-ops (intet at gøre)

fail = defaultdict(list)
sku_group = {}
for p in EX:
    key = p["key"]; skus = [m["sku"] for m in p["variant_creates"]]
    if not skus:
        fail["tom_gruppe_ingen_varianter"].append(key)
        continue
    # 1) SKU-bevarelse: ingen SKU i to grupper
    for s in skus:
        if s in sku_group:
            fail["sku_i_2_grupper"].append(f"{s}: {sku_group[s]} + {key}")
        sku_group[s] = key
    if p["action"] == "atomize":
        continue  # single-produkter: ingen delt akse/titel
    # 2) akse-grænse ≤3
    axes = sorted({k for m in p["variant_creates"] for k in (m["option_values"] or {})})
    if len(axes) > 3:
        fail["over_3_akser"].append(f"{key}: {axes}")
    # 3) option-kollision
    sig = Counter(tuple(sorted((m["option_values"] or {}).items())) for m in p["variant_creates"])
    if any(v > 1 for v in sig.values()):
        fail["option_kollision"].append(key)
    # 4) titel-dækning (plan.new_title = det eksekutoren sætter)
    title = p.get("new_title")
    if not title:
        fail["mangler_titel"].append(key)
        continue
    # 6) titel-renhed: KUN merge/fix (delt titel). split = enkelt-produkter hvor typen
    #    (fx 'Sofa', 'Fodskammel') ER produktidentiteten og hører til i titlen.
    if p["action"] not in ("merge", "fix_mismerge_rest"):
        continue
    tl = title.lower()
    # ord der er FÆLLES for alle en akses værdier er delt identitet (fx alle Model = '5-personers …')
    # → hører til i titlen; kun DISTINGVERENDE akse-værdier må ikke stå i den delte titel
    axis_vals = defaultdict(list)
    for m in p["variant_creates"]:
        for k, v in (m["option_values"] or {}).items():
            if v: axis_vals[k].append(str(v).lower())
    common = {k: set.intersection(*[set(v.split()) for v in vv]) if vv else set()
              for k, vv in axis_vals.items()}
    for m in p["variant_creates"]:
        for k, v in (m["option_values"] or {}).items():
            if k.lower() == "farve" or not v:
                continue
            v0 = str(v).split()[0].lower()
            if v0 in common.get(k, set()):
                continue  # fælles prefix = delt identitet, ok i titel
            if len(v0) > 3 and re.search(r"\b" + re.escape(v0) + r"\b", tl) and not v0.isdigit():
                fail["titel_har_aksevaerdi"].append(f"{key}: '{title}' ⊃ {k}={v}")
                break
        else:
            continue
        break
    # 7) redirects: hvert slettet produkt har en redirect
    dh = {f"/products/{d['handle']}" for d in p["product_deletes"]}
    rf = {r["from"] for r in p["redirects"]}
    if dh - rf:
        fail["sletning_uden_redirect"].append(f"{key}: {dh - rf}")

print(f"eksekverbare grupper tjekket: {len(EX)} | unikke SKUs: {len(sku_group)}")
print("=" * 60)
if not fail:
    print("✅ INGEN BRUD — alt hænger sammen")
else:
    for k, v in sorted(fail.items(), key=lambda x: -len(x[1])):
        print(f"❌ {k}: {len(v)}")
        for ex in v[:5]:
            print(f"     {ex}")
