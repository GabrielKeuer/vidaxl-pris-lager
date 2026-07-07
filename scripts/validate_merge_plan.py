"""VALIDÉR merge-planen mod invarianter (DRY-RUN). Enhver overtrædelse = plan må ikke eksekveres."""
import json, sys, csv
from collections import Counter, defaultdict
sys.stdout.reconfigure(encoding="utf-8")
PLAN = r"C:\Users\APC\vidaxl-pris-lager\output\merge_plan.jsonl"
CACHE2 = r"C:\Users\APC\AppData\Local\Temp\claude\C--Users-APC\c0b60326-0d7f-46aa-bec2-7289b435d558\scratchpad\plan_data_cache.json"

d = json.load(open(CACHE2, encoding="utf-8"))
prods, varz = d["prods"], d["vars"]
plans = [json.loads(l) for l in open(PLAN, encoding="utf-8")]
print(f"{len(plans)} plan-grupper | {len(varz)} live SKUs")

fails = []
def chk(name, bad, detail=""):
    print(("✅" if not bad else "❌") + f" {name}: {bad if bad else 'OK'} {detail}")
    if bad: fails.append(name)

# 1) SKU-bevarelse: hver create-SKU findes live præcis én gang; ingen SKU oprettes to gange
create_skus = Counter()
for p in plans:
    for m in p["variant_creates"]: create_skus[m["sku"]] += 1
dup = [s for s, c in create_skus.items() if c > 1]
chk("ingen SKU oprettes 2 gange", len(dup), str(dup[:3]))
unknown = [s for s in create_skus if s not in varz]
chk("alle create-SKUs findes live (pris/kilde kendt)", len(unknown), str(unknown[:3]))

# 2) sletninger: intet produkt slettes hvis det stadig er keeper et andet sted
keepers = {p["keeper"] for p in plans}
del_pids = {dd["pid"] for p in plans for dd in p["product_deletes"]}
chk("keeper slettes aldrig", len(keepers & del_pids), str(list(keepers & del_pids)[:3]))

# 3) intet produkt slettes to gange / af to grupper
delc = Counter(dd["pid"] for p in plans for dd in p["product_deletes"])
chk("produkt slettes max én gang", len([x for x, c in delc.items() if c > 1]))

# 4) SKU-total: efter plan = alle live SKUs bevaret (creates lander på keeper; donors slettes EFTER)
#    hver slettet donors SKUs skal alle være dækket af en create et sted
covered = set(create_skus)
by_pid = defaultdict(list)
for s, v in varz.items(): by_pid[v["pid"]].append(s)
lost = [s for pid in del_pids for s in by_pid[pid] if s not in covered]
chk("ingen SKU tabes ved sletning (alle donor-SKUs genskabes)", len(lost), str(lost[:5]))

# 5) redirects: to-handle er en keeper der IKKE selv slettes; ingen kæder/loops
del_handles = {dd["handle"] for p in plans for dd in p["product_deletes"]}
bad_red = [r for p in plans for r in p["redirects"] if r["to"].split("/products/")[-1] in del_handles]
chk("redirect peger aldrig på slettet produkt", len(bad_red), str(bad_red[:2]))
selfr = [r for p in plans for r in p["redirects"] if r["from"] == r["to"]]
chk("ingen selv-redirects", len(selfr))

# 6) Shopify-grænser: max 3 options; variantantal (2000 hard limit)
# kun EKSEKVERBARE grupper (ikke karantæne) skal overholde 3-akse-grænsen
too_many_axes = [p["key"] for p in plans if len(p["target_axes"]) > 3 and not p.get("unresolved_collisions")]
chk("max 3 option-akser (eksekverbare)", len(too_many_axes), str(too_many_axes[:3]))
too_many_vars = [p["key"] for p in plans if p["n_variants_final"] > 2000]
chk("max 2000 varianter", len(too_many_vars))

# 7) titler: alle handlings-grupper har titel (eller er flaget ny_gruppe)
no_title = [p["key"] for p in plans if not p["new_title"] and not any("orakel" in w for w in p["warnings"])]
chk("titel til alle (orakel eller flaget)", len(no_title))

# 8) options-kollision: alle kollisioner SKAL være eksplicit klassificeret (dup-karantæne eller uløst-liste)
coll = 0; coll_ex = []; dup_pairs = 0; unresolved_sets = 0
for p in plans:
    covered = {s for pair in (p.get("dup_sku_quarantine") or []) for s in pair} | \
              {s for st in (p.get("unresolved_collisions") or []) for s in st}
    dup_pairs += len(p.get("dup_sku_quarantine") or [])
    unresolved_sets += len(p.get("unresolved_collisions") or [])
    seen = {}
    for m in p["variant_creates"]:
        sig = tuple(sorted((m["option_values"] or {}).items()))
        if sig in seen:
            if m["sku"] in covered or seen[sig] in covered: continue  # eksplicit klassificeret
            coll += 1
            if len(coll_ex) < 3: coll_ex.append((p["key"], m["sku"], seen[sig]))
        else: seen[sig] = m["sku"]
chk("alle kollisioner klassificeret (ingen skjulte)", coll, str(coll_ex[:2]))
print(f"   ⏸ dup-SKU-par i karantæne (afventer keep-regler): {dup_pairs}")
print(f"   🔍 uløste kollisions-sæt (→ vidaxl.dk option-scrape): {unresolved_sets}")

print()
if fails: print(f"❌ PLAN IKKE KLAR: {fails}")
else: print("✅ ALLE INVARIANTER HOLDER — planen er konsistent (stadig dry-run)")
