"""OFFLINE kategorisering (ingen live-kald) der afspejler den NYE process_group-logik: KUN autoritativ
item_variant, døde legacy-options fjernes, scrape-huller/multi-værdi-legacy/inkonsistens flagges.
Rapporterer hvor mange der ville MERGE RENT vs. FLAGGET (og hvorfor). READ-ONLY."""
import json, os, re, sys
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

SP = r"C:\Users\APC\AppData\Local\Temp\claude\C--Users-APC\c0b60326-0d7f-46aa-bec2-7289b435d558\scratchpad"

def main():
    show = "--examples" in sys.argv
    plans = [json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8")]
    cache = json.load(open(SP + r"\plan_data_cache.json", encoding="utf-8"))
    varz, prods = cache["vars"], cache["prods"]
    h2pid = {pr["handle"]: pid for pid, pr in prods.items() if isinstance(pr, dict) and pr.get("handle")}
    pid2skus = defaultdict(list)
    kopt_vals = defaultdict(lambda: defaultdict(set))   # pid → {option_navn: sæt(værdier)}
    for s, vv in varz.items():
        if vv.get("pid"):
            pid2skus[vv["pid"]].append(s)
            for a, val in (vv.get("opts") or {}).items():
                if a != "Title" and val:
                    kopt_vals[vv["pid"]][a].add(val)

    ex = [p for p in plans if p["action"] in ("merge", "fix_mismerge_rest")
          and not p.get("unresolved_collisions") and not p.get("dup_sku_quarantine")
          and (p["variant_creates"] or p["product_deletes"])]

    cat = Counter(); examples = defaultdict(list)
    for p in ex:
        master = p["key"].split("|")[1] if "|" in p["key"] else ""
        added = [m["sku"] for m in p["variant_creates"]]
        pid = h2pid.get(p["keeper_handle"])
        kskus = pid2skus.get(pid, [])
        km = ME.build_keyname(added + kskus, master)
        allsk = added + kskus
        # scrape-hul?
        if any(not ME.OPTS.get(str(s).strip()) for s in allsk):
            cat["scrape_hul"] += 1; examples["scrape_hul"].append(p["keeper_handle"]); continue
        opts = {s: ME.danish_opts(s, master, km) for s in allsk}
        av = defaultdict(set)
        for o in opts.values():
            for k, v in o.items():
                if v: av[k].add(v)
        target = sorted({k for k, vv in av.items() if len(vv) > 1})
        if any(re.search(r" \d+$", a) for a in target):
            cat["bogus_dublet"] += 1; examples["bogus_dublet"].append(p["keeper_handle"]); continue
        if len(target) > 3:
            cat["ægte_4_akser (split)"] += 1; examples["ægte_4_akser (split)"].append(p["keeper_handle"]); continue
        # legacy: keeper-options ikke i target. Døde (1 værdi) fjernes; multi-værdi = konflikt → flag
        kv = kopt_vals.get(pid, {})
        legacy_live = [a for a, vals in kv.items() if a not in target and len(vals) > 1]
        if legacy_live:
            cat["legacy_konflikt (multi-værdi)"] += 1; examples["legacy_konflikt (multi-værdi)"].append((p["keeper_handle"], legacy_live, target)); continue
        # coverage: alle varianter har alle target-akser?
        if any(not opts[s].get(a) for a in target for s in allsk):
            cat["ægte_inkonsistent"] += 1; examples["ægte_inkonsistent"].append(p["keeper_handle"]); continue
        cat["REN"] += 1

    tot = sum(cat.values())
    print(f"=== NY LOGIK: kategorisering af {tot} merge/fix-grupper ===\n")
    for k, n in cat.most_common():
        print(f"  {n:5d} ({100*n/tot:4.1f}%)  {k}")
    print(f"\n  RENE (merger automatisk): {cat['REN']} ({100*cat['REN']/tot:.1f}%)")
    print(f"  FLAGGET:                  {tot - cat['REN']} ({100*(tot-cat['REN'])/tot:.1f}%)")
    if show:
        for k in ("legacy_konflikt (multi-værdi)", "ægte_inkonsistent", "bogus_dublet", "ægte_4_akser (split)", "scrape_hul"):
            print(f"\n--- {k} (3 eks.) ---")
            for e in examples[k][:3]:
                print(f"   {e}")

if __name__ == "__main__":
    main()
