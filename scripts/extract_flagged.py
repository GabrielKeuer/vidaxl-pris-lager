"""Udtræk de flaggede merge-grupper (samme logik som categorize_groups) → output/flagged_groups.json
med key, keeper_handle, kategori, alle SKUs. Samler scrape-hul-SKUs til re-scrape."""
import json, os, re, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

SP = r"C:\Users\APC\AppData\Local\Temp\claude\C--Users-APC\c0b60326-0d7f-46aa-bec2-7289b435d558\scratchpad"

def main():
    plans = [json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8")]
    cache = json.load(open(SP + r"\plan_data_cache.json", encoding="utf-8"))
    varz, prods = cache["vars"], cache["prods"]
    h2pid = {pr["handle"]: pid for pid, pr in prods.items() if isinstance(pr, dict) and pr.get("handle")}
    pid2skus = defaultdict(list)
    kopt_vals = defaultdict(lambda: defaultdict(set))
    for s, vv in varz.items():
        if vv.get("pid"):
            pid2skus[vv["pid"]].append(s)
            for a, val in (vv.get("opts") or {}).items():
                if a != "Title" and val:
                    kopt_vals[vv["pid"]][a].add(val)

    ex = [p for p in plans if p["action"] in ("merge", "fix_mismerge_rest")
          and not p.get("unresolved_collisions") and not p.get("dup_sku_quarantine")
          and (p["variant_creates"] or p["product_deletes"])]

    flagged = []
    scrape_skus = set()
    for p in ex:
        master = p["key"].split("|")[1] if "|" in p["key"] else ""
        added = [m["sku"] for m in p["variant_creates"]]
        pid = h2pid.get(p["keeper_handle"])
        kskus = pid2skus.get(pid, [])
        km = ME.build_keyname(added + kskus, master)
        allsk = [str(s).strip() for s in added + kskus]
        rec = {"key": p["key"], "keeper_handle": p["keeper_handle"], "skus": allsk}
        holes = [s for s in allsk if not ME.OPTS.get(s)]
        if holes:
            rec["cat"] = "scrape_hul"; rec["holes"] = holes
            scrape_skus.update(holes); flagged.append(rec); continue
        opts = {s: ME.danish_opts(s, master, km) for s in allsk}
        av = defaultdict(set)
        for o in opts.values():
            for k, v in o.items():
                if v: av[k].add(v)
        target = sorted({k for k, vv in av.items() if len(vv) > 1})
        if any(re.search(r" \d+$", a) for a in target):
            rec["cat"] = "bogus_dublet"; flagged.append(rec); continue
        if len(target) > 3:
            rec["cat"] = "ægte_4_akser"; flagged.append(rec); continue
        kv = kopt_vals.get(pid, {})
        legacy_live = [a for a, vals in kv.items() if a not in target and len(vals) > 1]
        if legacy_live and len(set(target) | set(legacy_live)) > 3:
            rec["cat"] = "ægte_4_akser"; flagged.append(rec); continue
        if any(not opts[s].get(a) for a in target for s in allsk):
            rec["cat"] = "ægte_inkonsistent"; flagged.append(rec); continue
        # REN — ikke flagget

    json.dump(flagged, open("output/flagged_groups.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    json.dump(sorted(scrape_skus), open(SP + r"\flagged_scrape_skus.json", "w"))
    from collections import Counter
    c = Counter(f["cat"] for f in flagged)
    print(f"flaggede grupper: {len(flagged)} → {dict(c)}")
    print(f"scrape-hul-SKUs at re-scrape: {len(scrape_skus)}")
    print("gemt: output/flagged_groups.json")

if __name__ == "__main__":
    main()
