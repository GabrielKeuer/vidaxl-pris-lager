"""Erstat LLM-fallback (mange singler) for STORE flaggede grupper med deterministisk split: behold Farve
+ de op-til-2 ikke-Farve-akser med FLEST værdier som varianter (≤3 options), split på resten. Titel fra
orakel + split-værdier (dublet-titler OK). Opdaterer output/flagged_specs.json for grupper m. >15 produkter."""
import json, os, sys, csv
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def det_split(skus, master, oracle):
    km = ME.build_keyname(skus, master)
    opts = {s: ME.danish_opts(s, master, km) for s in skus}
    axvals = defaultdict(set)
    for s in skus:
        for a, v in opts.get(s, {}).items():
            if v:
                axvals[a].add(v)
    non_farve = [a for a in axvals if a != "Farve"]
    non_farve.sort(key=lambda a: -len(axvals[a]))          # flest værdier først
    keep = non_farve[:2]                                    # Farve + op til 2 = ≤3 options
    split_on = non_farve[2:]
    has_farve = "Farve" in axvals
    prods = defaultdict(list)
    for s in skus:
        o = opts.get(s, {})
        splitkey = tuple(o.get(a, "Standard") for a in split_on)
        prods[splitkey].append(s)
    out = []
    for splitkey, ss in prods.items():
        base = None
        for s in ss:
            if oracle.get(s):
                base = oracle[s]; break
        suffix = " ".join(str(v) for v in splitkey if v and v != "Standard")
        title = (base or master)
        if suffix and suffix.lower() not in (title or "").lower():
            title = f"{title} {suffix}".strip()
        variants = []
        for s in ss:
            o = opts.get(s, {})
            v = {"sku": s}
            if has_farve and o.get("Farve"):
                v["Farve"] = o["Farve"]
            for a in keep:
                if o.get(a):
                    v["Konfiguration" if a != "Farve" else a] = o[a] if a == keep[0] else v.get("Konfiguration")
            # brug 'keep'-akserne som Konfiguration (sammensat) hvis >1
            kparts = [o.get(a) for a in keep if o.get(a)]
            if kparts:
                v["Konfiguration"] = " / ".join(kparts)
            variants.append(v)
        out.append({"title": title[:120], "variants": variants})
    return out

def main():
    live_write = "--write" in sys.argv
    specs = json.load(open("output/flagged_specs.json", encoding="utf-8"))
    flagged = {f["keeper_handle"]: f for f in json.load(open("output/flagged_groups.json", encoding="utf-8"))}
    oracle = {r["sku"]: r["approved_title"] for r in csv.DictReader(open("output/approved_titles_by_sku.csv", encoding="utf-8-sig")) if r["approved_title"]}
    fixed = 0
    for keeper, spec in specs.items():
        if len(spec["products"]) <= 15:
            continue
        # fallback (mange singler) → deterministisk split
        skus = [v["sku"] for p in spec["products"] for v in p["variants"]]
        master = flagged.get(keeper, {}).get("key", "").split("|")[1] if flagged.get(keeper) else ""
        newprods = det_split(skus, master, oracle)
        before = len(spec["products"])
        spec["products"] = newprods
        fixed += 1
        axes = sorted({k for p in newprods for v in p["variants"] for k in v if k != "sku"})
        print(f"  {keeper[:42]}: {before} → {len(newprods)} produkter (akser: {axes})")
    if live_write:
        json.dump(specs, open("output/flagged_specs.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print(f"\n✓ {fixed} store grupper erstattet med deterministisk split. Gemt.")
    else:
        print(f"\n(dry — {fixed} grupper ville rettes. --write for at gemme)")

if __name__ == "__main__":
    main()
