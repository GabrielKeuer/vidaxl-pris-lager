"""DRY-RUN af den korrekte disposition for alle 86 atomize/split-keepers.
Regel: konfiguration = item_variant-akse (ikke Farve). Er configs SAMMENSÆTNINGER ('Nx ... + ...') →
ÉT multi-variant produkt (ren basis-titel + Farve × Konfiguration, config→'N Dele'). Er de PRODUKT-
TYPER (Fodskammel/Sofa) → SPLIT til separate produkter (orakel-titel pr. type). Engelsk→dansk + antal-
dele + disambiguering. READ-ONLY."""
import json, os, re, sys, csv
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

TRANS = {"bench": "bænk", "benches": "bænke", "armchair": "lænestol", "armchairs": "lænestole",
         "table": "bord", "stool": "skammel", "corner": "hjørne", "chair": "stol", "sofa": "sofa",
         "footstool": "fodskammel", "middle": "midter", "seat": "sæde", "with": "med", "and": "og"}

def da(s):
    for e, d in TRANS.items():
        s = re.sub(r"\b" + e + r"\b", d, s, flags=re.I)
    return s

def is_composition(cfg):
    return bool(re.search(r"\d+\s*x", cfg)) or "+" in cfg

def pieces(cfg):
    n = 0
    for part in re.split(r"\s*\+\s*", cfg):
        m = re.match(r"(\d+)\s*x", part.strip())
        n += int(m.group(1)) if m else (1 if part.strip() else 0)
    return n

def titlecase(s):
    return " ".join(w[:1].upper() + w[1:] if w else w for w in s.split(" "))

def main():
    oracle = {r["sku"]: r["approved_title"] for r in csv.DictReader(open("output/approved_titles_by_sku.csv", encoding="utf-8-sig")) if r["approved_title"]}
    plans = [json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8")]
    m4 = {}
    for p in plans:
        if p["action"] in ("atomize", "split") and p["variant_creates"]:
            m4.setdefault(p["keeper_handle"], p["key"].split("|")[1] if "|" in p["key"] else "")

    n_split = n_multi = 0; tot_prod = 0; show = int(sys.argv[sys.argv.index("--n")+1]) if "--n" in sys.argv else 8
    shown = 0
    for h, master in m4.items():
        d = ME.gql("query($h:String!){productByHandle(handle:$h){title variants(first:100){edges{node{sku}}}}}", {"h": h})
        pr = (d.get("data") or {}).get("productByHandle")
        if not pr:
            continue
        skus = [(e["node"]["sku"] or "").strip() for e in pr["variants"]["edges"]]
        km = ME.build_keyname(skus, master)
        opts = {s: ME.danish_opts(s, master, km) for s in skus}
        cfgkey = None
        for k in ("variationAttribute1", "variationAttribute2", "variationAttribute3", "Model", "numberOfNumber"):
            if any(k == a for s in skus for a in ME.OPTS.get(s, {})):
                cfgkey = k; break
        raw_cfg = {s: ME.OPTS.get(s, {}).get(cfgkey, "") for s in skus}
        comps = [c for c in raw_cfg.values() if c]
        composition = comps and sum(1 for c in comps if is_composition(c)) >= 0.5 * len(comps)

        if composition:
            n_multi += 1; tot_prod += 1
            # basis-titel = hyppigste orakel-titel strippet for 'N Dele' + farve
            base = Counter(titlecase(da(re.sub(r"\d+\s*[Dd]ele\s*", "", oracle.get(s, "")).strip())) for s in skus if oracle.get(s))
            title = base.most_common(1)[0][0] if base else h
            # config-labels
            lab = {}
            for s in skus:
                c = raw_cfg.get(s, "")
                lab[s] = f"{pieces(c)} Dele" if is_composition(c) else titlecase(da(c)) or "Standard"
            l2c = defaultdict(set)
            for s in skus:
                l2c[lab[s]].add(raw_cfg.get(s, ""))
            farver = sorted({opts[s].get("Farve") for s in skus if opts.get(s, {}).get("Farve")})
            if shown < show:
                print(f"● MULTI-VARIANT: {h[:42]}\n    titel=\"{title}\" | Farve={farver or '—'} | Konfiguration:")
                for l in sorted(set(lab.values())):
                    disp = l if len(l2c[l]) == 1 else l + f" ({titlecase(da(sorted(l2c[l])[0]))})"
                    print(f"       {disp}")
                shown += 1
        else:
            # SPLIT: hver type/SKU → eget produkt (orakel-titel)
            groups = defaultdict(list)
            for s in skus:
                groups[titlecase(da(raw_cfg.get(s, ""))) or s].append(s)
            n_split += 1; tot_prod += len(groups)
            if shown < show:
                print(f"● SPLIT: {h[:48]} → {len(groups)} produkter:")
                for g, ss in list(groups.items())[:5]:
                    t = next((oracle.get(x) for x in ss if oracle.get(x)), g)
                    print(f"       \"{t[:52]}\" ({len(ss)} SKU)")
                shown += 1

    print(f"\n=== 86 keepers: {n_multi} MULTI-VARIANT + {n_split} SPLIT → {tot_prod} produkter ===")

if __name__ == "__main__":
    main()
