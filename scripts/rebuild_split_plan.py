"""DIAGNOSTIK (read-only): regenerér den KORREKTE struktur for atomize/split-keepers fra autoritativ
item_variant. For hver keeper: udled SPLIT-AKSEN (den akse hvis værdi bestemmer produkt-identiteten,
fra planens new_titles) og grupper ALLE keeperens live-SKUs efter den → foreslåede produkter (split-
værdi = separat produkt, resten som varianter). Viser huller + kollisioner. Ingen mutationer."""
import json, os, sys
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def infer_split_axis(groups):
    """Split-aksen = den akse hvor SKUs med SAMME værdi får SAMME new_title (bestemmer identiteten),
    mens andre akser varierer inden for samme titel. Udledes af planens grupper."""
    # saml (option_values, title) pr. SKU fra planen
    per = []
    for g in groups:
        for m in g["variant_creates"]:
            per.append((m.get("option_values") or {}, g["new_title"]))
    if not per:
        return None
    axes = set()
    for ov, _ in per:
        axes |= set(ov)
    best = None
    for a in axes:
        # er title en funktion af DENNE akses værdi alene? (samme værdi → samme titel)
        val2titles = defaultdict(set)
        for ov, t in per:
            if a in ov:
                val2titles[ov[a]].add(t)
        if val2titles and all(len(ts) == 1 for ts in val2titles.values()):
            # og forskellige værdier → forskellige titler (aksen diskriminerer)
            if len({next(iter(ts)) for ts in val2titles.values()}) == len(val2titles):
                best = a
                break
    return best

def main():
    plans = [json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8")]
    limit = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else 6
    byhandle = defaultdict(list)
    for p in plans:
        if p["action"] in ("atomize", "split") and p["variant_creates"]:
            byhandle[(p["action"], p["keeper_handle"])].append(p)

    cat = Counter(); ex = defaultdict(list)
    shown = 0
    for (act, h), groups in byhandle.items():
        master = groups[0]["key"].split("|")[1] if "|" in groups[0]["key"] else ""
        d = ME.gql("query($h:String!){productByHandle(handle:$h){variants(first:100){edges{node{sku}}}}}", {"h": h})
        pr = (d.get("data") or {}).get("productByHandle")
        if not pr:
            continue
        skus = [(e["node"]["sku"] or "").strip() for e in pr["variants"]["edges"]]
        km = ME.build_keyname(skus, master)
        opts = {s: ME.danish_opts(s, master, km) for s in skus}
        # REGEL (Gabriel): identitet = alle item_variant-akser UNDTAGEN Farve; Farve = variant.
        # Grupper SKUs efter identitet → hvert produkt (Farve som variant hvis >1 farve, ellers single).
        def ident(s):
            o = opts.get(s, {})
            if not o:                       # genuint single (intet item_variant på vidaXL) → eget produkt
                return ("__single__", s)
            return tuple(sorted((a, v) for a, v in o.items() if a != "Farve" and v))
        prods = defaultdict(list)
        gap = []                            # ingen huller mere: tomt item_variant = genuint single
        for s in skus:
            prods[ident(s)].append(s)
        multi = sum(1 for ss in prods.values() if len({opts[x].get("Farve") for x in ss}) > 1)
        singles = len(prods) - multi
        c = "MED-HUL" if gap else "OK"
        cat[c] += 1
        ex[c].append((h, len(prods), multi, singles, len(gap)))
        if shown < limit and not gap:
            print(f"● {act} {h[:44]} → {len(prods)} produkter ({multi} m. farve-variant, {singles} singler)")
            for idv, ss in list(prods.items())[:3]:
                farver = sorted({opts[x].get('Farve') for x in ss if opts[x].get('Farve')})
                idstr = ", ".join(f"{a}={v}" for a, v in idv) or "(kun farve)"
                print(f"     [{idstr}] → {len(ss)} SKU, farver={farver or '—'}")
            shown += 1

    print(f"\n=== {sum(len(v) for v in byhandle.values())} grupper / {len(byhandle)} keepers ===")
    for k, n in cat.most_common():
        print(f"  {n:3d} keepers {k}")
    tot_prod = sum(e[1] for lst in ex.values() for e in lst)
    tot_multi = sum(e[2] for lst in ex.values() for e in lst)
    print(f"  → i alt {tot_prod} produkter ({tot_multi} m. farve-variant, {tot_prod-tot_multi} singler)")
    print(f"  keepers med item_variant-hul (kræver re-scrape): {len(ex.get('MED-HUL', []))}")

if __name__ == "__main__":
    main()
