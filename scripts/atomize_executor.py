"""ATOMIZE/SPLIT-EXECUTOR: deler de 86 fejl-grupperede keepers op i korrekte produkter (Gabriels regel:
identitet = alle item_variant-akser undtagen Farve; Farve = variant; genuint single = eget produkt).
Pr. keeper: beregn produkter (titel + varianter m. fuld opskrift) → slet original → opret produkter →
redirect. DRY-RUN default; --live for udførelse; --keeper <handle> for én; --n <N> for antal i dry-run.
Genbruger merge_executor's opskrift (feed/enrich/pris/billeder/redirect)."""
import json, os, re, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import pricing

def compute_products(keeper_handle, master, live_skus, plan_title_by_sku, feed, enrich):
    """Grupper SKUs efter identitet (ikke-Farve) → liste af produkt-specs."""
    km = ME.build_keyname(live_skus, master)
    opts = {s: ME.danish_opts(s, master, km) for s in live_skus}

    def ident(s):
        o = opts.get(s, {})
        if not o:
            return ("__single__", s)   # genuint single
        return tuple(sorted((a, v) for a, v in o.items() if a != "Farve" and v))

    groups = defaultdict(list)
    for s in live_skus:
        groups[ident(s)].append(s)

    specs = []
    for idv, skus in groups.items():
        colors = [opts[s].get("Farve") for s in skus if opts.get(s, {}).get("Farve")]
        multi = len(set(colors)) > 1
        # titel: planens new_title for en dækket SKU i gruppen, ellers feed-titel renset
        title = next((plan_title_by_sku[s] for s in skus if s in plan_title_by_sku), None)
        if not title:
            e = next((enrich.get(s) for s in skus if enrich.get(s)), {})
            raw = (e or {}).get("title") or ""
            title = ME._clean_vidaxl(raw) if raw else keeper_handle
        specs.append({"title": title, "farve_variant": multi, "skus": skus,
                      "colors_by_sku": {s: opts.get(s, {}).get("Farve") for s in skus}})
    return specs

def main():
    live = "--live" in sys.argv
    only = sys.argv[sys.argv.index("--keeper") + 1] if "--keeper" in sys.argv else None
    limit = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else 4
    plans = [json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8")]
    plan_title_by_sku = {m["sku"]: p["new_title"] for p in plans
                         if p["action"] in ("atomize", "split") for m in p["variant_creates"] if p.get("new_title")}
    byhandle = {}
    for p in plans:
        if p["action"] in ("atomize", "split") and p["variant_creates"]:
            byhandle.setdefault(p["keeper_handle"], p["key"].split("|")[1] if "|" in p["key"] else "")

    feed = ME.load_feed() if (live or "--feed" in sys.argv) else {}
    enrich = {}
    if live:
        enrich = ME.load_enrich(os.environ["FEED_URL"])

    handles = [only] if only else list(byhandle)[:limit] if not live else list(byhandle)
    tot_p = tot_v = 0
    for h in handles:
        master = byhandle.get(h, "")
        d = ME.gql("query($h:String!){productByHandle(handle:$h){id variants(first:100){edges{node{sku}}}}}", {"h": h})
        pr = (d.get("data") or {}).get("productByHandle")
        if not pr:
            print(f"  ⚠ {h}: ikke fundet live"); continue
        skus = [(e["node"]["sku"] or "").strip() for e in pr["variants"]["edges"]]
        specs = compute_products(h, master, skus, plan_title_by_sku, feed, enrich)
        tot_p += len(specs); tot_v += sum(len(s["skus"]) for s in specs)
        print(f"\n● {h}  →  {len(specs)} produkter:")
        for sp in specs:
            var = f"{len(sp['skus'])} farve-varianter" if sp["farve_variant"] else "single"
            print(f"     \"{sp['title'][:55]}\"  ({var})  SKUs={sp['skus'][:4]}")

    print(f"\n=== {'LIVE' if live else 'DRY-RUN'}: {len(handles)} keepers → {tot_p} produkter, {tot_v} varianter ===")
    if not live:
        print("(dry-run — ingen mutationer. Kør med --live for at slette originaler + oprette produkter + redirect)")

if __name__ == "__main__":
    main()
