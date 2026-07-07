"""DRY-RUN (read-only): for de master_pids der er berørt af de afvegne atomize/flagged/collision-produkter,
vis det KORREKTE mål-produkt (fra simulationen) vs. de NUVÆRENDE rodede live-produkter der holder SKU'erne.
Så vi ser præcis hvad master_pid-executoren ville konsolidere/rette FØR noget live."""
import sys, os, json
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def main():
    n = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else 12
    sim = json.load(open("output/catalog_simulation.json", encoding="utf-8"))
    # SKUs fra de afvegne produkter (atomize/flagged/collision)
    skus = set()
    for f in ("output/atomize_specs.json", "output/flagged_specs.json", "output/collision_specs.json"):
        if not os.path.exists(f):
            continue
        for spec in json.load(open(f, encoding="utf-8")).values():
            prods = spec["products"] if isinstance(spec, dict) else spec
            for p in prods:
                for v in p["variants"]:
                    skus.add(str(v["sku"]))
    print(f"SKUs fra afvegne produkter: {len(skus)}")
    # deres master_pid
    sb = ME.get_supabase_client()
    sku2mid = {}
    sl = list(skus)
    for i in range(0, len(sl), 300):
        for x in (sb.table("vidaxl_sku_master").select("sku,master_pid").in_("sku", sl[i:i + 300]).execute().data or []):
            sku2mid[str(x["sku"])] = x["master_pid"]
    mids = sorted(set(sku2mid.values()))
    print(f"berørte master_pids: {len(mids)}\n")
    # klassificér: hvor mange bliver rene multi/single vs flagget
    from collections import Counter
    types = Counter(sim.get(m, {}).get("type", "ukendt") for m in mids)
    print("berørte master_pids efter mål-type:", dict(types))
    print()
    shown = 0
    for mid in mids:
        s = sim.get(mid)
        if not s or shown >= n:
            continue
        if s["type"] not in ("single", "multi"):
            continue
        # nuværende live-produkter der holder disse SKUs
        cur = defaultdict(list)
        for sku in s["skus"][:20]:
            d = ME.gql('query($q:String!){productVariants(first:3,query:$q){edges{node{sku product{title handle}}}}}', {"q": f"sku:{sku}"})
            for e in (((d.get("data") or {}).get("productVariants") or {}).get("edges") or []):
                if (e["node"]["sku"] or "").strip() == sku:
                    cur[e["node"]["product"]["handle"]].append(sku)
        print(f"● {mid} [{s['type']}, {s['n']} SKU]")
        print(f"    MÅL-titel: \"{s['title']}\"" + (f" | akser={s.get('axes')}" if s['type'] == 'multi' else ""))
        print(f"    NU spredt på {len(cur)} produkter: {[h[:34] for h in list(cur)[:4]]}")
        shown += 1

if __name__ == "__main__":
    main()
