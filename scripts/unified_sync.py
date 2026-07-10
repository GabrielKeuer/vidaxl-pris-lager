"""SAMLET CREATE+MERGE (Fase 1: driver + change-detektion + klassificering, DRY-RUN).
Én grupperings-logik (regroup) over HELE feedet. Pr. mål-gruppe klassificeres:
  CREATE   = ingen live-produkter holder gruppens SKU + opfylder create-filtre → opret nyt
  MERGE    = live-produkt findes men gruppen er ændret (ny SKU, spredt, eller titel afviger) → opdatér in-place
  UNCHANGED= live-produkt matcher gruppen præcist → rør ikke
  SKIP     = ingen live + opfylder ikke create-filtre (lav lager / ingen pris)
  PARK     = gruppens SKU spredt så merge ville kræve SPLIT (fremmede SKU) → parkeret (som combine)
Dette er KLASSIFICERINGEN (byggeplanens Fase 1-2). Selve apply (process_group live) bygges + testes bagefter.
Kør: python scripts/unified_sync.py [--refresh] [--limit N] [--only MID]. Intet live.
Se UNIFIED_CREATE_MERGE_PLAN.md."""
import sys, os, json, argparse
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, __import__("os").environ.get("DROPXL_SCRIPTS", r"C:\Users\APC\dropxl-product-automation\scripts"))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import build_complete_feed as B
import scope_split as SS
import fix_live as FL
import cleanup_engine as CE

MIN_STOCK_PRIMARY = 20   # samme tærskel som daily_create for at OPRETTE et nyt produkt

def load_live_snapshot(force=False):
    """Alle live vidaXL-produkter (id,handle,title,skus) → cache output/live_vidaxl.json. Genbrug fra recent_fix."""
    cache = "output/live_vidaxl.json"
    if not force and os.path.exists(cache):
        d = json.load(open(cache, encoding="utf-8"))
        for p in d: p["skuset"] = set(p.get("skus", []))
        print(f"live-snapshot (cache): {len(d)} produkter (brug --refresh for frisk)")
        return d
    Q = ('query($a:String){products(first:80,query:"vendor:vidaXL",after:$a){pageInfo{hasNextPage endCursor} '
         'edges{node{id handle title createdAt variantsCount{count} variants(first:250){edges{node{sku}}}}}}}')
    after = None; out = []; pg = 0; big = []
    while True:
        d = ME.gql(Q, {"a": after}); pr = (d.get("data") or {}).get("products") or {}
        for e in pr.get("edges", []):
            n = e["node"]
            n["skus"] = [v["node"]["sku"] for v in n["variants"]["edges"] if v["node"].get("sku")]
            if (n.get("variantsCount") or {}).get("count", 0) > 250:
                big.append(n["id"])   # >250 varianter → capped, hentes fuldt bagefter
            del n["variants"]; out.append(n)
        pg += 1
        if pg % 40 == 0: print(f"  …{len(out)} live", flush=True)
        if pr.get("pageInfo", {}).get("hasNextPage"): after = pr["pageInfo"]["endCursor"]
        else: break
    # >250-varianter: paginér fuldt så snapshot IKKE misser SKU >250 (ellers fejl-klassificering)
    bymap = {p["id"]: p for p in out}
    for pid in big:
        allsk = []; af = None
        while True:
            d = ME.gql('query($id:ID!,$a:String){product(id:$id){variants(first:250,after:$a){'
                       'pageInfo{hasNextPage endCursor} edges{node{sku}}}}}', {"id": pid, "a": af})
            pv = (d.get("data") or {}).get("product", {}).get("variants", {})
            allsk += [x["node"]["sku"] for x in pv.get("edges", []) if x["node"].get("sku")]
            if pv.get("pageInfo", {}).get("hasNextPage"): af = pv["pageInfo"]["endCursor"]
            else: break
        bymap[pid]["skus"] = allsk
    if big: print(f"  (paginerede {len(big)} store produkter >250 var fuldt)")
    json.dump(out, open(cache, "w", encoding="utf-8"), ensure_ascii=False)
    for p in out: p["skuset"] = set(p["skus"])
    print(f"live-snapshot (frisk): {len(out)} produkter → {cache}")
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true", help="frisk live-snapshot (ellers cache)")
    ap.add_argument("--limit", type=int, default=0, help="kun første N masters (test)")
    ap.add_argument("--only", default="", help="kun én master_pid")
    a = ap.parse_args()

    # feed + universer
    feed = CE.load_feed_df(); titles = feed["Title"].to_dict()
    stock = {}
    if "Stock" in feed.columns:
        for s, v in feed["Stock"].items():
            try: stock[s] = int(float(v))
            except (ValueError, TypeError): stock[s] = 0
    SS.setup_universe(list(feed.index))
    for w in ("cremehvid", "cremehvide", "råhvid", "gråhvid", "offwhite", "sølvgrå", "koksgrå"):
        B.COLOR_UNIVERSE.add(w)
    B.build_color_re()
    lbl = json.load(open("output/axis_labels.json", encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}

    # live-snapshot → sku2pid + pid→product
    snap = load_live_snapshot(force=a.refresh)
    prod_by_id = {p["id"]: p for p in snap}
    sku2pid = {}
    for p in snap:
        for s in p["skuset"]: sku2pid[s] = p["id"]

    # alle masters → SKU
    sb = ME.get_supabase_client()
    bym = defaultdict(list); fr = 0
    while True:
        b = sb.table("vidaxl_sku_master").select("sku,master_pid").range(fr, fr + 999).execute().data or []
        for x in b: bym[x["master_pid"]].append(str(x["sku"]).strip())
        if len(b) < 1000: break
        fr += 1000
    masters = [a.only] if a.only else sorted(bym)
    if a.limit: masters = masters[:a.limit]
    print(f"masters: {len(masters)} | feed-SKU: {len(feed.index)} | live-SKU i Shopify: {len(sku2pid)}")

    cats = Counter(); examples = defaultdict(list); create_variants = 0; merge_variants = 0
    for mid in masters:
        live = [s for s in bym.get(mid, []) if s in feed.index]
        if not live:
            continue
        opts = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        for g in FL.regroup_master(mid, live, opts, titles, {mid: lbl.get(mid, {})}):
            gskus = set(g["skus"])
            live_pids = {sku2pid[s] for s in gskus if s in sku2pid}
            covered = {s for s in gskus if s in sku2pid}
            if not live_pids:
                # intet i Shopify → CREATE hvis mindst ét SKU opfylder primær-tærskel
                if any(stock.get(s, 0) >= MIN_STOCK_PRIMARY for s in gskus):
                    cats["CREATE"] += 1; create_variants += len(gskus)
                    if len(examples["CREATE"]) < 8: examples["CREATE"].append((mid, g["title"], len(gskus)))
                else:
                    cats["SKIP_lavt_lager"] += 1
                continue
            # der findes live-produkt(er) for gruppen
            if len(live_pids) == 1:
                pid = next(iter(live_pids))
                prod_skus = prod_by_id[pid]["skuset"]
                foreign = prod_skus - gskus
                if foreign:
                    # produktet holder også SKU der IKKE er i gruppen → merge ville kræve split → PARK
                    cats["PARK_split"] += 1
                    if len(examples["PARK_split"]) < 6: examples["PARK_split"].append((mid, g["title"], len(gskus), len(foreign)))
                elif covered == gskus and prod_skus == gskus:
                    cats["UNCHANGED"] += 1
                else:
                    # samme produkt, men gruppen har nye SKU (ny variant) → MERGE
                    cats["MERGE_nyvariant"] += 1; merge_variants += len(gskus - covered)
                    if len(examples["MERGE_nyvariant"]) < 8:
                        examples["MERGE_nyvariant"].append((mid, g["title"], len(gskus), len(gskus - covered)))
            else:
                # gruppens SKU spredt på FLERE produkter → konsolidér (combine-agtigt merge)
                allprod = set()
                for pid in live_pids: allprod |= prod_by_id[pid]["skuset"]
                if allprod - gskus:
                    cats["PARK_split"] += 1
                    if len(examples["PARK_split"]) < 6: examples["PARK_split"].append((mid, g["title"], len(gskus), len(allprod - gskus)))
                else:
                    cats["MERGE_konsolidér"] += 1; merge_variants += len(gskus - covered)
                    if len(examples["MERGE_konsolidér"]) < 8:
                        examples["MERGE_konsolidér"].append((mid, g["title"], len(gskus), len(live_pids)))

    print(f"\n=== UNIFIED DRY-RUN KLASSIFICERING ===")
    tot = sum(cats.values())
    for k in ("CREATE", "MERGE_nyvariant", "MERGE_konsolidér", "UNCHANGED", "PARK_split", "SKIP_lavt_lager"):
        print(f"  {k:20s} {cats.get(k, 0)}")
    print(f"  {'I ALT grupper':20s} {tot}")
    print(f"\n  nye variant-oprettelser (CREATE): ~{create_variants} | tilføjede varianter (MERGE): ~{merge_variants}")
    for k in ("CREATE", "MERGE_nyvariant", "MERGE_konsolidér", "PARK_split"):
        if examples.get(k):
            print(f"\n  [{k}] eksempler:")
            for ex in examples[k][:6]:
                print(f"     {ex}")
    json.dump({"cats": dict(cats), "create_variants": create_variants, "merge_variants": merge_variants,
               "examples": {k: v for k, v in examples.items()}},
              open("output/unified_dryrun.json", "w", encoding="utf-8"), ensure_ascii=False)
    print(f"\n  → output/unified_dryrun.json")

if __name__ == "__main__":
    main()
