"""BEVÆGELSES-OPTÆLLING: sammenlign NY gruppering mod NUVÆRENDE live-gruppering, pr. SKU.
combined = SKU rykker til et STØRRE produkt (fragmenteret → samlet). separated = SKU rykker til et
MINDRE produkt (samlet → adskilt). Rapporterer også de 171 live-rørte masters (ny vs de 906). READ-ONLY."""
import sys, os, json, re
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, __import__("os").environ.get("DROPXL_SCRIPTS", r"C:\Users\APC\dropxl-product-automation\scripts"))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import build_complete_feed as B
import scope_split as SS
import cleanup_engine as CE
import fix_live as FL

def main():
    feed = CE.load_feed_df()
    titles = feed["Title"].to_dict()
    SS.setup_universe(list(feed.index))
    for w in ("cremehvid", "cremehvide", "råhvid", "gråhvid", "offwhite", "sølvgrå", "koksgrå"):
        B.COLOR_UNIVERSE.add(w)
    B.build_color_re()
    lbl = json.load(open("output/axis_labels.json", encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}
    sb = ME.get_supabase_client()
    bym = defaultdict(list); fr = 0
    while True:
        b = sb.table("vidaxl_sku_master").select("sku,master_pid").range(fr, fr + 999).execute().data or []
        for x in b:
            bym[x["master_pid"]].append(str(x["sku"]).strip())
        if len(b) < 1000:
            break
        fr += 1000
    print(f"master_pids: {len(bym)}")

    # NY gruppering: sku → ny produkt-størrelse
    sku_new = {}
    n180 = set(json.load(open("output/pilot_check.json", encoding="utf-8"))["affected"])
    new180 = 0
    for mid, skus in bym.items():
        live = [s for s in skus if s in feed.index]
        if not live:
            continue
        opts = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        prods = FL.regroup_master(mid, live, opts, titles, {mid: lbl.get(mid, {})})
        if mid in n180:
            new180 += len(prods)
        for p in prods:
            for s in p["skus"]:
                sku_new[s] = len(p["skus"])
    print(f"nye produkter (171 live-rørte masters): {new180}  (var 906 med gammel logik)")

    # NUVÆRENDE live: sku → nuværende produkt-størrelse
    Q = 'query($a:String){products(first:100,query:"vendor:vidaXL",after:$a){pageInfo{hasNextPage endCursor} edges{node{variantsCount{count} variants(first:100){edges{node{sku}}}}}}}'
    after = None; sku_cur = {}; pages = 0
    while True:
        d = ME.gql(Q, {"a": after}); pr = (d.get("data") or {}).get("products") or {}
        for e in pr.get("edges", []):
            n = e["node"]; cnt = (n.get("variantsCount") or {}).get("count") or 1
            for v in n["variants"]["edges"]:
                s = (v["node"]["sku"] or "").strip()
                if s:
                    sku_cur[s] = cnt
        pages += 1
        if pages % 30 == 0:
            print(f"  …{len(sku_cur)} live-SKUs", flush=True)
        if pr.get("pageInfo", {}).get("hasNextPage"):
            after = pr["pageInfo"]["endCursor"]
        else:
            break
    print(f"live-SKUs: {len(sku_cur)} | ny-gruppering-SKUs: {len(sku_new)}")

    both = [s for s in sku_new if s in sku_cur]
    combined = sum(1 for s in both if sku_new[s] > sku_cur[s])
    separated = sum(1 for s in both if sku_new[s] < sku_cur[s])
    same = sum(1 for s in both if sku_new[s] == sku_cur[s])
    print(f"\n=== BEVÆGELSE (pr. SKU, {len(both)} SKUs i begge) ===")
    print(f"  SAMLES (fragmenteret → større produkt): {combined}  ({round(combined/len(both)*100)}%)")
    print(f"  ADSKILLES (samlet → mindre produkt):    {separated}  ({round(separated/len(both)*100)}%)")
    print(f"  UÆNDRET (samme størrelse):              {same}  ({round(same/len(both)*100)}%)")
    only_new = [s for s in sku_new if s not in sku_cur]
    only_cur = [s for s in sku_cur if s not in sku_new]
    print(f"  kun i ny (ikke live endnu): {len(only_new)} | kun live (ikke i ny/feed): {len(only_cur)}")
    json.dump({"combined": combined, "separated": separated, "same": same,
               "new180_products": new180, "only_new": len(only_new), "only_cur": len(only_cur)},
              open("output/move_count.json", "w", encoding="utf-8"), ensure_ascii=False)

if __name__ == "__main__":
    main()
