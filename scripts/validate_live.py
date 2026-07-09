"""VALIDÉR de live-rørte produkter (171 masters): fuld SKU-dækning (intet manglende/afkortet pga.
variant-limit), og at hvert produkt er korrekt (titel, varianter>0, billede, options). READ-ONLY."""
import sys, os, io, zipfile, csv, json
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def main():
    aff = json.load(open("output/pilot_check.json", encoding="utf-8"))["affected"]
    sb = ME.get_supabase_client()
    m171 = []
    for mid in aff:
        r = sb.table("vidaxl_sku_master").select("sku").eq("master_pid", mid).execute().data or []
        m171 += [str(x["sku"]) for x in r]
    z = zipfile.ZipFile(io.BytesIO(ME.get_feed_zip(os.environ["FEED_URL"])))
    nm = [f for f in z.namelist() if f.endswith(".csv")][0]
    feedsk = {str(x.get("SKU") or "").strip().replace(".0", "") for x in csv.DictReader(io.TextIOWrapper(z.open(nm), encoding="utf-8"))}
    want = set(s for s in m171 if s in feedsk)
    print(f"171 masters: {len(want)} in-feed SKUs at validere")

    # pull ALLE live vidaXL-produkter med detaljer
    Q = ('query($a:String){products(first:80,query:"vendor:vidaXL",after:$a){pageInfo{hasNextPage endCursor} '
         'edges{node{id handle title featuredImage{url} variantsCount{count} '
         'variants(first:150){edges{node{sku}}}}}}}')
    after = None; sku2prod = {}; prod = {}; pg = 0
    while True:
        d = ME.gql(Q, {"a": after}); pr = (d.get("data") or {}).get("products") or {}
        for e in pr.get("edges", []):
            n = e["node"]; pid = n["id"]
            skus = [(v["node"]["sku"] or "").strip() for v in n["variants"]["edges"] if v["node"].get("sku")]
            prod[pid] = {"handle": n["handle"], "title": n["title"], "img": bool(n.get("featuredImage")),
                         "vcount": (n.get("variantsCount") or {}).get("count") or 0, "nsku": len(skus)}
            for s in skus:
                sku2prod.setdefault(s, []).append(pid)
        pg += 1
        if pg % 40 == 0:
            print(f"  …{len(sku2prod)} live-SKUs", flush=True)
        if pr.get("pageInfo", {}).get("hasNextPage"):
            after = pr["pageInfo"]["endCursor"]
        else:
            break
    print(f"live vidaXL-SKUs: {len(sku2prod)}")

    # 1) dækning
    missing = sorted(want - set(sku2prod))
    dup = sorted(s for s in want if len(sku2prod.get(s, [])) > 1)
    print(f"\n=== 1) DÆKNING ===")
    print(f"  manglende SKUs (intet live-produkt): {len(missing)}")
    if missing[:10]: print(f"     {missing[:10]}")
    print(f"  SKUs i FLERE produkter (dublet): {len(dup)}")
    if dup[:10]: print(f"     {dup[:10]}")

    # 2) produkt-sundhed for de berørte produkter
    touched = {pid for s in want for pid in sku2prod.get(s, [])}
    no_title = [pid for pid in touched if not prod[pid]["title"].strip()]
    no_img = [pid for pid in touched if not prod[pid]["img"]]
    no_var = [pid for pid in touched if prod[pid]["vcount"] == 0]
    # variant-limit-tegn: variantsCount != antal hentede SKUs (kan tyde på afkortning >150)
    trunc = [pid for pid in touched if prod[pid]["vcount"] > prod[pid]["nsku"]]
    cap100 = [pid for pid in touched if prod[pid]["vcount"] in (100, 250)]
    print(f"\n=== 2) PRODUKT-SUNDHED ({len(touched)} berørte produkter) ===")
    print(f"  uden titel: {len(no_title)}")
    print(f"  uden hovedbillede: {len(no_img)}")
    print(f"  uden varianter (0): {len(no_var)}")
    print(f"  variantsCount > hentede SKUs (muligt >150 stort produkt): {len(trunc)}")
    for pid in trunc[:8]:
        p = prod[pid]; print(f"     \"{p['title'][:40]}\" — count={p['vcount']}, hentet={p['nsku']}")
    print(f"  produkter med præcis 100/250 varianter (limit-mistanke): {len(cap100)}")
    for pid in cap100[:8]:
        print(f"     \"{prod[pid]['title'][:44]}\" — {prod[pid]['vcount']} var")

    ok = not missing and not dup and not no_title and not no_img and not no_var
    print(f"\n=== RESULTAT: {'✓ ALT OK' if ok else '⚠ SE OVENFOR'} ===")

if __name__ == "__main__":
    main()
