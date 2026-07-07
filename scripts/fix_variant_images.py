"""Robust billede-fixer for atomize-produkter: for hver spec-SKU uden native variant-billede — hvis
feedet har billeder: tilføj media (hvis <250) + link til varianten, ELLER link til eksisterende media
(hvis produktet har ramt 250-grænsen). SKUs hvor feedet har 0 billeder = ægte vidaXL-hul (rapporteres)."""
import json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

CM = 'mutation($p:ID!,$m:[CreateMediaInput!]!){productCreateMedia(productId:$p,media:$m){media{id}userErrors{message}}}'
AM = 'mutation($p:ID!,$vm:[ProductVariantAppendMediaInput!]!){productVariantAppendMedia(productId:$p,variantMedia:$vm){userErrors{message}}}'

def main():
    enrich = ME.load_enrich(os.environ["FEED_URL"])
    specs = json.load(open("output/atomize_specs.json", encoding="utf-8"))
    all_skus = [v["sku"] for prods in specs.values() for p in prods for v in p["variants"]]
    fixed = linked = gap = already = 0
    for s in all_skus:
        d = ME.gql('query($q:String!){productVariants(first:3,query:$q){edges{node{sku id image{url} '
                   'product{id mediaCount{count} media(first:1){edges{node{id}}}}}}}}', {"q": f"sku:{s}"})
        edges = [e["node"] for e in (((d.get("data") or {}).get("productVariants") or {}).get("edges") or [])
                 if (e["node"]["sku"] or "").strip() == str(s)]
        if not edges:
            continue
        n = edges[0]
        if n.get("image"):
            already += 1; continue
        imgs = (enrich.get(s, {}) or {}).get("images") or []
        if not imgs:
            gap += 1; continue
        pid, vid = n["product"]["id"], n["id"]
        mcount = (n["product"].get("mediaCount") or {}).get("count", 0)
        if mcount < 249:
            r = ME.gql(CM, {"p": pid, "m": [{"originalSource": u, "mediaContentType": "IMAGE"} for u in imgs[:8]]})
            mids = [m["id"] for m in ((r.get("data", {}).get("productCreateMedia") or {}).get("media") or [])]
            if mids:
                ME.gql(AM, {"p": pid, "vm": [{"variantId": vid, "mediaIds": [mids[0]]}]})
                fixed += 1
        else:
            med = n["product"]["media"]["edges"]
            if med:
                ME.gql(AM, {"p": pid, "vm": [{"variantId": vid, "mediaIds": [med[0]["node"]["id"]]}]})
                linked += 1
        time.sleep(0.2)
    print(f"=== billede-fix: {fixed} tilføjet+linket, {linked} linket-til-eksisterende, {already} havde allerede, {gap} ægte vidaXL-hul (ingen feed-billede) ===")

if __name__ == "__main__":
    main()
