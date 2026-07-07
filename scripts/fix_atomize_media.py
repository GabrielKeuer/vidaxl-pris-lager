"""Ret produkt-media på multi-variant atomize-produkter så de matcher create-modellen: produkt-media =
variant 1's fulde galleri + hver variants native billede (IKKE alle varianters fulde gallerier).
Matcher feed↔shopify via filnavn-stem. Beholder ALTID alle variant-natives (fra Shopify). Sletter resten.
--live for udførelse; ellers dry-run (viser hvad der slettes)."""
import sys, os, io, json, re, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

DEL = 'mutation($p:ID!,$m:[ID!]!){productDeleteMedia(productId:$p,mediaIds:$m){deletedMediaIds userErrors{message}}}'

def stem(u):
    return re.sub(r"\.[a-z]+$", "", (u or "").split("/")[-1].split("?")[0]).lower()

def main():
    live = "--live" in sys.argv
    limit = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else None
    enrich = ME.load_enrich(os.environ["FEED_URL"])
    specs = json.load(open("output/atomize_specs.json", encoding="utf-8"))
    multi = [(h, p) for h, prods in specs.items() for p in prods if len(p["variants"]) > 1]
    if limit:
        multi = multi[:limit]
    print(f"{len(multi)} multi-variant produkter")
    tot_del = done = 0
    for h, p in multi:
        v1 = p["variants"][0]["sku"]
        d = ME.gql('query($q:String!){productVariants(first:1,query:$q){edges{node{product{id title '
                   'media(first:250){edges{node{id ... on MediaImage{image{url}}}}} '
                   'variants(first:250){edges{node{image{url}}}}}}}}}', {"q": f"sku:{v1}"})
        edges = d.get("data", {}).get("productVariants", {}).get("edges") or []
        if not edges:
            continue
        prod = edges[0]["node"]["product"]
        pid = prod["id"]
        # KEEP = variant 1's feed-galleri (stems) + alle variant-natives (fra Shopify)
        keep = {stem(u) for u in (enrich.get(v1, {}) or {}).get("images", [])}
        for ve in prod["variants"]["edges"]:
            img = ve["node"].get("image")
            if img and img.get("url"):
                keep.add(stem(img["url"]))
        media = [(m["node"]["id"], (m["node"].get("image") or {}).get("url")) for m in prod["media"]["edges"]]
        todelete = [mid for mid, url in media if url and stem(url) not in keep]
        if todelete:
            tot_del += len(todelete)
            if live:
                for i in range(0, len(todelete), 100):
                    r = ME.gql(DEL, {"p": pid, "m": todelete[i:i + 100]})
                    errs = (((r.get("data") or {}).get("productDeleteMedia") or {}).get("userErrors")) or []
                    if errs:
                        print(f"   ⚠ {prod['title'][:30]}: {errs[:1]}")
                done += 1
                time.sleep(0.2)
            else:
                print(f"  \"{prod['title'][:34]}\": {len(media)} media → beholder {len(media)-len(todelete)}, sletter {len(todelete)}")
    print(f"\n=== {'LIVE' if live else 'DRY'}: {tot_del} media slettet fra {done if live else len(multi)} produkter ===")

if __name__ == "__main__":
    main()
