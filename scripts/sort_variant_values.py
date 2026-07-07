"""Sortér variant-værdier natural (tal stigende, ord alfabetisk) ved at omordne varianterne i option-
positions-rækkefølge. Ændrer KUN variant-position (productVariantsBulkReorder) — rører ikke pris/lager/
metafelter/billeder/options. --live; --product <handle>; ellers alle aktive multi-variant vidaXL-produkter."""
import sys, os, re, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

REORDER = 'mutation($p:ID!,$pos:[ProductVariantPositionInput!]!){productVariantsBulkReorder(productId:$p,positions:$pos){userErrors{message}}}'

def nat(v):
    nums = re.findall(r"\d+\.?\d*", v or "")
    return (0, [float(n) for n in nums], (v or "").lower()) if nums else (1, [], (v or "").lower())

def sort_product(pid, dry, log=print):
    d = ME.gql('query($id:ID!){product(id:$id){options{position name} '
               'variants(first:250){edges{node{id selectedOptions{name value}}}}}}', {"id": pid})
    p = (d.get("data") or {}).get("product")
    if not p:
        return "ikke fundet"
    opts = sorted(p["options"], key=lambda o: o["position"])
    vs = [e["node"] for e in p["variants"]["edges"]]
    if len(vs) < 2:
        return "single"
    def vkey(v):
        so = {o["name"]: o["value"] for o in v["selectedOptions"]}
        return tuple(nat(so.get(o["name"], "")) for o in opts)
    svs = sorted(vs, key=vkey)
    if [v["id"] for v in svs] == [v["id"] for v in vs]:
        return "allerede sorteret"
    pos = [{"id": v["id"], "position": i + 1} for i, v in enumerate(svs)]
    if not dry:
        for i in range(0, len(pos), 250):
            r = ME.gql(REORDER, {"p": pid, "pos": pos[i:i + 250]})
            errs = (((r.get("data") or {}).get("productVariantsBulkReorder") or {}).get("userErrors")) or []
            if errs:
                return f"fejl: {errs[:1]}"
    return "sorteret"

def main():
    live = "--live" in sys.argv
    only = sys.argv[sys.argv.index("--product") + 1] if "--product" in sys.argv else None
    if only:
        d = ME.gql('query($h:String!){productByHandle(handle:$h){id}}', {"h": only})
        pid = (d.get("data") or {}).get("productByHandle", {}).get("id")
        print(f"{only}: {sort_product(pid, not live)}")
        return
    # alle aktive multi-variant vidaXL-produkter
    cursor = None; n = sorted_ = already = 0
    while True:
        q = 'query($c:String){products(first:100,after:$c,query:"vendor:vidaXL status:active"){pageInfo{hasNextPage endCursor} edges{node{id variantsCount{count}}}}}'
        d = ME.gql(q, {"c": cursor})
        pr = (d.get("data") or {}).get("products") or {}
        for e in pr.get("edges", []):
            node = e["node"]
            if (node.get("variantsCount") or {}).get("count", 0) < 2:
                continue
            n += 1
            r = sort_product(node["id"], not live)
            if r == "sorteret":
                sorted_ += 1
            elif r == "allerede sorteret":
                already += 1
            if n % 200 == 0:
                print(f"  …{n} produkter ({sorted_} sorteret)")
            time.sleep(0.05)
        if not pr.get("pageInfo", {}).get("hasNextPage"):
            break
        cursor = pr["pageInfo"]["endCursor"]
    print(f"=== {'LIVE' if live else 'DRY'}: {n} multi-variant produkter | {sorted_} sorteret, {already} allerede ===")

if __name__ == "__main__":
    main()
