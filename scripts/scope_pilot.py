"""Re-scope pilot: de 681 live-rørte produkter → deres SKUs → nuværende product_keys i br_variant_feed.
Output: output/pilot_product_keys.json."""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def main():
    tl = json.load(open("output/touched_live.json", encoding="utf-8"))
    created = tl["created"]
    skus = set()
    for p in created:
        d = ME.gql("query($id:ID!){product(id:$id){variants(first:100){edges{node{sku}}}}}", {"id": p["id"]})
        for e in (((d.get("data") or {}).get("product") or {}).get("variants") or {}).get("edges", []):
            s = (e["node"]["sku"] or "").strip()
            if s:
                skus.add(s)
    sb = ME.get_supabase_client()
    sl = list(skus); pkeys = set()
    for i in range(0, len(sl), 200):
        r = sb.table("br_variant_feed").select("product_key").in_("sku", sl[i:i+200]).execute().data or []
        for x in r:
            pkeys.add(x["product_key"])
    json.dump(sorted(pkeys), open("output/pilot_product_keys.json", "w"), ensure_ascii=False)
    print(f"pilot: {len(created)} rørte → {len(skus)} SKUs → {len(pkeys)} product_keys")

if __name__ == "__main__":
    main()
