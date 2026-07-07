"""Komplet backfill: scan ALLE SKUs fra atomize+flagged+collision-specs → find deres LIVE produkter
(inkl. split_dupes-oprettede singler der ikke er i specs) → sæt body_html/tags/seo/type/vendor hvor
descriptionHtml mangler. Bruger create-funktionerne + produktets FØRSTE variant som kilde. --live."""
import sys, os, io, zipfile, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, r"C:\Users\APC\dropxl-product-automation\scripts")
sys.stdout.reconfigure(encoding="utf-8")
import pandas as pd
import merge_executor as ME
import product_utils as PU

UPD = """mutation($i:ProductInput!){productUpdate(input:$i){userErrors{message}}}"""

def main():
    live = "--live" in sys.argv
    data = ME.get_feed_zip(os.environ["FEED_URL"])
    z = zipfile.ZipFile(io.BytesIO(data)); name = [f for f in z.namelist() if f.endswith(".csv")][0]
    df = pd.read_csv(z.open(name), low_memory=False)
    df["SKU"] = df["SKU"].astype(str).str.replace(".0", "", regex=False).str.strip()
    feed = {r["SKU"]: r for _, r in df.iterrows()}
    sb = ME.get_supabase_client()
    r = sb.table("hub_settings").select("value").eq("key", "vidaxl_rum_mapping").execute()
    rum = (r.data[0]["value"] if r.data else {}) or {}
    # saml alle SKUs
    skus = set()
    for f in ("output/atomize_specs.json", "output/flagged_specs.json", "output/collision_specs.json"):
        if not os.path.exists(f):
            continue
        d = json.load(open(f, encoding="utf-8"))
        for spec in d.values():
            prods = spec["products"] if isinstance(spec, dict) else spec
            for p in prods:
                for v in p["variants"]:
                    skus.add(v["sku"])
    print(f"scanner {len(skus)} SKUs for manglende body_html…")
    seen_pid = set(); fixed = already = 0
    for s in skus:
        d = ME.gql('query($q:String!){productVariants(first:3,query:$q){edges{node{sku product{id descriptionHtml '
                   'variants(first:1){edges{node{sku}}}}}}}}', {"q": f"sku:{s}"})
        edges = [e["node"] for e in (((d.get("data") or {}).get("productVariants") or {}).get("edges") or [])
                 if (e["node"]["sku"] or "").strip() == str(s)]
        if not edges:
            continue
        prod = edges[0]["product"]; pid = prod["id"]
        if pid in seen_pid:
            continue
        seen_pid.add(pid)
        if (prod.get("descriptionHtml") or "").strip():
            already += 1; continue
        first_sku = (prod["variants"]["edges"][0]["node"]["sku"] or "").strip()
        row = feed.get(first_sku) or feed.get(s)
        if row is None:
            continue
        body = PU.format_body_html(row.get("HTML_description", ""))
        tags = PU.build_tags(row, rum)
        tag_list = [t.strip() for t in tags.split(",") if t.strip()] if isinstance(tags, str) else list(tags)
        cat = row.get("Category")
        ptype = str(cat).split(" > ")[-1].strip() if pd.notna(cat) else ""
        if live:
            ME.gql(UPD, {"i": {"id": pid, "descriptionHtml": body, "tags": tag_list,
                               "seo": {"description": PU.generate_seo_description(body)[:320]},
                               "productType": ptype, "vendor": str(row.get("Brand") or "vidaXL")}})
        fixed += 1
    print(f"=== {'LIVE' if live else 'DRY'}: {fixed} produkter manglede body_html (nu fikset), {already} havde allerede ===")

if __name__ == "__main__":
    main()
