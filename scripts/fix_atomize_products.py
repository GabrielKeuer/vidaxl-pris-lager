"""Fix produkt-niveau-felter på atomize-produkter så de matcher create-flowet 1:1: descriptionHtml
(body_html fra variant 1's feed-HTML), tags (build_tags m. rum-tags), seo_title/description, productType
(feed-Category), vendor (Brand). Bruger de PRÆCISE product_utils-funktioner. productUpdate (kun produkt-
niveau, rører ikke varianter). --live for udførelse; ellers dry-run (viser de beregnede værdier)."""
import sys, os, io, zipfile, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, r"C:\Users\APC\dropxl-product-automation\scripts")
sys.stdout.reconfigure(encoding="utf-8")
import pandas as pd
import merge_executor as ME
import product_utils as PU

UPD = """mutation($i:ProductInput!){productUpdate(input:$i){product{id} userErrors{field message}}}"""

def load_feed_df():
    data = ME.get_feed_zip(os.environ["FEED_URL"])
    z = zipfile.ZipFile(io.BytesIO(data))
    name = [f for f in z.namelist() if f.endswith(".csv")][0]
    df = pd.read_csv(z.open(name), low_memory=False)
    df["SKU"] = df["SKU"].astype(str).str.replace(".0", "", regex=False).str.strip()
    return {r["SKU"]: r for _, r in df.iterrows()}

def load_rum():
    sb = ME.get_supabase_client()
    r = sb.table("hub_settings").select("value").eq("key", "vidaxl_rum_mapping").execute()
    rd = (r.data[0]["value"] if r.data else {}) or {}
    return rd if isinstance(rd, dict) else {}

def main():
    live = "--live" in sys.argv
    limit = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else 6
    feed = load_feed_df()
    rum = load_rum()
    print(f"feed {len(feed)} SKUs | rum_dict {len(rum)} mappings")
    spath = sys.argv[sys.argv.index("--specs") + 1] if "--specs" in sys.argv else "output/atomize_specs.json"
    specs = json.load(open(spath, encoding="utf-8"))
    done = fail = shown = 0
    for h, spec in specs.items():
        prods = spec["products"] if isinstance(spec, dict) else spec   # flagged_specs har {products,delete_handles}
        for p in prods:
            first_sku = p["variants"][0]["sku"]
            row = feed.get(first_sku)
            if row is None:
                continue
            # find live produkt-id via første SKU
            d = ME.gql('query($q:String!){productVariants(first:3,query:$q){edges{node{sku product{id descriptionHtml}}}}}', {"q": f"sku:{first_sku}"})
            edges = [e["node"] for e in (((d.get("data") or {}).get("productVariants") or {}).get("edges") or [])
                     if (e["node"]["sku"] or "").strip() == str(first_sku)]
            if not edges:
                continue
            pid = edges[0]["product"]["id"]
            had = bool((edges[0]["product"].get("descriptionHtml") or "").strip())
            body = PU.format_body_html(row.get("HTML_description", ""))
            tags = PU.build_tags(row, rum)
            tag_list = [t.strip() for t in tags.split(",") if t.strip()] if isinstance(tags, str) else list(tags)
            title = p["title"]
            seo_t = title[:70] if len(title) <= 70 else title[:67] + "..."
            seo_d = PU.generate_seo_description(body)
            cat = row.get("Category")
            ptype = str(cat).split(" > ")[-1].strip() if pd.notna(cat) else ""
            vendor = str(row.get("Brand") or "vidaXL")
            if shown < limit:
                print(f"\n● {title[:44]} (SKU {first_sku}) — havde body: {had}")
                print(f"   body_html: {len(body)} tegn | tags({len(tag_list)}): {tag_list[:6]}")
                print(f"   seo_title: {seo_t[:50]} | product_type: {ptype} | vendor: {vendor}")
                shown += 1
            if live:
                inp = {"id": pid, "descriptionHtml": body, "tags": tag_list,
                       "seo": {"title": seo_t, "description": seo_d[:320]},
                       "productType": ptype, "vendor": vendor}
                r = ME.gql(UPD, {"i": inp})
                errs = (((r.get("data") or {}).get("productUpdate") or {}).get("userErrors")) or []
                if errs:
                    print(f"   ❌ {first_sku}: {errs[:2]}"); fail += 1
                else:
                    done += 1
    print(f"\n=== {'LIVE' if live else 'DRY'}: {done} opdateret, {fail} fejl ===")

if __name__ == "__main__":
    main()
