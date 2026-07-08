"""OPRYDNINGS-MOTOR (Flow A) — opret produkter FRISKT fra den predefinerede source of truth.
STRUKTUR (titel, options, rækkefølge) fra br_variant_feed — INGEN scrape, INGEN titel-generering.
INDHOLD (body_html, billeder, EAN, vægt, lager) frisk fra vidaXL-feed pr. SKU. PRIS fra hub-tiers.
Genbruger de beviste byggere (format_body_html, build_tags, get_all_images, resolve_variant_pricing,
metafelter). DRY-RUN default: bygger + validerer specs, opretter INTET. --keys <fil> for pilot-scope.
Output: output/cleanup_specs.json + konsol-rapport + flag."""
import sys, os, io, zipfile, json, argparse
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, r"C:\Users\APC\dropxl-product-automation\scripts")
sys.stdout.reconfigure(encoding="utf-8")
import pandas as pd
import merge_executor as ME
import product_utils as PU
import pricing as PR

def load_feed_df():
    z = zipfile.ZipFile(io.BytesIO(ME.get_feed_zip(os.environ["FEED_URL"])))
    name = [f for f in z.namelist() if f.endswith(".csv")][0]
    df = pd.read_csv(io.TextIOWrapper(z.open(name), encoding="utf-8"), dtype=str, low_memory=False)
    df["SKU"] = df["SKU"].astype(str).str.strip().str.replace(".0", "", regex=False)
    return df.set_index("SKU")

def build_spec(pkey, variants, feed, cfg, rum):
    """variants: liste af br_variant_feed-rækker (dicts) sorteret på variant_position. Returnér (spec, flags)."""
    flags = []
    live = [v for v in variants if v["sku"] in feed.index]
    missing = [v["sku"] for v in variants if v["sku"] not in feed.index]
    if missing:
        flags.append(f"udgået_i_feed:{len(missing)}")
    if not live:
        return None, flags + ["ingen_live_sku"]
    live.sort(key=lambda v: (v.get("variant_position") or 9999))
    first = live[0]
    frow = feed.loc[first["sku"]]
    if isinstance(frow, pd.DataFrame):        # dublet-SKU i feed → tag første
        frow = frow.iloc[0]
    # option-navne (Farve først) fra feedet — i den rækkefølge de er gemt (option1/2/3)
    opt_names = []
    for i in (1, 2, 3):
        nm = first.get(f"option{i}_name")
        if nm and nm not in opt_names:
            opt_names.append(nm)
    # PRODUKT-NIVEAU
    raw_html = str(frow.get("HTML_description") or "")
    body_html = PU.format_body_html(raw_html)
    title = first["product_title"]
    cat = str(frow.get("Category") or "")
    ptype = cat.split(">")[-1].strip() if cat else ""
    vendor = str(frow.get("Brand") or "vidaXL").strip() or "vidaXL"
    media = PU.get_all_images(frow)
    spec = {"product_key": pkey, "title": title, "handle_intended": None,
            "body_html": body_html, "vendor": vendor, "product_type": ptype,
            "tags": PU.build_tags(frow, rum), "status": "ACTIVE",
            "seo_title": title[:70], "seo_description": PU.generate_seo_description(body_html),
            "options_definition": opt_names, "media_urls": media, "variants": []}
    # VARIANT-NIVEAU
    for idx, v in enumerate(live):
        row = feed.loc[v["sku"]]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        # option-værdier for dette produkts akser
        ovals = []
        for i in (1, 2, 3):
            nm = v.get(f"option{i}_name"); val = v.get(f"option{i}_value")
            if nm in opt_names:
                if not val:
                    flags.append(f"tom_option:{v['sku']}")
                ovals.append((nm, val or ""))
        try:
            b2b = float(str(row.get("B2B price")).replace(",", "."))
        except Exception:
            b2b = None
            flags.append(f"ingen_b2b_pris:{v['sku']}")
        price, cap = (None, None)
        if b2b is not None:
            price, cap = PR.resolve_variant_pricing(b2b, cfg, seed=pkey, on_sale=False)
        all_img = PU.get_all_images(row)
        is_first = (idx == 0)
        mf = [{"namespace": "custom", "key": "sku", "type": "single_line_text_field", "value": v["sku"]}]
        if not is_first:
            if raw_html_v := str(row.get("HTML_description") or ""):
                mf.append({"namespace": "custom", "key": "produktinfo", "type": "multi_line_text_field", "value": raw_html_v})
            if all_img:
                mf.append({"namespace": "custom", "key": "variantbilleder", "type": "list.single_line_text_field", "value": json.dumps(all_img)})
        try:
            wt = int(float(str(row.get("Weight") or 0).replace(",", ".")) * 1000)
        except Exception:
            wt = 0
        spec["variants"].append({
            "sku": v["sku"], "price": int(price) if price else None,
            "compare_at_price": int(cap) if cap else None, "cost": b2b,
            "weight_grams": wt, "inventory_quantity": int(float(str(row.get("Stock") or 0) or 0)),
            "barcode": str(row.get("EAN") or ""), "option_values": ovals,
            "image_url": all_img[0] if all_img else None, "metafields": mf, "n_metafields": len(mf),
            "status_feed": v.get("status")})
    return spec, flags

def to_product_spec(CP, spec):
    variants = []
    for v in spec["variants"]:
        variants.append(CP.VariantSpec(
            sku=v["sku"], price=v["price"] or 0, cost=v["cost"] or 0,
            weight_grams=v["weight_grams"], inventory_quantity=v["inventory_quantity"],
            barcode=v["barcode"], compare_at_price=v["compare_at_price"],
            option_values=[tuple(ov) for ov in v["option_values"]],
            image_url=v["image_url"], metafields=v.get("metafields", []), google_mpn=v["sku"]))
    tags = [t for t in (spec.get("tags") or "").split(",") if t]
    return CP.ProductSpec(handle=None, title=spec["title"], body_html=spec["body_html"],
                          vendor=spec["vendor"], product_type=spec["product_type"], tags=tags, status="ACTIVE",
                          seo_title=spec["seo_title"], seo_description=spec["seo_description"],
                          options_definition=spec["options_definition"], media_urls=spec["media_urls"], variants=variants)

def old_products_for_skus(skus, exclude_id):
    """Find live-produkter (≠ det nye) der holder disse SKUs → {product_id: handle}."""
    found = {}
    for sku in skus:
        d = ME.gql('query($q:String!){productVariants(first:20,query:$q){edges{node{sku product{id handle}}}}}', {"q": f"sku:{sku}"})
        for e in (((d.get("data") or {}).get("productVariants") or {}).get("edges") or []):
            if (e["node"]["sku"] or "").strip() == str(sku):
                p = e["node"]["product"]
                if p["id"] != exclude_id:
                    found[p["id"]] = p["handle"]
    return found

def execute_live(specs, CP, location_id, sb, log):
    created = redirected = deleted = 0
    for spec in specs:
        skus = [v["sku"] for v in spec["variants"]]
        ps = to_product_spec(CP, spec)
        res = CP.call_product_set(ps, location_id)
        errs = (res or {}).get("userErrors") or []
        if errs or not (res or {}).get("product"):
            log(f"  ✗ {spec['product_key']}: {errs[:2] or 'intet produkt'}"); continue
        prod = res["product"]; new_id = prod["id"]; new_handle = prod["handle"]
        try:
            CP.publish_to_all_channels(new_id)
        except Exception as e:
            log(f"    (publish-advarsel: {e})")
        created += 1
        log(f"  ✓ oprettet {spec['product_key']} → {new_handle} ({len(skus)} var)")
        ME.del_self_redirect(f"/products/{new_handle}", False, log)
        olds = old_products_for_skus(skus, new_id)
        for oid, ohandle in olds.items():
            ME.create_redirect(f"/products/{ohandle}", f"/products/{new_handle}", False, log, sb)
            redirected += 1
            ME.delete_product(oid, ohandle, False, log)
            deleted += 1
    log(f"\n=== LIVE: {created} oprettet, {redirected} redirects, {deleted} gamle slettet ===")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys", default="output/pilot_product_keys.json")
    ap.add_argument("--show", type=int, default=3)
    ap.add_argument("--live", action="store_true", help="opret/redirect/slet LIVE (ellers dry-run)")
    ap.add_argument("--only", default="", help="komma-separerede product_keys (fx til pilot-test)")
    a = ap.parse_args()
    keys = json.load(open(a.keys, encoding="utf-8"))
    print(f"pilot product_keys: {len(keys)}")
    feed = load_feed_df()
    print(f"feed-rækker: {len(feed)}")
    sb = ME.get_supabase_client()
    cfg = PR.load_pricing_config(sb, vendor="vidaXL")
    rum = {}
    try:
        r = sb.table("hub_settings").select("value").eq("key", "vidaxl_rum_mapping").execute().data
        rum = (r[0]["value"] if r else {}) or {}
    except Exception:
        pass
    print(f"pricing-cfg: {'ok' if cfg else 'MANGLER'} | rum-mapping: {len(rum)} kategorier")
    # hent variant-feed-rækker for pilot-keys (paginér — .in_() har 1000-rækkers-grænse)
    bym = defaultdict(list)
    for i in range(0, len(keys), 100):
        batch = keys[i:i+100]; fr = 0
        while True:
            r = sb.table("br_variant_feed").select("*").in_("product_key", batch).order("sku").range(fr, fr+999).execute().data or []
            for x in r:
                bym[x["product_key"]].append(x)
            if len(r) < 1000:
                break
            fr += 1000
    specs = []; allflags = Counter(); skipped = []
    for pkey in keys:
        vs = bym.get(pkey)
        if not vs:
            skipped.append(pkey); continue
        spec, flags = build_spec(pkey, vs, feed, cfg, rum)
        for f in flags:
            allflags[f.split(":")[0]] += 1
        if spec is None:
            skipped.append(pkey); continue
        spec["_flags"] = flags
        specs.append(spec)
    json.dump(specs, open("output/cleanup_specs.json", "w", encoding="utf-8"), ensure_ascii=False)
    print(f"\n=== DRY-RUN: {len(specs)} produkter bygget, {len(skipped)} sprunget over ===")
    print("flag:", dict(allflags))
    # vis fulde eksempler
    for spec in specs[:a.show]:
        print(f"\n━━━━━━ {spec['product_key']} ━━━━━━")
        print(f"  titel: {spec['title']}")
        print(f"  type: {spec['product_type']} | vendor: {spec['vendor']} | tags: {spec['tags'][:60]}")
        print(f"  options: {spec['options_definition']} | media: {len(spec['media_urls'])} billeder")
        print(f"  seo_title: {spec['seo_title']}")
        print(f"  body_html: {spec['body_html'][:90].strip()}...")
        print(f"  varianter ({len(spec['variants'])}):")
        for v in spec["variants"][:6]:
            print(f"     {v['sku']}: {v['option_values']} | {v['price']} kr (før {v['compare_at_price']}) | lager {v['inventory_quantity']} | {v['n_metafields']} mf | img={'ja' if v['image_url'] else 'nej'}")
        if spec["_flags"]:
            print(f"  ⚠ flags: {spec['_flags'][:5]}")

    # ===== LIVE-UDFØRSEL =====
    if a.live:
        only = set(x.strip() for x in a.only.split(",") if x.strip())
        tolive = [s for s in specs if not only or s["product_key"] in only]
        if only:
            tolive = [s for s in tolive if s["product_key"] in only]
        import create_products_v2 as CP
        location_id = CP.get_primary_location_id()   # GID-format (gid://shopify/Location/...)
        print(f"\n>>> LIVE-UDFØRSEL på {len(tolive)} produkter (location {location_id}) <<<", flush=True)
        for s in tolive:
            print(f"   → {s['product_key']}: \"{s['title']}\" ({len(s['variants'])} var)")
        execute_live(tolive, CP, location_id, sb, lambda m: print(m, flush=True))

if __name__ == "__main__":
    main()
