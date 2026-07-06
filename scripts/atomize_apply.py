"""ATOMIZE/SPLIT-APPLY: opret de korrekte produkter (fra output/atomize_specs.json = LLM-struktur) med
fuld opskrift (pris/cost/lager/metafelter/billeder) via productSet → slet det oprindelige keeper-produkt
→ redirect keeper-URL til primær-produkt. DRY-RUN default; --live; --keeper <handle>; --n N."""
import json, os, re, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import pricing
from pricing import resolve_variant_pricing

PS = """mutation($input:ProductSetInput!,$sync:Boolean){productSet(input:$input,synchronous:$sync){
  product{id handle} userErrors{field message}}}"""
PUB = """mutation($id:ID!,$input:[PublicationInput!]!){publishablePublish(id:$id,input:$input){userErrors{message}}}"""

def gen_handle(title, used):
    h = title.lower().replace("æ", "ae").replace("ø", "oe").replace("å", "aa")
    h = re.sub(r"[^a-z0-9]+", "-", h).strip("-")[:80] or "produkt"
    base, n = h, 2
    while h in used:
        h = f"{base}-{n}"; n += 1
    used.add(h)
    return h

def _pubs():
    d = ME.gql("{publications(first:20){edges{node{id}}}}")
    return [e["node"]["id"] for e in (((d.get("data") or {}).get("publications") or {}).get("edges") or [])]

def variant_input(sku, opts, is_first, feed, enrich, cfg, seed, loc):
    b2b, stock = feed.get(sku, (0, 0))
    price, cap = resolve_variant_pricing(b2b, cfg, seed=seed, on_sale=True) if b2b > 0 else (0, None)
    e = enrich.get(sku, {})
    mf = [{"namespace": "custom", "key": "sku", "type": "single_line_text_field", "value": sku}]
    if not is_first:
        if e.get("html"):
            mf.append({"namespace": "custom", "key": "produktinfo", "type": "multi_line_text_field", "value": e["html"]})
        if e.get("images"):
            mf.append({"namespace": "custom", "key": "variantbilleder", "type": "list.single_line_text_field", "value": json.dumps(e["images"])})
    ov = [{"optionName": a, "name": v} for a, v in opts.items()] or [{"optionName": "Title", "name": "Default Title"}]
    vi = {"optionValues": ov, "price": str(int(price)), "sku": sku,
          "inventoryItem": {"cost": str(b2b), "tracked": True, "requiresShipping": True,
                            "measurement": {"weight": {"value": (e.get("weight") or 0) / 1000.0, "unit": "KILOGRAMS"}}},
          "inventoryPolicy": "DENY",
          "inventoryQuantities": [{"locationId": loc, "name": "available", "quantity": stock}],
          "metafields": mf, "taxable": True}
    if cap:
        vi["compareAtPrice"] = str(int(cap))
    if e.get("ean"):
        vi["barcode"] = e["ean"]
    img = (e.get("images") or [None])[0]
    if img:
        vi["file"] = {"originalSource": img, "contentType": "IMAGE"}
    return vi

def build_input(spec, feed, enrich, cfg, loc, handle, ptype):
    variants = spec["variants"]
    axes = []
    if any(v.get("Farve") for v in variants): axes.append("Farve")            # Farve = option 1
    if any(v.get("Konfiguration") for v in variants): axes.append("Konfiguration")
    optvals = defaultdict(list)
    for v in variants:
        for a in axes:
            if v.get(a) and v[a] not in optvals[a]:
                optvals[a].append(v[a])
    product_options = [{"name": a, "values": [{"name": x} for x in optvals[a]]} for a in axes] \
        or [{"name": "Title", "values": [{"name": "Default Title"}]}]
    vin = [variant_input(v["sku"], {a: v[a] for a in axes if v.get(a)}, i == 0, feed, enrich, cfg, spec["title"], loc)
           for i, v in enumerate(variants)]
    files, seen = [], set()
    for v in variants:
        for u in (enrich.get(v["sku"], {}).get("images") or []):
            if u not in seen:
                seen.add(u); files.append({"originalSource": u, "contentType": "IMAGE"})
    files = files[:245]
    kept = {f["originalSource"] for f in files}
    for vi in vin:
        if vi.get("file") and vi["file"]["originalSource"] not in kept:
            del vi["file"]
    return {"title": spec["title"], "handle": handle, "status": "ACTIVE", "productType": ptype or "",
            "vendor": "vidaXL", "productOptions": product_options, "variants": vin, "files": files}

def main():
    live = "--live" in sys.argv
    only = sys.argv[sys.argv.index("--keeper") + 1] if "--keeper" in sys.argv else None
    n = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else None
    specs = json.load(open("output/atomize_specs.json", encoding="utf-8"))
    sb = ME.get_supabase_client()
    cfg = pricing.load_pricing_config(sb, vendor="vidaXL")
    feed = ME.load_feed() if live else ME._MockFeed() if hasattr(ME, "_MockFeed") else {}
    enrich, loc, pubs = {}, None, []
    if live:
        enrich = ME.load_enrich(os.environ["FEED_URL"])
        loc = ME.gql('{locations(first:1,query:"status:active"){edges{node{id}}}}')["data"]["locations"]["edges"][0]["node"]["id"]
        pubs = _pubs()
    used_handles = set()
    handles = [only] if only else (list(specs)[:n] if n else list(specs))
    tot_p = 0
    for h in handles:
        prods = specs.get(h) or []
        d = ME.gql("query($h:String!){productByHandle(handle:$h){id productType}}", {"h": h})
        pr = (d.get("data") or {}).get("productByHandle")
        if not pr:
            print(f"  ⚠ {h}: ikke fundet"); continue
        ptype = pr.get("productType")
        print(f"\n▶ {h} → {len(prods)} produkter" + ("" if live else " (DRY)"))
        if live:
            ME.delete_product(pr["id"], h, False, print)          # slet original (frigør SKUs)
        primary = None
        for i, spec in enumerate(prods):
            handle = gen_handle(spec["title"], used_handles)
            if i == 0:
                primary = handle
            tot_p += 1
            if not live:
                axes = [a for a in ("Farve", "Konfiguration") if any(v.get(a) for v in spec["variants"])]
                print(f"     \"{spec['title'][:50]}\" | {len(spec['variants'])} var | akser={axes or 'single'} | handle={handle}")
                continue
            inp = build_input(spec, feed, enrich, cfg, loc, handle, ptype)
            r = ME.gql(PS, {"input": inp, "sync": True})
            errs = (((r.get("data") or {}).get("productSet") or {}).get("userErrors")) or []
            if errs:
                print(f"     ❌ {handle}: {errs[:2]}"); continue
            newid = r["data"]["productSet"]["product"]["id"]
            if pubs:
                ME.gql(PUB, {"id": newid, "input": [{"publicationId": p} for p in pubs]})
            print(f"     ✓ {handle} ({len(spec['variants'])} var)")
        if live and primary:
            ME.create_redirect(f"/products/{h}", f"/products/{primary}", False, print, sb)
    print(f"\n=== {'LIVE' if live else 'DRY-RUN'}: {len(handles)} keepers → {tot_p} produkter ===")

if __name__ == "__main__":
    main()
