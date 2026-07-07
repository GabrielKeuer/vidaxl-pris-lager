"""Eksekvér de 61 flaggede grupper (output/flagged_specs.json): slet keeper + ALLE donorer → opret de
korrekte produkter (fuld opskrift via atomize_apply's build_input) → redirect alle slettede handles til
primær. Idempotent. --live for udførelse. Herefter køres fix_atomize_products + fix_atomize_media."""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import pricing
import atomize_apply as AA

def main():
    live = "--live" in sys.argv
    n = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else None
    specs = json.load(open("output/flagged_specs.json", encoding="utf-8"))
    sb = ME.get_supabase_client()
    cfg = pricing.load_pricing_config(sb, vendor="vidaXL")
    feed, enrich, loc, pubs = {}, {}, None, []
    if live:
        feed = ME.load_feed()
        enrich = ME.load_enrich(os.environ["FEED_URL"])
        loc = ME.gql('{locations(first:1,query:"status:active"){edges{node{id}}}}')["data"]["locations"]["edges"][0]["node"]["id"]
        pubs = AA._pubs()
    DELF = "output/deleted_flagged.json"
    deleted = set(json.load(open(DELF, encoding="utf-8")) if os.path.exists(DELF) else [])
    items = list(specs.items())[:n] if n else list(specs.items())
    tot_p = 0
    for keeper, spec in items:
        prods = spec["products"]; del_handles = spec["delete_handles"]
        print(f"\n▶ {keeper} → {len(prods)} produkter (slet {len(del_handles)})" + ("" if live else " (DRY)"))
        try:
            ptype = ""
            # slet keeper + donorer (kun hvis ikke allerede slettet)
            for h in del_handles:
                d = ME.gql("query($h:String!){productByHandle(handle:$h){id productType}}", {"h": h})
                pr = (d.get("data") or {}).get("productByHandle")
                if pr and not ptype:
                    ptype = pr.get("productType") or ""
                if live and pr and h not in deleted:
                    ME.delete_product(pr["id"], h, False, print)
                    deleted.add(h); json.dump(sorted(deleted), open(DELF, "w", encoding="utf-8"))
            primary = None
            expanded = [s for p in prods for s in AA.split_dupes(p)]
            for i, sp in enumerate(expanded):
                tot_p += 1
                if not live:
                    axes = [a for a in ("Farve", "Konfiguration") if any(v.get(a) for v in sp["variants"])]
                    print(f"     \"{sp['title'][:46]}\" ({len(sp['variants'])}var {axes or 'single'})")
                    continue
                ex = AA.live_product_for_sku(sp["variants"][0]["sku"])
                if ex and ex[0] == sp["title"]:
                    primary = primary or ex[1]
                    print(f"     ⏭ {ex[1]} findes"); continue
                inp = AA.build_input(sp, feed, enrich, cfg, loc, ptype)
                r = ME.gql(AA.PS, {"input": inp, "sync": True})
                errs = (((r.get("data") or {}).get("productSet") or {}).get("userErrors")) or []
                if errs:
                    print(f"     ❌ {sp['title'][:34]}: {errs[:1]}"); continue
                newid = r["data"]["productSet"]["product"]["id"]
                actual = r["data"]["productSet"]["product"]["handle"]
                primary = primary or actual
                if pubs:
                    ME.gql(AA.PUB, {"id": newid, "input": [{"publicationId": p} for p in pubs]})
                print(f"     ✓ {actual} ({len(sp['variants'])}var)")
            if live and primary:
                for h in del_handles:
                    try:
                        ME.del_self_redirect(f"/products/{primary}", False, print)
                        ME.create_redirect(f"/products/{h}", f"/products/{primary}", False, print, sb)
                    except Exception as e:
                        print(f"    ⚠ redirect {h[:24]} sprunget: {e}")
        except Exception as e:
            print(f"  ❌ {keeper}: FEJL — {e}")
            continue
    print(f"\n=== {'LIVE' if live else 'DRY'}: {len(items)} grupper → {tot_p} produkter ===")

if __name__ == "__main__":
    main()
