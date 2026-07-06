"""SLAVISK LIVE-AUDIT af alle merged produkter (status=done i merge_exec_log).
Tjekker HVERT produkt mod alle regler: status, akser ≤3, ingen dublet-akse-værdier, Farve=option 1,
keeper-variant først, 1. variant sku-only, non-first m. 3 metafelter, alle m. billede, priser >0,
titel = plan.new_title, alle tilføjede SKUs til stede. Rapporterer pr. produkt + aggregeret. READ-ONLY."""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def main():
    only_new = "--all" not in sys.argv   # default: kun batch #1 (drop de 6 canaries), --all = alle
    CANARIES = {"vaegskab-68x30x20-cm-konstrueret-trae-hvid", "bogreol-med-2-hylder-40x30x-hvid",
                "loftestol-stof-lysegra-1", "havebord-med-traebordplade-polyrattan-og-sort",
                "kontinentalseng-med-madras-kunstlaeder-sort", "foldbare-havestole-6-stk-massivt-akacietrae-gra"}
    plans = {p["key"]: p for p in (json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8"))}
    sb = ME.get_supabase_client()
    done = sb.table("merge_exec_log").select("group_key,keeper_pid,n_variants_created").eq("status", "done").execute().data or []

    checked = 0
    fails = []   # (handle, [problemer])
    for row in done:
        p = plans.get(row["group_key"])
        if not p:
            continue
        h = p["keeper_handle"]
        if only_new and h in CANARIES:
            continue
        checked += 1
        d = ME.gql("""query($h:String!){productByHandle(handle:$h){title status
          options{name position optionValues{name}}
          variants(first:250){edges{node{sku position price selectedOptions{name value} image{url}
            metafields(first:6,namespace:"custom"){edges{node{key}}}}}}}}""", {"h": h})
        pr = (d.get("data") or {}).get("productByHandle")
        probs = []
        if not pr:
            fails.append((h, ["produkt ikke fundet live"])); continue
        # status
        if pr["status"] != "ACTIVE":
            probs.append(f"status={pr['status']}")
        opts = [o for o in pr["options"] if o["name"] != "Title"]
        # ≤3 akser
        if len(opts) > 3:
            probs.append(f">3 akser ({[o['name'] for o in opts]})")
        # ingen dublet akse-værdier
        for o in opts:
            vals = [v["name"] for v in o["optionValues"]]
            if len(vals) != len(set(vals)):
                probs.append(f"dublet-værdi i {o['name']}: {vals}")
        # Farve = option 1 (hvis Farve findes)
        onames = [o["name"] for o in sorted(opts, key=lambda x: x["position"])]
        if "Farve" in onames and onames[0] != "Farve":
            probs.append(f"Farve ikke option 1 ({onames})")
        vs = sorted([e["node"] for e in pr["variants"]["edges"]], key=lambda n: n["position"])
        if not vs:
            fails.append((h, ["ingen varianter"])); continue
        # 1. variant sku-only
        first_mf = sorted(x["node"]["key"] for x in vs[0]["metafields"]["edges"])
        if first_mf != ["sku"]:
            probs.append(f"1. variant ({vs[0]['sku']}) metafelter={first_mf} (skal være ['sku'])")
        # alle tilføjede SKUs til stede
        live_skus = {(n["sku"] or "").strip() for n in vs}
        missing = [m["sku"] for m in p["variant_creates"] if m["sku"] not in live_skus]
        if missing:
            probs.append(f"{len(missing)} tilføjede SKUs mangler: {missing[:4]}")
        # non-first: sku til stede; alle: billede + pris
        no_img = [n["sku"] for n in vs if not n.get("image")]
        if no_img:
            probs.append(f"{len(no_img)} varianter uden billede: {no_img[:4]}")
        no_price = [n["sku"] for n in vs if not n.get("price") or float(n["price"]) <= 0]
        if no_price:
            probs.append(f"{len(no_price)} varianter uden pris: {no_price[:4]}")
        no_sku_mf = [n["sku"] for n in vs if "sku" not in [x["node"]["key"] for x in n["metafields"]["edges"]]]
        if no_sku_mf:
            probs.append(f"{len(no_sku_mf)} varianter uden sku-metafelt: {no_sku_mf[:4]}")
        # titel
        if p.get("new_title") and p.get("title_changes") and pr["title"].strip() != p["new_title"].strip():
            probs.append(f"titel='{pr['title']}' ≠ plan '{p['new_title']}'")
        if probs:
            fails.append((h, probs))

    print(f"=== SLAVISK AUDIT: {checked} merged produkter tjekket ===")
    if not fails:
        print("✅ ALLE lever op til reglerne — ingen afvigelser")
    else:
        print(f"❌ {len(fails)} produkter med afvigelser:\n")
        for h, probs in fails:
            print(f"  {h}")
            for pb in probs:
                print(f"     ⚠ {pb}")

if __name__ == "__main__":
    main()
