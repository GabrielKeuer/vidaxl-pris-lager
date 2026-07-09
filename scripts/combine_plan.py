"""COMBINE-ONLY plan (read-only). Vi SAMLER kun — splitter IKKE.
For hver mål-variantgruppe (fra ny gruppering, ≥2 SKU): find de nuværende live-produkter der holder dens
SKUs. Er de spredt over FLERE produkter, OG holder ingen af dem fremmede SKUs (så samling ikke kræver
split) → COMBINE: vælg anker (flest SKUs), flet donorer ind, forud-genereret titel. Ellers → PARKERET
(kræver split, rører vi ikke nu). Output: Desktop/combine_plan.csv + output/combine_plan.json + optælling."""
import sys, os, io, zipfile, csv, json
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, r"C:\Users\APC\dropxl-product-automation\scripts")
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import build_complete_feed as B
import scope_split as SS
import cleanup_engine as CE
import fix_live as FL

def main():
    feed = CE.load_feed_df()
    titles = feed["Title"].to_dict()
    SS.setup_universe(list(feed.index))
    for w in ("cremehvid", "cremehvide", "råhvid", "gråhvid", "offwhite", "sølvgrå", "koksgrå"):
        B.COLOR_UNIVERSE.add(w)
    B.build_color_re()
    lbl = json.load(open("output/axis_labels.json", encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}
    sb = ME.get_supabase_client()
    bym = defaultdict(list); fr = 0
    while True:
        b = sb.table("vidaxl_sku_master").select("sku,master_pid").range(fr, fr + 999).execute().data or []
        for x in b:
            bym[x["master_pid"]].append(str(x["sku"]).strip())
        if len(b) < 1000:
            break
        fr += 1000

    # MÅL-variantgrupper (≥2 SKU) fra ny gruppering
    targets = []
    for mid, skus in bym.items():
        live = [s for s in skus if s in feed.index]
        if not live:
            continue
        opts = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        for p in FL.regroup_master(mid, live, opts, titles, {mid: lbl.get(mid, {})}):
            if len(p["skus"]) >= 2:
                targets.append({"mid": mid, "title": p["title"], "skus": p["skus"]})
    print(f"mål-variantgrupper (≥2 SKU): {len(targets)}")

    # NUVÆRENDE live: pid → {handle, sku-set}, sku → pid
    Q = ('query($a:String){products(first:80,query:"vendor:vidaXL",after:$a){pageInfo{hasNextPage endCursor} '
         'edges{node{id handle title variants(first:200){edges{node{sku}}}}}}}')
    after = None; prod = {}; sku2pid = {}; pg = 0
    while True:
        d = ME.gql(Q, {"a": after}); pr = (d.get("data") or {}).get("products") or {}
        for e in pr.get("edges", []):
            n = e["node"]; pid = n["id"]
            sks = set((v["node"]["sku"] or "").strip() for v in n["variants"]["edges"] if v["node"].get("sku"))
            prod[pid] = {"handle": n["handle"], "title": n["title"], "skus": sks}
            for s in sks:
                sku2pid[s] = pid
        pg += 1
        if pg % 40 == 0:
            print(f"  …{len(prod)} live-produkter", flush=True)
        if pr.get("pageInfo", {}).get("hasNextPage"):
            after = pr["pageInfo"]["endCursor"]
        else:
            break
    print(f"live-produkter: {len(prod)}")

    combines = []; parked = 0; already = 0; titlefix = 0; reduction = 0
    for t in targets:
        live_skus = [s for s in t["skus"] if s in sku2pid]
        if len(live_skus) < 2:
            continue
        pids = {sku2pid[s] for s in live_skus}
        if len(pids) == 1:
            pid = next(iter(pids))
            if prod[pid]["title"].strip() != t["title"].strip():
                titlefix += 1
            else:
                already += 1
            continue
        # kræver samling ikke split? (ingen af de nuværende produkter holder fremmede SKUs)
        tangled = any(s not in set(t["skus"]) for pid in pids for s in prod[pid]["skus"])
        if tangled:
            parked += 1
            continue
        anchor = max(pids, key=lambda p: len(prod[p]["skus"] & set(t["skus"])))
        donors = [p for p in pids if p != anchor]
        reduction += len(donors)
        combines.append({"mid": t["mid"], "title": t["title"], "anchor": anchor,
                         "anchor_handle": prod[anchor]["handle"], "n_skus": len(t["skus"]),
                         "n_donors": len(donors), "donor_handles": [prod[p]["handle"] for p in donors],
                         "skus": t["skus"]})

    after_ct = len(prod) - reduction
    print(f"\n=== COMBINE-ONLY PLAN ===")
    print(f"  COMBINE (samles): {len(combines)} grupper, absorberer {reduction} donor-produkter")
    print(f"  allerede korrekt samlet: {already}")
    print(f"  kun titel-rettelse (in-place): {titlefix}")
    print(f"  PARKERET (kræver split — rører ikke): {parked}")
    print(f"\n  live-produkter nu: {len(prod)}")
    print(f"  → efter combine-only: {after_ct}  ({len(prod)} − {reduction})")

    json.dump(combines, open("output/combine_plan.json", "w", encoding="utf-8"), ensure_ascii=False)
    out = r"C:\Users\APC\Desktop\combine_plan.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f); w.writerow(["master_pid", "ny_titel", "anker_handle", "antal_skus", "antal_donorer", "donor_handles"])
        for c in sorted(combines, key=lambda z: -z["n_donors"]):
            w.writerow([c["mid"], c["title"], c["anchor_handle"], c["n_skus"], c["n_donors"], " | ".join(c["donor_handles"][:8])])
    print(f"\n  ✓ {out} + output/combine_plan.json")
    print("\n  største combines:")
    for c in sorted(combines, key=lambda z: -z["n_donors"])[:12]:
        print(f"     +{c['n_donors']} donorer → \"{c['title'][:46]}\" ({c['n_skus']} SKU)")

if __name__ == "__main__":
    main()
