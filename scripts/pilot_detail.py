"""PILOT-DETALJE: dyk ned i de 180 live vidaXL-produkter (sidste 3 dage). READ-ONLY.
1) MERGE-grupper: flere live-produkter der deler master_pid → sim samler dem som varianter
2) SPLIT-grupper: master_pids sim deler i flere
3) BILLEDE-fejl: live-produkter uden featured-billede / variant uden billede
4) Solbadekar-verifikation
Output: konsol + Desktop/pilot_detalje.csv"""
import sys, os, io, zipfile, csv, json, re
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

SINCE = "2026-07-05"

def main():
    Q = ("query($q:String,$a:String){products(first:60,query:$q,after:$a){pageInfo{hasNextPage endCursor} "
         "edges{node{id title featuredImage{url} images(first:1){edges{node{url}}} "
         "variants(first:100){edges{node{sku image{url}}}}}}}}")
    live = {}; after = None
    while True:
        d = ME.gql(Q, {"q": f"created_at:>{SINCE} vendor:vidaXL", "a": after})
        pr = (d.get("data") or {}).get("products") or {}
        for e in pr.get("edges", []):
            n = e["node"]
            vs = [(x["node"]["sku"], x["node"].get("image")) for x in n["variants"]["edges"]]
            live[n["id"]] = {"title": n["title"], "feat": bool(n.get("featuredImage")),
                             "nimg": len(n["images"]["edges"]),
                             "skus": [s for s, _ in vs if s],
                             "var_noimg": [s for s, im in vs if s and not im]}
        if pr.get("pageInfo", {}).get("hasNextPage"):
            after = pr["pageInfo"]["endCursor"]
        else:
            break
    print(f"live vidaXL siden {SINCE}: {len(live)}")

    allskus = sorted({s for v in live.values() for s in v["skus"]})
    sb = ME.get_supabase_client()
    sku2mid = {}
    for i in range(0, len(allskus), 300):
        for x in (sb.table("vidaxl_sku_master").select("sku,master_pid").in_("sku", allskus[i:i+300]).execute().data or []):
            sku2mid[str(x["sku"]).strip()] = x["master_pid"]
    # master_pid pr. live-produkt (via første mappede SKU)
    live_mid = {}
    for pid, v in live.items():
        mids = {sku2mid[s] for s in v["skus"] if s in sku2mid}
        live_mid[pid] = mids
    # MERGE: master_pids med >1 live-produkt
    by_mid = defaultdict(list)
    for pid, mids in live_mid.items():
        for m in mids:
            by_mid[m].append(pid)
    merges = {m: pids for m, pids in by_mid.items() if len(pids) > 1}
    print(f"\n=== 1) MERGE-grupper (flere live-produkter deler master_pid → bliver varianter) ===")
    print(f"  {len(merges)} master_pids samler {sum(len(v) for v in merges.values())} live-produkter")
    for m, pids in list(merges.items())[:20]:
        print(f"   {m}:")
        for pid in pids:
            print(f"       \"{live[pid]['title'][:52]}\"")

    # sim-produkter for de berørte + split-tælling
    affected = set(m for mids in live_mid.values() for m in mids)
    sim = defaultdict(list)
    for p in json.load(open("output/complete_feed.json", encoding="utf-8")):
        if p["mid"] in affected:
            sim[p["mid"]].append(p)
    splits = {m: ps for m, ps in sim.items() if len(ps) > 1}
    print(f"\n=== 2) SPLIT-grupper (sim deler master_pid i flere) ===")
    print(f"  {len(splits)} master_pids → flere sim-produkter")
    for m, ps in list(splits.items())[:12]:
        print(f"   {m}: {[p['title'][:34] for p in ps]}")

    # BILLEDE-fejl
    noimg = [(pid, v) for pid, v in live.items() if not v["feat"] or v["nimg"] == 0]
    varnoimg = [(pid, v) for pid, v in live.items() if v["var_noimg"]]
    print(f"\n=== 3) BILLEDE-fejl ===")
    print(f"  uden featured/hoved-billede: {len(noimg)}")
    for pid, v in noimg[:15]:
        print(f"       \"{v['title'][:50]}\"  (feat={v['feat']} nimg={v['nimg']})")
    print(f"  produkter med variant(er) UDEN billede: {len(varnoimg)}")
    for pid, v in varnoimg[:15]:
        print(f"       \"{v['title'][:44]}\"  {len(v['var_noimg'])}/{len(v['skus'])} variant-SKUs uden billede")

    out = r"C:\Users\APC\Desktop\pilot_detalje.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["type", "master_pid", "titel", "detalje"])
        for m, pids in merges.items():
            for pid in pids:
                w.writerow(["MERGE", m, live[pid]["title"], f"{len(pids)} live deler master_pid"])
        for pid, v in noimg:
            w.writerow(["INTET_BILLEDE", ";".join(live_mid[pid]), v["title"], f"feat={v['feat']} nimg={v['nimg']}"])
        for pid, v in varnoimg:
            w.writerow(["VARIANT_UDEN_BILLEDE", ";".join(live_mid[pid]), v["title"], f"{len(v['var_noimg'])} SKUs"])
    print(f"\n✓ {out}")

if __name__ == "__main__":
    main()
