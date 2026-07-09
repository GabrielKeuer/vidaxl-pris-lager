"""FIX-LISTE for de 180 live vidaXL-produkter (sidste 3 dage). Rent master_pid-baseret.
Én række pr. SKU: master_pid · sku · original_feed_titel · nuvaerende_live_titel · ny_shopify_titel(sim) ·
type · option1..3(navn+vaerdi) · status. READ-ONLY. Output: Desktop/fix_liste_180.csv + konsol-resume."""
import sys, os, io, zipfile, csv, json, re
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

SINCE = "2026-07-05"

def clean(t):
    return re.sub(r"\s+", " ", re.sub(r"(?i)\bvidaxl\b", "", t or "")).strip()

def main():
    # 1) live-produkter fra Shopify → sku → live-titel
    Q = ("query($q:String,$a:String){products(first:60,query:$q,after:$a){pageInfo{hasNextPage endCursor} "
         "edges{node{id title variants(first:100){edges{node{sku}}}}}}}")
    sku_live = {}; after = None; nlive = 0
    while True:
        d = ME.gql(Q, {"q": f"created_at:>{SINCE} vendor:vidaXL", "a": after})
        pr = (d.get("data") or {}).get("products") or {}
        for e in pr.get("edges", []):
            n = e["node"]; nlive += 1
            for x in n["variants"]["edges"]:
                s = (x["node"]["sku"] or "").strip()
                if s:
                    sku_live[s] = n["title"]
        if pr.get("pageInfo", {}).get("hasNextPage"):
            after = pr["pageInfo"]["endCursor"]
        else:
            break
    print(f"live vidaXL siden {SINCE}: {nlive} produkter, {len(sku_live)} SKUs")

    # 2) sku → master_pid
    sb = ME.get_supabase_client()
    sku2mid = {}
    L = list(sku_live)
    for i in range(0, len(L), 300):
        for x in (sb.table("vidaxl_sku_master").select("sku,master_pid").in_("sku", L[i:i+300]).execute().data or []):
            sku2mid[str(x["sku"]).strip()] = x["master_pid"]
    affected = {sku2mid[s] for s in sku_live if s in sku2mid}
    print(f"berørte master_pids: {len(affected)}")

    # 3) feed-titler
    z = zipfile.ZipFile(io.BytesIO(ME.get_feed_zip(os.environ["FEED_URL"])))
    nm = [f for f in z.namelist() if f.endswith(".csv")][0]
    feedt = {}
    for r in csv.DictReader(io.TextIOWrapper(z.open(nm), encoding="utf-8")):
        s = str(r.get("SKU") or "").strip().replace(".0", "")
        if s:
            feedt[s] = r.get("Title") or ""

    # 4) sim-produkter for berørte master_pids
    sim = [p for p in json.load(open("output/complete_feed.json", encoding="utf-8")) if p["mid"] in affected]
    sim.sort(key=lambda p: (p["mid"], p["title"]))

    out = r"C:\Users\APC\Desktop\fix_liste_180.csv"
    rows = 0; changed = 0; live_skus_covered = set()
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["master_pid", "product_key", "sku", "original_feed_titel", "nuvaerende_live_titel",
                    "ny_shopify_titel", "aendres", "type", "option1_navn", "option1_vaerdi",
                    "option2_navn", "option2_vaerdi", "option3_navn", "option3_vaerdi", "sku_er_live_nu"])
        for p in sim:
            names = p["specs"][:3]
            typ = "variant" if p["specs"] else "single"
            for v in sorted(p["variants"], key=lambda x: x.get("pos", 0)):
                s = v["sku"]
                livet = sku_live.get(s, "")
                if livet:
                    live_skus_covered.add(s)
                change = "JA" if (livet and clean(livet).lower() != clean(p["title"]).lower()) else ""
                if change:
                    changed += 1
                row = [p["mid"], p["key"], s, clean(feedt.get(s, "")), livet, p["title"], change, typ]
                for i in range(3):
                    row += [names[i], v["values"].get(names[i], "")] if i < len(names) else ["", ""]
                row += ["JA" if livet else ""]
                w.writerow(row); rows += 1
    print(f"\n✓ FIX-LISTE: {rows} SKU-rækker ({len(sim)} sim-produkter) → {out}")
    print(f"  SKU-rækker hvor titel ÆNDRES fra live: {changed}")
    print(f"  live-SKUs dækket: {len(live_skus_covered)}/{len(sku_live)}")
    miss = [s for s in sku_live if s not in live_skus_covered and s in sku2mid]
    print(f"  live-SKUs i berørt master_pid men IKKE i sim (tjek!): {len(miss)}")
    if miss[:5]:
        print(f"     fx: {miss[:5]}")

if __name__ == "__main__":
    main()
