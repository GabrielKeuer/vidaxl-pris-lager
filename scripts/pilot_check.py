"""PILOT-TJEK (frisk): tag de produkter der ER LIVE NU og oprettet nyligt → map til simulationen → er
den ren nok til at rette dem? Sammenlign live-titel vs sim-titel + audit de berørte sim-produkter.
READ-ONLY. Output: konsol + output/pilot_check.json."""
import sys, os, json, re
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, __import__("os").environ.get("DROPXL_SCRIPTS", r"C:\Users\APC\dropxl-product-automation\scripts"))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import title_rules as TR

SINCE = os.environ.get("SINCE", "2026-06-28")
SIZE = {"Størrelse", "Højde", "Bredde", "Bordlængde", "Længde", "Dybde", "Diameter", "Størrelse 2"}

def audit_one(p):
    t = p["title"]; tl = " " + t.lower() + " "; e = []
    if re.search(r"\b(\w+)\s+\1\b", t, re.IGNORECASE): e.append("DUP")
    if re.search(r'["“”](?=[a-zæøåA-ZÆØÅ])|(?<=[a-zæøåA-ZÆØÅ])["“”]', t): e.append("QUOTE")
    if len(re.findall(r"[a-zæøå0-9]+", tl)) < 1: e.append("TOM")
    if p["specs"]:
        av = {(v["values"].get(nm) or "").strip() for v in p["variants"] for nm in p["specs"]}
        if [x for x in av if x and len(x) > 1 and (" " + x.lower() + " ") in tl]: e.append("LEAK")
        if any(nm in SIZE for nm in p["specs"]) and re.search(r"\d+\s*[x×]\s*\d+|\d+(?:[.,]\d+)?\s*cm\b|ø\s*\d", tl): e.append("DIM")
    return e

def oldbug(t):
    """gamle generator-bugs i den LIVE titel som simulationen ville rette."""
    b = []
    if re.search(r"\bPvc\b|\bPe\b|\bLed\b|\bMdf\b|\bUsb\b|\bTv\b(?!-)", t): b.append("casing")
    if re.search(r"(?:\bog|\bmed|\btil)\s*$", t, re.I): b.append("dangling")
    if re.search(r"\b(\w+)\s+\1\b", t, re.I): b.append("dup")
    if '"' in t: b.append("quote")
    if re.search(r"\bpcs\b", t, re.I): b.append("pcs")
    return b

def main():
    Q = "query($q:String,$a:String){products(first:100,query:$q,after:$a){pageInfo{hasNextPage endCursor} edges{node{id title createdAt variants(first:100){edges{node{sku}}}}}}}"
    live = {}; after = None
    while True:
        d = ME.gql(Q, {"q": f"created_at:>{SINCE} vendor:vidaXL", "a": after})
        pr = (d.get("data") or {}).get("products") or {}
        for e in pr.get("edges", []):
            n = e["node"]
            sk = [(x["node"]["sku"] or "").strip() for x in n["variants"]["edges"] if x["node"].get("sku")]
            live[n["id"]] = {"title": n["title"], "created": n["createdAt"][:10], "skus": [s for s in sk if s]}
        if pr.get("pageInfo", {}).get("hasNextPage"):
            after = pr["pageInfo"]["endCursor"]
        else:
            break
    allskus = sorted({s for v in live.values() for s in v["skus"]})
    print(f"LIVE oprettet siden {SINCE}: {len(live)} produkter, {len(allskus)} SKUs")

    sb = ME.get_supabase_client()
    sku2mid = {}
    for i in range(0, len(allskus), 300):
        r = sb.table("vidaxl_sku_master").select("sku,master_pid").in_("sku", allskus[i:i+300]).execute().data or []
        for x in r:
            sku2mid[str(x["sku"]).strip()] = x["master_pid"]
    affected = sorted({sku2mid[s] for s in allskus if s in sku2mid})
    print(f"berørte master_pids: {len(affected)} | SKUs uden master_pid: {len(allskus)-len(sku2mid)}")

    sim = defaultdict(list)
    for p in json.load(open("output/complete_feed.json", encoding="utf-8")):
        if p["mid"] in affected:
            sim[p["mid"]].append(p)
    sim_products = [p for ps in sim.values() for p in ps]
    missing = [m for m in affected if m not in sim]
    print(f"sim-produkter for berørte: {len(sim_products)} (fra {len(sim)} master_pids) | master_pids UDEN sim: {len(missing)}")

    # audit sim-produkterne (skal de være rene FØR vi retter live)
    iss = defaultdict(list)
    for p in sim_products:
        for e in audit_one(p):
            iss[e].append(p["title"])
    print(f"\n=== A) er SIMULATIONEN ren for de berørte? ===")
    if any(iss.values()):
        for k in iss:
            print(f"  {k}: {len(iss[k])}  fx {iss[k][0][:50]!r}")
    else:
        print("  ✓ INGEN titel-fejl i de berørte sim-produkter — simulationen er ren")

    # gamle bugs i de LIVE titler (som vi ville rette)
    livebugs = defaultdict(list)
    for v in live.values():
        for b in oldbug(v["title"]):
            livebugs[b].append(v["title"])
    print(f"\n=== B) hvor mange LIVE-titler har gamle bugs simulationen retter? ===")
    tot = len({t for L in livebugs.values() for t in L})
    for k in sorted(livebugs, key=lambda z: -len(livebugs[z])):
        print(f"  {k}: {len(livebugs[k])}  fx {livebugs[k][0][:48]!r}")
    print(f"  → {tot} live-produkter med mindst én gammel bug")

    print(f"\n=== C) gruppering ===")
    print(f"  live-produkter: {len(live)} | sim-produkter (samme master_pids): {len(sim_products)} | netto {len(sim_products)-len(live):+d}")

    # D) direkte titel-sammenligning: match live→sim via delt SKU
    sku2sim = {}
    for p in sim_products:
        for v in p["variants"]:
            sku2sim[v["sku"]] = p
    changed = []; same = 0; nomatch = 0
    cmp_rows = []
    for lp in live.values():
        simp = next((sku2sim[s] for s in lp["skus"] if s in sku2sim), None)
        if not simp:
            nomatch += 1; continue
        if lp["title"].strip() == simp["title"].strip():
            same += 1
        else:
            changed.append((lp["title"], simp["title"]))
        cmp_rows.append([lp["title"], simp["title"], "SAMME" if lp["title"].strip() == simp["title"].strip() else "ÆNDRES"])
    print(f"\n=== D) live-titel vs sim-titel (matchet på SKU) ===")
    print(f"  uændret: {same} | ÆNDRES: {len(changed)} | uden sim-match: {nomatch}")
    for a, b in changed[:20]:
        print(f"     LIVE: {a[:46]!r}\n      SIM: {b[:46]!r}")
    out = r"C:\Users\APC\Desktop\pilot_titel_diff.csv"
    import csv as _csv
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = _csv.writer(f); w.writerow(["live_titel", "sim_titel", "status"]); w.writerows(cmp_rows)
    print(f"  → {out}")

    json.dump({"since": SINCE, "n_live": len(live), "affected": affected, "missing_sim": missing,
               "sim_issues": {k: iss[k] for k in iss}, "live_bugs": {k: len(livebugs[k]) for k in livebugs}},
              open("output/pilot_check.json", "w", encoding="utf-8"), ensure_ascii=False)
    print("\n✓ output/pilot_check.json")

if __name__ == "__main__":
    main()
