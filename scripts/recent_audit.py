"""READ-ONLY audit af de vidaXL-produkter VI har oprettet/pillet ved live de sidste dage (createdAt-baseret),
mod de NYESTE regler (fuzzy ental/flertal, mørk-farve-strip, tal-først-sortering, Farve→Størrelse→rest-kolonner,
metafelt-model, rent handle). For hver berørt master: kør ny-regel-gruppering og sammenlign med live-opsætning.
Klassificér hvert recent-produkt: OK / SKAL-MERGES (fragment der nu hører sammen) / VARIANT-SORT (forkert
rækkefølge) / KOLONNE-ORDEN / TITEL. Ingen skrivning. Output: output/recent_audit.json + konsol-resume."""
import sys, os, io, zipfile, csv, json, re
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, __import__("os").environ.get("DROPXL_SCRIPTS", r"C:\Users\APC\dropxl-product-automation\scripts"))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import build_complete_feed as B
import scope_split as SS
import regroup as RG
import fix_live as FL
import cleanup_engine as CE

SINCE = os.environ.get("SINCE", "2026-07-04")

def natv(v):
    n = re.findall(r"\d+\.?\d*", v or "")
    return (0, [float(x) for x in n]) if n else (1, [(v or "").lower()])

def main():
    # 1) recent vidaXL-produkter (createdAt) + deres live-varianter/options/handles
    Q = ('query($a:String){products(first:80,query:"vendor:vidaXL created_at:>=%s",after:$a){'
         'pageInfo{hasNextPage endCursor} edges{node{id handle title createdAt '
         'options{name position} variants(first:200){edges{node{sku selectedOptions{name value}}}}}}}}' % SINCE)
    after = None; recent = [];
    while True:
        d = ME.gql(Q, {"a": after}); pr = (d.get("data") or {}).get("products") or {}
        for e in pr.get("edges", []):
            n = e["node"]
            n["skus"] = [v["node"]["sku"] for v in n["variants"]["edges"] if v["node"].get("sku")]
            n["_vorder"] = [(v["node"]["sku"], {o["name"]: o["value"] for o in v["node"]["selectedOptions"]})
                            for v in n["variants"]["edges"]]
            recent.append(n)
        if pr.get("pageInfo", {}).get("hasNextPage"): after = pr["pageInfo"]["endCursor"]
        else: break
    print(f"recent vidaXL-produkter (createdAt≥{SINCE}): {len(recent)}")

    # 2) SKU → master_pid (fra Supabase) for alle recent-SKUs
    allsku = sorted({s for p in recent for s in p["skus"]})
    sb = ME.get_supabase_client()
    sku2m = {}
    for i in range(0, len(allsku), 500):
        chunk = allsku[i:i+500]
        r = sb.table("vidaxl_sku_master").select("sku,master_pid").in_("sku", chunk).execute().data or []
        for x in r: sku2m[str(x["sku"])] = x["master_pid"]
    masters = sorted({sku2m.get(s) for p in recent for s in p["skus"] if sku2m.get(s)})
    print(f"berørte masters: {len(masters)}")

    # 3) feed + universer (til ny-regel-gruppering)
    feed = CE.load_feed_df(); titles = feed["Title"].to_dict()
    SS.setup_universe(list(feed.index))
    for w in ("cremehvid","cremehvide","råhvid","gråhvid","offwhite","sølvgrå","koksgrå"): B.COLOR_UNIVERSE.add(w)
    B.build_color_re()
    lbl = json.load(open("output/axis_labels.json", encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}

    # master → alle live-SKUs (fra mapping)
    bym = defaultdict(list); fr = 0
    mset = set(masters)
    while True:
        b = sb.table("vidaxl_sku_master").select("sku,master_pid").range(fr, fr+999).execute().data or []
        for x in b:
            if x["master_pid"] in mset: bym[x["master_pid"]].append(str(x["sku"]).strip())
        if len(b) < 1000: break
        fr += 1000

    # 4) ny-regel-mål pr. master: {frozenset(skus): product-spec}
    target_for_sku = {}   # sku -> (mid, group_index, sorted_skus, specs, title)
    for mid in masters:
        live = [s for s in bym.get(mid, []) if s in feed.index]
        if not live: continue
        opts = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        for gi, p in enumerate(FL.regroup_master(mid, live, opts, titles, {mid: lbl.get(mid, {})})):
            rows = FL.to_rows(p, opts)  # allerede tal-først sorteret
            want_order = [r["sku"] for r in rows]
            for s in p["skus"]:
                target_for_sku[s] = {"mid": mid, "skus": set(p["skus"]), "title": p["title"],
                                     "specs": [nm for nm, _ in p["specs"]], "order": want_order}

    # 5) klassificér hvert recent-produkt
    _BC = ("graa","gra","blaa","bla","groen","gron","hvid","sort","brun","roed","rod","gul","beige","creme","antracit","natur","eg")
    def ugly(h):
        if not h: return False
        h=h.lower()
        if re.search(r"\d+\s*[-x]\s*\d+|-cm(-|$)|-\d{2,}(-|$)",h): return True
        tail="-".join(h.split("-")[-2:]); return any(c in tail for c in _BC)

    cats = defaultdict(list)
    for p in recent:
        sks = set(p["skus"])
        tgt = None
        for s in p["skus"]:
            if s in target_for_sku: tgt = target_for_sku[s]; break
        if not tgt:
            cats["INGEN-MÅL"].append(p); continue
        # a) skal merges? (målgruppen har SKUs som dette produkt ikke har → fragment)
        if tgt["skus"] != sks:
            if tgt["skus"] > sks: cats["SKAL-MERGES"].append((p, tgt))
            else: cats["MÅL-MINDRE"].append((p, tgt))   # produktet har SKUs der iflg regler ikke hører sammen (split — parkeret)
            continue
        # b) samme SKU-sæt → tjek variant-rækkefølge (tal-først) + kolonne-orden + titel
        cur_order = [s for s, _ in p["_vorder"]]
        want_order = [s for s in tgt["order"] if s in sks]
        cur_cols = [o["name"] for o in sorted(p.get("options", []), key=lambda o: o["position"])]
        want_cols = tgt["specs"]
        issues = []
        if len(sks) > 1 and cur_order != want_order: issues.append("VARIANT-SORT")
        if want_cols and cur_cols != want_cols and cur_cols != ["Title"]: issues.append("KOLONNE-ORDEN")
        if ugly(p["handle"]): issues.append("GRIMT-HANDLE")
        if issues:
            cats["FIX-INPLACE"].append((p, tgt, issues))
        else:
            cats["OK"].append(p)

    print("\n=== RECENT-AUDIT RESULTAT ===")
    for k in ("OK","SKAL-MERGES","FIX-INPLACE","MÅL-MINDRE","INGEN-MÅL"):
        print(f"  {k}: {len(cats.get(k,[]))}")
    # detaljer
    def title_of(x): return x[0]["title"] if isinstance(x, tuple) else x["title"]
    if cats.get("SKAL-MERGES"):
        print("\n  SKAL-MERGES (fragment der nu hører sammen iflg. nye regler) — eksempler:")
        for p, tgt in cats["SKAL-MERGES"][:10]:
            print(f"     \"{p['title'][:40]}\" ({len(p['skus'])} SKU) → mål \"{tgt['title'][:40]}\" ({len(tgt['skus'])} SKU) [{tgt['mid']}]")
    if cats.get("FIX-INPLACE"):
        print("\n  FIX-INPLACE (samme produkt, men rækkefølge/kolonne/handle) — eksempler:")
        ic = Counter()
        for p, tgt, iss in cats["FIX-INPLACE"]:
            for i in iss: ic[i]+=1
        print(f"     issue-fordeling: {dict(ic)}")
        for p, tgt, iss in cats["FIX-INPLACE"][:8]:
            print(f"     \"{p['title'][:40]}\"  {iss}")
    if cats.get("MÅL-MINDRE"):
        print("\n  MÅL-MINDRE (produktet rummer SKUs reglerne vil SPLITTE — parkeret nu) — eksempler:")
        for p, tgt in cats["MÅL-MINDRE"][:6]:
            print(f"     \"{p['title'][:40]}\" ({len(p['skus'])} SKU) → mål-fragment {len(tgt['skus'])} SKU [{tgt['mid']}]")

    json.dump({k: [ (x[0]["id"] if isinstance(x,tuple) else x["id"]) for x in v ] for k,v in cats.items()},
              open("output/recent_audit.json","w",encoding="utf-8"), ensure_ascii=False)
    print("\n  → output/recent_audit.json")

if __name__ == "__main__":
    main()
