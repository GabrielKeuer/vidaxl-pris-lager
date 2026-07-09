"""SPLIT-FORSLAG (read-only, INGEN ændringer): for hver detekteret master_pid med ægte produkt-navns-
divergens, vis hvad den NUVÆRENDE sim giver vs. hvad et SPLIT ville give — med rigtige genererede titler
(samme logik som build_complete_feed). Kun master_pids MED item_variant-akser (hvor gruppering er mulig);
no-axes-master er allerede korrekte singler. Output: konsol + Desktop/split_forslag.csv (til gennemsyn)."""
import sys, os, io, zipfile, csv, re, json
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, __import__("os").environ.get("DROPXL_SCRIPTS", r"C:\Users\APC\dropxl-product-automation\scripts"))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import build_complete_feed as B
import scope_split as SS

def noun_base(s, feed, opts, axes):
    av = [opts[s].get(k) for k in axes]
    b = B.strip_axes(B.clean(feed[s]), av, strip_colors=True, strip_dims=True)
    b = re.sub(r"\b\d+\s*(?:stk|dele|pcs|sæt|pk|personers?|sæders?)\.?\b", " ", b.lower())
    return " ".join(w for w in re.findall(r"[a-zæøå]+", b) if w not in SS.MATERIAL_STOP and len(w) > 2)

def gen_title(cluster, opts, feed, lblmid):
    axvals = defaultdict(set)
    for s in cluster:
        for k, v in opts[s].items():
            axvals[k].add(v)
    axes = sorted(k for k, vv in axvals.items() if len(vv) > 1)
    names = [("Farve" if k == "color" else (lblmid.get(k) or B.option_name(k, axvals[k]))) for k in axes]
    if not axes:
        return B.housestyle(B.clean(feed[max(cluster, key=len)])), []   # 1 SKU → egen titel
    base = max(cluster, key=lambda s: len(opts[s]))
    avals = []
    for k in axes:
        for s in cluster:
            x = opts[s].get(k)
            if x and x not in avals:
                avals.append(x)
    strip_dims = any(nm in SS.SIZE_AXES for nm in names)
    t = B.strip_axes(B.clean(feed[base]), avals, strip_colors=any(k == "color" for k in axes), strip_dims=strip_dims)
    return (t or B.housestyle(feed[base])), names

def main():
    sb = ME.get_supabase_client()
    bym = defaultdict(list); fr = 0
    while True:
        b = sb.table("vidaxl_sku_master").select("sku,master_pid").range(fr, fr + 999).execute().data or []
        for x in b:
            bym[x["master_pid"]].append(str(x["sku"]).strip())
        if len(b) < 1000:
            break
        fr += 1000
    z = zipfile.ZipFile(io.BytesIO(ME.get_feed_zip(os.environ["FEED_URL"])))
    name = [f for f in z.namelist() if f.endswith(".csv")][0]
    feed = {}
    for r in csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8")):
        s = str(r.get("SKU") or "").strip().replace(".0", "")
        if s:
            feed[s] = r.get("Title") or ""
    SS.setup_universe(feed)
    lbl = json.load(open("output/axis_labels.json", encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}
    cur = {}
    for p in json.load(open("output/complete_feed.json", encoding="utf-8")):
        cur.setdefault(p["mid"], p["title"])
    flagged = json.load(open("output/scope_split.json", encoding="utf-8"))

    rows = []; proposals = []
    for x in flagged:
        mid = x["mid"]
        live = [s for s in bym.get(mid, []) if s in feed]
        opts = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        axvals = defaultdict(set)
        for s in live:
            for k, v in opts[s].items():
                axvals[k].add(v)
        axes = sorted(k for k, vv in axvals.items() if len(vv) > 1)
        if not axes:
            continue   # no-axes → allerede korrekte singler, intet at gruppere
        # klyng efter produkt-navn
        base = {s: noun_base(s, feed, opts, axes) for s in live}
        clusters = []
        for s in live:
            for grp in clusters:
                if SS.same_product(base[s], base[grp[0]]):
                    grp.append(s); break
            else:
                clusters.append([s])
        if len(clusters) < 2:
            continue
        lblmid = lbl.get(mid, {}) or {}
        subs = []
        for grp in sorted(clusters, key=len, reverse=True):
            t, names = gen_title(grp, opts, feed, lblmid)
            subs.append({"titel": t, "akser": names, "n": len(grp), "skus": grp[:3]})
        proposals.append({"mid": mid, "n_skus": len(live), "nu": cur.get(mid, "?"), "subs": subs})
        for i, sub in enumerate(subs, 1):
            rows.append([mid, cur.get(mid, "?"), len(live), i, sub["titel"], "+".join(sub["akser"]) or "single",
                         sub["n"], " ".join(sub["skus"])])

    out = r"C:\Users\APC\Desktop\split_forslag.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["master_pid", "nuvaerende_titel", "master_n_sku", "forslag_nr", "forslag_titel",
                    "forslag_akser", "forslag_n_sku", "eksempel_skus"])
        w.writerows(rows)

    print(f"=== SPLIT-FORSLAG: {len(proposals)} master_pids (kun MED akser, actionable) ===\n")
    for p in proposals:
        print(f"▸ {p['mid']} [{p['n_skus']} SKU]")
        print(f"    NU:  \"{p['nu']}\"  (én titel til ALLE)")
        print(f"    FORSLAG → {len(p['subs'])} produkter:")
        for sub in p["subs"]:
            print(f"       • \"{sub['titel']}\"  [{'+'.join(sub['akser']) or 'single'}, {sub['n']} SKU]")
        print()
    print(f"✓ Fuldt forslag → {out}")

if __name__ == "__main__":
    main()
