"""REN KOMPLET SIMULATION (read-only) efter det AUTORITATIVE princip: hver master_pid = ét produkt.
Ingen opfundet logik. Titel 2-vejs: single = fuld renset feed-titel; multi = basis (feed-titel minus
variant-akse-værdier). Flagger KUN master_pids der ikke danner et rent produkt (til manuel stilling).
Output: output/catalog_simulation.json + summary. INGEN mutationer."""
import sys, os, io, zipfile, csv, re, json
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def clean(t):
    t = re.sub(r"(?i)\bvidaxl\b", "", t or "")
    return re.sub(r"\s+", " ", t).strip()

def housestyle(t):
    return " ".join(w[:1].upper() + w[1:] if w else w for w in clean(t).split())

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
    print(f"master_pids: {len(bym)}")
    z = zipfile.ZipFile(io.BytesIO(ME.get_feed_zip(os.environ["FEED_URL"])))
    name = [f for f in z.namelist() if f.endswith(".csv")][0]
    feed = {}
    for r in csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8")):
        s = str(r.get("SKU") or "").strip().replace(".0", "")
        if s:
            feed[s] = r.get("Title") or ""
    print(f"feed-SKUs: {len(feed)}")

    sim = {}; cat = Counter()
    for mid, skus in bym.items():
        live = [s for s in skus if s in feed]
        if not live:
            cat["udgaaet_ingen_feed"] += 1; continue
        opts = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        axisvals = defaultdict(set)
        for s in live:
            for k, v in opts[s].items():
                axisvals[k].add(v)
        axes = sorted({k for k, vv in axisvals.items() if len(vv) > 1})
        # multi-titel = FÆLLES tokens på tværs af varianternes feed-titler (renset) — stol på master_pid
        def gen_title():
            cl = [clean(feed[s]) for s in live if feed.get(s)]
            if not cl:
                return ""
            common = set.intersection(*[set(t.lower().split()) for t in cl])
            return housestyle(" ".join(w for w in cl[0].split() if w.lower() in common))
        if len(live) == 1:
            sim[mid] = {"type": "single", "title": housestyle(feed[live[0]]), "n": 1, "skus": live}
            cat["OK_single"] += 1
        elif not axes:
            sim[mid] = {"type": "FLAG_no_axes", "n": len(live), "skus": live, "titler": [feed[s][:40] for s in live[:3]]}
            cat["FLAG_no_axes"] += 1
        else:
            combos = [tuple(opts[s].get(a, "") for a in axes) for s in live]
            if len(combos) != len(set(combos)):
                sim[mid] = {"type": "FLAG_collision", "n": len(live), "skus": live, "axes": axes,
                            "titler": [feed[s][:40] for s in live[:3]]}
                cat["FLAG_collision"] += 1
            else:
                sim[mid] = {"type": "multi", "title": gen_title(), "axes": axes, "n": len(live), "skus": live}
                cat["OK_multi"] += 1
    json.dump(sim, open("output/catalog_simulation.json", "w", encoding="utf-8"), ensure_ascii=False)
    tot = len(sim)
    print(f"\n=== SIMULATION: {tot} master_pids (produkter) ===")
    for k, n in cat.most_common():
        print(f"  {n:6d}  {k}")
    ok = cat["OK_single"] + cat["OK_multi"]
    fl = sum(v for k, v in cat.items() if k.startswith("FLAG"))
    print(f"\n  RENE (auto-korrekt): {ok} ({100*ok/tot:.1f}%) | FLAGGET (manuel stilling): {fl} ({100*fl/tot:.1f}%)")
    for typ in ("FLAG_base_differ", "FLAG_collision", "FLAG_no_axes"):
        exs = [(m, r) for m, r in sim.items() if r["type"] == typ][:3]
        if exs:
            print(f"\n--- {typ} (eks.) ---")
            for m, r in exs:
                print(f"   {m}: {r.get('bases') or r.get('axes') or r.get('titler')}")

if __name__ == "__main__":
    main()
