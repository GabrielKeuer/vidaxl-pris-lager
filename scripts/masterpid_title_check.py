"""Verificér at MULTI-titlen (fra simulationen) beholder alle FASTE attributter når vi konsoliderer.
For hver multi-master_pid: fælles tokens på tværs af varianternes feed-titler MINUS variant-akse-værdier
= den faste basis. Hvis et fast-attribut-token (tal/mål/g/m²/enhed) er i basen men MANGLER i titlen →
titlen dropper en attribut (som skridsikker g/m²-buggen) → flag til manuel titel-rettelse. READ-ONLY."""
import sys, os, io, zipfile, csv, re, json
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def toks(t):
    return set(re.findall(r"[a-zæøå0-9.,/²x]+", (t or "").lower()))

def main():
    only_affected = "--all" not in sys.argv
    sim = json.load(open("output/catalog_simulation.json", encoding="utf-8"))
    z = zipfile.ZipFile(io.BytesIO(ME.get_feed_zip(os.environ["FEED_URL"])))
    name = [f for f in z.namelist() if f.endswith(".csv")][0]
    feed = {}
    for r in csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8")):
        s = str(r.get("SKU") or "").strip().replace(".0", "")
        if s:
            feed[s] = re.sub(r"(?i)\bvidaxl\b", "", r.get("Title") or "").strip()

    affected = None
    if only_affected:
        skus = set()
        for f in ("output/atomize_specs.json", "output/flagged_specs.json", "output/collision_specs.json"):
            if os.path.exists(f):
                for spec in json.load(open(f, encoding="utf-8")).values():
                    prods = spec["products"] if isinstance(spec, dict) else spec
                    for p in prods:
                        for v in p["variants"]:
                            skus.add(str(v["sku"]))
        sb = ME.get_supabase_client(); sku2mid = {}; sl = list(skus)
        for i in range(0, len(sl), 300):
            for x in (sb.table("vidaxl_sku_master").select("sku,master_pid").in_("sku", sl[i:i + 300]).execute().data or []):
                sku2mid[str(x["sku"])] = x["master_pid"]
        affected = set(sku2mid.values())

    bad = []; checked = 0
    for mid, s in sim.items():
        if s["type"] != "multi":
            continue
        if affected is not None and mid not in affected:
            continue
        live = [x for x in s["skus"] if x in feed]
        if len(live) < 2:
            continue
        checked += 1
        # akse-værdier (variant-variationer)
        axisvals = set()
        for x in live:
            for k, v in (ME.OPTS.get(x) or {}).items():
                if v:
                    axisvals |= toks(v)
        # fælles fast-basis = tokens i ALLE feed-titler, minus akse-værdier
        common = set.intersection(*[toks(feed[x]) for x in live]) - axisvals
        # fast-attribut-tokens = dem med tal/g/m/enhed
        fixed = {t for t in common if re.search(r"\d", t) or "g/m" in t or t in ("cm", "mm", "m", "l", "kg")}
        ttoks = toks(s["title"])
        dropped = fixed - ttoks
        if dropped:
            bad.append({"mid": mid, "title": s["title"], "dropped": sorted(dropped)[:6],
                        "feed": feed[live[0]][:55]})
    print(f"=== TITEL-CHECK: {checked} multi-master_pids {'(berørte)' if only_affected else '(alle)'} ===")
    print(f"  titler der dropper en fast attribut: {len(bad)}")
    for b in bad[:15]:
        print(f"   {b['mid']}: \"{b['title']}\" dropper {b['dropped']} (feed: {b['feed']})")

if __name__ == "__main__":
    main()
