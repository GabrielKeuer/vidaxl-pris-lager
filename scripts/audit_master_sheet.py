"""Slavisk check af den korrekte plan (master_pid + item_variant + strippet titel). Flager til MANUEL:
  over_3_akser   — >3 variant-akser (Shopify max 3 options)
  titel_har_akse — titlen indeholder stadig en variant-akse-værdi (fx farve ikke strippet)
  titel_tom      — tom/for kort titel
  titel_kun_maal — titlen er kun tal/mål (intet produktnavn)
Output: Desktop/flaggede_til_manuel.csv + summary. READ-ONLY."""
import sys, os, io, zipfile, csv, re, json
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def clean(t):
    return re.sub(r"\s+", " ", re.sub(r"(?i)\bvidaxl\b", "", t or "")).strip()

def housestyle(t):
    return " ".join(w[:1].upper() + w[1:] if w else w for w in clean(t).split())

COLOR_UNIVERSE = set()   # fyldes i main: alle item_variant color-værdier på tværs af kataloget

def strip_axes(title, values, strip_colors=False):
    t = " " + title.lower() + " "
    vals = list(values)
    if strip_colors:
        vals = vals + sorted(COLOR_UNIVERSE, key=len, reverse=True)  # længste farve-ord først
    for v in vals:
        if not v:
            continue
        vn = re.sub(r"\s*x\s*", "x", v.lower().strip())
        for cand in {v.lower().strip(), vn, v.lower().split(",")[0].strip()}:
            if len(cand) > 1:
                t = re.sub(r"(?<=\W)" + re.escape(cand) + r"(?=\W)", " ", t)
    return housestyle(re.sub(r"\s+", " ", t).strip(" -,·"))

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

    # byg farve-univers = ALLE item_variant color-værdier på tværs af kataloget
    global COLOR_UNIVERSE
    for s in feed:
        c = (ME.OPTS.get(s) or {}).get("color")
        if c and len(c) > 2:
            COLOR_UNIVERSE.add(c.lower().strip())
    print(f"farve-univers: {len(COLOR_UNIVERSE)} unikke farve-ord")

    cat = Counter(); flagged = []
    for mid, skus in bym.items():
        live = [s for s in skus if s in feed]
        if not live:
            continue
        opts = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        axvals = defaultdict(set)
        for s in live:
            for k, v in opts[s].items():
                axvals[k].add(v)
        axes = sorted({k for k, vv in axvals.items() if len(vv) > 1})
        base_sku = max(live, key=lambda s: len(opts[s]))
        avals = [opts[base_sku].get(a) for a in axes] or list(opts[base_sku].values())
        has_color = "color" in axes
        title = strip_axes(clean(feed[base_sku]), avals, strip_colors=has_color) or ""
        # leak-check: KUN reelle akse-værdier (ikke faste attributter/mål) + farve-univers hvis farve-akse
        allvals = {v for a in axes for v in axvals[a]}
        if has_color:
            allvals |= COLOR_UNIVERSE
        tl = " " + title.lower() + " "
        leak = [v for v in allvals if len(v) > 3 and re.search(r"(?<=\W)" + re.escape(v.lower().split(',')[0]) + r"(?=\W)", tl)]
        reasons = []
        if len(axes) > 3:
            reasons.append(f"over_3_akser ({len(axes)}: {axes})")
        if not title or len(title) < 4:
            reasons.append("titel_tom")
        elif not re.search(r"[a-zæøå]{3}", title.lower()):
            reasons.append("titel_kun_maal")
        if leak:
            reasons.append(f"titel_har_akse ({leak[:3]})")
        if reasons:
            for r in reasons:
                cat[r.split(" ")[0]] += 1
            flagged.append({"mid": mid, "titel": title, "akser": axes, "n": len(live),
                            "grunde": " | ".join(reasons), "feed": clean(feed[base_sku])[:55]})

    out = r"C:\Users\APC\Desktop\flaggede_til_manuel.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f); w.writerow(["master_pid", "genereret_titel", "akser", "n_varianter", "grunde", "feed_titel_eksempel"])
        for x in flagged:
            w.writerow([x["mid"], x["titel"], " · ".join(x["akser"]), x["n"], x["grunde"], x["feed"]])
    print(f"=== {len(bym)} master_pids | FLAGGET til manuel: {len(flagged)} ===")
    for k, n in cat.most_common():
        print(f"  {n:5d}  {k}")
    print(f"\n  CSV: {out}")
    json.dump(flagged, open("output/manual_flagged.json", "w", encoding="utf-8"), ensure_ascii=False)

if __name__ == "__main__":
    main()
