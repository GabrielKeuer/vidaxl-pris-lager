"""KORREKT arbejdsgang (ingen LLM, ingen opfundet logik):
  1) gruppér på master_pid (source of truth)  2) varianter/options fra item_variant (scrapet)
  3) titel = feed-titel MINUS item_variant-akse-værdier  4) SKUs med samme akse-kombo = nær-identiske
     produkter → separate produkter (samme titel OK, ikke dubletter)
Output: MASTER-SHEET (Desktop CSV) — én række pr. SKU med produkt, titel, akser+værdier, variant-titel."""
import sys, os, io, zipfile, csv, re, json
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def clean(t):
    return re.sub(r"\s+", " ", re.sub(r"(?i)\bvidaxl\b", "", t or "")).strip()

def housestyle(t):
    return " ".join(w[:1].upper() + w[1:] if w else w for w in clean(t).split())

COLOR_UNIVERSE = set()   # alle item_variant color-værdier på tværs af kataloget

def strip_axes(title, values, strip_colors=False):
    """Fjern item_variant-akse-værdier fra feed-titlen. Hvis farve er en akse: fjern ETHVERT farve-ord
    (fra farve-universet), da feed-titlens farve-ord ofte afviger fra netop denne SKU's item_variant-værdi."""
    t = " " + title.lower() + " "
    vals = list(values) + (sorted(COLOR_UNIVERSE, key=len, reverse=True) if strip_colors else [])
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
            feed[s] = {"t": r.get("Title") or "", "b2b": r.get("B2B price"), "stock": r.get("Stock")}
    global COLOR_UNIVERSE
    for s in feed:
        c = (ME.OPTS.get(s) or {}).get("color")
        if c and len(c) > 2:
            cl = c.lower().strip()
            COLOR_UNIVERSE.add(cl)
            if cl.endswith("t") and len(cl) > 6:
                COLOR_UNIVERSE.add(cl[:-1])   # egetræsfarvet → egetræsfarve (feed-titlens form)
    print(f"master_pids: {len(bym)} | feed: {len(feed)} | farve-univers: {len(COLOR_UNIVERSE)}")

    out = r"C:\Users\APC\Desktop\master_sheet.csv"
    cat = Counter(); rows = 0
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["master_pid", "produkt_nr", "produkt_titel", "n_varianter", "akse_navne",
                    "denne_variant_options", "sku", "vidaXL_feed_titel", "titel_status", "b2b", "lager"])
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
            # titel = feed strippet for akse-værdier (brug en SKU der har alle akser hvis muligt)
            base_sku = max(live, key=lambda s: len(opts[s]))
            all_axis_vals = [opts[base_sku].get(a) for a in axes] or list(opts[base_sku].values())
            title = strip_axes(clean(feed[base_sku]["t"]), all_axis_vals, strip_colors="color" in axes) or housestyle(feed[base_sku]["t"])
            # gruppér i produkter: samme akse-kombo → separate produkter (nær-identiske)
            prod_of = {}; combo_count = defaultdict(int)
            for s in live:
                combo = tuple(opts[s].get(a, "") for a in axes)
                prod_of[s] = combo_count[combo]     # 0=hovedprodukt, 1=2. næsten-ens produkt, ...
                combo_count[combo] += 1
            nprod = max(prod_of.values()) + 1 if live else 1
            n_extra = sum(1 for c in combo_count.values() if c > 1)
            cat["multi" if axes else "single" if len(live) == 1 else "no_axes"] += 1
            if not title or len(title) < 3:
                cat["titel_mangler"] += 1
            # rækker
            per_prod = defaultdict(list)
            for s in live:
                per_prod[prod_of[s]].append(s)
            for pnr, plist in sorted(per_prod.items()):
                for s in plist:
                    ov = " · ".join(f"{a}={opts[s][a]}" for a in axes if opts[s].get(a)) or ("single" if len(live) == 1 else "?")
                    tstat = "ok" if (title and len(title) >= 3) else "MANGLER_TITEL"
                    if nprod > 1:
                        tstat += f" | {nprod} nær-ens produkter (samme titel)"
                    w.writerow([mid, pnr + 1, title, len(plist), " · ".join(axes), ov, s,
                                clean(feed[s]["t"]), tstat, feed[s]["b2b"], feed[s]["stock"]])
                    rows += 1
    print(f"\n✓ MASTER-SHEET: {rows} rækker (SKU) → {out}")
    for k, n in cat.most_common():
        print(f"  {n:6d}  {k}")

if __name__ == "__main__":
    main()
