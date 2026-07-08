"""KOMPLET FEED (source of truth for den store merge). Én række pr. SKU. KUN beslutnings-lag:
product_key · titel · option1/2/3 (navn+værdi) · SKU. Indhold (billeder/beskrivelse/pris) hentes
fra vidaXL-feedet ved merge — IKKE her. Gruppering=master_pid, varianter=item_variant, titel=strippet
feed-titel + de 32 manuelle fixes (manual_fixes.json). Nær-identiske (samme kombo)=separate produkter.
Kører til sidst en fuld AUDIT af feedet. Output: Desktop/komplet_feed.csv + output/complete_feed.json."""
import sys, os, io, zipfile, csv, re, json
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def clean(t):
    return re.sub(r"\s+", " ", re.sub(r"(?i)\bvidaxl\b", "", t or "")).strip()

def housestyle(t):
    return " ".join(w[:1].upper() + w[1:] if w else w for w in clean(t).split())

COLOR_UNIVERSE = set()

def strip_axes(title, values, strip_colors=False):
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

def option_name(key, values):
    if key == "color":
        return "Farve"
    vl = [str(v).lower() for v in values]
    if all(re.search(r"\d", v) and re.search(r"(cm|mm|\bm\b|x)", v) for v in vl):
        return "Størrelse"
    if all("kg" in v for v in vl):
        return "Vægt"
    if all(re.match(r"^\d+$", v.strip()) for v in vl):
        return "Antal"
    if any(("personer" in v or "sædet" in v or "dele" in v) for v in vl):
        return "Størrelse"
    if any("g/m" in v for v in vl):
        return "Kvalitet"
    if any(("træ" in v or "læder" in v or "stål" in v or "stof" in v or "velour" in v) for v in vl):
        return "Materiale"
    return "Model"

def load_manual():
    raw = json.load(open("output/manual_fixes.json", encoding="utf-8"))
    fixes = {}
    for k, v in raw.items():
        if k.startswith("_"):
            if isinstance(v, dict):
                for mk, mv in v.items():
                    if mk.startswith("_"):
                        continue
                    fixes[mk] = mv       # keep-gruppen: kun rene option-navne
            continue
        fixes[k] = v
    return fixes

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
    global COLOR_UNIVERSE
    for s in feed:
        c = (ME.OPTS.get(s) or {}).get("color")
        if c and len(c) > 2:
            cl = c.lower().strip(); COLOR_UNIVERSE.add(cl)
            if cl.endswith("t") and len(cl) > 6:
                COLOR_UNIVERSE.add(cl[:-1])
    manual = load_manual()
    print(f"master_pids: {len(bym)} | feed: {len(feed)} | manuelle: {len(manual)}")

    products = []   # {key, title, specs:[(name,[keys])], variants:[{sku, values:{name:val}}]}
    for mid, skus in bym.items():
        live = [s for s in skus if s in feed]
        if not live:
            continue
        opts = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        fix = manual.get(mid)
        # bestem akse-specs (navn → nøgle-liste)
        if fix and "axes" in fix:
            drop = set(fix.get("drop", []))
            specs = []
            for keyspec, nm in fix["axes"].items():
                keys = [k for k in keyspec.split("+") if k not in drop]
                if keys:
                    specs.append((nm, keys))
            title = fix.get("title")
        else:
            axvals = defaultdict(set)
            for s in live:
                for k, v in opts[s].items():
                    axvals[k].add(v)
            axes = sorted(k for k, vv in axvals.items() if len(vv) > 1)
            specs = [(option_name(k, axvals[k]), [k]) for k in axes]
            title = None
        # titel (hvis ikke manuel)
        if not title:
            base = max(live, key=lambda s: len(opts[s]))
            avals = [v for k in [k for _, ks in specs for k in ks] for v in [opts[base].get(k)] if v] or list(opts[base].values())
            title = strip_axes(clean(feed[base]), avals, strip_colors=any("color" in ks for _, ks in specs)) or housestyle(feed[base])
        # per-SKU option-værdier
        def sku_values(s):
            return {nm: " ".join(opts[s].get(k, "") for k in ks).strip() for nm, ks in specs}
        # split: samme kombo → separate produkter (nær-identiske, samme titel)
        combo_seen = defaultdict(int); byprod = defaultdict(list)
        for s in live:
            vals = sku_values(s)
            combo = tuple(vals[nm] for nm, _ in specs)
            pnr = combo_seen[combo]; combo_seen[combo] += 1
            byprod[pnr].append((s, vals))
        for pnr, variants in sorted(byprod.items()):
            products.append({"key": f"{mid}" + (f"_{pnr+1}" if pnr else ""), "mid": mid,
                             "title": title, "specs": [nm for nm, _ in specs],
                             "variants": [{"sku": s, "values": v} for s, v in variants],
                             "manual": bool(fix)})
    # skriv CSV
    out = r"C:\Users\APC\Desktop\komplet_feed.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_key", "titel", "option1_navn", "option1_vaerdi", "option2_navn",
                    "option2_vaerdi", "option3_navn", "option3_vaerdi", "sku", "n_varianter", "manuel"])
        for p in products:
            names = p["specs"][:3]
            for v in p["variants"]:
                row = [p["key"], p["title"]]
                for i in range(3):
                    if i < len(names):
                        row += [names[i], v["values"].get(names[i], "")]
                    else:
                        row += ["", ""]
                row += [v["sku"], len(p["variants"]), "JA" if p["manual"] else ""]
                w.writerow(row)
    json.dump(products, open("output/complete_feed.json", "w", encoding="utf-8"), ensure_ascii=False)
    print(f"\n✓ KOMPLET FEED: {sum(len(p['variants']) for p in products)} SKU-rækker, {len(products)} produkter → {out}")

    # ===== FULD AUDIT =====
    print("\n=== AUDIT AF FEEDET ===")
    iss = Counter(); ex = defaultdict(list)
    seen_sku = set(); dup_sku = 0
    for p in products:
        specs = p["specs"]
        if len(specs) > 3:
            iss["over_3_options"] += 1; ex["over_3_options"].append(p["key"])
        if not p["title"] or len(p["title"]) < 3:
            iss["titel_tom"] += 1; ex["titel_tom"].append(p["key"])
        combos = [tuple(v["values"].get(n, "") for n in specs) for v in p["variants"]]
        if len(combos) != len(set(combos)):
            iss["dup_kombo_i_produkt"] += 1; ex["dup_kombo_i_produkt"].append(p["key"])
        if specs and any(not all(v["values"].get(n, "") for n in specs) for v in p["variants"]):
            iss["tom_option_vaerdi"] += 1; ex["tom_option_vaerdi"].append(p["key"])
        # titel indeholder stadig en akse-værdi?
        tl = " " + p["title"].lower() + " "
        avset = {v["values"].get(n, "") for v in p["variants"] for n in specs}
        leak = [a for a in avset if a and len(a) > 3 and (" " + a.lower() + " ") in tl]
        if leak:
            iss["titel_har_option"] += 1; ex["titel_har_option"].append(f'{p["key"]}:{leak[0][:16]}')
        for v in p["variants"]:
            if v["sku"] in seen_sku:
                dup_sku += 1
            seen_sku.add(v["sku"])
    print(f"produkter: {len(products)} | SKU'er: {len(seen_sku)} | SKU på >1 produkt: {dup_sku}")
    if not iss:
        print("  ✅ INGEN fejl fundet")
    for k, n in iss.most_common():
        print(f"  ⚠ {k}: {n}  {ex[k][:4]}")
    # option-navn-fordeling
    nm = Counter(n for p in products for n in p["specs"])
    print("\noption-navne brugt:", dict(nm.most_common()))

if __name__ == "__main__":
    main()
