"""KOMPLET FEED (source of truth for den store merge). Én række pr. SKU. KUN beslutnings-lag:
product_key · titel · option1/2/3 (navn+værdi) · SKU. Indhold (billeder/beskrivelse/pris) hentes
fra vidaXL-feedet ved merge — IKKE her. Gruppering=master_pid, varianter=item_variant, titel=strippet
feed-titel + de 32 manuelle fixes (manual_fixes.json). Nær-identiske (samme kombo)=separate produkter.
Kører til sidst en fuld AUDIT af feedet. Output: Desktop/komplet_feed.csv + output/complete_feed.json."""
import sys, os, io, zipfile, csv, re, json
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, __import__("os").environ.get("DROPXL_SCRIPTS", r"C:\Users\APC\dropxl-product-automation\scripts"))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import title_rules as TR   # FEED_SPELLING (Utrækkelig→Udtrækkelig), pcs→dele, casing (LED/PVC/MDF)

def clean(t):
    return re.sub(r"\s+", " ", re.sub(r"(?i)\bvidaxl\b", "", t or "")).strip()

def housestyle(t):
    t = clean(t)
    # citationstegn OM ord fjernes ("Lyon"→Lyon), men tomme-mål bevares (0,5" = tommer)
    t = re.sub(r'(?<=[a-zæøåA-ZÆØÅ])["“”]|["“”](?=[a-zæøåA-ZÆØÅ])', "", t)
    t = TR.fix_feed_spelling(t)          # systematiske feed-fejl (case-bevarende)
    t = TR.fix_pcs_to_dele(t)            # pcs → dele
    t = " ".join(w[:1].upper() + w[1:] if w else w for w in t.split())  # Title Case
    t, _ = TR.fix_casing(t)              # LED/TV/USB/PVC/MDF/WPC osv. → versaler
    t = re.sub(r"\b(\w+)(?:\s+\1)+\b", r"\1", t, flags=re.IGNORECASE)  # kollapsér gentagne ord (Træ Træ, Cm Cm)
    t = re.sub(r"(?:\s+(?:og|med|til|på|af|for))+\s*$", "", t, flags=re.IGNORECASE)  # hængende bindeord
    return re.sub(r"\s+", " ", t).strip(" -,·")

COLOR_UNIVERSE = set()
_COLOR_RES = []   # LISTE af kompilerede chunk-regexes (én stor regex matcher ikke i Python re ved >~5000 ord)
AXIS_LABELS = {}   # master_pid → {item_variant-nøgle: vidaXL's eget akse-navn} (fra scrape)

def build_color_re():
    """Præ-kompilér farve-universet i CHUNKS (Python re matcher upålideligt i én kæmpe alternation)."""
    global _COLOR_RES
    words = sorted((re.escape(c) for c in COLOR_UNIVERSE if len(c) >= 2), key=len, reverse=True)
    _COLOR_RES = []
    # FORSTAVELSE = mørk/lys foran en farve (feed: "Mørk blå" mens item_variant: "Mørkeblå" → strip begge)
    pre = r"(?:mørke?\s+|lyse?\s+)?"
    # SUFFIKS = danske tillægsords-bøjninger: hvid→hvide/hvidt, brun→brune, grøn→grønne (ne),
    # creme→cremefarvede. Så vi kun har grund-farver i universet.
    suf = r"(?:e|t|s|de|ne|ede|nt|farvet|farvede|farve)?"
    for i in range(0, len(words), 800):
        chunk = words[i:i + 800]
        _COLOR_RES.append(re.compile(r"(?<=\W)" + pre + r"(?:" + "|".join(chunk) + r")" + suf + r"(?=\W)"))

_QTY = r"stk\.?|dele|delt|personers?|ruller|pk|pcs|sæt|pakke"
# ét sammenhængende mål: NxN(xN)... evt. + NxN, evt. med enhed cm/mm/m
_DIM = (r"\d+(?:[.,]\d+)?(?:\s*[x×]\s*\d+(?:[.,]\d+)?)+(?:\s*(?:cm|mm|m))?"
        r"(?:\s*\+\s*\d+(?:[.,]\d+)?(?:\s*[x×]\s*\d+(?:[.,]\d+)?)+(?:\s*(?:cm|mm|m))?)?")

def strip_axes(title, values, strip_colors=False, strip_dims=False):
    t = " " + re.sub(r"\s+", " ", title.lower()) + " "
    # (1) MÅL-STRIP hvis en størrelse-akse varierer → fjern ALLE mål-mønstre (robust for format/enhed)
    if strip_dims:
        t = re.sub(r"(?<=\s)ø\s*" + _DIM + r"(?=\s)", " ", t)            # Ø-dimensioner: "Ø40x2,5 cm"
        t = re.sub(r"(?<=\s)\(?\d[\d.,\-–/x×() ]*\)?\s*(?:cm|mm)(?=\s)", " ", t)  # sammensat/range m. cm/mm
        t = re.sub(r"(?<=\s)" + _DIM + r"(?=\s)", " ", t)
        t = re.sub(r"(?<=\s)\d+(?:[.,]\d+)?[-/]\d+(?:[.,]\d+)?\s*(?:cm|mm|m)(?=\s)", " ", t)  # tykkelse "7/9 mm"
        t = re.sub(r"(?<=\s)\d+(?:[.,]\d+)?\s*(?:cm|mm)(?=\s)", " ", t)   # enkelt "N cm"/"N mm"
        t = re.sub(r"(?<=\s)\d+(?:[.,]\d+)?\s*[µμΜ]m(?=\s)", " ", t)      # tykkelse i mikrometer "200 μm"
        t = re.sub(r"(?<=\s)ø\s*\d+(?:[.,]\d+)?\s*(?:cm|mm)?(?=\s)", " ", t)  # Ø N cm
    # (2) akse-værdier
    for v in values:
        if not v:
            continue
        vl = v.lower().strip()
        if re.fullmatch(r"\d+", vl):
            # "N stk" (antal + enhed)
            t = re.sub(r"(?<=\s)" + vl + r"[\s\-]*(?:" + _QTY + r")(?=\s)", " ", t)
            # bart antal-tal ("10 middagsservietter") — men IKKE hvis det er del af et mål
            # (ikke foran x/enhed, ikke efter x, ikke del af større tal). Enhed = HELT ord (m må ikke
            # ramme "middagsservietter"), derfor (?![a-zæøå]) efter enheden.
            t = re.sub(r"(?<!\d)(?<![x×]\s)(?<=\s)" + vl +
                       r"(?=\s)(?!\s*(?:[x×]|(?:cm|mm|m|l|kg|g|ml|cl|pr)(?![a-zæøå])))", " ", t)
        else:
            vn = re.sub(r"\s*x\s*", "x", vl)
            cands = {vl, vn, vl.split(",")[0].strip()}
            if "-" in vl:
                cands.add(vl.replace("-", " "))     # sonoma-eg → sonoma eg
            for cand in cands:
                if len(cand) > 1:
                    t = re.sub(r"(?<=\W)" + re.escape(cand) + r"(farvet|farve)?(?=\W)", " ", t)
    if strip_colors:
        for cre in _COLOR_RES:
            t = cre.sub(" ", t)
    return housestyle(re.sub(r"\s+", " ", t).strip(" -,·"))

def nat_val(v):
    """Natural-sort-nøgle: tal stigende (0,1,2,90,100), derefter alfabetisk; værdier uden tal alfabetisk."""
    nums = re.findall(r"\d+\.?\d*", v or "")
    return (0, [float(n) for n in nums], (v or "").lower()) if nums else (1, [], (v or "").lower())

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
    # generiske MATERIALE-ord: må ALDRIG i det globale farve-univers (vidaXL blander materiale ind i
    # 'color'-feltet, fx farve='Træ') — ellers stripper de det FASTE materiale fra titler ("Konstrueret træ").
    # Den specifikke variant-farve strippes stadig via produktets EGNE akse-værdier.
    MATERIAL_STOP = {"træ", "stof", "metal", "stål", "jern", "glas", "plast", "plastik", "beton", "læder",
                     "kunstlæder", "rattan", "polyrattan", "bambus", "aluminium", "polyester", "keramik",
                     "resin", "marmor", "gummi", "kork", "filt", "skum", "krydsfiner", "spånplade", "finér",
                     "velour", "fløjl", "bomuld", "jute", "sten", "kobber", "messing", "krom", "zink"}
    global COLOR_UNIVERSE
    def add_color_forms(cl):
        """Tilføj farve-VÆRDIEN + space/bindestreg-varianter. Bøjning (hvid→hvide) håndteres i
        regex-SUFFIKSET (build_color_re), ikke ved for-generering."""
        if not cl or len(cl) < 3 or cl in MATERIAL_STOP:
            return
        forms = {cl, cl.replace("-", " "), cl.replace(" ", ""), cl.replace("-", "")}
        # vidaXL sammensætter "mørk/lys X" til ét ord med -e (Mørk lilla → mørkelilla; Lys brun → lysebrun)
        for p2, joined in (("mørk ", "mørke"), ("lys ", "lyse")):
            if cl.startswith(p2):
                rest = cl[len(p2):]
                forms |= {joined + rest, joined + " " + rest}
        for f in forms:
            if f and len(f) >= 2 and f not in MATERIAL_STOP:
                COLOR_UNIVERSE.add(f)
    for s in feed:
        add_color_forms((ME.OPTS.get(s) or {}).get("color", ""))
    # curated grund-farver (garanterer hvid/brun/creme/grøn/sort... selv hvis katalog kun har sammensatte)
    for c in getattr(TR, "COLOR_LEX", set()):
        cl = c.lower().strip()
        if cl and len(cl) >= 2 and cl not in MATERIAL_STOP:
            COLOR_UNIVERSE.add(cl)
    # TRÆFINISH-ord (item_variant trunkerer "Sort Eg"→"Sort", så feed-titlens "eg" overlever)
    for w in ("eg", "sonoma", "artisan", "røget"):
        COLOR_UNIVERSE.add(w)
    # ekstra ny-danske/sammensatte farver feedet bruger men item_variant/COLOR_LEX mangler
    for w in ("skygrå", "himmelblå", "flaskegrøn", "nougat", "champagne", "gyldenbrun", "grafitgrå",
              "stengrå", "perlehvid", "råhvid", "antikbrun", "støvet", "pudderrosa", "koralrød",
              "smaragdgrøn", "safirblå", "rustrød", "karrygul", "lavendel", "syren"):
        COLOR_UNIVERSE.add(w)
    build_color_re()
    global AXIS_LABELS
    if os.path.exists("output/axis_labels.json"):
        AXIS_LABELS = json.load(open("output/axis_labels.json", encoding="utf-8"))
    print(f"akse-labels: {len(AXIS_LABELS)} master_pids")
    manual = load_manual()
    print(f"master_pids: {len(bym)} | feed: {len(feed)} | manuelle: {len(manual)}")

    # SPLIT-OVERRIDES: godkendte master_pids der blander forskellige produkter (fx M3042959 køkkenskabe,
    # M3003939 sidebord+havestole). For disse klynges SKU'erne efter produkt-NAVN og hver klynge bliver
    # sit eget produkt (egne akser + egen titel). split_overrides.json = liste af godkendte master_pids.
    def _toks(s):
        return set(re.findall(r"[a-zæøå0-9]+", (s or "").lower()))
    def _cmatch(a, b):
        return a == b or (len(a) >= 4 and b.endswith(a)) or (len(b) >= 4 and a.endswith(b))
    def _same_prod(a, b):
        ta, tb = _toks(a), _toks(b)
        if not ta or not tb:
            return True
        sm, bg = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
        if all(any(_cmatch(x, y) for y in bg) for x in sm):
            return True
        return len(ta & tb) / len(ta | tb) >= 0.6
    def _noun_base(s, o, ax):
        b = strip_axes(clean(feed[s]), [o.get(k) for k in ax], strip_colors=True, strip_dims=True)
        b = re.sub(r"\b\d+\s*(?:stk|dele|pcs|sæt|pk|personers?|sæders?)\.?\b", " ", b.lower())
        return " ".join(w for w in re.findall(r"[a-zæøå]+", b) if w not in MATERIAL_STOP and len(w) > 2)
    split_appr = json.load(open("output/split_overrides.json", encoding="utf-8")) if os.path.exists("output/split_overrides.json") else []
    split_groups = {}
    for mid in split_appr:
        live = [s for s in bym.get(mid, []) if s in feed]
        if len(live) < 2:
            continue
        o = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        axv = defaultdict(set)
        for s in live:
            for k, v in o[s].items():
                axv[k].add(v)
        ax = sorted(k for k, vv in axv.items() if len(vv) > 1)
        nb = {s: _noun_base(s, o[s], ax) for s in live}
        cl = []
        for s in live:
            for g in cl:
                if _same_prod(nb[s], nb[g[0]]):
                    g.append(s); break
            else:
                cl.append([s])
        if len(cl) > 1:
            split_groups[mid] = sorted(cl, key=len, reverse=True)
    if split_groups:
        print(f"split-overrides: {len(split_groups)} master_pids → {sum(len(v) for v in split_groups.values())} produkt-grupper")

    # arbejdsliste: (mid, live-SKUs, produkt-nøgle-base). split-master → én enhed pr. klynge.
    worklist = []
    for mid, skus in bym.items():
        live = [s for s in skus if s in feed]
        if not live:
            continue
        if mid in split_groups:
            for gi, grp in enumerate(split_groups[mid]):
                worklist.append((mid, grp, mid if gi == 0 else f"{mid}_split{gi+1}"))
        else:
            worklist.append((mid, live, mid))

    products = []   # {key, title, specs:[(name,[keys])], variants:[{sku, values:{name:val}}]}
    for mid, live, keybase in worklist:
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
            # navn: Farve for swatch, ellers vidaXL's EGET akse-label (scrape), ellers inferens
            lbl = AXIS_LABELS.get(mid, {}) or {}
            specs = [("Farve" if k == "color" else (lbl.get(k) or option_name(k, axvals[k])), [k]) for k in axes]
            title = None
        # titel (hvis ikke manuel) = base-SKU's feed-titel MINUS ALLE varianters akse-værdier (union)
        if not title:
            base = max(live, key=lambda s: len(opts[s]))
            avals = []
            for _, ks in specs:
                for k in ks:
                    for s in live:
                        x = opts[s].get(k)
                        if x and x not in avals:
                            avals.append(x)
            if not avals:
                avals = list(opts[base].values())
            SIZE_AXES = {"Størrelse", "Højde", "Bredde", "Længde", "Dybde", "Bordlængde", "Diameter", "Størrelse 2", "Tykkelse"}
            strip_dims = any(nm in SIZE_AXES for nm, _ in specs)
            title = strip_axes(clean(feed[base]), avals, strip_colors=any("color" in ks for _, ks in specs), strip_dims=strip_dims) or housestyle(feed[base])
        # KOLLAPS redundante akser: to specs med IDENTISKE værdier på tværs af ALLE SKUs = samme akse
        # (vidaXL lagrer redundant, fx variationAttribute1+2 = Sofa/sofabord/spisebord) → behold kun den
        # første. Sikrer også unikke option-navne (samme navn + andre værdier → gør unikt).
        def _spec_vals(ns):
            _, ks = ns
            return tuple(" ".join(opts[s].get(k, "") for k in ks).strip() for s in live)
        uniq = []; seen_vals = set(); seen_names = set()
        for ns in specs:
            vt = _spec_vals(ns)
            if vt in seen_vals:
                continue
            seen_vals.add(vt)
            nm = ns[0]
            if nm in seen_names:
                c = 2
                while f"{nm} {c}" in seen_names:
                    c += 1
                ns = (f"{nm} {c}", ns[1]); nm = ns[0]
            seen_names.add(nm)
            uniq.append(ns)
        specs = uniq
        # KOLONNE-RÆKKEFØLGE: Farve → Størrelse → resten (vilkårligt)
        specs = sorted(specs, key=lambda ns: 0 if ns[0] == "Farve" else (1 if ns[0] in SIZE_AXES else 2))
        # per-SKU option-værdier (kapitalisér første bogstav — "sofabord"→"Sofabord", "90 cm" urørt)
        def cap1(v):
            v = v.strip()
            for i, ch in enumerate(v):
                if ch.isalpha():
                    return v[:i] + ch.upper() + v[i+1:]
            return v
        def sku_values(s):
            return {nm: cap1(" ".join(opts[s].get(k, "") for k in ks).strip()) for nm, ks in specs}
        names = [nm for nm, _ in specs]
        # REGEL 1: INGEN akser → hver SKU er sit eget SINGLE-produkt med SIN EGEN fulde feed-titel
        # (genuine singler + no-axes-master som plænetromle, hvor SKUs er forskellige produkter)
        if not names:
            for oi, s in enumerate(sorted(live)):
                products.append({"key": f"{keybase}" + (f"_s{oi+1}" if oi else ""), "mid": mid,
                                 "title": housestyle(clean(feed[s])), "specs": [],
                                 "variants": [{"sku": s, "values": {}, "pos": 1}],
                                 "manual": bool(fix), "single": True, "orphan": (oi > 0)})
            continue
        # MULTI: split samme kombo → separate produkter (nær-identiske). ORPHAN: SKU uden akse-værdi →
        # eget single-produkt med SIN EGEN feed-titel (ikke master-titlen).
        combo_seen = defaultdict(int); byprod = defaultdict(list); orphans = []
        for s in live:
            vals = sku_values(s)
            if any(not vals[nm] for nm in names):
                orphans.append(s); continue
            combo = tuple(vals[nm] for nm in names)
            pnr = combo_seen[combo]; combo_seen[combo] += 1
            byprod[pnr].append((s, vals))
        for pnr, variants in sorted(byprod.items()):
            variants.sort(key=lambda sv: tuple(nat_val(sv[1].get(n, "")) for n in names))
            products.append({"key": f"{keybase}" + (f"_{pnr+1}" if pnr else ""), "mid": mid,
                             "title": title, "specs": names,
                             "variants": [{"sku": s, "values": v, "pos": i + 1} for i, (s, v) in enumerate(variants)],
                             "manual": bool(fix)})
        for oi, s in enumerate(orphans):
            products.append({"key": f"{keybase}_s{oi+1}", "mid": mid, "title": housestyle(clean(feed[s])), "specs": [],
                             "variants": [{"sku": s, "values": {}, "pos": 1}],
                             "manual": bool(fix), "single": True, "orphan": True})
    # ANVEND manuelle titel-overrides (residuale flaggede) + markér review
    ovr = json.load(open("output/title_overrides.json", encoding="utf-8")) if os.path.exists("output/title_overrides.json") else {}
    n_ov = 0
    for p in products:
        o = ovr.get(p["key"])
        if o and isinstance(o, dict):
            if o.get("title"):
                p["title"] = o["title"]; n_ov += 1
            if o.get("review"):
                p["review"] = True
    print(f"titel-overrides anvendt: {n_ov}")

    # skriv CSV
    out = r"C:\Users\APC\Desktop\komplet_feed.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_key", "titel", "option1_navn", "option1_vaerdi", "option2_navn",
                    "option2_vaerdi", "option3_navn", "option3_vaerdi", "sku", "variant_position", "n_varianter", "manuel"])
        for p in products:
            names = p["specs"][:3]
            for v in p["variants"]:
                row = [p["key"], p["title"]]
                for i in range(3):
                    if i < len(names):
                        row += [names[i], v["values"].get(names[i], "")]
                    else:
                        row += ["", ""]
                row += [v["sku"], v["pos"], len(p["variants"]), "JA" if p["manual"] else ""]
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
