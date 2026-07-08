"""NY GRUPPERINGS-ALGORITME (prototype, read-only). Per PID-gruppe:
  1) strip hver SKU's egne VARIERENDE options fra feed-titlen → residual
  2) kanonisk nøgle = residual i småt + ord sorteret (case + ordrækkefølge ligegyldig)
  3) grupper SKU'er med samme nøgle (partiel: 8 af 10 matcher → de 8 grupperes, resten singles)
  4) værn: en gruppe dannes kun hvis den fælles titel har et PRODUKT-NAVNEORD; ellers singles m. fuld titel
Kør: SCOPE=180 (kun live-berørte) eller SCOPE=all. Output: konsol + Desktop/regroup_<scope>.csv"""
import sys, os, io, zipfile, csv, json, re
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, r"C:\Users\APC\dropxl-product-automation\scripts")
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import build_complete_feed as B
import scope_split as SS

SCOPE = os.environ.get("SCOPE", "180")
SIZE_AXES = {"Størrelse", "Højde", "Bredde", "Længde", "Dybde", "Bordlængde", "Diameter", "Størrelse 2", "Tykkelse"}
PREP = {"til", "med", "og", "i", "på", "af", "for", "uden", "samt", "den", "det", "en", "et"}
ADJ = {"massivt", "massiv", "konstrueret", "udendørs", "indendørs", "foldbar", "foldbart", "justerbar",
       "sammenklappelig", "imprægneret", "ubehandlet", "hærdet", "pulverlakeret", "galvaniseret", "rustfrit",
       "vævet", "blødt", "flydende", "høj", "lav", "lille", "stor", "rund", "firkantet", "oval", "manuel"}

def canonical(r):
    """intelligent nøgle: format-forskelle er ligegyldige (sonoma-eg=sonoma eg, 39 x 35=39x35,
    39,5=39.5), kun reelle ord + rækkefølge-uafhængig."""
    r = r.lower().replace("-", " ").replace(",", ".")
    r = re.sub(r"\s*[x×]\s*", "x", r)          # 39 x 35 x 80 → 39x35x80
    toks = re.findall(r"[a-zæøå0-9.]+", r)
    toks = [t.strip(".") for t in toks if t.strip(".")]
    return " ".join(sorted(toks))

def _is_mat(w):
    return w in SS.MATERIAL_STOP or w.endswith("træ") or w.endswith("læder")

def valid_group_title(r):
    """gruppe-titel er gyldig KUN hvis den har et rigtigt PRODUKT-navneord og ikke starter med præposition
    (så 'Til Haven Med Hynde' / 'Aluminium Antracitgrå' / 'Massivt Akacietræ' bliver singles i stedet)."""
    toks = r.lower().split()
    # spring ledende adjektiver over (Udendørs/Massivt...); hvis så en præposition følger → degenereret
    i = 0
    while i < len(toks) and (toks[i] in ADJ or _is_mat(toks[i])):
        i += 1
    if i >= len(toks) or toks[i] in PREP:
        return False
    for w in re.findall(r"[a-zæøå]+", r.lower()):
        if len(w) >= 3 and w not in PREP and w not in ADJ and not _is_mat(w) and w not in B.COLOR_UNIVERSE:
            return True
    return False

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
    nm = [f for f in z.namelist() if f.endswith(".csv")][0]
    feed = {}
    for r in csv.DictReader(io.TextIOWrapper(z.open(nm), encoding="utf-8")):
        s = str(r.get("SKU") or "").strip().replace(".0", "")
        if s:
            feed[s] = r.get("Title") or ""
    SS.setup_universe(feed)
    for w in ("cremehvid", "cremehvide", "råhvid", "gråhvid", "offwhite", "sølvgrå", "koksgrå"):
        B.COLOR_UNIVERSE.add(w)
    B.build_color_re()
    lbl = json.load(open("output/axis_labels.json", encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}

    if SCOPE == "180":
        target = set(json.load(open("output/pilot_check.json", encoding="utf-8"))["affected"])
    else:
        target = set(bym)

    products = []
    n_before = 0
    for mid in target:
        live = [s for s in bym.get(mid, []) if s in feed]
        if not live:
            continue
        n_before += 1
        opts = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        axv = defaultdict(set)
        for s in live:
            for k, v in opts[s].items():
                axv[k].add(v)
        axes = sorted(k for k, vv in axv.items() if len(vv) > 1)
        namef = lambda k: ("Farve" if k == "color" else (lbl.get(mid, {}).get(k) or B.option_name(k, axv[k])))
        has_color = "color" in axes
        has_size = any(namef(k) in SIZE_AXES for k in axes)
        # residual pr. SKU
        resid = {}
        for s in live:
            vals = [opts[s].get(k) for k in axes]
            resid[s] = B.strip_axes(B.clean(feed[s]), vals, strip_colors=has_color, strip_dims=has_size)
        # grupper efter kanonisk nøgle
        groups = defaultdict(list)
        for s in live:
            groups[canonical(resid[s])].append(s)
        for key, gsk in groups.items():
            # varierende akser INDEN for gruppen
            gav = defaultdict(set)
            for s in gsk:
                for k in axes:
                    gav[k].add(opts[s].get(k, ""))
            gaxes = [k for k in axes if len({x for x in gav[k] if x}) > 1]
            rep = Counter(resid[s] for s in gsk).most_common(1)[0][0]
            if len(gsk) >= 2 and gaxes and valid_group_title(rep):
                products.append({"mid": mid, "title": rep, "axes": [namef(k) for k in gaxes],
                                 "skus": gsk, "type": "variant"})
            else:
                for s in gsk:
                    products.append({"mid": mid, "title": B.housestyle(B.clean(feed[s])), "axes": [],
                                     "skus": [s], "type": "single"})

    nv = sum(1 for p in products if p["type"] == "variant")
    ns = sum(1 for p in products if p["type"] == "single")
    print(f"=== NY GRUPPERING (scope={SCOPE}) ===")
    print(f"  master_pids: {n_before} → produkter: {len(products)}  (variant: {nv}, single: {ns})")

    out = rf"C:\Users\APC\Desktop\regroup_{SCOPE}.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f); w.writerow(["master_pid", "type", "titel", "akser", "n_sku", "eksempel_sku"])
        for p in sorted(products, key=lambda z: (z["mid"], z["title"])):
            w.writerow([p["mid"], p["type"], p["title"], "+".join(p["axes"]), len(p["skus"]), p["skus"][0]])
    print(f"  ✓ {out}")
    if SCOPE == "180":
        print("\n--- alle nye produkter for de 180 (til slavisk review) ---")
        for p in sorted(products, key=lambda z: (z["mid"], z["title"])):
            print(f"  [{p['mid']}] {p['type'][:3]} {len(p['skus'])}SKU  \"{p['title']}\"  {('/'.join(p['axes'])) if p['axes'] else ''}")

if __name__ == "__main__":
    main()
