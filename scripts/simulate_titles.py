"""SIMULATION + korrekt titel-generering (fuld logik 1:1) mod det endelige katalog.
Bruger audit_titles.analyze (hele pipelinen) på autoritativ kilde:
  merge/behold → keeper's nuværende titel + merged options
  split        → variantens FEED-titel minus farve/størrelse (Model beholdes)
  single       → SKU'ens FEED-titel
Skriver C:\\Users\\APC\\Desktop\\catalog_titles_simulation.csv (original → genereret). READ-ONLY."""
import csv, io, json, os, re, sys, time, urllib.request, zipfile
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
from audit_titles import analyze          # HELE titel-pipelinen (guards, dangling-X, farve, casing, feed-spelling)
from bulk_repricing import _shop_gql
FEED_URL = "https://feed.vidaxl.io/api/v1/feeds/download/f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping.csv.zip"

def _clean_vidaxl(t):
    for v in ["vidaXL ", "vidaxl ", "VidaXL ", "VIDAXL ", "fra vidaXL", "vidaXL", "vidaxl"]:
        t = (t or "").replace(v, "")
    return t.strip()
def _cap(w): return w[0].upper() + w[1:].lower() if w else w
def _titlecase(t):  # feed-titler er lowercase → title-case, bindestregs-bevidst ('2-personers'→'2-Personers')
    return " ".join("-".join(_cap(p) for p in word.split("-")) for word in (t or "").split())
def prep_feed(t):
    return _titlecase(_clean_vidaxl(t))
_ENG_BAD = {"with","and","the","black","white","grey","gray","mirror","cabinet","chair","garden","wall","wood",
            "steel","bathroom","kitchen","folding","outdoor","cushion","frame","bench","shelf","table","drawer",
            "corner","middle","seat","cover","piece","clock","door","design","modern","spoon","fork","iron"}
def looks_bad(t):  # nuværende titel er ubrugelig (leading SKU-nummer eller engelsk) → brug feed-titlen
    if re.match(r"^\s*\d{4,}\b", t or ""): return True
    return sum(1 for w in re.findall(r"\b[a-z]+\b", (t or "").lower()) if w in _ENG_BAD) >= 2
def expand_opts(opts, skus, fcolors):
    """Tilføj feed-Color (autoritativ farve, samme kilde som titlen) + mellemrums-normaliserede
    størrelser til strip-sættet, så farve/størrelse fjernes selv ved format-mismatch."""
    fc = set(opts.get("Farve", []))
    for s in skus:
        c = (fcolors.get(s) or "").strip()
        if c:
            fc.add(c)  # hele farve-strengen, fx 'Natur og cremefarvet'
            for w in re.split(r"[\s/]+", c):
                if len(w.strip(".,")) >= 3 and w.lower() != "og":
                    fc.add(w.strip(".,"))  # enkelt-ord, fx 'cremefarvet'
    if fc: opts["Farve"] = sorted(fc)
    for name, vals in list(opts.items()):
        extra = {re.sub(r"\s*[x×]\s*", "x", v) for v in vals}
        opts[name] = sorted(set(vals) | {e for e in extra if e})
    return opts

def load_mapping():
    m = {}
    for l in open(r"C:\Users\APC\vidaxl-pris-lager\output\master_pid_map.jsonl", encoding="utf-8"):
        try: d = json.loads(l); m[d["sku"]] = d["master_pid"]
        except Exception: pass
    return m

def load_bearing():
    clean, messy = set(), set()
    def is_messy(v): return bool(re.search(r"\dx |\(\d+ stk|chair|corner|middle|table|footrest|seat", v, re.I)) or (v.count("|") + 1) > 8
    for r in csv.DictReader(open(r"C:\Users\APC\Desktop\bearing_variants.csv", encoding="utf-8-sig")):
        if r["beslutning"] == "split":
            (messy if is_messy(r["option_values"]) else clean).add(r["master_pid"])
    return clean, messy

_RUN = "mutation($q:String!){bulkOperationRunQuery(query:$q){bulkOperation{id status} userErrors{message}}}"
_STAT = "query{currentBulkOperation(type:QUERY){id status errorCode objectCount url}}"
def export():
    inner = ('{ products(query: "vendor:\'vidaXL\'") { edges { node { id handle title productType '
             'variants { edges { node { id sku selectedOptions { name value } } } } } } } }')
    for _ in range(60):
        s = _shop_gql(_STAT)["data"]["currentBulkOperation"]
        if not s or s["status"] not in ("CREATED", "RUNNING"): break
        time.sleep(10)
    r = _shop_gql(_RUN, {"q": inner})["data"]["bulkOperationRunQuery"]
    if r.get("userErrors"): raise SystemExit(r["userErrors"])
    url = None; start = time.time()
    while True:
        time.sleep(8)
        s = _shop_gql(_STAT)["data"]["currentBulkOperation"]
        if not s: continue
        if s["status"] == "COMPLETED": url = s.get("url"); break
        if s["status"] in ("FAILED", "CANCELED", "EXPIRED"): raise SystemExit(s.get("errorCode"))
    prods, meta = {}, {}
    with urllib.request.urlopen(urllib.request.Request(url), timeout=300) as resp:
        for raw in resp:
            line = raw.decode("utf-8").strip()
            if not line: continue
            o = json.loads(line); oid = o.get("id", "") or ""
            if "/Product/" in oid:
                prods[oid] = {"handle": o.get("handle"), "title": o.get("title"), "ptype": o.get("productType") or ""}
            elif "/ProductVariant/" in oid:
                pid = o.get("__parentId"); sk = (o.get("sku") or "").strip().replace(".0", "")
                if not sk: continue
                meta[sk] = {"opts": {so["name"]: so["value"] for so in (o.get("selectedOptions") or [])}, "pid": pid}
    return prods, meta

def load_feed(wanted):
    import requests, time as _t
    H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
         "Accept": "*/*", "Accept-Language": "da,en;q=0.9"}
    data = None
    for a in range(1, 9):
        r = requests.get(FEED_URL, headers=H, timeout=300)
        if r.status_code == 200 and len(r.content) > 10000: data = r.content; break
        print(f"   feed {r.status_code} ({len(r.content)}b) — retry {a}/8 om {30*a}s")
        _t.sleep(30 * a)
    if data is None: raise SystemExit("feed stadig blokeret efter retries")
    zf = zipfile.ZipFile(io.BytesIO(data)); name = [n for n in zf.namelist() if n.endswith(".csv")][0]
    ft = {}
    with zf.open(name) as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")):
            s = (row.get("SKU") or "").strip().replace(".0", "")
            if s in wanted: ft[s] = (row.get("Title") or "", row.get("Color") or "")
    return ft

_DIM_OPT = {"størrelse","stoerrelse","længde","laengde","bredde","dybde","højde","hojde","diameter","mål","maal","size","tykkelse"}
_NUM = r"(?:\d+(?:[.,]\d+)?|\([\d.,\s\-–]+\))"
_COMPLETE_DIM = re.compile(r"(?i)(?<![A-Za-zÆØÅæøå])[øØ⌀]?" + _NUM + r"(?:\s*[xX×]\s*" + _NUM + r")+\s*(?:cm|mm|m)?")
_SINGLE_DIM = re.compile(r"(?i)(?<![A-Za-zÆØÅæøå0-9\-])[øØ⌀]?\d+(?:[.,]\d+)?\s*(?:cm|mm)\b(?!\s*[-–])")
_TAIL = {"og","med","i","til","på","af","samt","for","uden","x"}
_COL = {"sort","hvid","hvidt","grå","gråt","brun","brunt","rød","rødt","orange","gul","gult","grøn","grønt",
        "blå","blåt","lilla","pink","lyserød","turkis","beige","creme","cremefarvet","naturfarvet","natur",
        "naturlig","sølv","sølvfarvet","guld","guldfarvet","gylden","gyldenbrun","bronze","kobber","antracit",
        "antracitgrå","lysegrå","mørkegrå","mørkebrun","betongrå","gråbrun","taupe","oliven","olivengrøn",
        "bordeaux","vinrød","koral","marineblå","egetræsfarve","egetræsfarvet","sonoma","røget","transparent",
        "mat","blank","højglans","messing","krom","nikkel","flerfarvet"}
def cleanup_conn(t):
    toks = t.split()
    while toks and toks[-1].lower().strip(".,-–") in _TAIL: toks.pop()
    return " ".join(toks)
def strip_trail_color(t, opts):
    if not any((n or "").lower().strip() in ("farve", "color", "colour", "kulør") for n in opts): return t
    toks = t.split()
    while len(toks) > 1 and toks[-1].lower().strip(".,-–") in (_COL | _TAIL): toks.pop()
    return " ".join(toks)
def _has_dim(opts):
    for n, vals in opts.items():
        if n.lower().strip() in _DIM_OPT: return True
        if any(re.search(r"\d\s*[x×]\s*\d|\b\d+\s*cm\b", v) for v in vals): return True
    return False
def strip_variant_dims(t, opts):
    """Fjern komplet mål når størrelse ER en variant (generisk titel skal ikke have specifik størrelse)."""
    if not _has_dim(opts): return t
    t = _COMPLETE_DIM.sub(" ", t)
    t = _SINGLE_DIM.sub(" ", t)
    t = re.sub(r"(?<!\d)\s+(cm|mm|m)\b", "", t, flags=re.I)
    return re.sub(r"\s+", " ", t).strip(" ,-–")
_MALFORMED_DIM = re.compile(r"(?i)(?<![A-Za-zÆØÅæøå])\d[\dxX×,.\s]*\d(?:\s*(?:cm|mm|m))?\b")
def strip_malformed_dim(t):
    """Drop en mål-blok hvis den er defekt (x rører komma / dobbelt-x) — fx vidaXL-garble '51X,5'."""
    def repl(m):
        b = m.group(0)
        return " " if re.search(r"[xX×]\s*,|,\s*[xX×]|[xX×]\s*[xX×]", b) else b
    return _MALFORMED_DIM.sub(repl, t)
_UP = ["LED","TV","USB","UV","PVC","RGB","HDMI","HD","3D","WC","CD","DVD","MDF","HDPE","WPC","ABS","XXL","XL","SPA","WiFi"]
def _casew(w):  # title-case pr. ord, bevidst om bindestreg OG skråstreg ('sand/vand'→'Sand/Vand')
    return "/".join("-".join(_cap(p) for p in seg.split("-")) for seg in w.split("/"))
def final_case(t):
    t = " ".join(_casew(w) for w in (t or "").split())
    for tok in _UP: t = re.sub(r"\b" + re.escape(tok) + r"\b", tok, t, flags=re.IGNORECASE)
    return re.sub(r"(?i)\bip(\d{2})\b", lambda m: "IP" + m.group(1), t)
def dedup_words(t):
    return re.sub(r"\b(\w{3,})(\s+\1)\b", r"\1", t, flags=re.IGNORECASE)

def optmap(skus, meta, drop_model=False):
    d = defaultdict(set)
    for s in skus:
        for n, v in meta[s]["opts"].items():
            if n.lower() == "title": continue
            if drop_model and n.lower() == "model": continue
            if v: d[n].add(v)
    return {k: sorted(vv) for k, vv in d.items()}

CACHE = r"C:\Users\APC\AppData\Local\Temp\claude\C--Users-APC\c0b60326-0d7f-46aa-bec2-7289b435d558\scratchpad\sim_data_cache.json"
def get_data():
    if os.path.exists(CACHE):
        d = json.load(open(CACHE, encoding="utf-8"))
        return d["prods"], d["meta"], {k: tuple(v) for k, v in d["ft"].items()}
    prods, meta = export()
    ft = load_feed(set(meta))
    json.dump({"prods": prods, "meta": meta, "ft": {k: list(v) for k, v in ft.items()}},
              open(CACHE, "w", encoding="utf-8"))
    return prods, meta, ft

def main():
    mapping = load_mapping()
    clean_split, messy = load_bearing()
    prods, meta, ft = get_data()
    fcolors = {s: v[1] for s, v in ft.items()}
    print(f"🗺️ {len(mapping)} mappet | {len(prods)} produkter | {len(ft)} feed-titler\n")

    groups = defaultdict(list)
    for sk, mt in meta.items():
        mp = mapping.get(sk)
        if mp in messy: key = ("single", sk)
        elif mp in clean_split: key = ("split", mp, mt["opts"].get("Model") or mt["opts"].get("model") or "?")
        elif mp: key = ("merge", mp)
        else: key = ("nomaster", mt["pid"])
        groups[key].append(sk)

    rows = []
    for key, skus in groups.items():
        typ = key[0]
        cur_pids = Counter(meta[s]["pid"] for s in skus)
        keeper_pid = cur_pids.most_common(1)[0][0]
        original = prods.get(keeper_pid, {}).get("title", "")
        ptype = prods.get(keeper_pid, {}).get("ptype", "")
        vc = len(skus)
        if typ == "merge":
            src = "behold" if len(cur_pids) <= 1 else "merge"
            title_in = original; opts = optmap(skus, meta)
        elif typ == "split":
            src = "split"; title_in = prep_feed(ft.get(skus[0], ("", ""))[0]); opts = optmap(skus, meta, drop_model=True)
        elif typ == "single":
            src = "single"; title_in = prep_feed(ft.get(skus[0], ("", ""))[0]); opts = optmap(skus, meta)
        else:
            src = "uændret"; title_in = original; opts = optmap(skus, meta)
        if typ in ("merge", "nomaster") and looks_bad(title_in):  # engelsk/SKU-titel → brug dansk feed-titel
            title_in = prep_feed(ft.get(skus[0], ("", ""))[0])
        opts = expand_opts(opts, skus, fcolors)
        is_single = (typ == "single")        # single = ét SKU → farve/størrelse ER identitet, strip ikke
        opts_a = {} if is_single else opts
        suggested, changed, issues, removed, needs_llm = analyze(title_in, opts_a, vc, ptype)
        gen = suggested if suggested else title_in
        if not is_single:
            gen = strip_variant_dims(gen, opts)   # fjern mål når størrelse er variant
            gen = strip_trail_color(gen, opts)    # trailing-farve efter mål-strip (rækkefølge)
        gen = strip_malformed_dim(gen)        # drop defekte mål-blokke (vidaXL-garble)
        gen = cleanup_conn(gen)               # ryd bindeord/x der blev hængende
        gen = dedup_words(gen)                # fjern dublerede nabo-ord
        gen = final_case(gen)                 # ensartet title-case + akronymer
        gen = re.sub(r"\s+", " ", gen).strip(" ,-–")
        rows.append({
            "source_type": src, "n_variants": vc, "n_current_products": len(cur_pids),
            "model_value": key[2] if typ == "split" else "",
            "original_title": original, "title_input": title_in, "generated_title": gen,
            "changed": "ja" if gen != original else "nej",
            "option_names": " · ".join(sorted(opts.keys())), "issues": "; ".join(issues),
            "needs_llm": needs_llm, "keeper_handle": prods.get(keeper_pid, {}).get("handle", ""),
        })
    rows.sort(key=lambda r: (r["source_type"], -r["n_variants"]))
    out = r"C:\Users\APC\Desktop\catalog_titles_simulation.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
    by = Counter(r["source_type"] for r in rows); llm = sum(1 for r in rows if r["needs_llm"])
    print(f"✅ {len(rows)} produkter → {out}")
    print(f"   typer: {dict(by)} | → LLM (engelsk/sku): {llm}")
    for typ in ("merge", "split", "single"):
        print(f"\n— {typ} (original → genereret) —")
        for r in [x for x in rows if x["source_type"] == typ][:5]:
            print(f"   {r['original_title'][:50]!r}\n     → {r['generated_title'][:60]!r}  [{r['option_names']}]")

if __name__ == "__main__":
    main()
