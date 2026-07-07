"""SIMULATION af det endelige katalog efter alle merge-beslutninger (READ-ONLY).
Anvender: master_pid-gruppering (merge + auto-fix af fejl-merges) + 49 rene splits
(master,Model) + 35 rodede singles (pr. SKU) + master-løse (uændret).
Skriver C:\\Users\\APC\\Desktop\\catalog_simulation.csv + summary. Rører INTET i Shopify."""
import csv, io, json, os, re, sys, time, urllib.request, zipfile
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
from bulk_repricing import _shop_gql, _gid_num

def clean_vidaxl(t):
    if not t: return ""
    for v in ["vidaXL ", "vidaxl ", "VidaXL ", "VIDAXL ", "fra vidaXL", "vidaXL", "vidaxl"]:
        t = t.replace(v, "")
    return t.strip()
_FEED_SP = [(re.compile(r"(?i)\bu(træk)"), "dtræk"), (re.compile(r"(?i)\b(e)træsfarve\b"), "getræsfarve"),
            (re.compile(r"(?i)\bsofa(esæt)"), "ofasæt")]
def fix_feed_spelling(t):
    for pat, tail in _FEED_SP: t = pat.sub(lambda m, x=tail: m.group(0)[0] + x, t)
    return t
def title_case_danish(t):
    return " ".join(w[0].upper() + w[1:].lower() if w else w for w in (t or "").split())
_UPPER = ["LED","TV","USB","UV","PVC","RGB","HDMI","HD","3D","WC","CD","DVD","MDF","HDPE","WPC","ABS","XXL","XL","SPA","WiFi"]
def fix_casing(t):
    for tok in _UPPER: t = re.sub(r"\b" + re.escape(tok) + r"\b", tok, t, flags=re.IGNORECASE)
    t = re.sub(r"(?i)\bip(\d{2})\b", lambda m: "IP" + m.group(1), t)
    return t, []
FEED_URL = "https://feed.vidaxl.io/api/v1/feeds/download/f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping.csv.zip"

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
    inner = ('{ products(query: "vendor:\'vidaXL\'") { edges { node { id handle title '
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
    prods, sku_meta, sku_prod = {}, {}, {}
    with urllib.request.urlopen(urllib.request.Request(url), timeout=300) as resp:
        for raw in resp:
            line = raw.decode("utf-8").strip()
            if not line: continue
            o = json.loads(line); oid = o.get("id", "") or ""
            if "/Product/" in oid:
                prods[oid] = {"handle": o.get("handle"), "title": o.get("title")}
            elif "/ProductVariant/" in oid:
                pid = o.get("__parentId"); sk = (o.get("sku") or "").strip().replace(".0", "")
                if not sk: continue
                opts = {so["name"]: so["value"] for so in (o.get("selectedOptions") or [])}
                sku_meta[sk] = {"opts": opts, "pid": pid}
                sku_prod[sk] = pid
    return prods, sku_meta

def load_feed_titles(wanted):
    data = urllib.request.urlopen(FEED_URL, timeout=300).read()
    zf = zipfile.ZipFile(io.BytesIO(data)); name = [n for n in zf.namelist() if n.endswith(".csv")][0]
    ft = {}
    with zf.open(name) as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")):
            s = (row.get("SKU") or "").strip().replace(".0", "")
            if s in wanted: ft[s] = (row.get("Title") or "")
    return ft

def gen_title(feed_titles):
    """Kandidat generisk titel = fælles tokens på tværs af varianternes feed-titler, renset."""
    cl = [clean_vidaxl(t) for t in feed_titles if t]
    if not cl: return ""
    common = set.intersection(*[set(t.lower().split()) for t in cl]) if cl else set()
    base = " ".join(w for w in cl[0].split() if w.lower() in common)
    base = fix_feed_spelling(base)
    base = title_case_danish(base)
    base, _ = fix_casing(base)
    return base.strip()

def main():
    mapping = load_mapping()
    clean_split, messy = load_bearing()
    print(f"🗺️ {len(mapping)} SKU→master | {len(clean_split)} rene-split masters | {len(messy)} rodede masters")
    prods, sku_meta = export()
    print(f"📦 {len(prods)} nuværende produkter, {len(sku_meta)} SKUs")
    ft = load_feed_titles(set(sku_meta))
    print(f"📥 {len(ft)} feed-titler\n")

    # bestem endeligt gruppe-key pr. SKU
    groups = defaultdict(list)
    for sk, meta in sku_meta.items():
        mp = mapping.get(sk)
        if mp in messy:
            key = ("single", sk)
        elif mp in clean_split:
            model = meta["opts"].get("Model") or meta["opts"].get("model") or "?"
            key = ("split", mp, model)
        elif mp:
            key = ("merge", mp)
        else:
            key = ("nomaster", meta["pid"])
        groups[key].append(sk)

    rows = []
    for key, skus in groups.items():
        cur_pids = {sku_meta[s]["pid"] for s in skus}
        typ = key[0]
        if typ == "merge":
            src = "behold" if len(cur_pids) <= 1 else "merge"
        elif typ == "split": src = "split"
        elif typ == "single": src = "single"
        else: src = "uændret"
        # option-navne (ekskl. Model for splits)
        onames = set()
        for s in skus:
            for n in sku_meta[s]["opts"]:
                if n.lower() == "title": continue
                if typ == "split" and n.lower() == "model": continue
                onames.add(n)
        title = gen_title([ft.get(s, "") for s in skus])
        khandle = prods.get(list(cur_pids)[0], {}).get("handle", "")
        rows.append({
            "final_id": "|".join(str(k) for k in key), "source_type": src,
            "n_variants": len(skus), "n_current_products": len(cur_pids),
            "option_names": " · ".join(sorted(onames)), "candidate_title": title,
            "model_value": key[2] if typ == "split" else "",
            "sample_feed_title": ft.get(skus[0], ""), "keeper_handle": khandle,
        })
    rows.sort(key=lambda r: (r["source_type"], -r["n_variants"]))
    out = r"C:\Users\APC\Desktop\catalog_simulation.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)

    by = Counter(r["source_type"] for r in rows)
    over100 = sum(1 for r in rows if r["n_variants"] > 100)
    print("=" * 60)
    print(f"SIMULERET KATALOG: {len(rows)} produkter (fra {len(prods)})")
    for k, c in by.most_common(): print(f"   {k}: {c}")
    print(f"   produkter >100 varianter: {over100}")
    print(f"\n✅ {out}")
    print("\n— eksempler pr. type —")
    for typ in ("merge", "split", "single", "behold", "uændret"):
        ex = [r for r in rows if r["source_type"] == typ][:2]
        for r in ex:
            print(f"  [{typ}] {r['n_variants']}var, {r['n_current_products']}→1 | {r['candidate_title']!r} (opt: {r['option_names']})")

if __name__ == "__main__":
    main()
