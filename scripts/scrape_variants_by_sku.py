"""PER-SKU VARIANT-SCRAPE (READ-ONLY): hent hver karantæne-SKUs autoritative variant-attributter
direkte fra dens egen produktside via 'item_variant'-JSON (color + variationAttribute2/3).
Langt mere pålideligt end combo-enumering (fetch_variant_skus) for omstrukturerede masters.
Resumable checkpoint + ThreadPool. Output: output/sku_variants.jsonl."""
import csv, io, json, os, re, sys, time, zipfile, functools, html as _html
from concurrent.futures import ThreadPoolExecutor
import threading
import requests
sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

PLAN = "output/merge_plan.jsonl"
MAP = "output/master_pid_map.jsonl"
LINKS = "output/all_sku_links.json"
CKPT = "output/sku_variants.jsonl"
FEED = ("https://feed.vidaxl.io/api/v1/feeds/download/"
        "f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping.csv.zip")
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"}
WORKERS = 12

def quar_skus():
    skus = set()
    for l in open(PLAN, encoding="utf-8"):
        p = json.loads(l)
        for st in (p.get("unresolved_collisions") or []): skus |= set(st)
        for pr in (p.get("dup_sku_quarantine") or []): skus |= set(pr)
    return skus

def all_mapped_skus():
    """Alle SKUs med en master_pid = alle variant-SKUs i kataloget (verifikations-scope)."""
    skus = set()
    for l in open(MAP, encoding="utf-8"):
        try:
            d = json.loads(l)
            if d.get("master_pid"): skus.add(str(d["sku"]).strip())
        except Exception: pass
    return skus

def load_links(wanted):
    if os.path.exists(LINKS):
        d = json.load(open(LINKS, encoding="utf-8"))
        if wanted <= set(d): return d
    print("📥 henter feed for Links…")
    data = None
    for a in range(1, 7):
        r = requests.get(FEED, headers=UA, timeout=300)
        if r.status_code == 200 and len(r.content) > 10000: data = r.content; break
        print(f"   feed {r.status_code} retry {a}"); time.sleep(20 * a)
    if data is None: raise SystemExit("feed blokeret")
    zf = zipfile.ZipFile(io.BytesIO(data)); name = [n for n in zf.namelist() if n.endswith(".csv")][0]
    links = {}
    with zf.open(name) as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")):
            s = (row.get("SKU") or "").strip().replace(".0", "")
            if s in wanted: links[s] = row.get("Link") or ""
    json.dump(links, open(LINKS, "w", encoding="utf-8"))
    return links

_BLOCK = threading.Event()  # sat ved gentagne 403/429 → alle tråde bremser
def variant_of(sku, url):
    try:
        if _BLOCK.is_set(): time.sleep(5)
        r = requests.get(url, headers=UA, timeout=30)
        if r.status_code in (403, 429):
            _BLOCK.set(); time.sleep(15); _BLOCK.clear()
            return {"sku": sku, "note": f"http{r.status_code}"}
        if r.status_code != 200: return {"sku": sku, "note": f"http{r.status_code}"}
        txt = _html.unescape(r.text)
        m = re.search(r'"sku":"' + re.escape(sku) + r'","item_variant":(\{.*?\})', txt) \
            or re.search(r'"item_variant":(\{.*?\})', txt)
        if not m: return {"sku": sku, "note": "ingen_item_variant"}
        v = json.loads(m.group(1))
        # gem HELE item_variant-dicten (alle nøgler: color, variationAttribute1/2/3,
        # numberOfNumber m.fl.) — tidligere version missede variationAttribute1 + numberOfNumber
        return {"sku": sku, "opts": {k: val for k, val in v.items() if val not in (None, "")}, "note": "ok"}
    except Exception as e:
        return {"sku": sku, "note": f"fejl:{str(e)[:40]}"}

def main():
    scope = "quarantine" if "--quarantine" in sys.argv else "all"
    skus = quar_skus() if scope == "quarantine" else all_mapped_skus()
    print(f"🎯 scope={scope}: {len(skus)} SKUs")
    done = set()
    if os.path.exists(CKPT):
        for l in open(CKPT, encoding="utf-8"):
            try: done.add(json.loads(l)["sku"])
            except Exception: pass
        print(f"↩️ checkpoint: {len(done)}")
    links = load_links(skus)
    todo = [(s, links[s]) for s in skus if s not in done and links.get(s)]
    print(f"   {len(todo)} at scrape ({sum(1 for s in skus if not links.get(s))} uden link)")
    lock = threading.Lock(); ck = open(CKPT, "a", encoding="utf-8"); n = [0]
    def work(pair):
        rec = variant_of(*pair)
        with lock:
            ck.write(json.dumps(rec, ensure_ascii=False) + "\n"); ck.flush()
            n[0] += 1
            if n[0] % 200 == 0: print(f"  …{n[0]}/{len(todo)}")
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(work, todo))
    ck.close()
    ok = sum(1 for l in open(CKPT, encoding="utf-8") if '"note": "ok"' in l)
    print(f"✅ færdig — {ok} med variant-data")

if __name__ == "__main__":
    main()
