"""Hent EKSAKTE danske akse-navne pr. master DIREKTE fra vidaXL-siden (data-attr → label).
Fanger ALLE akser inkl. dropdowns (Model), som scrape_vidaxl missede → 0% gæt.
Kun masters i merge/split/fix/atomize-grupper. Output: output/axis_labels.json {master:{key:label}}.
Threaded + checkpoint. READ-ONLY."""
import json, os, sys, re, html as _htmlmod, functools, threading
import requests
from concurrent.futures import ThreadPoolExecutor
sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

PLAN = "output/merge_plan.jsonl"
LINKS = "output/all_sku_links.json"
OUT = "output/axis_labels.json"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"}
FIXED = {"numberOfNumber": "Antal personer"}   # har ikke altid egen data-attr-blok
WORKERS = 8

def extract_labels(txt):
    """{item_variant_nøgle: dansk_label} fra 'data-attr="KEY" ... >Label<'."""
    t = _htmlmod.unescape(txt)
    out = {}
    for m in re.finditer(r'data-attr="([^"]+)"', t):
        k = m.group(1)
        chunk = t[m.start():m.start() + 700]
        lm = re.search(r">\s*([A-ZÆØÅa-zæøå][A-Za-zÆØÅæøå ]{1,28}?)\s*<", chunk)
        if lm:
            lab = lm.group(1).strip()
            if lab and lab.lower() not in ("option", "options"):
                out[k] = lab
    return out

def main():
    links = json.load(open(LINKS, encoding="utf-8"))
    plans = [json.loads(l) for l in open(PLAN, encoding="utf-8")]
    rep = {}
    for p in plans:
        if p["action"] not in ("merge", "split", "atomize", "fix_mismerge_rest"):
            continue
        master = p["key"].split("|")[1] if "|" in p["key"] else p["key"]
        if master in rep:
            continue
        for m in p["variant_creates"]:
            if links.get(m["sku"]):
                rep[master] = links[m["sku"]]; break
    print(f"🏷️ {len(rep)} masters at labelle (direkte HTML-udtræk)")
    done = {}
    todo = list(rep.items())
    lock = threading.Lock(); n = [0]
    def work(pair):
        mp, url = pair
        labels = {}
        try:
            r = requests.get(url, headers=UA, timeout=25)
            if r.status_code == 200:
                labels = extract_labels(r.text)
        except Exception:
            pass
        labels = {**FIXED, **labels}
        with lock:
            done[mp] = labels; n[0] += 1
            if n[0] % 200 == 0:
                json.dump(done, open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
                print(f"  …{n[0]}/{len(todo)}")
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(work, todo))
    # _binary-aliaser: item_variant normaliserer '_binary'-nøgler til base → labelen skal også være der
    for d in done.values():
        for k in list(d):
            if k.endswith("_binary"):
                d.setdefault(k[:-len("_binary")], d[k])
    json.dump(done, open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
    cov = sum(1 for v in done.values() for k in v if k != "numberOfNumber")
    va3 = sum(1 for v in done.values() if "variationAttribute3" in v)
    print(f"✅ {len(done)} masters | {cov} labels udtrukket | variationAttribute3 dækket: {va3}")

if __name__ == "__main__":
    main()
