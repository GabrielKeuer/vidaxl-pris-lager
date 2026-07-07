"""OPTION-SCRAPE (READ-ONLY): hent vidaXL's autoritative option-matrix for de masters
hvor merge-planen har uløste kollisioner. Genbruger scrape_vidaxl + fetch_variant_skus
fra dropxl-product-automation (den beviste create-kodesti). Resumable via checkpoint.
Output: output/scraped_options.jsonl  ({master, url, options, variant_map sku→{akse:værdi}})"""
import csv, io, json, os, re, sys, time, zipfile, functools
from collections import defaultdict
sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, r"C:\Users\APC\dropxl-product-automation\scripts")
import requests
from product_utils import scrape_vidaxl, fetch_variant_skus, count_combinations

PLAN = r"C:\Users\APC\vidaxl-pris-lager\output\merge_plan.jsonl"
CKPT = r"C:\Users\APC\vidaxl-pris-lager\output\scraped_options.jsonl"
LINKS = r"C:\Users\APC\vidaxl-pris-lager\output\feed_links.json"
FEED_URL = "https://feed.vidaxl.io/api/v1/feeds/download/f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping.csv.zip"
MAX_COMBOS = 800

def load_links(wanted):
    if os.path.exists(LINKS):
        d = json.load(open(LINKS, encoding="utf-8"))
        if wanted <= set(d): return d
    print("📥 henter feed for Links…")
    H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"}
    data = requests.get(FEED_URL, headers=H, timeout=300).content
    zf = zipfile.ZipFile(io.BytesIO(data)); name = [n for n in zf.namelist() if n.endswith(".csv")][0]
    links = {}
    with zf.open(name) as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")):
            s = (row.get("SKU") or "").strip().replace(".0", "")
            if s in wanted: links[s] = row.get("Link") or ""
    json.dump(links, open(LINKS, "w", encoding="utf-8"))
    return links

def main():
    plans = [json.loads(l) for l in open(PLAN, encoding="utf-8")]
    # BÅDE uløste kollisioner OG dup-karantæne: feedets Color er upålidelig (fx 'gennemsigtig'
    # for både hvid og transparent variant) — kun vidaXL's egen dwvar-matrix er sandhed
    targets = [p for p in plans if p.get("unresolved_collisions") or p.get("dup_sku_quarantine")]
    print(f"🎯 {len(targets)} masters (uløste kollisioner + dup-karantæne)")
    done = set()
    if os.path.exists(CKPT):
        for l in open(CKPT, encoding="utf-8"):
            try: done.add(json.loads(l)["master"])
            except Exception: pass
        print(f"↩️ checkpoint: {len(done)} allerede scrapet")
    todo = [p for p in targets if p["key"].split("|")[1] not in done]
    wanted = {m["sku"] for p in todo for m in p["variant_creates"]}
    links = load_links(wanted)
    ck = open(CKPT, "a", encoding="utf-8")
    for i, p in enumerate(todo, 1):
        master = p["key"].split("|")[1]
        url = next((links.get(m["sku"]) for m in p["variant_creates"] if links.get(m["sku"])), None)
        rec = {"master": master, "url": url, "options": None, "variant_map": None, "note": ""}
        if not url:
            rec["note"] = "ingen_feed_link"
        else:
            sc = scrape_vidaxl(url)
            if not sc.get("success") or not sc.get("options"):
                rec["note"] = "scrape_fejl_eller_ingen_options"
            elif count_combinations(sc["options"]) > MAX_COMBOS:
                rec["note"] = f"for_mange_kombinationer({count_combinations(sc['options'])})"
                rec["options"] = {k: v["display_name"] for k, v in sc["options"].items()}
            else:
                vm = fetch_variant_skus(sc.get("master_pid") or master, sc["options"])
                rec["options"] = {k: v["display_name"] for k, v in sc["options"].items()}
                rec["variant_map"] = vm
                rec["note"] = f"ok({len(vm)} SKUs)"
        ck.write(json.dumps(rec, ensure_ascii=False) + "\n"); ck.flush()
        print(f"[{i}/{len(todo)}] {master}: {rec['note']}")
    ck.close()
    print("✅ færdig")

if __name__ == "__main__":
    main()
