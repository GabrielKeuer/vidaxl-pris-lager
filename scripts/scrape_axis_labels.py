"""Hent EKSAKTE danske akse-navne pr. master via scrape_vidaxl (option A).
Kun masters i merge/split/fix/atomize-grupper (~3k, ikke 161k). numberOfNumber dækkes
ikke af scrape_vidaxl → fast 'Antal personer'. Output: output/axis_labels.json {master:{key:label}}.
Threaded + checkpoint. READ-ONLY."""
import json, os, sys, time, functools
from concurrent.futures import ThreadPoolExecutor
import threading
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, r"C:\Users\APC\dropxl-product-automation\scripts")
print = functools.partial(print, flush=True)
from product_utils import scrape_vidaxl

PLAN = "output/merge_plan.jsonl"
LINKS = "output/all_sku_links.json"
OUT = "output/axis_labels.json"
FIXED = {"numberOfNumber": "Antal personer"}   # scrape_vidaxl dækker ikke denne
WORKERS = 6

def main():
    links = json.load(open(LINKS, encoding="utf-8"))
    plans = [json.loads(l) for l in open(PLAN, encoding="utf-8")]
    # master → repræsentativ SKU (med link)
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
    print(f"🏷️ {len(rep)} masters at labelle")
    done = json.load(open(OUT, encoding="utf-8")) if os.path.exists(OUT) else {}
    todo = [(mp, url) for mp, url in rep.items() if mp not in done]
    print(f"   {len(todo)} tilbage (checkpoint: {len(done)})")
    lock = threading.Lock(); n = [0]
    def work(pair):
        mp, url = pair
        try:
            sc = scrape_vidaxl(url)
            labels = {k: v.get("display_name") for k, v in (sc.get("options") or {}).items() if v.get("display_name")}
        except Exception:
            labels = {}
        labels = {**FIXED, **labels}   # scrape vinder hvor den har label; ellers fast
        with lock:
            done[mp] = labels; n[0] += 1
            if n[0] % 100 == 0:
                json.dump(done, open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
                print(f"  …{n[0]}/{len(todo)}")
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(work, todo))
    json.dump(done, open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"✅ {len(done)} masters med labels → {OUT}")

if __name__ == "__main__":
    main()
