"""Re-scrap de scrape-hul-SKUs (item_variant manglede) fra vidaXL — henter item_variant + akse-labels
og opdaterer output/sku_variants.jsonl + output/axis_labels.json, så deres grupper kan kategoriseres/merges."""
import json, os, re, sys, time, html as _htmlmod
import requests
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
from scrape_axis_labels import extract_labels

SP = r"C:\Users\APC\AppData\Local\Temp\claude\C--Users-APC\c0b60326-0d7f-46aa-bec2-7289b435d558\scratchpad"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"}

def item_variant_of(txt, sku):
    m = (re.search(r'"sku":"' + re.escape(str(sku)) + r'","item_variant":(\{.*?\})', txt)
         or re.search(r'"item_variant":(\{.*?\})', txt))
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None

def main():
    gaps = json.load(open(SP + r"\gap_skus.json", encoding="utf-8"))
    links = json.load(open("output/all_sku_links.json", encoding="utf-8"))
    sku2master = {}
    for l in open("output/master_pid_map.jsonl", encoding="utf-8"):
        try:
            d = json.loads(l); sku2master[str(d.get("sku")).strip()] = d.get("master_pid")
        except Exception:
            pass
    labels = json.load(open("output/axis_labels.json", encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}

    new_rows = []; got = 0; empty = 0; nolink = 0
    for s in gaps:
        url = links.get(s)
        if not url:
            nolink += 1; continue
        try:
            r = requests.get(url, headers=UA, timeout=25)
            if r.status_code != 200:
                new_rows.append({"sku": s, "opts": {}, "note": "http_%s" % r.status_code}); continue
            txt = _htmlmod.unescape(r.text)
        except Exception as e:
            new_rows.append({"sku": s, "opts": {}, "note": "err"}); continue
        iv = item_variant_of(txt, s)
        opts = {k: v for k, v in (iv or {}).items() if v}
        if opts:
            got += 1
            new_rows.append({"sku": s, "opts": opts, "note": "ok"})
            # labels for masteren
            mp = sku2master.get(s)
            if mp:
                lab = extract_labels(txt)
                for k in list(lab):
                    if k.endswith("_binary"):
                        lab.setdefault(k[:-len("_binary")], lab[k])
                labels.setdefault(mp, {}).update(lab)
        else:
            empty += 1
            new_rows.append({"sku": s, "opts": {}, "note": "no_item_variant"})
        time.sleep(0.2)

    # append til sku_variants.jsonl
    with open("output/sku_variants.jsonl", "a", encoding="utf-8") as f:
        for row in new_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    json.dump(labels, open("output/axis_labels.json", "w", encoding="utf-8"), ensure_ascii=False)
    print(f"re-scrapet {len(gaps)} SKUs: {got} fik item_variant, {empty} har genuint INGEN varianter (=single-produkt), {nolink} uden URL")
    print("→ opdateret sku_variants.jsonl + axis_labels.json")

if __name__ == "__main__":
    main()
