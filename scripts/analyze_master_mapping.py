"""Load master_pid-mapping til Supabase + grupperings-analyse (READ-ONLY, rører IKKE Shopify).

Beregner: manglende merges (master delt af flere Shopify-produkter), fejl-merges
(produkt med SKUs fra flere masters), Shopify-variant-grænse-tjek, single-produkter.
"""
import csv, io, json, os, re, sys, time, urllib.request
from collections import defaultdict, Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
_HUB = r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local"
for l in open(_HUB, encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
from bulk_repricing import _shop_gql, _gid_num

CKPT = r"C:\Users\APC\vidaxl-pris-lager\output\master_pid_map.jsonl"
SB = os.environ["NEXT_PUBLIC_SUPABASE_URL"].rstrip("/")
SKEY = os.environ["SUPABASE_SERVICE_KEY"]

def load_mapping():
    m = {}
    for l in open(CKPT, encoding="utf-8"):
        try:
            d = json.loads(l); m[d["sku"]] = d["master_pid"]
        except Exception:
            pass
    return m

def load_to_supabase(mapping):
    print(f"📤 loader {len(mapping)} mappings til Supabase...")
    H = {"apikey": SKEY, "Authorization": f"Bearer {SKEY}", "Content-Type": "application/json",
         "Prefer": "resolution=merge-duplicates,return=minimal"}
    rows = [{"sku": s, "master_pid": p} for s, p in mapping.items()]
    for i in range(0, len(rows), 1000):
        req = urllib.request.Request(f"{SB}/rest/v1/vidaxl_sku_master?on_conflict=sku",
            data=json.dumps(rows[i:i + 1000]).encode(), headers=H, method="POST")
        try:
            urllib.request.urlopen(req, timeout=60)
        except Exception as e:
            print(f"  ⚠ batch {i}: {e}");
        if i % 20000 == 0: print(f"   …{i}")
    print("   ✅ loadet til Supabase")

_RUN = "mutation($q:String!){bulkOperationRunQuery(query:$q){bulkOperation{id status} userErrors{message}}}"
_STAT = "query{currentBulkOperation(type:QUERY){id status errorCode objectCount url}}"

def export_products():
    inner = ('{ products(query: "vendor:\'vidaXL\'") { edges { node { id handle title '
             'variants { edges { node { id sku } } } } } } }')
    for _ in range(60):
        s = _shop_gql(_STAT)["data"]["currentBulkOperation"]
        if not s or s["status"] not in ("CREATED", "RUNNING"): break
        time.sleep(10)
    res = _shop_gql(_RUN, {"q": inner})["data"]["bulkOperationRunQuery"]
    if res.get("userErrors"): raise SystemExit(res["userErrors"])
    print(f"🚀 produkt-eksport {res['bulkOperation']['id']}")
    url = None; start = time.time()
    while True:
        time.sleep(8)
        s = _shop_gql(_STAT)["data"]["currentBulkOperation"]
        if not s: continue
        if s["status"] == "COMPLETED": url = s.get("url"); break
        if s["status"] in ("FAILED", "CANCELED", "EXPIRED"): raise SystemExit(s.get("errorCode"))
    prods = {}
    with urllib.request.urlopen(urllib.request.Request(url), timeout=300) as resp:
        for raw in resp:
            line = raw.decode("utf-8").strip()
            if not line: continue
            o = json.loads(line); oid = o.get("id", "") or ""
            if "/Product/" in oid:
                prods[oid] = {"handle": o.get("handle"), "title": o.get("title"), "skus": []}
            elif "/ProductVariant/" in oid:
                pid = o.get("__parentId"); sku = (o.get("sku") or "").strip().replace(".0", "")
                if pid in prods and sku: prods[pid]["skus"].append(sku)
    return prods

def main():
    mapping = load_mapping()
    print(f"🗺️  {len(mapping)} SKU→master, {len(set(mapping.values()))} distinkte masters")
    load_to_supabase(mapping)
    prods = export_products()
    print(f"📦 {len(prods)} Shopify vidaXL-produkter\n")

    master_products = defaultdict(set)   # master -> {pid}
    product_masters = defaultdict(set)   # pid -> {master}
    master_skucount = Counter()          # master -> antal af VORES SKUs
    fully_single = []                    # produkter helt uden master
    for pid, p in prods.items():
        masters = set()
        for s in p["skus"]:
            mp = mapping.get(s)
            if mp:
                masters.add(mp); master_products[mp].add(pid); master_skucount[mp] += 1
        product_masters[pid] = masters
        if not masters:
            fully_single.append(pid)

    # MANGLENDE MERGES: master delt af >1 produkt
    missing = {mp: pids for mp, pids in master_products.items() if len(pids) > 1}
    prods_in_missing = set().union(*missing.values()) if missing else set()
    # FEJL-MERGES: produkt med SKUs fra >1 master
    mismerge = {pid: ms for pid, ms in product_masters.items() if len(ms) > 1}
    # SHOPIFY-GRÆNSE: variant-antal pr. merget produkt (= SKUs pr. master i vores katalog)
    over100 = {mp: c for mp, c in master_skucount.items() if c > 100}
    over2000 = {mp: c for mp, c in master_skucount.items() if c > 2000}

    clean_single_master = sum(1 for ms in product_masters.values() if len(ms) == 1)
    resulting = len(set(mapping.values())) + len(fully_single)  # masters + singletons

    print("=" * 62)
    print("GRUPPERINGS-ANALYSE")
    print(f"  Shopify-produkter nu            : {len(prods)}")
    print(f"  → med præcis 1 master (rene)    : {clean_single_master}")
    print(f"  → helt master-løse (singles)    : {len(fully_single)}")
    print(f"  → FEJL-MERGE (SKUs fra ≥2 master): {len(mismerge)}")
    print()
    print(f"  MANGLENDE MERGES: {len(missing)} masters deles af flere produkter")
    print(f"    → involverer {len(prods_in_missing)} Shopify-produkter der burde blive til {len(missing)} produkter")
    print(f"    → dvs. ~{len(prods_in_missing) - len(missing)} færre produkter efter merge")
    print()
    print(f"  Katalog efter merge: ~{resulting} produkter (fra {len(prods)}) — ~{len(prods)-resulting} færre")
    print()
    print(f"  SHOPIFY-GRÆNSE-TJEK (variant-antal pr. merget produkt):")
    print(f"    >100 varianter (kræver large-product-flow): {len(over100)}")
    print(f"    >2000 varianter (Shopify HARD-grænse!)     : {len(over2000)}")
    if over100:
        top = sorted(master_skucount.items(), key=lambda x: -x[1])[:5]
        print(f"    største: {top}")

    print("\n— 8 eksempler på MANGLENDE MERGE (produkter der burde være ét) —")
    for mp, pids in list(missing.items())[:8]:
        titles = [prods[p]["title"] for p in pids][:4]
        print(f"  master {mp} ({len(pids)} produkter, {master_skucount[mp]} SKUs):")
        for t in titles: print(f"     - {t}")

    print("\n— fejl-merge eksempler (SKUs fra flere masters) —")
    for pid, ms in list(mismerge.items())[:6]:
        print(f"  {prods[pid]['title']}  → masters: {ms}")

    # gem analyse
    out = {"products": len(prods), "distinct_masters": len(set(mapping.values())),
           "clean_single": clean_single_master, "fully_single": len(fully_single),
           "mismerge": len(mismerge), "missing_merge_masters": len(missing),
           "products_in_missing": len(prods_in_missing), "resulting_products": resulting,
           "over100": len(over100), "over2000": len(over2000)}
    json.dump(out, open(r"C:\Users\APC\vidaxl-pris-lager\output\grouping_analysis.json", "w"), indent=2)
    print("\n✅ analyse gemt: output/grouping_analysis.json")

if __name__ == "__main__":
    main()
