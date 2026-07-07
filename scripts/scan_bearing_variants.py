"""Scan bærende-variant-produkter: hvor en option-VÆRDI er en produkt-TYPE/model
(fodskammel/hjørnesofa/bord osv.) frem for farve/størrelse. Grupperet pr. master.
READ-ONLY → skriver C:\\Users\\APC\\Desktop\\bearing_variants.csv."""
import csv, json, os, re, sys, time, urllib.request
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
from bulk_repricing import _shop_gql

# Modulære dele = STÆRKT signal (næsten altid bærende-variant når de er option-værdier)
MODULAR = {"fodskammel", "hjørnesofa", "hjørnedel", "hjørnemodul", "hjørnesektion", "midtersofa",
           "midterdel", "midtermodul", "midtersektion", "endemodul", "endedel", "chaiselong",
           "ottoman", "armlæn", "sofadel", "moduldel", "hjørnestol"}
# Produkt-typer = MEDIUM signal (bærende når option-værdierne spænder over flere typer)
TYPES = {"sofa", "lænestol", "stol", "barstol", "taburet", "puf", "bænk", "bord", "sofabord",
         "sidebord", "spisebord", "skrivebord", "konsolbord", "natbord", "reol", "bogreol",
         "skab", "kommode", "seng", "sengeramme", "daybed", "hylde", "garderobe", "vitrine",
         "postkasse", "plantekasse", "stativ", "kurv", "spejl", "bakke", "buffetskab", "sengebord",
         "tv-bord", "tv-skab", "displayhylde", "rullebord", "klapbord", "væghylde", "plantekasse"}
ALLTYPE = MODULAR | TYPES

_RUN = "mutation($q:String!){bulkOperationRunQuery(query:$q){bulkOperation{id status} userErrors{message}}}"
_STAT = "query{currentBulkOperation(type:QUERY){id status errorCode objectCount url}}"

def export():
    inner = ('{ products(query: "vendor:\'vidaXL\'") { edges { node { id handle title '
             'variants { edges { node { id sku selectedOptions { name value } } } } } } } }')
    for _ in range(60):
        s = _shop_gql(_STAT)["data"]["currentBulkOperation"]
        if not s or s["status"] not in ("CREATED", "RUNNING"): break
        time.sleep(10)
    res = _shop_gql(_RUN, {"q": inner})["data"]["bulkOperationRunQuery"]
    if res.get("userErrors"): raise SystemExit(res["userErrors"])
    print(f"🚀 {res['bulkOperation']['id']}")
    url = None; start = time.time()
    while True:
        time.sleep(8)
        s = _shop_gql(_STAT)["data"]["currentBulkOperation"]
        if not s: continue
        if s["status"] == "COMPLETED": url = s.get("url"); break
        if s["status"] in ("FAILED", "CANCELED", "EXPIRED"): raise SystemExit(s.get("errorCode"))
    prods = {}
    vopts = defaultdict(lambda: defaultdict(set))
    vsku = defaultdict(set)
    with urllib.request.urlopen(urllib.request.Request(url), timeout=300) as resp:
        for raw in resp:
            line = raw.decode("utf-8").strip()
            if not line: continue
            o = json.loads(line); oid = o.get("id", "") or ""
            if "/Product/" in oid:
                prods[oid] = {"handle": o.get("handle"), "title": o.get("title")}
            elif "/ProductVariant/" in oid:
                pid = o.get("__parentId")
                if not pid: continue
                sk = (o.get("sku") or "").strip().replace(".0", "")
                if sk: vsku[pid].add(sk)
                for so in (o.get("selectedOptions") or []):
                    nm = (so.get("name") or "").strip(); vl = (so.get("value") or "").strip()
                    if vl and vl.lower() != "default title" and nm.lower() != "title":
                        vopts[pid][nm].add(vl)
    return prods, vopts, vsku

def type_nouns_in(values):
    """Returnér (distinkte type-navneord, om modulær) i en samling option-værdier."""
    found = set(); modular = False
    for v in values:
        for tok in re.split(r"[\s/,+()-]+", v.lower()):
            tok = tok.strip(".")
            if tok in ALLTYPE:
                found.add(tok)
                if tok in MODULAR: modular = True
    return found, modular

def main():
    mapping = {}
    for l in open(r"C:\Users\APC\vidaxl-pris-lager\output\master_pid_map.jsonl", encoding="utf-8"):
        try: d = json.loads(l); mapping[d["sku"]] = d["master_pid"]
        except Exception: pass
    prods, vopts, vsku = export()
    print(f"📦 {len(prods)} produkter\n")

    # aggregér pr. master
    master_opts = defaultdict(lambda: defaultdict(set))   # master -> option -> values
    master_prods = defaultdict(set)                        # master -> {pid}
    for pid in prods:
        masters = {mapping[s] for s in vsku.get(pid, set()) if s in mapping}
        for mp in masters:
            master_prods[mp].add(pid)
            for nm, vals in vopts.get(pid, {}).items():
                master_opts[mp][nm] |= vals

    rows = []
    for mp, opts in master_opts.items():
        for nm, vals in opts.items():
            nouns, modular = type_nouns_in(vals)
            if not nouns:
                continue
            strength = "STÆRK" if (modular or len(nouns) >= 2) else "medium"
            pids = list(master_prods[mp])
            titles = [prods[p]["title"] for p in pids][:3]
            rows.append({
                "master_pid": mp, "strength": strength, "type_nouns": ", ".join(sorted(nouns)),
                "option_name": nm, "n_products": len(pids),
                "n_variants": sum(len(vsku.get(p, set())) for p in pids),
                "option_values": " | ".join(sorted(vals))[:300],
                "sample_titles": " || ".join(titles),
                "handles": ", ".join(prods[p]["handle"] for p in pids[:5]),
            })
    rows.sort(key=lambda r: (r["strength"] != "STÆRK", -r["n_variants"]))
    out = r"C:\Users\APC\Desktop\bearing_variants.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
    strong = sum(1 for r in rows if r["strength"] == "STÆRK")
    print(f"✅ {out}")
    print(f"   {len(rows)} bærende-variant-masters ({strong} STÆRK, {len(rows)-strong} medium)")
    print("\n— 12 STÆRKE eksempler —")
    for r in rows[:12]:
        print(f"  [{r['type_nouns']}] {r['sample_titles'][:70]}")
        print(f"     option '{r['option_name']}' = {r['option_values'][:90]}")

if __name__ == "__main__":
    main()
