"""KOMPLET fejl-audit af hele simulationen (complete_feed.json) mod feed + variantdata. READ-ONLY.
Kategorier:
  DUP    dobbelt-ord i titel ("Træ Træ")
  QUOTE  literal citationstegn i titel
  LEAK   titlen indeholder en af produktets EGNE varierende option-værdier
  COLOR  farve-ord i titel mens Farve er akse
  DIM    mål i titel mens en størrelse-akse varierer
  CNT    "N stk/dele" i titel mens Antal/count-akse varierer
  SHORT  titel < 2 ord eller tom
  LOWER  titel starter med småt/ikke-bogstav
  ODD    mistænkelig tegnsætning (dobbelt mellemrum, hængende bindestreg/komma)
Output: konsol-optælling + eksempler + output/audit_sim.json (alle fund pr. kategori)."""
import sys, os, io, zipfile, csv, json, re
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, r"C:\Users\APC\dropxl-product-automation\scripts")
sys.stdout.reconfigure(encoding="utf-8")
import title_rules as TR
import merge_executor as ME

STOP_NOUN = {"med", "og", "til", "på", "af", "for", "i", "stk", "dele", "sæt", "cm", "mm"}

COLORS = set(TR.COLOR_LEX) | {"skygrå", "himmelblå", "flaskegrøn", "nougat", "champagne", "gyldenbrun",
    "grafitgrå", "stengrå", "perlehvid", "råhvid", "antikbrun", "cognac", "graphite", "army", "sand"}
SIZE = {"Størrelse", "Højde", "Bredde", "Bordlængde", "Længde", "Dybde", "Diameter", "Størrelse 2"}
CNT = {"Antal", "Mængde", "Pakke", "Antal Dele", "Sæt"}

def main():
    P = json.load(open("output/complete_feed.json", encoding="utf-8"))
    z = zipfile.ZipFile(io.BytesIO(ME.get_feed_zip(os.environ["FEED_URL"])))
    nm = [f for f in z.namelist() if f.endswith(".csv")][0]
    feedt = {}
    for r in csv.DictReader(io.TextIOWrapper(z.open(nm), encoding="utf-8")):
        s = str(r.get("SKU") or "").strip().replace(".0", "")
        if s:
            feedt[s] = (r.get("Title") or "").lower()
    F = defaultdict(list)
    for p in P:
        t = p["title"]; tl = " " + t.lower() + " "
        specs = p["specs"]
        # DUP dobbelt-ord
        if re.search(r"\b(\w+)\s+\1\b", t, re.IGNORECASE):
            F["DUP"].append(t)
        # QUOTE
        if '"' in t or "”" in t or "“" in t:
            F["QUOTE"].append(t)
        # SHORT
        if len(re.findall(r"[a-zæøå0-9]+", tl)) < 2:
            F["SHORT"].append(t)
        # LOWER
        if t and not t[0].isupper() and t[0].isalpha():
            F["LOWER"].append(t)
        # ODD
        if "  " in t or re.search(r"[-,]\s*$|^\s*[-,]", t) or " - " in t and t.count(" - ") > 0 and re.search(r"[-]\s*$", t):
            F["ODD"].append(t)
        if not specs:
            continue
        # variant-værdier
        axvals = set()
        for v in p["variants"]:
            for nm in specs:
                x = (v["values"].get(nm) or "").strip()
                if x:
                    axvals.add(x)
        # LEAK: titel indeholder en varierende option-værdi
        leak = [x for x in axvals if x and len(x) > 1 and (" " + x.lower() + " ") in tl]
        if leak:
            F["LEAK"].append(f"{t}  ← {leak[:2]}")
        # COLOR
        if "Farve" in specs:
            hit = [c for c in COLORS if len(c) > 3 and re.search(r"(?<=\W)" + re.escape(c) + r"(e|t|de|ne)?(?=\W)", tl)]
            if hit:
                F["COLOR"].append(f"{t}  ← {hit[:2]}")
        # DIM
        if any(nm in SIZE for nm in specs) and re.search(r"\d+\s*[x×]\s*\d+|\d+(?:[.,]\d+)?\s*cm\b|\d+\s*mm\b|ø\s*\d", tl):
            F["DIM"].append(t)
        # CNT
        if any(nm in CNT for nm in specs) and re.search(r"\b\d+\s*(?:stk|dele|pcs|sæt)\b", tl):
            F["CNT"].append(t)
        # NOUN-dækning (præcis): en variant der deler INTET betydningsbærende titel-ord med sin egen
        # feed-titel er mis-grupperet (titel "Højskab" men SKUens feed siger "skænk"). Ordet må gerne
        # matche i bøjet/afkortet form. Kun "deler intet" flagges → få falske positiver.
        tw = {w for w in re.findall(r"[a-zæøå]+", tl) if w not in STOP_NOUN and len(w) > 3}
        if tw and specs:
            bad = []
            for v in p["variants"]:
                x = feedt.get(v["sku"], "")
                if x and not any(w in x or w[:-1] in x or w[:-2] in x for w in tw):
                    bad.append(v["sku"])
            if bad:
                F["NOUN"].append(f"{t}  ← {len(bad)}/{len(p['variants'])} varianter deler INTET titel-ord m. feed (fx SKU {bad[0]})")

    print(f"=== AUDIT: {len(P)} produkter ===")
    order = ["DUP", "QUOTE", "LEAK", "COLOR", "DIM", "CNT", "NOUN", "SHORT", "LOWER", "ODD"]
    for k in order:
        v = F.get(k, [])
        print(f"\n  {k}: {len(v)}")
        for x in v[:8]:
            print(f"       {x[:70]}")
    json.dump({k: F.get(k, []) for k in order}, open("output/audit_sim.json", "w", encoding="utf-8"), ensure_ascii=False)

if __name__ == "__main__":
    main()
