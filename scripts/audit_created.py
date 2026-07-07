"""READ-ONLY audit af ALLE produkter oprettet/ændret de sidste dage. Finder 3 fejltyper vha. feed-titlen
som autoritativ kilde:
  1) dup_title       — flere oprettede produkter med samme titel (over-split / strippet identitet)
  2) single_stripped — 1-variant produkt hvis titel mangler attributter der er i feed-titlen
  3) multi_mixed     — multi-variant produkt hvis varianter har FORSKELLIG fast-attribut (fx g/m²) —
                       dvs. feed-titlerne adskiller sig ud over de faktiske variant-akser (farve/størrelse)
INGEN mutationer. Output: output/audit_created.json + konsol-oversigt."""
import sys, os, io, zipfile, csv, re, json
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

CUTOFF = "2026-07-06"   # de sidste par dage

def clean(t):
    t = re.sub(r"(?i)\bvidaxl\b", "", t or "").strip()
    return re.sub(r"\s+", " ", t)

def toks(t):
    return set(re.findall(r"[a-zæøå0-9.,/²]+", (t or "").lower()))

def main():
    # feed-titler (cache)
    z = zipfile.ZipFile(io.BytesIO(ME.get_feed_zip(os.environ["FEED_URL"])))
    name = [f for f in z.namelist() if f.endswith(".csv")][0]
    ftitle = {}
    for r in csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8")):
        s = str(r.get("SKU") or "").strip().replace(".0", "")
        if s:
            ftitle[s] = r.get("Title") or ""
    print(f"feed-titler: {len(ftitle)}")

    # hent alle produkter oprettet siden CUTOFF
    prods = []; cursor = None
    while True:
        d = ME.gql('query($c:String){products(first:80,after:$c,sortKey:CREATED_AT,reverse:true,query:"vendor:vidaXL"){'
                   'pageInfo{hasNextPage endCursor} edges{node{handle title createdAt '
                   'options{name} variants(first:200){edges{node{sku selectedOptions{name value}}}}}}}}', {"c": cursor})
        pr = (d.get("data") or {}).get("products") or {}
        stop = False
        for e in pr.get("edges", []):
            node = e["node"]
            if node["createdAt"][:10] < CUTOFF:
                stop = True; break
            prods.append(node)
        if stop or not pr.get("pageInfo", {}).get("hasNextPage"):
            break
        cursor = pr["pageInfo"]["endCursor"]
    print(f"produkter oprettet siden {CUTOFF}: {len(prods)}")

    errs = {"dup_title": [], "single_stripped": [], "multi_mixed": []}
    title_count = Counter(p["title"] for p in prods)
    for p in prods:
        vs = [e["node"] for e in p["variants"]["edges"]]
        skus = [(v["sku"] or "").strip() for v in vs]
        title = p["title"]
        # variant-akse-værdier (farve/størrelse osv.) — de OK-variationer
        axisvals = set()
        for v in vs:
            for o in v["selectedOptions"]:
                if o["value"] and o["value"] != "Default Title":
                    axisvals |= toks(o["value"])
        # base-feed-titel pr. variant = feed-titel MINUS akse-værdier
        bases = set()
        for s in skus:
            ft = toks(clean(ftitle.get(s, "")))
            bases.add(frozenset(ft - axisvals))
        # 3) multi_mixed: multi-variant men baser adskiller sig (skjult fast-attribut varierer)
        if len(vs) > 1 and len(bases) > 1:
            # find de forskellige tokens
            allb = set().union(*bases); common = set.intersection(*[set(b) for b in bases])
            diff = allb - common
            errs["multi_mixed"].append({"handle": p["handle"], "title": title, "n": len(vs), "diff": sorted(diff)[:6]})
        # 2) single_stripped: 1 variant, feed-titel har tokens (mål/attribut) som produkt-titlen mangler
        if len(vs) == 1 and skus[0]:
            ft = toks(clean(ftitle.get(skus[0], "")))
            tt = toks(title)
            missing = {x for x in (ft - tt) if re.search(r"\d", x) or "g/m" in x}   # tal/attributter
            if missing:
                errs["single_stripped"].append({"handle": p["handle"], "title": title,
                                                 "feed": clean(ftitle.get(skus[0], ""))[:55], "mangler": sorted(missing)[:6]})
    # 1) dup_title
    for t, c in title_count.items():
        if c > 1:
            errs["dup_title"].append({"title": t, "antal": c})

    json.dump(errs, open("output/audit_created.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"\n=== FEJL blandt {len(prods)} oprettede produkter ===")
    print(f"  dup_title (samme titel flere gange): {len(errs['dup_title'])} unikke titler ({sum(x['antal'] for x in errs['dup_title'])} produkter)")
    print(f"  single_stripped (single m. strippet titel): {len(errs['single_stripped'])}")
    print(f"  multi_mixed (multi m. blandet fast-attribut): {len(errs['multi_mixed'])}")
    print("\n--- dup_title top 8 ---")
    for x in sorted(errs["dup_title"], key=lambda x: -x["antal"])[:8]:
        print(f"   {x['antal']}× \"{x['title'][:50]}\"")
    print("\n--- single_stripped (5 eks.) ---")
    for x in errs["single_stripped"][:5]:
        print(f"   \"{x['title'][:40]}\" mangler {x['mangler']} (feed: {x['feed']})")
    print("\n--- multi_mixed (5 eks.) ---")
    for x in errs["multi_mixed"][:5]:
        print(f"   \"{x['title'][:40]}\" ({x['n']}var) blander: {x['diff']}")

if __name__ == "__main__":
    main()
