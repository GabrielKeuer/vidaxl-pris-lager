"""SLAVISK validering af ALLE flagged_resolved-beslutninger (håndterer både enkelt- og multi-akse-struktur).
Tjekker: unikke akse-kombinationer (ingen Shopify-kollision), akse ikke lækket i titel, tom titel,
tomme akse-værdier, falsk variant (1 kombo). READ-ONLY. Output: output/flagged_validation.json."""
import sys, os, json
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")

def variant_combo(v):
    """Returnér (kombo-tuple, alle-værdier-liste) uanset struktur."""
    if "akse_vaerdier" in v and isinstance(v["akse_vaerdier"], dict):
        d = {k: (str(x).strip()) for k, x in v["akse_vaerdier"].items()}
        return tuple(sorted(d.items())), list(d.values())
    val = (v.get("akse_vaerdi") or "").strip()
    return ((val,) if val else ()), ([val] if val else [])

def prod_axis(p):
    if p.get("akser"):
        return p["akser"]
    return [p["variant_akse"]] if p.get("variant_akse") else []

def main():
    d = json.load(open("output/flagged_resolved.json", encoding="utf-8"))
    issues = defaultdict(list); ok = 0
    for mid, prods in d.items():
        prob = False
        for p in prods:
            title = (p.get("titel") or "").strip()
            axes = prod_axis(p)
            vs = p.get("varianter") or []
            combos, allvals = [], []
            for v in vs:
                c, vals = variant_combo(v)
                combos.append(c); allvals += vals
            if not title:
                issues["titel_tom"].append(mid); prob = True
            if axes:
                if any(not c for c in combos):
                    issues["tom_akse"].append({"mid": mid, "titel": title}); prob = True
                if len(combos) != len(set(combos)):
                    dups = [c for c, n in Counter(combos).items() if n > 1]
                    issues["dup_kombo"].append({"mid": mid, "titel": title, "akser": axes, "dubletter": [str(x) for x in dups[:3]]}); prob = True
                elif len(vs) > 1 and len(set(combos)) <= 1:
                    issues["enkelt_kombo"].append({"mid": mid, "titel": title}); prob = True
                tl = title.lower()
                leak = [x for x in set(allvals) if x and len(x) > 2 and x.lower() in tl]
                if leak:
                    issues["titel_har_akse"].append({"mid": mid, "titel": title, "laekket": leak[:3]}); prob = True
        if not prob:
            ok += 1
    json.dump({k: v for k, v in issues.items()}, open("output/flagged_validation.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"=== SLAVISK VALIDERING: {len(d)} beslutninger | ✅ ok: {ok} ({100*ok/len(d):.1f}%) ===")
    for k in ("dup_kombo", "tom_akse", "enkelt_kombo", "titel_har_akse", "titel_tom"):
        v = issues.get(k, [])
        if v:
            print(f"  ⚠ {k}: {len(v)}")
            for x in v[:3]:
                print(f"       {x}")

if __name__ == "__main__":
    main()
