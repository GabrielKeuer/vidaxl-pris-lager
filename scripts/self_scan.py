"""SELV-SCAN af de genererede titler for mistænkelige mønstre (proaktiv gennemgang):
  seat_count   — sæde-antal i titel (Tosæders/N-personers/N-sæders) mens Bredde/Model/Antal varierer → variant
  residual_col — et farve-ord i titlen mens Farve er en akse (farve ikke strippet)
  seat_word    — 'stol'/'sofa'/'sovesofa' som varierer (form-variant i titel)
READ-ONLY. Output: konsol + output/self_scan.json."""
import sys, os, json, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, r"C:\Users\APC\dropxl-product-automation\scripts")
sys.stdout.reconfigure(encoding="utf-8")
import title_rules as TR

# bredt farve-leksikon (grund + almindelige) til at fange residual-farver min strip missede
COLORS = set(TR.COLOR_LEX) | {"skygrå", "himmelblå", "flaskegrøn", "army", "sand", "nougat", "cognac",
    "gyldenbrun", "grafitgrå", "stengrå", "perlehvid", "råhvid", "antikbrun", "champagne", "graphite"}

def main():
    P = json.load(open("output/complete_feed.json", encoding="utf-8"))
    seat, colr, form = [], [], []
    for p in P:
        if not p["specs"]:
            continue
        t = " " + p["title"].lower() + " "
        # a) sæde-antal
        if re.search(r"\b(to|tre|fire|fem|seks)sæders?\b|\b\d+[- ]personers?\b|\b\d+[- ]sæders?\b", t):
            seat.append(p["title"])
        # b) residual farve (Farve er akse)
        if "Farve" in p["specs"]:
            hit = [c for c in COLORS if len(c) > 3 and re.search(r"(?<=\W)" + re.escape(c) + r"(e|t|de|ne)?(?=\W)", t)]
            if hit:
                colr.append((p["title"], hit[:2]))
        # c) form-ord der varierer (Stol/Sofa/Sovesofa i variationAttribute)
        if re.search(r"\bsovesofa\b", t) and any("sofa" in v["values"].get(n, "").lower() for v in p["variants"] for n in p["specs"]):
            form.append(p["title"])
    print(f"=== SELV-SCAN: {len(P)} produkter ===")
    print(f"  a) sæde-antal i titel: {len(seat)}")
    for x in seat[:12]:
        print(f"       {x[:52]}")
    print(f"  b) residual FARVE i titel (Farve er akse): {len(colr)}")
    for x in colr[:12]:
        print(f"       {x[0][:46]} ← {x[1]}")
    json.dump({"seat": seat, "residual_color": colr}, open("output/self_scan.json", "w", encoding="utf-8"), ensure_ascii=False)

if __name__ == "__main__":
    main()
