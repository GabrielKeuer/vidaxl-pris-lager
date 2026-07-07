"""POLISH-lag oven på genererede titler: aggressiv farve-fjernelse (delte titler) + dedup +
slash/dangling-oprydning + akronymer. Retter dommerens fund. Patcher CSV + kan porteres til sim.
READ-ONLY ift. Shopify."""
import csv, re, sys
sys.stdout.reconfigure(encoding="utf-8")
F = r"C:\Users\APC\Desktop\catalog_titles_simulation.csv"

# Farve-akser (når en af dem er variant, må farve IKKE stå i den delte titel)
FARVE_AXES = ("farve", "color", "colour", "kulør", "hyndefarve", "stelfarve", "rammefarve", "betrækfarve", "hynde farve")
# Rene farveord (IKKE materialer — 'eg','træ','stål' er materialer og bevares)
COLORS = {  # kun entydige farveord (tvetydige som lys/sand/rust/naturlig udeladt — fanges via -farvet-suffiks)
 "sort","sorte","hvid","hvidt","hvide","grå","gråt","gråbrun","brun","brunt","brune","rød","rødt","røde",
 "orange","gul","gult","gule","grøn","grønt","grønne","blå","blåt","lilla","violet","pink","lyserød",
 "turkis","beige","creme","cremehvid","cremehvide","antracit","antracitgrå","taupe","oliven","olivengrøn",
 "bordeaux","vinrød","koral","marineblå","sølv","guld","gylden","bronze","kobber","messing","krom",
 "nikkel","transparent","klar","meleret","flerfarvet","aubergine","cappuccino","mokka","terracotta",
 "røget","antikgrå","stengrå","betongrå","grafitgrå","sølvgrå","perlehvid","råhvid","offwhite","anthracit",
}
LIGHTDARK = re.compile(r"(?i)^(lyse?|mørke?|antik|mat|blank)(grå|brun|blå|grøn|rød|beige|gul|lilla|rosa|violet|natur)$")
CONN = {"og","med","i","på","af","samt","for","uden","til","+","&"}
ACR = {"pp":"PP","pe":"PE","pu":"PU","pvc":"PVC","eva":"EVA","pet":"PET","mdf":"MDF","hdpe":"HDPE","abs":"ABS",
       "wpc":"WPC","led":"LED","uv":"UV","tv":"TV","usb":"USB","pp-":"PP"}

def is_color(w):
    lw = w.lower().strip(".,-–+&/")
    if not lw: return False
    if lw in COLORS: return True
    if re.search(r"farvet?$", lw) and lw not in ("ufarvet",): return True   # sandfarvet, kanelfarvet, egetræsfarvet…
    if LIGHTDARK.match(lw): return True
    return False

def strip_colors(t):
    toks = t.split()
    out = []
    for i, w in enumerate(toks):
        nxt = toks[i + 1].lower().strip(".,-–") if i + 1 < len(toks) else ""
        prv = toks[i - 1].lower().strip(".,-–") if i > 0 else ""
        # beskyt lysfarve-temperatur: 'Varmt Hvidt Lys' (kun når naboordet er præcis 'lys')
        if is_color(w) and nxt != "lys" and prv not in ("varm", "varmt", "kold", "koldt"):
            continue
        # standalone 'Lys/Mørk' foran et farveord ('Lys Grå') → fjern begge (men ikke foran 'Farvet/Farve')
        if w.lower().strip(".,-–") in ("lys", "mørk", "lyse", "mørke"):
            if nxt not in ("farvet", "farve", "lys") and i + 1 < len(toks) and is_color(toks[i + 1]):
                continue
        # Eg/Sonoma er dekor-FARVER på konstrueret træ (ikke materiale, medmindre 'Massivt Eg')
        lw = w.lower().strip(".,-–")
        if lw in ("eg", "sonoma", "sonoma-eg", "røget") and prv not in ("massiv", "massivt"):
            continue
        out.append(w)
    return " ".join(out)

def cleanup(t):
    # dangling 'og' lige før et mål ('Markise Og 350 X 200 Cm' → 'Markise 350...')
    t = re.sub(r"(?i)\bog\s+(?=\d+(?:[.,]\d+)?\s*(?:[xX×]|cm|mm|m\b))", "", t)
    toks = t.split()
    while toks and toks[0].lower().strip(".,-–") in CONN: toks.pop(0)
    while toks and toks[-1].lower().strip(".,-–") in CONN: toks.pop()
    res = []
    for w in toks:
        if res and w.lower().strip(".,-–") in CONN and res[-1].lower().strip(".,-–") in CONN: continue
        res.append(w)
    return " ".join(res)

def dedup(t):  # KUN bogstavsord — aldrig tal/mål (ellers ødelægges '40 X 40 X 60')
    t = re.sub(r"\b([A-Za-zÆØÅæøå]+\s+[A-Za-zÆØÅæøå]+)\s+\1\b", r"\1", t, flags=re.I)  # bigram 'Med Pude Med Pude'
    t = re.sub(r"\b([A-Za-zÆØÅæøå]{2,})\s+\1\b", r"\1", t, flags=re.I)                 # ord 'Eg Eg'
    t = re.sub(r"\b([A-Za-zÆØÅæøå]{3,})\s+(?:Og|Med)\s+\1\b", r"\1", t, flags=re.I)    # 'Mørk Og Mørk', 'Gardiner Med Gardiner'
    return t

def fix_slash(t):
    t = re.sub(r"\s*/\s*(?=\s|$)", " ", t)      # trailing/orphan slash
    t = re.sub(r"(?<=\s)/\s+", " ", t)          # ' / ord' → ' ord'
    return re.sub(r"\s*/\s*", "/", t) if re.search(r"\w/\w", t) else t

def fix_acr(t):
    return " ".join(ACR.get(w.lower().strip("."), w) for w in t.split())

TRAIL_REST = {"farve", "farvet", "lys", "lyse", "mørk", "mørke", "eg", "sonoma", "sonoma-eg", "røget", "naturlig", "naturfarvet", "natur"}
def polish(t, option_names, source_type):
    orig = t
    onl = option_names.lower()
    if source_type != "single" and any(a in onl for a in FARVE_AXES):
        st = strip_colors(t)
        # guard: strip ikke hvis resultatet mangler et rigtigt navneord (≥3 bogstaver)
        mean = [w for w in st.split() if w.lower().strip(".,-–") not in CONN]
        if mean and any(len(re.sub(r"[^A-Za-zÆØÅæøå]", "", w)) >= 3 for w in mean):
            t = st
        # trailing farve-rester ('...Stof Lys', '...Metal Eg', '...Farve') — men bevar 'Varmt Hvidt Lys'
        toks = t.split()
        while len(toks) > 1 and toks[-1].lower().strip(".,-–") in (TRAIL_REST | CONN):
            if toks[-1].lower().strip(".,-–") == "lys" and toks[-2].lower().strip(".,-–") in ("hvid", "hvidt", "varm", "varmt", "kold", "koldt"):
                break
            toks.pop()
        t = " ".join(toks)
    if "stofbredde" in t.lower() and not re.search(r"(?i)stofbredde\s*:?\s*\d", t):
        t = re.sub(r"(?i)\bstofbredde\b", " ", t)   # orphan label uden tal
    t = re.sub(r"(?i)\betræ\b", "Egetræ", t)
    t = re.sub(r"(x\(\d+-\d+\))(?!\s*(?:cm|mm|m)\b)", r"\1 Cm", t, flags=re.I)  # '100x40x(2-4)' → '+ Cm'
    t = re.sub(r"^([A-ZÆØÅ][a-zæøå]+)\.\s", r"\1 ", t)            # 'Markise. LED' → 'Markise LED'
    t = re.sub(r"(?i)\b(\d+\s+dele|\d+\s+stk\.?|sæt)\s+og\s+(?=(polyrattan|rattan|stof|stål|træ|metal|fløjl|velour|kunstlæder|polyester|aluminium|glas|bambus|jern|plastik|akryl)\b[^ ]*$)", r"\1 ", t)  # '5 Dele Og Polyrattan' (dangling efter strippet farve) — IKKE 'Træ Og Metal'
    t = fix_slash(t); t = dedup(t); t = cleanup(t); t = fix_acr(t)
    t = re.sub(r"\((\w)", lambda m: "(" + m.group(1).upper(), t)  # '(stel' → '(Stel'
    t = re.sub(r"(?<![\d,.])\s+[A-Za-z]$", "", t)                 # trailing enkelt-bogstav 'Bambus T' (ikke '6 M')
    return re.sub(r"\s+", " ", t).strip(" ,-–+&/")

import sys as _s
WRITE = "--write" in _s.argv
rows = list(csv.DictReader(open(F, encoding="utf-8-sig")))
changes, warn = [], []
for r in rows:
    ng = polish(r["generated_title"], r["option_names"], r["source_type"])
    if ng != r["generated_title"]:
        changes.append((r["source_type"], r["generated_title"], ng))
        if len(ng.split()) < 2 or not re.search(r"[A-Za-zÆØÅæøå]{3}", ng): warn.append((r["generated_title"], ng))
        if WRITE:
            r["generated_title"] = ng; r["changed"] = "ja" if ng != r["original_title"] else "nej"
print(f"{'SKREVET' if WRITE else 'PREVIEW'}: {len(changes)} titler ændret")
print(f"⚠️ mistænkeligt korte/tomme: {len(warn)}")
for o, g in warn[:15]: print(f"     {o!r} → {g!r}")
print("\n— 25 tilfældige ændringer —")
import random; random.seed(2)
for t, o, g in random.sample(changes, min(25, len(changes))):
    print(f"  [{t}] {o[:48]!r} → {g[:48]!r}")
if WRITE:
    with open(F, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
    print(f"\n✅ skrevet → {F}")
