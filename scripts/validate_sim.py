"""Validér catalog_titles_simulation.csv — find ALLE fejlmønstre i genererede titler.
Rapporterer counts + eksempler pr. check → styrer hvad der skal fikses næste iteration."""
import csv, re, sys
from collections import Counter
sys.stdout.reconfigure(encoding="utf-8")
F = r"C:\Users\APC\Desktop\catalog_titles_simulation.csv"
rows = list(csv.DictReader(open(F, encoding="utf-8-sig")))
print(f"{len(rows)} produkter")

CONN = {"og","med","til","i","på","af","samt","for","uden"}
COLOR = {"sort","hvid","hvidt","grå","gråt","brun","brunt","rød","rødt","orange","gul","gult","grøn","grønt",
         "blå","blåt","lilla","pink","lyserød","turkis","beige","creme","cremefarvet","naturfarvet","sølv",
         "sølvfarvet","guld","guldfarvet","gylden","gyldenbrun","bronze","kobber","antracit","antracitgrå",
         "lysegrå","mørkegrå","mørkebrun","betongrå","gråbrun","taupe","oliven","olivengrøn","bordeaux",
         "vinrød","koral","marineblå","natur","naturlig","egetræsfarve","egetræsfarvet","sonoma","røget",
         "transparent","mat","blank","højglans","messing","krom","nikkel","flerfarvet"}
def tk(s): return [t for t in s.split() if t]

checks = {}
def C(name, pred): checks[name] = [r for r in rows if r["generated_title"] and "MANUAL_REVIEW" not in (r.get("issues") or "") and pred(r)]

C("tom/for kort", lambda r: len(tk(r["generated_title"])) == 0 or len(r["generated_title"].strip()) < 3)
C("starter på bindeord", lambda r: tk(r["generated_title"]) and tk(r["generated_title"])[0].lower().strip(".,-") in CONN)
C("ender på bindeord", lambda r: tk(r["generated_title"]) and tk(r["generated_title"])[-1].lower().strip(".,-") in CONN)
C("hængende bindestreg", lambda r: bool(re.search(r"\w-\s(?!(?:Eller|Og)\s)|\s-\w|-$|^-", r["generated_title"])))
C("dobbelt-mellemrum/tegn", lambda r: "  " in r["generated_title"] or bool(re.search(r"[,;]{2}|,\s*,", r["generated_title"])))
C("vidaxl/pcs/mojibake", lambda r: bool(re.search(r"(?i)vidaxl|\bpcs\b|Ã|Â|â€", r["generated_title"])))
C("stavefejl (etræs/utræk/sofae)", lambda r: bool(re.search(r"(?i)\betræsfarve|\butrækkelig|sofaesæt", r["generated_title"])))
C("defekt mål (XxX-hængende)", lambda r: bool(re.search(r"\d+[xX]\d+[xX](?!\s*[\d(])", r["generated_title"])))
C("orphan cm/mm", lambda r: bool(re.search(r"(?<![\d)])\s(cm|mm)\b", r["generated_title"], re.I)))
# farve-rest: har Farve-option OG slutter på et farveord
C("farve-rest (Farve-option + farve i slut)", lambda r: r["source_type"] != "single" and "Farve" in r["option_names"] and tk(r["generated_title"]) and tk(r["generated_title"])[-1].lower().strip(".,-") in COLOR)
# casing-miss: et ord (>2 bogstaver, kun bogstaver) er helt lowercase
C("casing-miss (lowercase ord)", lambda r: any(w.isalpha() and len(w) > 2 and w.islower() for w in tk(r["generated_title"])))
# størrelses-rest: har Størrelse/Længde/Bredde/Højde-option OG title har et mål-tal med enhed
C("størrelses-rest (dim-option + mål i titel)", lambda r: bool(re.search(r"(Størrelse|Længde|Bredde|Højde|Diameter)", r["option_names"])) and bool(re.search(r"\d+\s*[xX×]\s*\d+|\d+\s*cm\b", r["generated_title"])))

ENG = {"with","and","the","black","white","grey","gray","mirror","cabinet","chair","garden","wall","wood",
       "steel","bathroom","kitchen","folding","outdoor","cushion","frame","bench","shelf","table","drawer",
       "corner","middle","seat","cover","piece","for","set","sofa","stool","door","clock","rack","desk"}
C("engelsk-rest", lambda r: sum(1 for w in re.findall(r"\b[a-z]+\b", r["generated_title"].lower()) if w in ENG and w not in ("for","set","sofa")) >= 2)
C("dubleret nabo-ord", lambda r: bool(re.search(r"\b(\w{3,})\s+\1\b", r["generated_title"], re.I)))
C("kun-tal/tegn (intet navn)", lambda r: r["generated_title"] and not re.search(r"[A-Za-zÆØÅæøå]{3}", r["generated_title"]))
C("blev meget længere (merge/behold)", lambda r: r["source_type"] in ("merge", "behold", "uændret") and len(r["generated_title"]) > len(r["original_title"]) + 25)

print("\n===== VALIDERING =====")
tot = 0
for name, items in checks.items():
    tot += len(items)
    flag = "✅" if not items else "⚠️"
    print(f"{flag} {name}: {len(items)}")
    for r in items[:4]:
        print(f"      {r['original_title'][:40]!r} → {r['generated_title'][:55]!r} [{r['source_type']}|{r['option_names']}]")
print(f"\nTOTAL fejl-flag: {tot}  ({'PERFEKT ✅' if tot == 0 else 'skal fikses'})")
print(f"changed: {sum(1 for r in rows if r['changed']=='ja')} | needs_llm: {sum(1 for r in rows if r.get('needs_llm'))}")
