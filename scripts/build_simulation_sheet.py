"""KOMPLET SIMULATIONS-MASTER-SHEET til gennemsyn FØR oprettelse.
Én række pr. SKU: product_key · master_pid · sku · original_vidaXL_titel · genereret_titel · type
(single/variant) · option1/2/3_navn+værdi · flag · har_llm_forslag · llm_forslag.
FLAG: multi-produkt hvor den genererede titel STADIG indeholder en varierende akse-værdi (fx "12 Flasker"
hvor Antal 12/24/36 varierer). LLM foreslår KUN en ren titel til de flaggede. Filtrér i Excel på
'har_llm_forslag'. READ-ONLY (opretter intet). Output: Desktop/simulation_master.csv."""
import sys, os, io, zipfile, csv, re, json, time, urllib.request, urllib.error
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import build_complete_feed as B   # housestyle (spelling + casing)

_HUB = r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local"
if os.path.exists(_HUB):
    for l in open(_HUB, encoding="utf-8"):
        m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
        if m:
            os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

def clean(t):
    return re.sub(r"\s+", " ", re.sub(r"(?i)\bvidaxl\b", "", t or "")).strip()

SYSTEM = (
    "Du retter én produkt-titel i en dansk webshop. Produktet er en VARIANT-GRUPPE. Den nuværende "
    "genererede titel indeholder fejlagtigt en VARIERENDE variant-værdi (typisk et antal/stk-tal, en "
    "størrelse eller farve som kunden vælger). Fjern KUN de varierende variant-værdier, behold "
    "produktnavn + FASTE egenskaber (materiale, g/m², faste mål). Grammatisk korrekt dansk, ingen "
    "'vidaXL', stort forbogstav, LED/PVC/MDF med versaler. Svar KUN JSON: {\"titel\":\"...\"}"
)

def llm_title(pkey, cur_title, variant_titles, options):
    body = {"model": "claude-sonnet-5", "max_tokens": 300, "system": SYSTEM,
            "messages": [{"role": "user", "content":
                f"Nuværende genererede titel (FEJL): \"{cur_title}\"\n"
                f"Variant-options (det kunden vælger): {options}\n"
                f"Eksempler på varianters oprindelige vidaXL-titler:\n" +
                "\n".join(f"  - {t}" for t in variant_titles[:6])}]}
    H = {"x-api-key": API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}
    for a in range(1, 4):
        try:
            req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=json.dumps(body).encode(), headers=H)
            with urllib.request.urlopen(req, timeout=60) as r:
                d = json.loads(r.read().decode())
            txt = "".join(b.get("text", "") for b in d.get("content", []))
            return json.loads(re.search(r"\{.*\}", txt, re.DOTALL).group(0)).get("titel", "")
        except Exception:
            time.sleep(2 * a)
    return ""

def main():
    P = json.load(open("output/complete_feed.json", encoding="utf-8"))
    z = zipfile.ZipFile(io.BytesIO(ME.get_feed_zip(os.environ["FEED_URL"])))
    name = [f for f in z.namelist() if f.endswith(".csv")][0]
    feed = {}
    for r in csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8")):
        s = str(r.get("SKU") or "").strip().replace(".0", "")
        if s:
            feed[s] = r.get("Title") or ""

    # FLAG: multi hvor titlen (a) indeholder en varierende akse-værdi ELLER (b) har et mål i titlen mens
    # en størrelse-akse varierer (usædvanligt mål-format min strip ikke fangede).
    SIZE = {"Størrelse", "Højde", "Bredde", "Bordlængde", "Længde", "Dybde", "Diameter"}
    flagged = {}
    for p in P:
        if not p["specs"]:
            continue
        axvals = set()
        for v in p["variants"]:
            for nm in p["specs"]:
                x = (v["values"].get(nm) or "").strip()
                if x:
                    axvals.add(x)
        tl = " " + p["title"].lower() + " "
        leak = [x for x in axvals if x and (" " + x.lower() + " ") in tl]
        size_dim = bool(any(nm in SIZE for nm in p["specs"]) and re.search(r"\d+\s*[x×]\s*\d+|\d+\s*cm\b|\d+\s*mm\b|ø\s*\d", tl))
        if leak or size_dim:
            flagged[p["key"]] = p
    print(f"produkter: {len(P)} | flaggede (tal-i-titel + mål-misser): {len(flagged)}")

    # LLM-forslag KUN på de flaggede
    sugg = {}
    if os.path.exists("output/title_suggestions.json"):
        sugg = json.load(open("output/title_suggestions.json", encoding="utf-8"))
    todo = [k for k in flagged if k not in sugg]
    print(f"LLM-forslag: {len(sugg)} allerede, {len(todo)} tilbage")
    for i, k in enumerate(todo, 1):
        p = flagged[k]
        vtitles = [clean(feed.get(v["sku"], "")) for v in p["variants"]]
        opts = {nm: sorted({v["values"].get(nm, "") for v in p["variants"] if v["values"].get(nm)}) for nm in p["specs"]}
        sugg[k] = llm_title(k, p["title"], vtitles, opts)
        if i % 10 == 0:
            json.dump(sugg, open("output/title_suggestions.json", "w", encoding="utf-8"), ensure_ascii=False)
            print(f"  …{i}/{len(todo)}", flush=True)
    json.dump(sugg, open("output/title_suggestions.json", "w", encoding="utf-8"), ensure_ascii=False)

    # RESOLVER hver flagget SELV: brug LLM-forslag renset gennem housestyle HVIS validt, ellers kun-manuel
    def title_valid(title, size_varies):
        tl = " " + (title or "").lower() + " "
        if not re.search(r"[a-zæøå]{3}", tl):
            return False                                                   # intet produktnavn
        if size_varies and re.search(r"\d+\s*[x×]\s*\d+|\d+\s*cm\b|\d+\s*mm\b|ø\s*\d", tl):
            return False                                                   # residual mål
        if re.search(r"\b\d+\s*(?:stk|dele|pcs)\b", tl):
            return False                                                   # residual antal
        return True
    ovr = json.load(open("output/title_overrides.json", encoding="utf-8")) if os.path.exists("output/title_overrides.json") else {}
    resolved = {}
    for k, p in flagged.items():
        o = ovr.get(k, {}) if isinstance(ovr.get(k), dict) else {}
        if o.get("review") or not (o.get("title") or o.get("keep")):
            resolved[k] = (p["title"], "kraever_manuel")   # kun DISSE skal du kigge på
        else:
            resolved[k] = (p["title"], "rettet" if o.get("title") else "ok_fast_maal")
    json.dump({k: {"titel": v[0], "status": v[1]} for k, v in resolved.items()},
              open("output/title_resolved.json", "w", encoding="utf-8"), ensure_ascii=False)
    man = sum(1 for _, s in resolved.values() if s == "kraever_manuel")
    print(f"  RESOLVERET: {len(resolved) - man} auto-rettet, {man} kræver manuel")

    # MASTER-SHEET (én række pr. SKU) — endelig_titel = den vi bruger; status filtrerbar
    out = r"C:\Users\APC\Desktop\simulation_master.csv"
    rows = 0
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_key", "master_pid", "sku", "original_vidaXL_titel", "endelig_titel",
                    "raa_genereret", "type", "option1_navn", "option1_vaerdi", "option2_navn",
                    "option2_vaerdi", "option3_navn", "option3_vaerdi", "status", "llm_forslag"])
        for p in P:
            typ = "variant" if p["specs"] else "single"
            names = p["specs"][:3]
            fin, status = resolved.get(p["key"], (p["title"], "ok"))
            lf = sugg.get(p["key"], "")
            for v in sorted(p["variants"], key=lambda x: x.get("pos", 0)):
                row = [p["key"], p["mid"], v["sku"], clean(feed.get(v["sku"], "")), fin, p["title"], typ]
                for i in range(3):
                    row += [names[i], v["values"].get(names[i], "")] if i < len(names) else ["", ""]
                row += [status, lf]
                w.writerow(row)
                rows += 1
    print(f"\n✓ SIMULATIONS-MASTER-SHEET: {rows} rækker → {out}")

if __name__ == "__main__":
    main()
