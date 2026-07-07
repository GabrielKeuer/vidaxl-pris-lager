"""LLM-REPARATØR (sidste lag): retter alle ikke-OK titler fra dommeren — eller bekræfter OK.
Input: judge_results.csv (ikke-OK) + catalog_titles_simulation.csv. Strenge regler: opfind intet,
bevar produkt-identitet, fjern kun variant-akse-egenskaber, korrekt dansk Title Case.
Regex-gate på alle rettelser før de skrives. READ-ONLY ift. Shopify."""
import csv, json, os, re, sys, time, functools, requests
from collections import Counter
sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
KEY = os.environ["ANTHROPIC_API_KEY"]; MODEL = "claude-sonnet-5"
JUDGE = r"C:\Users\APC\Desktop\judge_results.csv"
CAT = r"C:\Users\APC\Desktop\catalog_titles_simulation.csv"
BATCH = 15

SYS = (
    "Du reparerer danske produkttitler for en møbel-webshop. For hver post får du: type, variant-akser, "
    "nuværende genereret titel og dommerens kritik.\n"
    "Opgave: Ret titlen så den er en fornuftig, korrekt dansk produkttitel — ELLER svar at den allerede er OK "
    "(dommeren kan være for pedantisk).\n"
    "Du får også ORIGINAL-titlen (før generering) som kilde.\n"
    "REGLER:\n"
    "- OPFIND ALDRIG information. Men du MÅ genskabe ord fra ORIGINALEN som blev tabt ved en fejl "
    "(fx materiale 'Stof'/'Fløjl' der IKKE er en variant-akse — tjek akse-listen!).\n"
    "- Fjern KUN egenskaber der matcher en variant-AKSE i listen. Står 'Materiale' ikke i akserne, skal materialet BLIVE.\n"
    "- Bevar produkt-identiteten (hovedordet). Du må omformulere klodset dansk og fjerne rester/garble.\n"
    "- For type single: farve/størrelse må gerne blive stående.\n"
    "- HUSETS STIL: ingen 'i'/'af' før materiale ('Sofabord Massivt Akacietræ' er korrekt). 'Konstrueret Træ' er korrekt fagterm. "
    "'N Dele' i sæt-navne er del af identiteten. Ental/flertal-blanding ('Lænestol Med Hynder') er OK.\n"
    "- Title Case. Behold forkortelser som LED, PVC, MDF.\n"
    '- Er titlen reelt fin som den er: {"fix":null}.\n'
    'Svar KUN JSON: [{"i":1,"fix":"Ny Titel"|null}, ...]'
)

def call(items):
    lines = [f'{it["i"]}. [{it["t"]}|akser: {it["o"]}] original: "{it["orig"]}" → genereret: "{it["gen"]}" — kritik: {it["note"]}' for it in items]
    body = {"model": MODEL, "max_tokens": 3000, "system": SYS,
            "messages": [{"role": "user", "content": "\n".join(lines)}]}
    H = {"x-api-key": KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}
    for a in range(1, 6):
        try:
            r = requests.post("https://api.anthropic.com/v1/messages", json=body, headers=H, timeout=(10, 90))
            if r.status_code == 429:
                if a < 5: time.sleep(5 * a); continue
                return {}
            txt = "".join(b.get("text", "") for b in r.json().get("content", []))
            return {int(o["i"]): o.get("fix") for o in json.loads(re.search(r"\[.*\]", txt, re.S).group(0))}
        except Exception:
            if a >= 5: return {}
            time.sleep(3 * a)
    return {}

_GCOL = {"sort","hvid","hvidt","grå","brun","brunt","rød","orange","gul","grøn","blå","lilla","pink","turkis",
         "beige","creme","antracit","lysegrå","mørkegrå","mørkebrun","gyldenbrun","cremefarvet","naturfarvet","sonoma",
         "eg","egetræsfarve","egetræsfarvet","røget"}
def gate(t, option_names="", source_type=""):
    """Regex-gate: afvis LLM-rettelser med åbenlyse fejl ELLER der tilføjer akse-egenskaber."""
    if not t or len(t.strip()) < 4: return False
    if not re.search(r"[A-Za-zÆØÅæøå]{3}", t): return False
    if re.search(r"(?i)vidaxl|Ã|â€", t): return False
    if re.search(r"\s{2}|^\s|\s$", t): return False
    toks = t.split()
    if toks[-1].lower().strip(".,-–") in ("og", "med", "til", "i", "på", "uden", "x"): return False
    if source_type != "single":
        if "Farve" in option_names and any(w.lower().strip(".,-–") in _GCOL for w in toks): return False
        if re.search(r"(Størrelse|Længde|Bredde|Højde|Diameter|Dybde)", option_names) and re.search(r"\d+\s*[xX×]\s*\d+|\b\d+(?:[.,]\d+)?\s*[Cc]m\b", t): return False
    return True

jr = list(csv.DictReader(open(JUDGE, encoding="utf-8-sig")))
_cat0 = list(csv.DictReader(open(CAT, encoding="utf-8-sig")))
_vetted = {r["rid"] for r in _cat0 if "fixer_confirmed_ok" in (r.get("issues") or "")}
bad = [r for r in jr if r["_dom"] in ("SUBTIL", "BRUDT") and r.get("rid") not in _vetted]
print(f"🔧 reparatør: {len(bad)} ikke-OK titler (efter fravalg af {len(_vetted)} vetted)")

fixes = {}    # rid -> fixed (række-identitet, aldrig titel-nøgle)
confirms = set()  # rid -> reparatør bekræfter eksplicit at titlen er fin (fix=null)
from concurrent.futures import ThreadPoolExecutor
import threading
_lk = threading.Lock(); _pg = [0]
def do_chunk(chunk):
    items = [{"i": j + 1, "t": r["source_type"], "o": r["option_names"] or "-", "orig": r["original_title"][:160],
              "gen": r["generated_title"], "note": r["_note"][:90]} for j, r in enumerate(chunk)]
    res = call(items)
    with _lk:
        for j, r in enumerate(chunk):
            if (j + 1) not in res: continue
            fx = res.get(j + 1)
            if fx is None:
                confirms.add(r["rid"])
            elif fx and fx != r["generated_title"] and gate(fx, r["option_names"], r["source_type"]):
                fixes[r["rid"]] = fx
        _pg[0] += len(chunk)
        if _pg[0] % 100 < BATCH: print(f"  …{_pg[0]}/{len(bad)}")
with ThreadPoolExecutor(max_workers=5) as ex:
    list(ex.map(do_chunk, [bad[b0:b0 + BATCH] for b0 in range(0, len(bad), BATCH)]))

print(f"✏️ {len(fixes)} rettelser, {len(confirms)} eksplicitte OK-bekræftelser")
cat = list(csv.DictReader(open(CAT, encoding="utf-8-sig")))
n = 0
for r in cat:
    if r.get("rid") in fixes:
        r["generated_title"] = fixes[r["rid"]]
        r["issues"] = (r["issues"] + "; " if r["issues"] else "") + "llm_fixed"
        n += 1
    elif r.get("rid") in confirms and "fixer_confirmed_ok" not in (r["issues"] or ""):
        r["issues"] = (r["issues"] + "; " if r["issues"] else "") + "fixer_confirmed_ok"
with open(CAT, "w", encoding="utf-8-sig", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(cat[0].keys())); w.writeheader(); w.writerows(cat)
print(f"✅ {n} rækker opdateret i katalog-CSV")
print("\n— 20 eksempler —")
for i, ((g, t), fx) in enumerate(list(fixes.items())[:20]):
    print(f"   [{t}] {g[:48]!r} → {fx[:48]!r}")
