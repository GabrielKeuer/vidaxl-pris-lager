"""LLM-DOMMER: bedøm semantisk korrekthed af genererede titler (giver mening / subtil / brudt).
Type-specifikke regler. Judger alle splits+singles+merges + stikprøve behold/uændret.
Output: C:\\Users\\APC\\Desktop\\judge_results.csv + summary. READ-ONLY."""
import csv, json, os, re, sys, time, random, functools, requests
from collections import Counter
sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
KEY = os.environ["ANTHROPIC_API_KEY"]; MODEL = "claude-sonnet-5"
CSV = r"C:\Users\APC\Desktop\catalog_titles_simulation.csv"; OUT = r"C:\Users\APC\Desktop\judge_results.csv"
BATCH = 20

SYS = (
    "Du er dansk produkttitel-dommer for en møbel-webshop. For hvert produkt får du: type, variant-akser (options), "
    "nuværende titel og GENERERET titel. Bedøm KUN den genererede titel.\n"
    "Dom: 'OK' | 'SUBTIL' | 'BRUDT'.\n"
    "Regler:\n"
    "- merge/split/behold/uændret = DELT produkttitel. Variant-akserne (fx Farve, Størrelse) må IKKE stå i titlen "
    "(kunden vælger dem som variant). Titlen skal have et hovedord (produkttype), være meningsfuld og korrekt dansk.\n"
    "- single = ÉT specifikt produkt: farve/størrelse MÅ gerne stå (det er identitet). Skal blot være meningsfuld "
    "dansk uden rester/garble.\n"
    "VIGTIGT for split OG single: den viste 'nuværende titel' er fra en ANDEN/blandet listing — produkttypen SKAL "
    "gerne afvige (det er hele pointen med at splitte). Døm KUN om den GENEREREDE titel i sig selv er en fornuftig, "
    "korrekt dansk titel for ét konkret produkt. Straf ALDRIG at typen/antal/farve afviger fra den nuværende titel.\n"
    "- SUBTIL = teknisk ok men skævt: mistet et vigtigt ord, beholdt en variant-egenskab der burde være væk, klodset sprog.\n"
    "- BRUDT = giver ikke mening / fragment / åbenlys fejl.\n"
    "HUSETS STIL (fejl-flag IKKE disse): ingen 'i'/'af' før materiale ('Sofabord Massivt Akacietræ' er korrekt stil); "
    "'Konstrueret Træ' er korrekt fagterm; 'N Dele' i sæt-navne er identitet (ikke variant); "
    "ental/flertal-blanding ('Lænestol Med Hynder') er OK; mål-format '60x40x30 Cm' og '60 X 40 X 30 Cm' er begge OK.\n"
    "MÅL-POLITIK (bevidst design, flag IKKE): når blot ÉN dimension (Bredde/Højde/Længde/Dybde/Størrelse) er variant-akse, "
    "fjernes HELE målblokken — delmål uden den varierende dimension er tvetydige. En titel helt uden mål er derfor "
    "KORREKT når produktet har en dimensions-akse.\n"
    'Svar KUN JSON: [{"i":1,"dom":"OK","note":"kort dansk"}, ...]'
)

def call(items):
    lines = [f'{it["i"]}. [{it["t"]}|{it["o"]}] "{it["orig"]}" → "{it["gen"]}"' for it in items]
    body = {"model": MODEL, "max_tokens": 3000, "system": SYS,
            "messages": [{"role": "user", "content": "\n".join(lines)}]}
    H = {"x-api-key": KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}
    for a in range(1, 6):
        try:
            r = requests.post("https://api.anthropic.com/v1/messages", json=body, headers=H, timeout=(10, 90))
            if r.status_code == 429:
                if a < 5: time.sleep(5 * a); continue
                return {}
            d = r.json()
            txt = "".join(b.get("text", "") for b in d.get("content", []))
            return {int(o["i"]): (o.get("dom", "?"), o.get("note", "")) for o in json.loads(re.search(r"\[.*\]", txt, re.S).group(0))}
        except Exception:
            if a >= 5: return {}
            time.sleep(3 * a)
    return {}

rows = list(csv.DictReader(open(CSV, encoding="utf-8-sig")))
random.seed(3)
if "--all" in sys.argv:
    sample = [r for r in rows if "MANUAL_REVIEW" not in (r.get("issues") or "")]
    print(f"🔎 FULD dom over {len(sample)} produkter (hele kataloget) — {MODEL}")
else:
    risky = [r for r in rows if r["source_type"] in ("split", "single", "merge")]
    rest = [r for r in rows if r["source_type"] in ("behold", "uændret")]
    sample = risky + random.sample(rest, min(800, len(rest)))
    print(f"🔎 dommer over {len(sample)} produkter (alle {len(risky)} risikable + 800 stikprøve) — {MODEL}")

CKPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "output", "judge_ckpt.jsonl")
done = {}
if os.path.exists(CKPT) and "--fresh" not in sys.argv:
    for l in open(CKPT, encoding="utf-8"):
        try:
            d = json.loads(l)
            if d.get("gen"): done[(d["rid"], d["gen"])] = (d["dom"], d["note"])
        except Exception: pass
    if done: print(f"↩️ checkpoint: {len(done)} allerede dømt (skippes)")

for r in sample: r["_dom"] = ""; r["_note"] = ""
ck = open(CKPT, "a", encoding="utf-8")
todo = []
for r in sample:
    k = (r.get("rid", ""), r["generated_title"])
    if k in done: r["_dom"], r["_note"] = done[k]
    else: todo.append(r)
print(f"   {len(todo)} skal dømmes nu")
from concurrent.futures import ThreadPoolExecutor
import threading
lock = threading.Lock()
prog = [0]
def do_batch(chunk):
    items = [{"i": j + 1, "t": r["source_type"], "o": r["option_names"] or "-",
              "orig": r["original_title"][:160], "gen": r["generated_title"][:160]} for j, r in enumerate(chunk)]
    res = call(items)
    with lock:
        for j, r in enumerate(chunk):
            dom, note = res.get(j + 1, ("?", "")); r["_dom"] = dom; r["_note"] = note
            if dom != "?":
                ck.write(json.dumps({"rid": r.get("rid", ""), "gen": r["generated_title"], "dom": dom, "note": note}, ensure_ascii=False) + "\n")
        ck.flush()
        prog[0] += len(chunk)
        if prog[0] % 100 < BATCH: print(f"  …{prog[0]}/{len(todo)}")
batches = [todo[b0:b0 + BATCH] for b0 in range(0, len(todo), BATCH)]
with ThreadPoolExecutor(max_workers=5) as ex:
    list(ex.map(do_batch, batches))
ck.close()

with open(OUT, "w", encoding="utf-8-sig", newline="") as f:
    cols = ["rid", "source_type", "option_names", "original_title", "generated_title", "_dom", "_note"]
    w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore"); w.writeheader(); w.writerows(sample)

c = Counter(r["_dom"] for r in sample)
by_type = {t: Counter(r["_dom"] for r in sample if r["source_type"] == t) for t in ("merge", "split", "single", "behold", "uændret")}
print(f"\n===== DOMMER-RESULTAT ({len(sample)}) =====")
print(f"   {dict(c)}")
ok = c.get("OK", 0); print(f"   OK-rate: {ok}/{len(sample)} = {round(100*ok/len(sample),1)}%")
for t, cc in by_type.items(): print(f"   {t}: {dict(cc)}")
print(f"\n✅ {OUT}")
print("\n— ikke-OK (til gennemgang) —")
n = 0
for r in sample:
    if r["_dom"] not in ("OK", "?"):
        print(f"   [{r['_dom']}|{r['source_type']}] {r['generated_title'][:55]!r}\n      {r['_note']}")
        n += 1
        if n >= 30: break
print(f"\n(i alt ikke-OK: {sum(1 for r in sample if r['_dom'] not in ('OK','?'))}, uvurderet: {c.get('?',0)})")
