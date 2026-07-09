"""INTELLIGENT håndtering af de flaggede master_pids (titel_manuel + collision + no_axes) vha. vidaXL-
feed-titlerne. Claude analyserer feed-titlerne pr. master_pid og beslutter:
  - VARIANT: forskellen er farve/materiale/størrelse/hynde-farve/dele-antal → ÉT produkt m. den akse
  - SPLIT: forskellen er fundamental (1-trin vs 2-trin pumpe, andet produkt) → SEPARATE produkter
+ genererer korrekt titel. Output: output/flagged_resolved.json. --n N for test."""
import sys, os, io, zipfile, csv, re, json, time, urllib.request, urllib.error
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

_HUB = r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local"
if os.path.exists(_HUB):
    for l in open(_HUB, encoding="utf-8"):
        m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
        if m:
            os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
API_KEY = os.environ["ANTHROPIC_API_KEY"]
MODEL = "claude-sonnet-5"

SYSTEM = (
    "Du rydder op i et vidaXL-produktkatalog. Du får ÉN vidaXL-produktfamilie (grupperet af vidaXL via "
    "master_pid) med dens SKUs og vidaXL's egne feed-titler. vidaXL grupperer nogle gange produkter der "
    "IKKE burde være samme produkt.\n"
    "Analysér feed-titlerne og beslut korrekt struktur:\n"
    "• VARIANTER af ÉT produkt: forskellen er farve, hynde-farve, materiale/træsort, størrelse/mål, "
    "antal dele/stk. → ét produkt hvor den varierende attribut er en variant-akse.\n"
    "• SEPARATE produkter: forskellen er FUNDAMENTAL (fx 1-trins vs 2-trins pumpe, håndklæde vs vaskeklud, "
    "helt forskellig funktion/type) → del i separate produkter.\n"
    "Titel-regler: ren dansk, UDEN de attributter der er variant-akser (dem vælger kunden). Behold FASTE "
    "egenskaber (materiale hvis ens, g/m², mål hvis fast). Ingen 'vidaXL'. Dublet-titler OK.\n"
    "Et produkt kan have FLERE variant-akser (fx Farve OG Størrelse). Identificér ALLE akser der varierer, "
    "og angiv for HVER variant en værdi for HVER akse (udledt af dens feed-titel). VIGTIGT: hver SKU SKAL "
    "have en UNIK kombination af akse-værdier — hvis to SKUs ville få samme kombination, mangler du en akse "
    "(fx både Farve og Størrelse), ELLER de er forskellige produkter der skal SPLITTES. Vælg den/de KORREKTE "
    "akser (put ikke farve ind i en 'Størrelse'-akse). Er produktet single: akser=[].\n"
    'Svar KUN med JSON: {"produkter":[{"titel":"...","akser":["Farve","Størrelse"],"varianter":[{"sku":"123",'
    '"akse_vaerdier":{"Farve":"Hvid","Størrelse":"120x35x45 cm"}}]}]}. HVER input-SKU i præcis ét produkt.'
)

def housestyle(t):
    t = re.sub(r"\s+", " ", (t or "").strip())
    return " ".join(w[:1].upper() + w[1:] if w else w for w in t.split())

def call(mid, rows):
    lines = [f'  SKU {r["sku"]}: "{r["title"]}"' for r in rows]
    body = {"model": MODEL, "max_tokens": min(16000, 1500 + 90 * len(rows)), "system": SYSTEM,
            "messages": [{"role": "user", "content": f"master_pid {mid}:\n" + "\n".join(lines)}]}
    H = {"x-api-key": API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}
    for a in range(1, 5):
        try:
            req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=json.dumps(body).encode(), headers=H)
            with urllib.request.urlopen(req, timeout=120) as r:
                d = json.loads(r.read().decode())
            txt = "".join(b.get("text", "") for b in d.get("content", []))
            return json.loads(re.search(r"\{.*\}", txt, re.DOTALL).group(0))
        except urllib.error.HTTPError as e:
            if e.code == 429 and a < 4:
                time.sleep(5 * a); continue
            print(f"    HTTP {e.code}"); return None
        except Exception as e:
            if a >= 4:
                print(f"    fejl: {e}"); return None
            time.sleep(3 * a)
    return None

def main():
    n = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else None
    only = sys.argv[sys.argv.index("--mid") + 1].split(",") if "--mid" in sys.argv else None
    z = zipfile.ZipFile(io.BytesIO(ME.get_feed_zip(os.environ["FEED_URL"])))
    name = [f for f in z.namelist() if f.endswith(".csv")][0]
    feed = {}
    for r in csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8")):
        s = str(r.get("SKU") or "").strip().replace(".0", "")
        if s:
            feed[s] = re.sub(r"(?i)\bvidaxl\b", "", r.get("Title") or "").strip()
    plan = json.load(open("output/final_catalog_plan.json", encoding="utf-8"))
    flagged = [m for m, r in plan.items() if r["status"] in ("titel_manuel", "manuel")]
    if only:
        flagged = [m for m in only if m in plan]
    elif n:
        flagged = flagged[:n]
    import threading
    from concurrent.futures import ThreadPoolExecutor
    OUTF = "output/flagged_resolved.json"
    out = json.load(open(OUTF, encoding="utf-8")) if os.path.exists(OUTF) else {}
    if "--redo-bad" in sys.argv:      # re-kør KUN de master_pids der fejlede validering
        val = json.load(open("output/flagged_validation.json", encoding="utf-8"))
        bad = set()
        for k, lst in val.items():
            for x in lst:
                bad.add(x if isinstance(x, str) else x["mid"])
        todo = [m for m in flagged if m in bad]
        print(f"--redo-bad: {len(todo)} problematiske master_pids re-køres med multi-akse-prompt")
    elif "--fresh" in sys.argv:
        out = {}; todo = flagged
    else:
        todo = [m for m in flagged if m not in out]
    lock = threading.Lock(); done = [0]; bad_cover = []; still_bad = []
    def work(mid):
        insk = [s for s in plan[mid]["skus"] if feed.get(s)]
        rows = [{"sku": s, "title": feed[s]} for s in insk]
        if not rows:
            return
        res = call(mid, rows)
        if not res or not res.get("produkter"):
            return
        prods = res["produkter"]
        for p in prods:
            p["titel"] = housestyle(p.get("titel", ""))
        covered = [v["sku"] for p in prods for v in (p.get("varianter") or [])]
        # validér unikke akse-kombinationer pr. produkt
        combo_ok = True
        for p in prods:
            combos = [tuple(sorted((v.get("akse_vaerdier") or {}).items())) for v in (p.get("varianter") or [])]
            if len(combos) != len(set(combos)):
                combo_ok = False
        with lock:
            out[mid] = prods
            if sorted(covered) != sorted(insk) or not combo_ok:
                still_bad.append(mid)
            done[0] += 1
            if done[0] % 20 == 0:
                json.dump(out, open(OUTF, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
                print(f"  …{done[0]}/{len(todo)}", flush=True)
    with ThreadPoolExecutor(max_workers=6) as ex:
        list(ex.map(work, todo))
    json.dump(out, open(OUTF, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"\n=== {len(out)} løst. Denne kørsel: {len(todo)} behandlet, {len(still_bad)} stadig m. problem {still_bad[:5]} ===")

if __name__ == "__main__":
    main()
