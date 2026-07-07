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
    "For HVER variant: angiv akse-værdi udledt af DENS feed-titel (fx materialet 'Mangotræ', hynde-farven "
    "'Antracitgrå', størrelsen '120x35x45 cm'). Er produktet single: variant_akse=null.\n"
    'Svar KUN med JSON: {"produkter":[{"titel":"...","variant_akse":"Farve"|"Materiale"|"Størrelse"|'
    '"Antal"|null,"varianter":[{"sku":"123","akse_vaerdi":"Mangotræ"}]}]}. HVER input-SKU i præcis ét '
    "produkt med sin akse-værdi (akse_vaerdi=null hvis single)."
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
    print(f"flaggede master_pids: {len(flagged)}")
    out = {}; bad_cover = []
    for i, mid in enumerate(flagged, 1):
        insk = [s for s in plan[mid]["skus"] if feed.get(s)]
        rows = [{"sku": s, "title": feed[s]} for s in insk]
        if not rows:
            continue
        res = call(mid, rows)
        if not res or not res.get("produkter"):
            continue
        prods = res["produkter"]
        # house-style titler
        for p in prods:
            p["titel"] = housestyle(p.get("titel", ""))
        # VALIDÉR: hver input-SKU dækket præcis én gang
        covered = [v["sku"] for p in prods for v in (p.get("varianter") or [])]
        if sorted(covered) != sorted(insk):
            bad_cover.append(mid)
        out[mid] = prods
        tag = "SPLIT" if len(prods) > 1 else "variant/single"
        print(f"[{i}/{len(flagged)}] {mid} → {len(prods)} produkt(er) [{tag}]", flush=True)
        time.sleep(0.1)
    json.dump(out, open("output/flagged_resolved.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"\n=== {len(out)} flaggede løst. VALIDERING: {len(bad_cover)} m. SKU-dæknings-fejl {bad_cover[:5]} ===")

if __name__ == "__main__":
    main()
