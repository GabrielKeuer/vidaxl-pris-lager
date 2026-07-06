"""LLM-vurdering af de 86 atomize/split-keepers: Claude beslutter pr. keeper den korrekte produkt-
struktur (grupper SKUs → produkter, rene danske titler, Farve/Konfiguration-akser). Dublet-titler OK;
kan noget ikke grupperes smart → single-produkt. Output: output/atomize_specs.json. --n N for test-antal."""
import csv, json, os, re, sys, time, urllib.request, urllib.error
from collections import defaultdict
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
    "Du strukturerer rodede vidaXL-møbelprodukter til en dansk webshop. Du får ÉT nuværende Shopify-produkt "
    "hvis 'varianter' i virkeligheden ofte er FORSKELLIGE produkter (fx et loungesæt-produkt hvis varianter "
    "er 3-dele/4-dele-konfigurationer, eller en fodskammel + en sofa + et hjørnesofa-modul bundtet sammen).\n"
    "Din opgave: opdel i de KORREKTE produkter.\n"
    "REGLER:\n"
    "1) Samme grundprodukt i flere FARVER = ÉT produkt med Farve som variant.\n"
    "2) Samme grundprodukt i flere KONFIGURATIONER/størrelser (fx antal dele) = ÉT produkt med Konfiguration "
    "som variant. Lav pæne danske Konfiguration-værdier (fx '3 Dele', '4 Dele'; oversæt engelsk: bench→bænk, "
    "armchair→lænestol, table→bord, stool→skammel, corner→hjørne).\n"
    "3) Genuint FORSKELLIGE produkttyper (fodskammel vs sofa vs hjørnemodul) = SEPARATE produkter.\n"
    "4) Kan en SKU ikke grupperes fornuftigt → eget single-produkt.\n"
    "5) Rene danske titler UDEN farve og UDEN konfigurations-tal (de er varianter). Bevar faste egenskaber "
    "(materiale, mål). Dublet-titler er HELT OK hvis to produkter reelt ligner hinanden.\n"
    "6) HVER input-SKU skal indgå i præcis ét output-produkt.\n"
    'Svar KUN med JSON: {"products":[{"title":"...","variants":[{"sku":"123","Farve":"Brun"|null,'
    '"Konfiguration":"3 Dele"|null}]}]}. Udelad Farve/Konfiguration (null) hvis aksen ikke varierer i produktet.'
)

def call_llm(keeper_title, rows):
    lines = [f'  SKU {r["sku"]}: config="{r["config"]}", farve="{r["color"] or "-"}", nuv.titel="{r["title"]}"' for r in rows]
    user = f'Nuværende produkt: "{keeper_title}"\nVarianter:\n' + "\n".join(lines)
    body = {"model": MODEL, "max_tokens": 4000, "system": SYSTEM, "messages": [{"role": "user", "content": user}]}
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
                print(f"    LLM-fejl: {e}"); return None
            time.sleep(3 * a)
    return None

def main():
    n = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else None
    oracle = {r["sku"]: r["approved_title"] for r in csv.DictReader(open("output/approved_titles_by_sku.csv", encoding="utf-8-sig")) if r["approved_title"]}
    plans = [json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8")]
    m4 = {}
    for p in plans:
        if p["action"] in ("atomize", "split") and p["variant_creates"]:
            m4.setdefault(p["keeper_handle"], p["key"].split("|")[1] if "|" in p["key"] else "")
    handles = list(m4)[:n] if n else list(m4)
    out = {}
    for i, h in enumerate(handles, 1):
        master = m4[h]
        d = ME.gql("query($h:String!){productByHandle(handle:$h){title variants(first:100){edges{node{sku}}}}}", {"h": h})
        pr = (d.get("data") or {}).get("productByHandle")
        if not pr:
            continue
        km = ME.build_keyname([(e["node"]["sku"] or "").strip() for e in pr["variants"]["edges"]], master)
        rows = []
        for e in pr["variants"]["edges"]:
            s = (e["node"]["sku"] or "").strip()
            iv = ME.OPTS.get(s, {})
            cfg = " / ".join(f"{k}={v}" for k, v in iv.items() if k != "color" and v) or "(ingen)"
            rows.append({"sku": s, "config": cfg, "color": iv.get("color"), "title": oracle.get(s, "")})
        res = call_llm(pr["title"], rows)
        if res and res.get("products"):
            out[h] = res["products"]
            print(f"[{i}/{len(handles)}] {h[:44]} → {len(res['products'])} produkter")
        else:
            out[h] = [{"title": oracle.get(r["sku"]) or pr["title"], "variants": [{"sku": r["sku"]}]} for r in rows]
            print(f"[{i}/{len(handles)}] {h[:44]} → FALLBACK singler ({len(rows)})")
    json.dump(out, open("output/atomize_specs.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    tp = sum(len(v) for v in out.values())
    print(f"\n=== {len(out)} keepers → {tp} produkter. Gemt til output/atomize_specs.json ===")

if __name__ == "__main__":
    main()
