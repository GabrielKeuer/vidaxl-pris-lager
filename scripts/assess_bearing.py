"""Vurdér HVER bærende-variant-master slavisk: behold (konfiguration/udgave af ét produkt)
eller split (reelt forskellige produkter). Fylder beslutning + begrundelse i CSV'en."""
import csv, json, os, re, sys, time, urllib.request, urllib.error
from collections import Counter
sys.stdout.reconfigure(encoding="utf-8")
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
API_KEY = os.environ["ANTHROPIC_API_KEY"]
MODEL = "claude-sonnet-5"
CSV = r"C:\Users\APC\Desktop\bearing_variants.csv"
BATCH = 20

SYSTEM = (
    "Du vurderer om et Shopify-produkt med en 'Model'-agtig variant-option skal BEHOLDES som ét "
    "produkt eller SPLITTES i separate produkter. Vurdér HVER for sig ud fra titel + option-værdier.\n\n"
    "Kernespørgsmål: Når kunden vælger mellem option-værdierne — vælger de en KONFIGURATION/udgave af "
    "ÉT produkt, eller vælger de mellem reelt FORSKELLIGE produkter der bare er klumpet sammen?\n\n"
    "- 'behold' = legitime varianter af samme produkt: modulsofa i forskellige opstillinger, med/uden "
    "bord, 2-sædet/3-sædet, antal hylder/skuffer, med/uden armlæn, størrelses-/konfigurations-valg.\n"
    "- 'split' = værdierne er reelt forskellige møbler: fx man vælger 'et havebord' ELLER 'en fodskammel' "
    "ELLER 'en hjørnesofa' som om det var samme produkt. Så skal de være separate produkter.\n\n"
    "Vær konkret pr. styk. Er det tvivlsomt/rodet, vælg det der giver mest logisk mening for en kunde.\n"
    'Svar KUN JSON: [{"i":1,"beslutning":"behold","begrundelse":"kort dansk"}, ...]'
)

def call(items):
    lines = [f'{it["i"]}. "{it["title"]}" | option {it["opt"]}: {it["vals"]}' for it in items]
    body = {"model": MODEL, "max_tokens": 4000, "system": SYSTEM,
            "messages": [{"role": "user", "content": "\n".join(lines)}]}
    data = json.dumps(body).encode()
    H = {"x-api-key": API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}
    for a in range(1, 5):
        try:
            req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=data, headers=H)
            with urllib.request.urlopen(req, timeout=120) as r:
                d = json.loads(r.read().decode())
            txt = "".join(b.get("text", "") for b in d.get("content", []))
            arr = json.loads(re.search(r"\[.*\]", txt, re.DOTALL).group(0))
            return {int(o["i"]): (o.get("beslutning", ""), o.get("begrundelse", "")) for o in arr if "i" in o}
        except urllib.error.HTTPError as e:
            if e.code == 429 and a < 4: time.sleep(5 * a); continue
            print(f"  HTTP {e.code}");
            if a >= 4: return {}
            time.sleep(3 * a)
        except Exception as e:
            if a >= 4: print(f"  fejl: {e}"); return {}
            time.sleep(3 * a)
    return {}

def main():
    rows = list(csv.DictReader(open(CSV, encoding="utf-8-sig")))
    print(f"📄 {len(rows)} bærende-variant-masters — vurderer hver for sig ({MODEL})")
    for b0 in range(0, len(rows), BATCH):
        chunk = rows[b0:b0 + BATCH]
        items = [{"i": j + 1, "title": r["sample_titles"][:80], "opt": r["option_name"],
                  "vals": r["option_values"][:250]} for j, r in enumerate(chunk)]
        res = call(items)
        for j, r in enumerate(chunk):
            bes, beg = res.get(j + 1, ("", ""))
            r["beslutning"] = bes; r["begrundelse"] = beg
        print(f"  …{min(b0 + BATCH, len(rows))}/{len(rows)}")

    cols = ["master_pid", "strength", "beslutning", "begrundelse", "type_nouns", "option_name",
            "n_products", "n_variants", "option_values", "sample_titles", "vidaxl_url", "handles"]
    with open(CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
        for r in rows: w.writerow({k: r.get(k, "") for k in cols})
    c = Counter(r["beslutning"] for r in rows)
    print(f"\n✅ {CSV}\n   fordeling: {dict(c)}")
    for lbl in ("split", "behold"):
        print(f"\n— {lbl} eksempler —")
        n = 0
        for r in rows:
            if r["beslutning"] == lbl:
                print(f"   {r['sample_titles'][:55]} | {r['option_values'][:60]}\n     → {r['begrundelse']}")
                n += 1
                if n >= 6: break

if __name__ == "__main__":
    main()
