"""LLM-trin (minimalt) ovenpå titel-audit v2.

Filosofi: LLM KUN til det deterministik ikke kan — oversættelse (engelsk) + fjern
foranstillet SKU. Ingen farve/mål-vurdering (det gør de deterministiske lag).
Læser titel_audit.csv, kører KUN rækker med needs_llm != "" gennem Claude Haiku.
Skriver titel_audit_full.csv (det_suggestion + llm_suggestion + final_title + decided_by).
"""
from __future__ import annotations
import csv, json, os, re, sys, time, urllib.request, urllib.error
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

_HUB = r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local"
if os.path.exists(_HUB):
    for l in open(_HUB, encoding="utf-8"):
        m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
        if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))

API_KEY = os.environ["ANTHROPIC_API_KEY"]
MODEL = "claude-sonnet-5"  # stærk model — kun ~14 rækker, kvalitet vigtigere end pris
IN_CSV = r"C:\Users\APC\Desktop\titel_audit.csv"
OUT_CSV = r"C:\Users\APC\Desktop\titel_audit_full.csv"
BATCH = 25

UPPER_TOKENS = ["LED","TV","USB","UV","PVC","RGB","HDMI","HD","3D","WC","CD","DVD","MDF","HDPE","WPC","ABS","XXL","XL","SPA","WiFi"]
def _tc(t): return " ".join(w[0].upper()+w[1:].lower() if w else w for w in (t or "").split())
def _fc(t):
    for tok in UPPER_TOKENS: t = re.sub(r"\b"+re.escape(tok)+r"\b", tok, t, flags=re.IGNORECASE)
    return re.sub(r"(?i)\bip(\d{2})\b", lambda m: "IP"+m.group(1), t)
def house_style(t): return _fc(_tc(t)).strip()

SYSTEM = (
    "Du er korrekturlæser for danske produkttitler (bolig/havemøbler). Du får KUN titler der enten "
    "er på engelsk eller starter med et varenummer/SKU. Din opgave er SNÆVER:\n"
    "1) Er titlen engelsk → oversæt til naturlig dansk.\n"
    "2) Starter titlen med et varenummer/SKU-tal → fjern tallet.\n"
    "3) Fjern KUN attributter der står i den medsendte variant-liste (farve/størrelse) — en farve/finish "
    "der IKKE er i listen er en FAST egenskab og skal BEVARES.\n"
    "Brug KORREKT dansk retstavning og naturlige danske produktnavne (fx 'Bathroom Mirror Cabinet' → "
    "'Spejlskab til badeværelse', ikke ord-for-ord). "
    "Ellers: ændr så lidt som muligt. Opfind ALDRIG ord/fakta. Gæt ALDRIG manglende mål. "
    "Store/små bogstaver er ligegyldigt (rettes automatisk). "
    'Svar KUN med JSON: [{"i":1,"title":"..."}, ...]'
)

def call(items):
    lines = [f'{it["i"]}. "{it["title"]}" | variant-liste: "{it["opts"]}"' for it in items]
    body = {"model": MODEL, "max_tokens": 3000, "system": SYSTEM,
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
            return {int(o["i"]): (o.get("title") or "").strip() for o in arr if "i" in o}
        except urllib.error.HTTPError as e:
            if e.code == 429 and a < 4: time.sleep(5*a); continue
            print(f"  HTTP {e.code}"); return {} if a >= 4 else time.sleep(3*a)
        except Exception as e:
            if a >= 4: print(f"  fejl: {e}"); return {}
            time.sleep(3*a)
    return {}

def main():
    rows = list(csv.DictReader(open(IN_CSV, encoding="utf-8-sig")))
    flagged = [r for r in rows if r.get("needs_llm")]
    print(f"📄 {len(rows)} rækker | {len(flagged)} → LLM (kun engelsk/SKU)")
    llm = {}
    for b0 in range(0, len(flagged), BATCH):
        chunk = flagged[b0:b0+BATCH]
        items = [{"i": j+1, "title": r["current_title"], "opts": r.get("option_values", "")} for j, r in enumerate(chunk)]
        res = call(items)
        for j, r in enumerate(chunk):
            if (j+1) in res and res[j+1]: llm[r["product_id"]] = res[j+1]
        print(f"  …{min(b0+BATCH, len(flagged))}/{len(flagged)}")

    out = []; changed = 0
    for r in rows:
        cur = r["current_title"]; det = r["suggested_title"]
        lt = house_style(llm[r["product_id"]]) if r["product_id"] in llm else ""
        if lt and lt != cur: final, by = lt, "llm"
        elif r["changed"] == "ja" and det: final, by = det, "det"
        else: final, by = cur, "uændret"
        if final != cur: changed += 1
        out.append({"handle": r["handle"], "product_id": r["product_id"], "product_type": r["product_type"],
                    "variant_count": r["variant_count"], "current_title": cur, "det_suggestion": det,
                    "llm_suggestion": lt, "final_title": final, "decided_by": by, "needs_llm": r.get("needs_llm", ""),
                    "issues": r["issues"], "removed": r.get("removed", ""), "option_values": r.get("option_values", ""),
                    "semantic_dist": r.get("semantic_dist", "")})
    with open(OUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out[0].keys())); w.writeheader(); w.writerows(out)
    by_c = Counter(o["decided_by"] for o in out)
    print(f"\n✅ {OUT_CSV}\n   Ændres: {changed}/{len(out)} | kilde: {dict(by_c)}")
    for lbl, key in [("LLM (engelsk/sku)", "llm")]:
        print(f"\n— {lbl} —")
        n = 0
        for o in out:
            if o["decided_by"] == key:
                print(f"   {o['current_title']}  →  {o['final_title']}"); n += 1
                if n >= 15: break

if __name__ == "__main__":
    main()
