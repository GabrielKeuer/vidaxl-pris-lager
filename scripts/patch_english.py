"""Patch de få engelske/SKU-titler i catalog_titles_simulation.csv → korrekt dansk via LLM.
Established residual-path (needs_llm), uafhængig af den blokerede feed. READ-ONLY ift. Shopify."""
import csv, json, os, re, sys, urllib.request
sys.stdout.reconfigure(encoding="utf-8")
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
KEY = os.environ["ANTHROPIC_API_KEY"]; F = r"C:\Users\APC\Desktop\catalog_titles_simulation.csv"
ENG = {"with","and","the","black","white","grey","gray","mirror","cabinet","chair","garden","wall","wood",
       "steel","bathroom","kitchen","folding","outdoor","cushion","frame","bench","shelf","table","drawer",
       "corner","middle","seat","cover","piece","clock","door","design","modern","spoon","fork","iron","tree"}
def bad(t):
    if re.match(r"^\s*\d{4,}\b", t or ""): return True
    return sum(1 for w in re.findall(r"\b[a-z]+\b", (t or "").lower()) if w in ENG) >= 2

rows = list(csv.DictReader(open(F, encoding="utf-8-sig")))
tgt = [r for r in rows if bad(r["generated_title"])]
print(f"{len(tgt)} titler til oversættelse")
SYS = ("Du er dansk produkttitel-korrektør. Oversæt hver engelsk titel til en KORREKT dansk produkttitel. "
       "Fjern ledende varenummer. Bevar mål/materiale/farve. Title Case (hvert ord stort). Opfind intet. "
       'Svar KUN JSON: [{"i":1,"da":"Dansk Titel"}, ...]')
lines = [f'{i+1}. {r["generated_title"]}' for i, r in enumerate(tgt)]
body = {"model": "claude-sonnet-5", "max_tokens": 2000, "system": SYS,
        "messages": [{"role": "user", "content": "\n".join(lines)}]}
req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=json.dumps(body).encode(),
    headers={"x-api-key": KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"})
d = json.loads(urllib.request.urlopen(req, timeout=120).read())
txt = "".join(b.get("text", "") for b in d.get("content", []))
res = {o["i"]: o["da"] for o in json.loads(re.search(r"\[.*\]", txt, re.S).group(0))}
for i, r in enumerate(tgt):
    da = res.get(i + 1)
    if da: print(f"   {r['generated_title'][:45]!r} → {da!r}"); r["generated_title"] = da; r["changed"] = "ja"
with open(F, "w", encoding="utf-8-sig", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
print(f"\n✅ patchet {F}")
