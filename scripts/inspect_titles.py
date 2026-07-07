"""Slavisk inspektion: (A) farver fjernet uden at være variant-option,
(B) hent faktiske varianter for 'manglende 3. dimension'-produkter fra Shopify."""
from __future__ import annotations
import csv, json, os, re, sys, urllib.request
sys.stdout.reconfigure(encoding="utf-8")

_HUB = r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local"
env = {}
for l in open(_HUB, encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m:
        env[m.group(1)] = m.group(2).strip().strip('"').strip("'")
STORE = env["SHOPIFY_STORE_URL"].replace("https://", "").replace("http://", "").rstrip("/")
TOK = env["SHOPIFY_ACCESS_TOKEN"]

def gql(q, v=None):
    data = json.dumps({"query": q, "variables": v or {}}).encode()
    req = urllib.request.Request(f"https://{STORE}/admin/api/2024-10/graphql.json", data=data,
                                 headers={"X-Shopify-Access-Token": TOK, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())["data"]

rows = list(csv.DictReader(open(r"C:\Users\APC\Desktop\titel_audit_full.csv", encoding="utf-8-sig")))
chg = [r for r in rows if r["final_title"] != r["current_title"]]

COLORS = ["mørkebrun","mørkegrå","lysegrå","antracitgrå","gyldenbrun","betongrå","sonoma",
          "sort","hvid","grå","brun","sølv","guld","bronze","antracit","beige","creme",
          "blå","grøn","rød","gul","natur","taupe","oliven","cappuccino","wenge","røget"]

# (A) farve i current, væk i final, men IKKE i option_values → mulig over-fjernelse
print("=== (A) FARVE FJERNET UDEN AT VÆRE VARIANT-OPTION ===")
susp = []
for r in chg:
    cur = r["current_title"].lower(); fin = r["final_title"].lower(); opt = r["option_values"].lower()
    for c in COLORS:
        if re.search(r"\b"+c, cur) and not re.search(r"\b"+c, fin) and c not in opt:
            susp.append((r, c)); break
print(f"antal: {len(susp)}")
from collections import Counter
print("kilde:", Counter(r["decided_by"] for r, c in susp))
for r, c in susp[:20]:
    print(f"  [{r['decided_by']}] fjernet '{c}'  (options='{r['option_values'][:60]}')")
    print(f"     FØR: {r['current_title']}")
    print(f"     EFT: {r['final_title']}")

# (B) manglende 3. dimension — hent faktiske varianter fra Shopify
print("\n=== (B) MANGLENDE 3. DIMENSION — FAKTISKE VARIANTER ===")
missing = [r for r in rows if re.search(r"\d+\s*[xX]\s*\d+\s*[xX](?!\s*[\d(])", r["current_title"])]
print(f"produkter med brudt mål i titel: {len(missing)}")
# tag et bredt udsnit på tværs af typer
seen_types = set(); picks = []
for r in missing:
    t = r["product_type"]
    if t not in seen_types or len([p for p in picks if p["product_type"] == t]) < 1:
        picks.append(r); seen_types.add(t)
    if len(picks) >= 8:
        break
for r in picks:
    d = gql("""query($h:String!){productByHandle(handle:$h){title options{name values}
      variants(first:60){edges{node{title selectedOptions{name value}}}}}}""", {"h": r["handle"]})
    p = d.get("productByHandle")
    print(f"\n— {r['current_title']}  (type={r['product_type']}, vc={r['variant_count']})")
    if not p:
        print("   (findes ikke)"); continue
    print(f"   options: {[(o['name'], o['values']) for o in p['options']]}")
    vs = [e['node'] for e in p['variants']['edges']]
    print(f"   {len(vs)} varianter — første 6:")
    for v in vs[:6]:
        so = "; ".join(f"{s['name']}={s['value']}" for s in v['selectedOptions'])
        print(f"      · {so}")
