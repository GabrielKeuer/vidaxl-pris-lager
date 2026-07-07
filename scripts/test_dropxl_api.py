"""Test: giver dropXL API grupperingen? Query rigtige fler-variant-produkters SKUs."""
import json, os, re, sys, base64, urllib.request
sys.stdout.reconfigure(encoding="utf-8")
env = {}
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: env[m.group(1)] = m.group(2).strip().strip('"').strip("'")
STORE = env["SHOPIFY_STORE_URL"].replace("https://", "").rstrip("/"); TOK = env["SHOPIFY_ACCESS_TOKEN"]
EMAIL = "kontakt@boligretning.dk"; APITOK = "8ba173af-c6f8-4e62-aa1f-3994ce852860"
AUTH = base64.b64encode(f"{EMAIL}:{APITOK}".encode()).decode()

def shop(q, v=None):
    d = json.dumps({"query": q, "variables": v or {}}).encode()
    r = urllib.request.Request(f"https://{STORE}/admin/api/2024-10/graphql.json", data=d,
        headers={"X-Shopify-Access-Token": TOK, "Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(r, timeout=60).read())["data"]

def dropxl(sku):
    r = urllib.request.Request(f"https://b2b.dropxl.com/api_customer/products?code_eq={sku}",
        headers={"Authorization": f"Basic {AUTH}"})
    try:
        d = json.loads(urllib.request.urlopen(r, timeout=30).read())
        return d.get("data", d) if isinstance(d, dict) else d
    except Exception as e:
        return f"fejl: {e}"

# hent 3 fler-variant vidaXL-produkter + deres variant-SKUs
d = shop("""{products(first:120,query:"vendor:vidaXL"){edges{node{title handle
  variants(first:30){edges{node{sku selectedOptions{name value}}}}}}}}""")
prods = [e["node"] for e in d["products"]["edges"]]
multi = [p for p in prods if len(p["variants"]["edges"]) >= 3][:3]

for p in multi:
    print(f"\n{'='*70}\nPRODUKT: {p['title']}  ({len(p['variants']['edges'])} varianter)")
    for e in p["variants"]["edges"][:6]:
        sku = (e["node"]["sku"] or "").strip()
        opts = "; ".join(f"{o['name']}={o['value']}" for o in e["node"]["selectedOptions"])
        res = dropxl(sku)
        if isinstance(res, list) and res:
            r0 = res[0]
            print(f"  SKU {sku} ({opts})")
            print(f"     → API: id={r0.get('id')} name={r0.get('name')!r}")
        else:
            print(f"  SKU {sku} ({opts}) → {res}")
