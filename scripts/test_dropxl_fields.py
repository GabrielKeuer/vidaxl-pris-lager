"""Dump ALLE API-felter for flere varianter af samme produkt → find delt gruppe-felt."""
import json, re, base64, urllib.request, sys
sys.stdout.reconfigure(encoding="utf-8")
env = {}
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: env[m.group(1)] = m.group(2).strip().strip('"').strip("'")
STORE = env["SHOPIFY_STORE_URL"].replace("https://", "").rstrip("/"); TOK = env["SHOPIFY_ACCESS_TOKEN"]
AUTH = base64.b64encode(b"kontakt@boligretning.dk:8ba173af-c6f8-4e62-aa1f-3994ce852860").decode()

def shop(q):
    r = urllib.request.Request(f"https://{STORE}/admin/api/2024-10/graphql.json",
        data=json.dumps({"query": q}).encode(), headers={"X-Shopify-Access-Token": TOK, "Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(r, timeout=60).read())["data"]

def api_full(sku):
    r = urllib.request.Request(f"https://b2b.dropxl.com/api_customer/products?code_eq={sku}",
        headers={"Authorization": f"Basic {AUTH}"})
    d = json.loads(urllib.request.urlopen(r, timeout=30).read())
    data = d.get("data", []) if isinstance(d, dict) else d
    return data[0] if data else None

# tag 2 fler-variant produkter
d = shop('{products(first:60,query:"vendor:vidaXL"){edges{node{title variants(first:6){edges{node{sku}}}}}}}')
prods = [e["node"] for e in d["products"]["edges"] if len(e["node"]["variants"]["edges"]) >= 3][:2]

for p in prods:
    skus = [e["node"]["sku"].strip() for e in p["variants"]["edges"] if e["node"]["sku"]][:4]
    print(f"\n{'='*74}\n{p['title']}  (SKUs: {skus})")
    recs = [api_full(s) for s in skus]
    recs = [r for r in recs if r]
    if not recs:
        continue
    print(f"ALLE FELTER i API-respons: {list(recs[0].keys())}")
    # vis hvert felt på tværs af varianterne → er noget DELT?
    for key in recs[0].keys():
        vals = [str(r.get(key)) for r in recs]
        shared = len(set(vals)) == 1
        mark = "  <<< DELT PÅ TVÆRS" if shared and key not in ("category_path", "currency") else ""
        print(f"  {key:16}: {vals}{mark}")
