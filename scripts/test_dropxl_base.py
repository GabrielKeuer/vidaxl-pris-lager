"""Demonstrér: udled ren base-titel ved cross-variant sammenligning af API-navne."""
import json, re, base64, urllib.request, sys
from collections import Counter
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

def api_name(sku):
    r = urllib.request.Request(f"https://b2b.dropxl.com/api_customer/products?code_eq={sku}",
        headers={"Authorization": f"Basic {AUTH}"})
    try:
        d = json.loads(urllib.request.urlopen(r, timeout=30).read())
        data = d.get("data", []) if isinstance(d, dict) else d
        return data[0]["name"] if data else None
    except Exception:
        return None

# hent nogle fler-variant vidaXL-produkter (inkl. fragment-typen med model-variant)
d = shop('{products(first:200,query:"vendor:vidaXL"){edges{node{title handle variants(first:40){edges{node{sku}}}}}}}')
prods = [e["node"] for e in d["products"]["edges"] if len(e["node"]["variants"]["edges"]) >= 4]

def common_base(names):
    """Fælles tokens på tværs af alle variant-navne, i rækkefølge fra første navn."""
    toksets = [set(n.lower().split()) for n in names]
    common = set.intersection(*toksets) if toksets else set()
    first = names[0].split()
    return " ".join(w for w in first if w.lower() in common)

for p in prods[:6]:
    skus = [e["node"]["sku"].strip() for e in p["variants"]["edges"] if e["node"]["sku"]][:10]
    names = [n for n in (api_name(s) for s in skus) if n]
    if len(names) < 2:
        continue
    print(f"\n{'='*72}")
    print(f"SHOPIFY-titel : {p['title']}")
    print(f"API-navne ({len(names)} varianter), fx:")
    for n in names[:3]:
        print(f"    - {n}")
    print(f"UDLEDT BASE   : {common_base(names)!r}   (= det der er FÆLLES på tværs = generisk titel, u. variant)")
