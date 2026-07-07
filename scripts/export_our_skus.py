"""Bulk-eksportér alle vores vidaXL variant-SKUs fra Shopify → output/our_skus.txt (read-only)."""
import os, re, sys, json, time, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
_HUB = r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local"
for l in open(_HUB, encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
from bulk_repricing import _shop_gql

RUN = "mutation($q:String!){bulkOperationRunQuery(query:$q){bulkOperation{id status} userErrors{message}}}"
STAT = "query{currentBulkOperation(type:QUERY){id status errorCode objectCount url}}"
inner = '{ products(query: "vendor:\'vidaXL\'") { edges { node { id variants { edges { node { id sku } } } } } } }'

for _ in range(60):
    s = _shop_gql(STAT)["data"]["currentBulkOperation"]
    if not s or s["status"] not in ("CREATED", "RUNNING"): break
    time.sleep(10)
res = _shop_gql(RUN, {"q": inner})["data"]["bulkOperationRunQuery"]
if res.get("userErrors"): raise SystemExit(res["userErrors"])
print(f"🚀 {res['bulkOperation']['id']}")
url = None; start = time.time()
while True:
    time.sleep(8)
    s = _shop_gql(STAT)["data"]["currentBulkOperation"]
    if not s: continue
    if s["status"] == "COMPLETED": url = s.get("url"); break
    if s["status"] in ("FAILED", "CANCELED", "EXPIRED"): raise SystemExit(s.get("errorCode"))
    print(f"   [{int(time.time()-start)}s] {s['status']} {s.get('objectCount')}")

skus = set()
with urllib.request.urlopen(urllib.request.Request(url), timeout=300) as resp:
    for raw in resp:
        line = raw.decode("utf-8").strip()
        if not line: continue
        o = json.loads(line)
        if "/ProductVariant/" in (o.get("id", "") or ""):
            s = (o.get("sku") or "").strip().replace(".0", "")
            if s: skus.add(s)
out = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "our_skus.txt")
os.makedirs(os.path.dirname(out), exist_ok=True)
open(out, "w").write("\n".join(sorted(skus)))
print(f"✅ {len(skus)} distinkte vidaXL-SKUs → {out}")
