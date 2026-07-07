"""Tjek om fjernede farver FAKTISK er variant-options (live fra Shopify)."""
from __future__ import annotations
import csv, json, re, sys, urllib.request
sys.stdout.reconfigure(encoding="utf-8")
env = {}
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: env[m.group(1)] = m.group(2).strip().strip('"').strip("'")
STORE = env["SHOPIFY_STORE_URL"].replace("https://", "").replace("http://", "").rstrip("/")
TOK = env["SHOPIFY_ACCESS_TOKEN"]
def gql(q, v):
    data = json.dumps({"query": q, "variables": v}).encode()
    req = urllib.request.Request(f"https://{STORE}/admin/api/2024-10/graphql.json", data=data,
        headers={"X-Shopify-Access-Token": TOK, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r: return json.loads(r.read().decode())["data"]

rows = list(csv.DictReader(open(r"C:\Users\APC\Desktop\titel_audit_full.csv", encoding="utf-8-sig")))
COLORS = ["mørkebrun","mørkegrå","lysegrå","antracitgrå","gyldenbrun","betongrå",
          "sort","hvid","grå","brun","sølv","guld","antracit","beige","creme","blå","grøn","rød","natur","taupe"]
def removed_color(r):
    cur=r["current_title"].lower(); fin=r["final_title"].lower()
    for c in COLORS:
        if re.search(r"\b"+c, cur) and not re.search(r"\b"+c, fin): return c
    return None

# tag et udsnit pr. kilde
picks=[]; seen=set()
for r in rows:
    if r["final_title"]==r["current_title"]: continue
    c=removed_color(r)
    if c and r["handle"] not in seen:
        picks.append((r,c)); seen.add(r["handle"])
    if len(picks)>=18: break

real_variant=0; over=0
for r,c in picks:
    d=gql("query($h:String!){productByHandle(handle:$h){options{name values}}}",{"h":r["handle"]})
    p=d.get("productByHandle")
    allvals=" | ".join(v.lower() for o in (p["options"] if p else []) for v in o["values"])
    names=[o["name"] for o in p["options"]] if p else []
    is_var = c in allvals
    if is_var: real_variant+=1
    else: over+=1
    print(f"[{r['decided_by']}] fjernet '{c}' — {'✅ ER variant' if is_var else '❌ IKKE variant (over-fjernet)'}")
    print(f"    {r['current_title']}  →  {r['final_title']}")
    print(f"    options: {names}  |  værdier: {allvals[:110]}")
print(f"\nOPSUMMERING af udsnit: ægte variant={real_variant}  over-fjernet={over}")
