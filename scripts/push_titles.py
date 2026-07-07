"""TITEL-BATCH: push godkendte orakel-titler til behold/uændret-produkter (ændres aldrig strukturelt).
Bulk Operation (productUpdate JSONL) + verifikations-pas. --live for at køre; ellers dry-run.
Kilde: output/title_batch.json (bygget fra catalog_titles_simulation.csv, 100%-valideret)."""
import json, os, re, sys, time, urllib.request
sys.stdout.reconfigure(encoding="utf-8")
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
STORE = os.environ["SHOPIFY_STORE_URL"].replace("https://", "").rstrip("/")
TOK = os.environ["SHOPIFY_ACCESS_TOKEN"]
GQL = f"https://{STORE}/admin/api/2024-10/graphql.json"

def gql(q, v=None):
    req = urllib.request.Request(GQL, data=json.dumps({"query": q, "variables": v or {}}).encode(),
                                 headers={"X-Shopify-Access-Token": TOK, "Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=120).read())

def main():
    live = "--live" in sys.argv
    batch = json.load(open("output/title_batch.json", encoding="utf-8"))
    print(f"{'LIVE' if live else 'DRY-RUN'}: {len(batch)} titel-opdateringer")
    if not live:
        for b in batch[:5]: print(f"  {b['fra'][:45]!r} → {b['til'][:45]!r}")
        print("✅ dry-run ok — kør med --live")
        return
    # staged upload
    su = gql("""mutation($input:[StagedUploadInput!]!){stagedUploadsCreate(input:$input){
        stagedTargets{url resourceUrl parameters{name value}} userErrors{message}}}""",
        {"input": [{"resource": "BULK_MUTATION_VARIABLES", "filename": "titles.jsonl",
                    "mimeType": "text/jsonl", "httpMethod": "POST"}]})
    tgt = su["data"]["stagedUploadsCreate"]["stagedTargets"][0]
    lines = "\n".join(json.dumps({"input": {"id": b["pid"], "title": b["til"]}}, ensure_ascii=False) for b in batch)
    import io, uuid
    boundary = uuid.uuid4().hex
    parts = []
    for p in tgt["parameters"]:
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{p["name"]}"\r\n\r\n{p["value"]}\r\n')
    parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="titles.jsonl"\r\nContent-Type: text/jsonl\r\n\r\n{lines}\r\n--{boundary}--\r\n')
    body = "".join(parts).encode("utf-8")
    req = urllib.request.Request(tgt["url"], data=body, headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    resp = urllib.request.urlopen(req, timeout=300)
    print(f"⬆ upload: {resp.status}")
    key = next(p["value"] for p in tgt["parameters"] if p["name"] == "key")
    run = gql("""mutation($mutation:String!,$path:String!){bulkOperationRunMutation(mutation:$mutation,stagedUploadPath:$path){
        bulkOperation{id status} userErrors{field message}}}""",
        {"mutation": "mutation($input: ProductInput!){productUpdate(input:$input){product{id} userErrors{field message}}}",
         "path": key})
    r = run["data"]["bulkOperationRunMutation"]
    if r["userErrors"]: sys.exit(f"❌ {r['userErrors']}")
    print(f"🚀 bulk mutation: {r['bulkOperation']['id']}")
    while True:
        time.sleep(10)
        s = gql("query{currentBulkOperation(type:MUTATION){status objectCount errorCode}}")["data"]["currentBulkOperation"]
        print(f"   {s['status']} {s.get('objectCount')}")
        if s["status"] in ("COMPLETED", "FAILED", "CANCELED"): break
    if s["status"] != "COMPLETED": sys.exit(f"❌ {s}")
    # verifikations-pas: stikprøve 300
    import random
    random.seed(1)
    sample = random.sample(batch, min(300, len(batch)))
    drift = 0
    for i in range(0, len(sample), 50):
        chunk = sample[i:i + 50]
        q = "query{" + " ".join(f'p{j}: product(id:"{b["pid"]}"){{title}}' for j, b in enumerate(chunk)) + "}"
        d = gql(q)["data"]
        for j, b in enumerate(chunk):
            if (d.get(f"p{j}") or {}).get("title") != b["til"]: drift += 1
    print(f"✅ FÆRDIG. {s['objectCount']} mutations | verifikation: {drift}/{len(sample)} afvigelser i stikprøve")

if __name__ == "__main__":
    main()
