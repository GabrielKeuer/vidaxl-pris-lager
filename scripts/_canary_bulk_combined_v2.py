"""End-to-end canary for de TO nye mekanismer i sync_prices_v2:
  1. Cost-i-variant: inventoryItem.cost i ProductVariantsBulkInput
     erstatter den gamle separate inventoryItemUpdate-mutation.
  2. Bulk Operations: bulkOperationRunMutation wrap af productVariantsBulkUpdate
     skalerer til 100k+ items server-side.

Test:
  Step 1: Vælg 3 tilfældige on_sale SKUs (read price, compareAt, cost)
  Step 2: Submit JSONL via bulkOperationRunMutation der sætter price=p+1,
          compareAtPrice=KEEP (omit), cost=cost+0.50
  Step 3: Poll til COMPLETED
  Step 4: Re-laes alle 3 → verificér ændringer
  Step 5: Restore via SECOND bulk operation → tilbage til original
  Step 6: Re-laes → verificér restored

Hvis det her passerer, har vi to mekanismer end-to-end verificeret:
  - Cost via variant.inventoryItem.cost virker
  - Bulk Operations submit/poll/parse virker
"""
import json
import os
import random
import sys
import tempfile
import time
from datetime import datetime

import requests

CACHE_PATH = "output/shop_skus.json"
ON_SALE_DIFFS_PATH = "output/on_sale_diffs.csv"

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE_URL') or 'b7916a-38.myshopify.com'
SHOPIFY_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json"
HEADERS = {'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json'}


def gql(query, variables=None):
    payload = {'query': query}
    if variables: payload['variables'] = variables
    r = requests.post(GRAPHQL, headers=HEADERS, json=payload, timeout=60)
    r.raise_for_status()
    d = r.json()
    if 'errors' in d:
        raise Exception(f"GraphQL errors: {d['errors']}")
    return d


def query_variant_full(var_id):
    q = """
    query v($id: ID!) {
      productVariant(id: $id) {
        id sku price compareAtPrice
        product { id title }
        inventoryItem { id unitCost { amount } }
      }
    }
    """
    d = gql(q, {'id': f"gid://shopify/ProductVariant/{var_id}"})
    return (d.get('data') or {}).get('productVariant')


BULK_MUTATION = '''
mutation call($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    userErrors { field message }
    productVariants { id }
  }
}
'''

STAGED_UPLOAD = """
mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
  stagedUploadsCreate(input: $input) {
    userErrors { field message }
    stagedTargets {
      url resourceUrl
      parameters { name value }
    }
  }
}
"""

BULK_RUN = """
mutation bulkOperationRunMutation($mutation: String!, $stagedUploadPath: String!) {
  bulkOperationRunMutation(mutation: $mutation, stagedUploadPath: $stagedUploadPath) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
"""

BULK_STATUS = """
query { currentBulkOperation(type: MUTATION) {
  id status errorCode objectCount url
} }
"""


def submit_bulk(jsonl_lines, label):
    """Submit JSONL → bulkOperationRunMutation → poll → return final status."""
    print(f"\n📤 {label} — submitting {len(jsonl_lines)} bulk mutations...")
    jsonl_path = tempfile.mktemp(suffix='.jsonl')
    with open(jsonl_path, 'w') as f:
        for line in jsonl_lines:
            f.write(json.dumps(line, separators=(',', ':')) + '\n')

    # 1. stagedUploadsCreate
    d = gql(STAGED_UPLOAD, {"input": [{
        "filename": "bulk_canary.jsonl",
        "mimeType": "text/jsonl",
        "httpMethod": "POST",
        "resource": "BULK_MUTATION_VARIABLES",
    }]})
    target = d['data']['stagedUploadsCreate']['stagedTargets'][0]
    parameters = {p['name']: p['value'] for p in target['parameters']}
    path = parameters.get('key', '')

    # 2. Upload til S3
    with open(jsonl_path, 'rb') as f:
        r = requests.post(target['url'],
                          data=list(parameters.items()),
                          files={'file': ('bulk_canary.jsonl', f, 'text/jsonl')},
                          timeout=60)
    if r.status_code not in (200, 201, 204):
        raise Exception(f"S3 upload failed: {r.status_code}: {r.text[:200]}")
    os.unlink(jsonl_path)
    print(f"   ✅ JSONL uploaded ({r.status_code})")

    # 3. bulkOperationRunMutation
    d = gql(BULK_RUN, {"mutation": BULK_MUTATION, "stagedUploadPath": path})
    bulk = d['data']['bulkOperationRunMutation']
    if bulk['userErrors']:
        raise Exception(f"bulkOperationRunMutation failed: {bulk['userErrors']}")
    op = bulk['bulkOperation']
    print(f"   ✅ Bulk operation started: {op['id']}")

    # 4. Poll
    start = time.time()
    while True:
        time.sleep(8)
        d = gql(BULK_STATUS)
        cur = d['data']['currentBulkOperation']
        elapsed = int(time.time() - start)
        if cur is None:
            print(f"   [{elapsed}s] currentBulkOperation=null → antager færdig")
            return None
        print(f"   [{elapsed}s] status={cur['status']} count={cur.get('objectCount')}")
        if cur['status'] in ('COMPLETED', 'FAILED', 'CANCELED', 'EXPIRED'):
            return cur
        if elapsed > 180:
            raise Exception(f"Bulk timeout efter 3 min")


def main():
    if not SHOPIFY_TOKEN:
        sys.exit("❌ SHOPIFY_ACCESS_TOKEN mangler")

    with open(CACHE_PATH, encoding='utf-8') as f:
        cache = json.load(f)
    variants_map = cache['variants']

    # Find 3 on_sale SKUs med complete data (price, compareAt, cost)
    import csv
    with open(ON_SALE_DIFFS_PATH, encoding='utf-8') as f:
        candidates = [r for r in csv.DictReader(f) if r['Compare At Action'] == 'KEEP']
    random.seed(datetime.utcnow().strftime("%Y%m%d%H%M"))
    samples = random.sample(candidates, min(50, len(candidates)))

    test_skus = []
    for row in samples:
        if len(test_skus) >= 3: break
        sku = row['Variant SKU']
        vm = variants_map.get(sku)
        if not vm: continue
        var_id, prod_id = vm
        try:
            v = query_variant_full(var_id)
        except Exception:
            continue
        if not v: continue
        if not v.get('price') or not v.get('compareAtPrice'): continue
        try:
            price = float(v['price']); cap = float(v['compareAtPrice'])
        except: continue
        cost_data = (v.get('inventoryItem') or {}).get('unitCost')
        cost = float(cost_data['amount']) if cost_data and cost_data.get('amount') else None
        if cost is None: continue
        test_skus.append({
            'sku': sku, 'var_id': var_id, 'prod_id': prod_id,
            'orig_price': price, 'orig_cap': cap, 'orig_cost': cost,
            'product_title': (v.get('product') or {}).get('title'),
        })

    if len(test_skus) < 3:
        sys.exit(f"❌ Kunne ikke finde 3 testbare on_sale SKUs (fandt {len(test_skus)})")

    print(f"🎯 Test-SKUs valgt:")
    for s in test_skus:
        print(f"   {s['sku']} ({s['product_title'][:50]})")
        print(f"      price={s['orig_price']}, compareAt={s['orig_cap']}, cost={s['orig_cost']}")

    # =================== MUTATE ===================
    mutate_lines = []
    for s in test_skus:
        new_price = s['orig_price'] + 1
        new_cost = s['orig_cost'] + 0.50
        mutate_lines.append({
            "productId": f"gid://shopify/Product/{s['prod_id']}",
            "variants": [{
                "id": f"gid://shopify/ProductVariant/{s['var_id']}",
                "price": str(new_price),
                "inventoryItem": {"cost": str(new_cost)},
            }]
        })

    cur = submit_bulk(mutate_lines, "MUTATE (price+1, cost+0.50, omit compareAt)")
    if not cur or cur['status'] != 'COMPLETED':
        sys.exit(f"❌ Mutate bulk fejlede: status={cur and cur['status']}")

    # =================== VERIFY ===================
    print(f"\n🔬 Verificerer ændringer i Shopify...")
    time.sleep(3)
    for s in test_skus:
        v = query_variant_full(s['var_id'])
        new_price = float(v['price'])
        new_cap = float(v['compareAtPrice']) if v.get('compareAtPrice') else None
        cost_data = (v.get('inventoryItem') or {}).get('unitCost')
        new_cost = float(cost_data['amount']) if cost_data else None

        expected_price = s['orig_price'] + 1
        expected_cost = s['orig_cost'] + 0.50

        print(f"   SKU {s['sku']}:")
        print(f"      price: {new_price} (forventet {expected_price}) {'✅' if new_price == expected_price else '❌'}")
        print(f"      compareAt: {new_cap} (forventet UÆNDRET {s['orig_cap']}) {'✅' if new_cap == s['orig_cap'] else '❌'}")
        print(f"      cost: {new_cost} (forventet ~{expected_cost}) {'✅' if abs(new_cost - expected_cost) < 0.01 else '❌'}")
        if new_price != expected_price or new_cap != s['orig_cap'] or abs(new_cost - expected_cost) >= 0.01:
            sys.exit(f"❌ MISMATCH på SKU {s['sku']}")

    # =================== RESTORE ===================
    restore_lines = []
    for s in test_skus:
        restore_lines.append({
            "productId": f"gid://shopify/Product/{s['prod_id']}",
            "variants": [{
                "id": f"gid://shopify/ProductVariant/{s['var_id']}",
                "price": str(s['orig_price']),
                "inventoryItem": {"cost": str(s['orig_cost'])},
            }]
        })
    cur = submit_bulk(restore_lines, "RESTORE")
    if not cur or cur['status'] != 'COMPLETED':
        sys.exit(f"❌ Restore bulk fejlede: status={cur and cur['status']}")

    time.sleep(3)
    print(f"\n🔬 Verificerer restore...")
    for s in test_skus:
        v = query_variant_full(s['var_id'])
        new_price = float(v['price'])
        cost_data = (v.get('inventoryItem') or {}).get('unitCost')
        new_cost = float(cost_data['amount']) if cost_data else None
        ok = (new_price == s['orig_price'] and abs(new_cost - s['orig_cost']) < 0.01)
        print(f"   SKU {s['sku']}: price={new_price} cost={new_cost} {'✅' if ok else '❌'}")
        if not ok:
            sys.exit(f"❌ Restore mislykkedes på {s['sku']}")

    print(f"\n{'='*60}")
    print(f"✅ BEGGE MEKANISMER BEKRAEFTET END-TO-END")
    print(f"   - Bulk Operations submit/poll/parse: virker")
    print(f"   - inventoryItem.cost i variant-input: virker")
    print(f"   - compareAtPrice omit (KEEP) bevarer eksisterende: virker")
    print(f"   Net change: 0")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
