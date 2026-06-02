"""Rotation-specifik canary: bekraefter "set compareAt til value via bulk".

Dette er den ene mutation-path vi ikke har testet end-to-end endnu.
sync_prices_v2 LIVE run satte compareAt=null (CLEAR) for 375 produkter
+ omitted compareAt (KEEP) for 29.442 produkter. Den NYE path for
rotate_groups er at SAETTE compareAt til en konkret vaerdi (når en
sale starter).

Test (round-trip, 3 SKUs):
  1. Pick 3 SKUs med status='normal' (compareAt SKAL vaere null/empty
     for at vi har en clean start)
  2. Via bulkOperationRunMutation: set price=current_price (uændret)
     + compareAtPrice="current+200" (set til vaerdi)
     + cost via inventoryItem (samme vaerdi som nu)
  3. Verificer: compareAtPrice er nu current+200
  4. Restore: bulk-mutation med compareAtPrice=null (clear)
  5. Verificer: compareAtPrice er null igen

Net change: 0 (alle 3 SKUs ender med compareAt=null igen).
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


def query_variant(var_id):
    q = """
    query v($id: ID!) {
      productVariant(id: $id) {
        id sku price compareAtPrice
        product { id title }
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
    stagedTargets { url resourceUrl parameters { name value } }
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
query { currentBulkOperation(type: MUTATION) { id status objectCount } }
"""


def submit_bulk(lines, label):
    print(f"\n📤 {label} — {len(lines)} mutations...")
    p = tempfile.mktemp(suffix='.jsonl')
    with open(p, 'w') as f:
        for ln in lines:
            f.write(json.dumps(ln, separators=(',', ':')) + '\n')

    d = gql(STAGED_UPLOAD, {"input": [{
        "filename": "rotation_canary.jsonl",
        "mimeType": "text/jsonl",
        "httpMethod": "POST",
        "resource": "BULK_MUTATION_VARIABLES",
    }]})
    target = d['data']['stagedUploadsCreate']['stagedTargets'][0]
    parameters = {x['name']: x['value'] for x in target['parameters']}

    with open(p, 'rb') as f:
        r = requests.post(target['url'], data=list(parameters.items()),
                          files={'file': ('rotation_canary.jsonl', f, 'text/jsonl')},
                          timeout=60)
    if r.status_code not in (200, 201, 204):
        raise Exception(f"S3: {r.status_code}")
    os.unlink(p)

    d = gql(BULK_RUN, {"mutation": BULK_MUTATION,
                       "stagedUploadPath": parameters.get('key', '')})
    bulk = d['data']['bulkOperationRunMutation']
    if bulk['userErrors']:
        raise Exception(f"BulkRun: {bulk['userErrors']}")
    print(f"   ✅ Started: {bulk['bulkOperation']['id']}")

    start = time.time()
    while True:
        time.sleep(8)
        d = gql(BULK_STATUS)
        cur = d['data']['currentBulkOperation']
        if cur is None: break
        elapsed = int(time.time() - start)
        print(f"   [{elapsed}s] status={cur['status']} count={cur.get('objectCount')}")
        if cur['status'] in ('COMPLETED', 'FAILED', 'CANCELED', 'EXPIRED'):
            return cur
        if elapsed > 180:
            raise Exception("Timeout 3 min")


def main():
    if not SHOPIFY_TOKEN:
        sys.exit("❌ SHOPIFY_ACCESS_TOKEN mangler")

    with open(CACHE_PATH, encoding='utf-8') as f:
        cache = json.load(f)
    variants_map = cache['variants']

    # Find 3 SKUs hvor compareAtPrice er null (= "normal" status)
    # Vi sampler tilfaeldigt og query'er Shopify
    random.seed(datetime.utcnow().strftime("%Y%m%d%H%M"))
    samples = random.sample(list(variants_map.items()), 50)

    test_skus = []
    for sku, (var_id, prod_id) in samples:
        if len(test_skus) >= 3: break
        try: v = query_variant(var_id)
        except Exception: continue
        if not v or not v.get('price'): continue
        if v.get('compareAtPrice') is not None: continue  # vi vil have null start
        try: price = float(v['price'])
        except (ValueError, TypeError): continue
        if price <= 0: continue
        test_skus.append({
            'sku': sku, 'var_id': var_id, 'prod_id': prod_id,
            'orig_price': price, 'orig_cap': None,
            'product_title': (v.get('product') or {}).get('title'),
        })

    if len(test_skus) < 3:
        sys.exit(f"❌ Fandt kun {len(test_skus)} testbare SKUs med compareAt=null")

    print(f"🎯 Test-SKUs (alle med compareAt=null):")
    for s in test_skus:
        print(f"   {s['sku']}: price={s['orig_price']} compareAt=None")
        print(f"      {s['product_title'][:60]}")

    # ================= SET compareAt to VALUE =================
    print("\n" + "="*60)
    print("TEST: Sæt compareAt til en konkret value (price + 200)")
    print("="*60)
    set_lines = []
    for s in test_skus:
        test_cap = s['orig_price'] + 200
        set_lines.append({
            "productId": f"gid://shopify/Product/{s['prod_id']}",
            "variants": [{
                "id": f"gid://shopify/ProductVariant/{s['var_id']}",
                "price": str(s['orig_price']),  # uændret
                "compareAtPrice": str(test_cap),  # NY value
            }]
        })

    cur = submit_bulk(set_lines, "SET compareAt to value")
    if not cur or cur['status'] != 'COMPLETED':
        sys.exit(f"❌ SET bulk fejlede: {cur and cur['status']}")

    time.sleep(3)
    print(f"\n🔬 Verificerer SET...")
    for s in test_skus:
        v = query_variant(s['var_id'])
        new_cap = float(v['compareAtPrice']) if v.get('compareAtPrice') else None
        expected = s['orig_price'] + 200
        ok = new_cap == expected
        print(f"   SKU {s['sku']}: compareAt={new_cap} (forventet {expected}) {'✅' if ok else '❌'}")
        if not ok: sys.exit(f"❌ MISMATCH paa {s['sku']}")

    # ================= RESTORE compareAt = null =================
    print("\n" + "="*60)
    print("RESTORE: compareAt tilbage til null")
    print("="*60)
    restore_lines = []
    for s in test_skus:
        restore_lines.append({
            "productId": f"gid://shopify/Product/{s['prod_id']}",
            "variants": [{
                "id": f"gid://shopify/ProductVariant/{s['var_id']}",
                "price": str(s['orig_price']),
                "compareAtPrice": None,  # back to null
            }]
        })

    cur = submit_bulk(restore_lines, "RESTORE compareAt to null")
    if not cur or cur['status'] != 'COMPLETED':
        sys.exit(f"❌ RESTORE bulk fejlede: {cur and cur['status']}")

    time.sleep(3)
    print(f"\n🔬 Verificerer RESTORE...")
    for s in test_skus:
        v = query_variant(s['var_id'])
        new_cap = v.get('compareAtPrice')
        ok = new_cap is None
        print(f"   SKU {s['sku']}: compareAt={new_cap} {'✅' if ok else '❌'}")
        if not ok: sys.exit(f"❌ Restore mislykkedes paa {s['sku']}")

    print(f"\n{'='*60}")
    print(f"✅ ROTATION-SPECIFIK CANARY PASSED")
    print(f"   Set compareAt to value via Bulk Operations: virker")
    print(f"   Clear compareAt via Bulk Operations: virker")
    print(f"   Net change: 0 (alle 3 SKUs har compareAt=null igen)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
