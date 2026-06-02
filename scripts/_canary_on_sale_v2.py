"""End-to-end canary for ON_SALE compareAt-semantik.

Test 1 — KEEP mode (omit compareAtPrice from mutation → Shopify bevarer):
  Picker en on_sale SKU der har compareAtPrice sat.
  Aendrer KUN price (omit compareAtPrice).
  Verificerer compareAtPrice er UAENDRET.

Test 2 — CLEAR mode (compareAtPrice: null → Shopify rydder):
  Picker en on_sale SKU der har compareAtPrice sat.
  Saetter compareAtPrice = null eksplicit.
  Verificerer compareAtPrice er null.
  Restorer compareAtPrice til original.

Begge tests retter prisen tilbage til original ved exit. Net change: 0.

Hvis BEGGE tests passerer er on_sale-mutationerne i sync_prices_v2
end-to-end-bekraeftede.
"""
import json
import os
import random
import sys
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


def update_variant(product_id, variant_id, **kwargs):
    """Kun de nævnte felter sendes i mutation. Omitted felter forbliver uændret i Shopify."""
    m = """
    mutation u($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
      productVariantsBulkUpdate(productId: $productId, variants: $variants) {
        userErrors { field message }
        productVariants { id price compareAtPrice }
      }
    }
    """
    v = {"id": f"gid://shopify/ProductVariant/{variant_id}"}
    v.update({k: v_ for k, v_ in kwargs.items() if v_ is not Ellipsis})
    # Eksplicit None forbliver, eksplicit Ellipsis (...) udelades
    d = gql(m, {
        'productId': f"gid://shopify/Product/{product_id}",
        'variants': [v],
    })
    return d['data']['productVariantsBulkUpdate']


def find_test_on_sale_sku():
    """Find en SKU fra on_sale_diffs.csv der har positiv compareAtPrice i Shopify."""
    if not os.path.exists(ON_SALE_DIFFS_PATH):
        sys.exit(f"❌ {ON_SALE_DIFFS_PATH} findes ikke — kør sync_prices_v2 dry-run først")
    with open(CACHE_PATH, encoding='utf-8') as f:
        cache = json.load(f)
    variants_map = cache['variants']

    import csv
    with open(ON_SALE_DIFFS_PATH, encoding='utf-8') as f:
        # Kun KEEP rows — vil ikke pille ved en edge case der allerede skal cleares
        candidates = [r for r in csv.DictReader(f) if r['Compare At Action'] == 'KEEP']

    if not candidates:
        sys.exit("❌ Ingen KEEP-rows i on_sale_diffs.csv — kan ikke teste KEEP mode")

    random.seed(datetime.utcnow().strftime("%Y%m%d%H%M"))
    for row in random.sample(candidates, min(30, len(candidates))):
        sku = row['Variant SKU']
        vm = variants_map.get(sku)
        if not vm: continue
        var_id, prod_id = vm
        try:
            v = query_variant(var_id)
        except Exception:
            continue
        if not v or not v.get('price') or not v.get('compareAtPrice'):
            continue
        try:
            price = float(v['price']); cap = float(v['compareAtPrice'])
        except (ValueError, TypeError):
            continue
        if price <= 0 or cap <= 0:
            continue
        return {
            'sku': sku, 'var_id': var_id, 'prod_id': prod_id,
            'orig_price': price, 'orig_cap': cap, 'info': v,
        }
    sys.exit("❌ Kunne ikke finde testbar on_sale SKU med positiv price + compareAt")


def main():
    if not SHOPIFY_TOKEN:
        sys.exit("❌ SHOPIFY_ACCESS_TOKEN mangler")

    candidate = find_test_on_sale_sku()
    sku = candidate['sku']
    var_id = candidate['var_id']
    prod_id = candidate['prod_id']
    orig_price = candidate['orig_price']
    orig_cap = candidate['orig_cap']

    print(f"🎯 Test-SKU: {sku} (status=on_sale)")
    print(f"   variant_id: {var_id}  product_id: {prod_id}")
    print(f"   Product: {candidate['info'].get('product', {}).get('title')}")
    print(f"   Admin: https://{SHOPIFY_STORE}/admin/products/{prod_id}")
    print(f"   Original: price={orig_price}, compareAt={orig_cap}\n")

    # ===== TEST 1: KEEP mode =====
    print("=" * 60)
    print("TEST 1 — KEEP MODE (omit compareAtPrice fra mutation)")
    print("=" * 60)
    test_price = orig_price + 1
    print(f"\n📝 Step 1.1: Saet price={test_price} (OMIT compareAtPrice)")
    r = update_variant(prod_id, var_id, price=str(test_price))   # ingen compareAtPrice
    if r['userErrors']:
        sys.exit(f"❌ Mutation fejlede: {r['userErrors']}")
    print(f"   ✅ Mutation accepteret")
    time.sleep(2)
    v = query_variant(var_id)
    new_price = float(v['price']); new_cap = float(v['compareAtPrice']) if v.get('compareAtPrice') else None
    print(f"\n🔬 Step 1.2: Re-laes")
    print(f"   price={new_price}  (forventet {test_price})")
    print(f"   compareAt={new_cap}  (forventet UAENDRET = {orig_cap})")
    if new_price != test_price:
        sys.exit(f"❌ Price flippede ikke: {new_price} vs {test_price}")
    if new_cap != orig_cap:
        sys.exit(f"❌ compareAt aendrede sig (skulle ikke!): {new_cap} vs {orig_cap}")
    print(f"   ✅ KEEP mode virker: compareAt BEVARET ved omit")

    print(f"\n📝 Step 1.3: Restore price til {orig_price}")
    r = update_variant(prod_id, var_id, price=str(orig_price))
    if r['userErrors']:
        sys.exit(f"❌ Restore fejlede: {r['userErrors']}")
    time.sleep(2)
    v = query_variant(var_id)
    if float(v['price']) != orig_price:
        sys.exit(f"❌ Restore #1 mislykkedes: {v['price']} vs {orig_price}")
    print(f"   ✅ Restored: price={v['price']}, compareAt={v['compareAtPrice']}")

    # ===== TEST 2: CLEAR mode =====
    print("\n" + "=" * 60)
    print("TEST 2 — CLEAR MODE (compareAtPrice: null eksplicit)")
    print("=" * 60)
    print(f"\n📝 Step 2.1: Saet compareAtPrice=null (clear), price unchanged")
    r = update_variant(prod_id, var_id, compareAtPrice=None)
    if r['userErrors']:
        sys.exit(f"❌ CLEAR mutation fejlede: {r['userErrors']}")
    print(f"   ✅ Mutation accepteret")
    time.sleep(2)
    v = query_variant(var_id)
    print(f"\n🔬 Step 2.2: Re-laes")
    print(f"   price={v['price']}  (forventet {orig_price})")
    print(f"   compareAt={v['compareAtPrice']}  (forventet null/None)")
    if float(v['price']) != orig_price:
        sys.exit(f"❌ Price aendrede sig (skulle ikke!): {v['price']}")
    if v.get('compareAtPrice') is not None:
        sys.exit(f"❌ compareAt blev ikke ryddet: {v['compareAtPrice']}")
    print(f"   ✅ CLEAR mode virker: compareAt nulled, price UAENDRET")

    print(f"\n📝 Step 2.3: Restore compareAtPrice til {orig_cap}")
    r = update_variant(prod_id, var_id, compareAtPrice=str(int(orig_cap)))
    if r['userErrors']:
        sys.exit(f"❌ Restore #2 fejlede: {r['userErrors']}")
    time.sleep(2)
    v = query_variant(var_id)
    if v.get('compareAtPrice') is None or float(v['compareAtPrice']) != orig_cap:
        sys.exit(f"❌ Restore #2 mislykkedes: compareAt={v.get('compareAtPrice')}")
    print(f"   ✅ Restored: price={v['price']}, compareAt={v['compareAtPrice']}")

    print(f"\n{'='*60}")
    print(f"✅ BEGGE TESTS PASSED — on_sale semantics virker")
    print(f"   KEEP mode (omit compareAt): compareAt bevares ✓")
    print(f"   CLEAR mode (compareAt:null): compareAt ryddes ✓")
    print(f"   Net change: 0 (shop er identisk med før)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
