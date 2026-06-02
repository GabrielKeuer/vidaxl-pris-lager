"""End-to-end ROUND-TRIP canary test af direct-API price update.

Plukker EN tilfaeldig SKU med variant+product_id i cachen, og:
  1. Laeser nuvaerende price + compareAtPrice (call A)
  2. Saetter price = current + 1 via productVariantsBulkUpdate
  3. Laeser igen — bekraefter price er current+1
  4. Restorer price til original vaerdi
  5. Laeser igen — bekraefter price er den oprindelige

Net effekt: NUL. Beviser at:
  - GraphQL credentials/endpoint virker
  - variant_id mapping er korrekt
  - product_id mapping er korrekt
  - productVariantsBulkUpdate payload er valid
  - Aendringer reflekteres oejeblikkeligt i Shopify reads

ADVARSEL: TestSKUens current price aendres MIDLERTIDIGT med +1 kr.
Hvis kunder ser produktet i den 2-sekunders periode hvor det er current+1,
ser de en pris der er 1 kr for hoej. Acceptable cost-of-validation.
"""
import json
import os
import random
import sys
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


def query_price(variant_id):
    """Returnér (price, compareAtPrice) som strings, eller (None, None)."""
    q = """
    query p($id: ID!) {
      productVariant(id: $id) {
        id sku price compareAtPrice
        product { id title }
      }
    }
    """
    d = gql(q, {'id': f"gid://shopify/ProductVariant/{variant_id}"})
    v = (d.get('data') or {}).get('productVariant')
    if not v: return None
    return {
        'price': v.get('price'),
        'compareAtPrice': v.get('compareAtPrice'),
        'sku': v.get('sku'),
        'product_title': (v.get('product') or {}).get('title'),
        'product_id': (v.get('product') or {}).get('id', '').rsplit('/', 1)[-1],
    }


def set_price(product_id, variant_id, price, compare_at=None):
    """Anvend productVariantsBulkUpdate for EN variant."""
    m = """
    mutation u($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
      productVariantsBulkUpdate(productId: $productId, variants: $variants) {
        userErrors { field message }
        productVariants { id price compareAtPrice }
      }
    }
    """
    v = {
        "id": f"gid://shopify/ProductVariant/{variant_id}",
        "price": str(price),
    }
    if compare_at is not None:
        v["compareAtPrice"] = str(compare_at)
    d = gql(m, {
        'productId': f"gid://shopify/Product/{product_id}",
        'variants': [v],
    })
    return d['data']['productVariantsBulkUpdate']


def main():
    if not SHOPIFY_TOKEN:
        sys.exit("❌ SHOPIFY_ACCESS_TOKEN mangler")

    with open(CACHE_PATH, encoding='utf-8') as f:
        cache = json.load(f)
    variants_map = cache.get('variants') or {}
    if not variants_map:
        sys.exit("❌ Cache mangler 'variants' — koer update_shop_cache.py foerst")

    random.seed(datetime.utcnow().strftime("%Y%m%d%H%M"))
    samples = random.sample(list(variants_map.items()), min(50, len(variants_map)))

    chosen = None
    for sku, (var_id, prod_id) in samples:
        try:
            info = query_price(var_id)
        except Exception:
            continue
        if not info or not info.get('price'):
            continue
        try:
            current_price = float(info['price'])
        except (ValueError, TypeError):
            continue
        if current_price <= 0:
            continue
        chosen = {
            'sku': sku, 'var_id': var_id, 'prod_id': prod_id,
            'orig_price': current_price,
            'orig_cap': info.get('compareAtPrice'),
            'info': info,
        }
        break

    if not chosen:
        sys.exit("❌ Kunne ikke finde testbar SKU med positiv pris i sample")

    sku = chosen['sku']
    var_id = chosen['var_id']
    prod_id = chosen['prod_id']
    orig = chosen['orig_price']
    orig_cap = chosen['orig_cap']
    test_price = orig + 1

    print(f"🎯 Test-SKU: {sku}")
    print(f"   product_id: {prod_id}  variant_id: {var_id}")
    print(f"   Product: {chosen['info'].get('product_title')}")
    print(f"   Admin: https://{SHOPIFY_STORE}/admin/products/{prod_id}\n")

    print(f"📖 Step 1: Current price = {orig}, compareAtPrice = {orig_cap}")

    print(f"\n📝 Step 2: Saet price = {test_price} (=current+1)")
    r1 = set_price(prod_id, var_id, test_price, compare_at=orig_cap)
    if r1['userErrors']:
        sys.exit(f"❌ Set #1 fejlede: {r1['userErrors']}")
    print(f"   ✅ Mutation accepteret")

    time.sleep(2)
    after1 = query_price(var_id)
    print(f"\n🔬 Step 3: Re-laes price = {after1['price']}")
    if float(after1['price']) != test_price:
        sys.exit(f"❌ MISMATCH: forventet {test_price}, fik {after1['price']}")
    print(f"   ✅ Vaerdi flippede korrekt fra {orig} til {after1['price']}")

    print(f"\n📝 Step 4: Restore price til {orig}")
    r2 = set_price(prod_id, var_id, orig, compare_at=orig_cap)
    if r2['userErrors']:
        sys.exit(f"❌ Restore fejlede: {r2['userErrors']}")
    print(f"   ✅ Mutation accepteret")

    time.sleep(2)
    after2 = query_price(var_id)
    print(f"\n🔬 Step 5: Re-laes price = {after2['price']}")
    if float(after2['price']) != orig:
        sys.exit(f"❌ MISMATCH efter restore: forventet {orig}, fik {after2['price']}")
    print(f"   ✅ Vaerdi restored til original {after2['price']}")

    print(f"\n{'='*60}")
    print(f"✅ END-TO-END BEKRAEFTET — direct-API price-path virker")
    print(f"   SKU {sku}: {orig} → {test_price} → {orig} kr")
    print(f"   Net change: 0 (shop er identisk med foer)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
