"""End-to-end ROUND-TRIP canary test af direct-API inventory.

Plukker EN tilfaeldig SKU fra cachen, og:
  1. Laeser nuvaerende Shopify on-hand-vaerdi (call A)
  2. Saetter den til vaerdi+1 via inventorySetOnHandQuantities
  3. Laeser igen — bekraefter den er vaerdi+1
  4. Saetter den tilbage til original vaerdi
  5. Laeser igen — bekraefter den er den oprindelige vaerdi

Net effekt paa shop: NUL (vi ender med original vaerdi). Beviser at hele
kaeden virker: GraphQL endpoint, credentials, inventory_item_id mapping,
location_id mapping, payload-struktur, mutation-resultat reflekteres
korrekt i efterfoelgende reads.

Bruges som validation FOER vi flipper sync_inventory_v2 til --live mode.
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
    if variables:
        payload['variables'] = variables
    r = requests.post(GRAPHQL, headers=HEADERS, json=payload, timeout=60)
    r.raise_for_status()
    d = r.json()
    if 'errors' in d:
        raise Exception(f"GraphQL errors: {d['errors']}")
    return d


def query_on_hand(inv_id: int, loc_id: str):
    q = """
    query qty($inventoryItemId: ID!, $locationId: ID!) {
      inventoryLevel(inventoryItemId: $inventoryItemId, locationId: $locationId) {
        quantities(names: ["on_hand"]) { name quantity }
      }
    }
    """
    d = gql(q, {
        'inventoryItemId': f"gid://shopify/InventoryItem/{inv_id}",
        'locationId': f"gid://shopify/Location/{loc_id}",
    })
    level = (d.get('data') or {}).get('inventoryLevel')
    if not level:
        return None
    return next((x['quantity'] for x in level['quantities'] if x['name'] == 'on_hand'), None)


def variant_info(inv_id: int):
    q = """
    query info($inventoryItemId: ID!) {
      inventoryItem(id: $inventoryItemId) {
        variant {
          sku title
          product { id title }
        }
      }
    }
    """
    d = gql(q, {'inventoryItemId': f"gid://shopify/InventoryItem/{inv_id}"})
    item = (d.get('data') or {}).get('inventoryItem')
    if not item:
        return {}
    v = item.get('variant') or {}
    p = v.get('product') or {}
    pid = (p.get('id') or '').rsplit('/', 1)[-1]
    return {
        'sku': v.get('sku'),
        'product_title': p.get('title'),
        'variant_title': v.get('title'),
        'admin_url': f"https://{SHOPIFY_STORE}/admin/products/{pid}/inventory" if pid else '',
    }


def set_on_hand(inv_id: int, loc_id: str, qty: int):
    m = """
    mutation setOnHand($input: InventorySetOnHandQuantitiesInput!) {
      inventorySetOnHandQuantities(input: $input) {
        userErrors { field message }
        inventoryAdjustmentGroup { createdAt reason }
      }
    }
    """
    d = gql(m, {
        'input': {
            'reason': 'correction',
            'referenceDocumentUri': f'logistics://boligretning/canary-{datetime.utcnow().isoformat()}',
            'setQuantities': [{
                'inventoryItemId': f"gid://shopify/InventoryItem/{inv_id}",
                'locationId': f"gid://shopify/Location/{loc_id}",
                'quantity': qty,
            }],
        }
    })
    return d['data']['inventorySetOnHandQuantities']


def main():
    if not SHOPIFY_TOKEN:
        sys.exit("❌ SHOPIFY_ACCESS_TOKEN mangler")

    with open(CACHE_PATH, encoding='utf-8') as f:
        cache = json.load(f)
    loc_id = cache['location_id']
    items = cache['inventory_items']
    print(f"📦 Cache: location={loc_id}, {len(items)} SKUs\n")

    # Vaelg en tilfaeldig SKU (deterministisk seed for reproducerbarhed pr. dag)
    random.seed(datetime.utcnow().strftime("%Y%m%d"))
    samples = random.sample(list(items.items()), 20)

    # Find foerste SKU hvor on_hand > 0 (saa vi har plads til +1 / -1 uden negativ stock)
    chosen = None
    for sku, inv_id in samples:
        try:
            current = query_on_hand(inv_id, loc_id)
        except Exception:
            continue
        if current is None or current <= 0:
            continue
        chosen = {'sku': sku, 'inv_id': inv_id, 'original': current}
        break

    if not chosen:
        sys.exit("❌ Kunne ikke finde testbar SKU (alle havde 0 stock eller fejlede)")

    sku = chosen['sku']; inv_id = chosen['inv_id']; orig = chosen['original']
    test_qty = orig + 1

    info = variant_info(inv_id)
    print(f"🎯 Test-SKU: {sku} (inventory_item_id {inv_id})")
    print(f"   Product: {info.get('product_title')}")
    print(f"   Variant: {info.get('variant_title')}")
    print(f"   Admin: {info.get('admin_url')}\n")

    print(f"📖 Step 1: Laes nuvaerende on_hand = {orig}")

    print(f"\n📝 Step 2: Saet on_hand = {test_qty} (= {orig}+1)")
    r1 = set_on_hand(inv_id, loc_id, test_qty)
    if r1['userErrors']:
        sys.exit(f"❌ Set #1 fejlede: {r1['userErrors']}")
    print(f"   ✅ Mutation accepteret: {r1['inventoryAdjustmentGroup']}")

    time.sleep(2)
    after1 = query_on_hand(inv_id, loc_id)
    print(f"\n🔬 Step 3: Re-laes on_hand = {after1}")
    if after1 != test_qty:
        sys.exit(f"❌ MISMATCH efter set #1: forventet {test_qty}, fik {after1}")
    print(f"   ✅ Vaerdi flippede korrekt fra {orig} til {after1}")

    print(f"\n📝 Step 4: Restore — saet on_hand tilbage til {orig}")
    r2 = set_on_hand(inv_id, loc_id, orig)
    if r2['userErrors']:
        sys.exit(f"❌ Set #2 fejlede: {r2['userErrors']}")
    print(f"   ✅ Mutation accepteret")

    time.sleep(2)
    after2 = query_on_hand(inv_id, loc_id)
    print(f"\n🔬 Step 5: Re-laes on_hand = {after2}")
    if after2 != orig:
        sys.exit(f"❌ MISMATCH efter restore: forventet {orig}, fik {after2}")
    print(f"   ✅ Vaerdi restored til original {after2}")

    print(f"\n{'='*60}")
    print(f"✅ END-TO-END BEKRAEFTET — direct-API path virker")
    print(f"   SKU {sku}: {orig} → {test_qty} → {orig}")
    print(f"   Net change: 0 (shop er identisk med foer)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
