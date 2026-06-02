"""Daily/twice-daily SKU + ID-mappings cache for downstream sync scripts.

Output (output/shop_skus.json) — UDVIDET 2026-06-02 til at understøtte
direct-API migration:
  - skus: list[str]                  — bagudkompatibelt (eksisterende læsere)
  - count: int
  - updated: ISO timestamp
  - location_id: str (numerisk)      — primary fulfillment location
  - inventory_items: {sku: int}      — sku → inventory_item_id (for sync_inventory_v2)
  - variants: {sku: [variant_id, product_id]}  — sku → (variant_id, product_id)
                                                  for sync_prices_v2 + rotate_groups_v2
                                                  (productVariantsBulkUpdate kraever begge)

Eksisterende læsere bruger kun `skus` og bliver ikke påvirket. Nye
direct-API scripts bruger de øvrige felter til at konstruere mutations
uden ekstra GraphQL-rundtur pr. kørsel.
"""
import json
import os
import time
from datetime import datetime

import requests

SHOPIFY_STORE = 'b7916a-38.myshopify.com'
SHOPIFY_TOKEN = os.environ['SHOPIFY_ACCESS_TOKEN']
GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json"
HEADERS = {'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json'}


def gql(query, variables=None, max_retries=4):
    """GraphQL kald med throttle-aware backoff."""
    payload = {'query': query}
    if variables:
        payload['variables'] = variables
    for attempt in range(1, max_retries + 1):
        r = requests.post(GRAPHQL, headers=HEADERS, json=payload, timeout=60)
        if r.status_code != 200:
            raise Exception(f"HTTP {r.status_code}: {r.text[:300]}")
        data = r.json()
        if 'errors' in data:
            throttled = any('Throttled' in str(e) or 'THROTTLED' in str(e) for e in data['errors'])
            if throttled and attempt < max_retries:
                wait = 2 ** attempt
                print(f"  ⏳ Throttled, retry {attempt}/{max_retries} in {wait}s")
                time.sleep(wait)
                continue
            raise Exception(f"GraphQL errors: {data['errors']}")
        # Frivillig backoff hvis cost-bucket er ved at være tom
        cost = data.get('extensions', {}).get('cost', {})
        throttle = cost.get('throttleStatus', {})
        if throttle.get('currentlyAvailable', 1000) < 200:
            time.sleep(0.5)
        return data
    raise Exception("Max retries exceeded")


def fetch_primary_location():
    """Hent primary fulfillment-location (numerisk ID)."""
    q = """
    query {
      locations(first: 5) {
        edges { node { id name isPrimary } }
      }
    }
    """
    data = gql(q)
    edges = data['data']['locations']['edges']
    primary = next((e['node'] for e in edges if e['node'].get('isPrimary')), edges[0]['node'])
    # gid://shopify/Location/97768178013 → 97768178013
    loc_id = primary['id'].rsplit('/', 1)[-1]
    print(f"📍 Primary location: {primary['name']} ({loc_id})")
    return loc_id


def fetch_all_variants():
    """Hent alle SKUs + inventory_item_id + variant_id + product_id.

    Cost-budgettet hos Shopify er rigeligt til at hente 165k varianter
    i én session. Vi henter pr. side med 250 varianter og pauser kun
    hvis throttle-bucket nærmer sig tom.
    """
    print("🚀 Fetching SKUs + inventory_item_ids + variant/product ids from Shopify...")
    skus = set()
    items = {}                # sku -> numeric inventory_item_id (for sync_inventory_v2)
    variants_map = {}         # sku -> [variant_id, product_id] (for sync_prices_v2)
    cursor = None
    page = 0
    while True:
        q = """
        query getVariants($cursor: String) {
          productVariants(first: 250, after: $cursor) {
            edges {
              node {
                id
                sku
                inventoryItem { id }
                product { id }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        data = gql(q, {'cursor': cursor})
        variants = data['data']['productVariants']
        for edge in variants['edges']:
            n = edge['node']
            sku = (n.get('sku') or '').strip()
            if not sku:
                continue
            sku = str(sku)
            inv = (n.get('inventoryItem') or {}).get('id') or ''
            var_id = n.get('id') or ''
            prod_id = (n.get('product') or {}).get('id') or ''
            try:
                inv_num = int(inv.rsplit('/', 1)[-1]) if inv else None
                var_num = int(var_id.rsplit('/', 1)[-1]) if var_id else None
                prod_num = int(prod_id.rsplit('/', 1)[-1]) if prod_id else None
            except ValueError:
                continue
            if not (var_num and prod_num):
                continue
            skus.add(sku)
            if inv_num:
                items[sku] = inv_num
            variants_map[sku] = [var_num, prod_num]
        page += 1
        if page % 20 == 0:
            print(f"  Page {page}: {len(skus)} SKUs cached so far")
        if not variants['pageInfo']['hasNextPage']:
            break
        cursor = variants['pageInfo']['endCursor']
    print(f"✅ Total: {len(skus)} SKUs, {len(items)} m. inventory_item_id, {len(variants_map)} m. variant+product")
    return sorted(skus), items, variants_map


def main():
    location_id = fetch_primary_location()
    skus, items, variants_map = fetch_all_variants()

    output = {
        'skus': skus,                   # bagudkompatibelt
        'count': len(skus),
        'updated': datetime.utcnow().isoformat() + 'Z',
        'location_id': location_id,     # primary fulfillment location
        'inventory_items': items,       # sku -> inventory_item_id
        'variants': variants_map,       # sku -> [variant_id, product_id]
    }

    os.makedirs('output', exist_ok=True)
    # Kompakt JSON (ingen indent) — sparer ~40% størrelse for 165k SKUs
    with open('output/shop_skus.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, separators=(',', ':'))

    size_mb = os.path.getsize('output/shop_skus.json') / 1024 / 1024
    print(f"💾 Saved output/shop_skus.json ({size_mb:.1f} MB)")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
