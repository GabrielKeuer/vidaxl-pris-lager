"""End-to-end canary test af sync_inventory_v2's direct-API kald.

Hvad CSV-diff bekraefter:
  Listen af SKUs + qty vi VIL aendre er identisk med det gamle script.

Hvad CSV-diff IKKE bekraefter:
  At vores GraphQL-mutation faktisk lander i Shopify, at inventory_item_id
  peger paa den rigtige variant, at payload-strukturen er valid.

Denne canary tager EN SKU fra seneste dry-run-output, og:
  1. Laeser nuvaerende Shopify on-hand-vaerdi for det SKU
  2. Sammenligner med vores intended-vaerdi
  3. Hvis allerede ens → vaelger naeste row (intet at validere)
  4. Hvis forskellige → kalder inventorySetOnHandQuantities for kun den ene
  5. Laeser igen, bekraefter Shopify reflekterer den nye vaerdi
  6. Rapporterer SKU + variant-URL saa Gabriel kan spot-checke i Admin

Risiko: NUL. Vi anvender en aendring der ALLIGEVEL ville ske via Matrixify
naar dens schedule kører senere. Vi gør det bare via direct-API i stedet
for via CSV-broker.
"""
import csv
import json
import os
import sys
import time
from datetime import datetime

import requests


CACHE_PATH = "output/shop_skus.json"
DRYRUN_CSV = "output/new_inventory_updates.csv"

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE_URL') or 'b7916a-38.myshopify.com'
SHOPIFY_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json"
HEADERS = {'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json'}


def gql(query, variables=None):
    """Simpel GraphQL kald."""
    payload = {'query': query}
    if variables:
        payload['variables'] = variables
    r = requests.post(GRAPHQL, headers=HEADERS, json=payload, timeout=60)
    r.raise_for_status()
    d = r.json()
    if 'errors' in d:
        raise Exception(f"GraphQL errors: {d['errors']}")
    return d


def query_on_hand(inventory_item_id: int, location_id: str) -> int:
    """Hent nuvaerende on-hand-vaerdi for et inventory_item paa en location."""
    q = """
    query qty($inventoryItemId: ID!, $locationId: ID!) {
      inventoryLevel(inventoryItemId: $inventoryItemId, locationId: $locationId) {
        quantities(names: ["on_hand", "available"]) {
          name
          quantity
        }
      }
    }
    """
    vars = {
        'inventoryItemId': f"gid://shopify/InventoryItem/{inventory_item_id}",
        'locationId': f"gid://shopify/Location/{location_id}",
    }
    d = gql(q, vars)
    level = (d.get('data') or {}).get('inventoryLevel')
    if not level:
        return None
    on_hand = next((q['quantity'] for q in level['quantities'] if q['name'] == 'on_hand'), None)
    return on_hand


def get_variant_admin_url(inventory_item_id: int) -> str:
    """Find Shopify Admin URL for varianten saa Gabriel kan spot-checke manuelt."""
    q = """
    query findVariant($inventoryItemId: ID!) {
      inventoryItem(id: $inventoryItemId) {
        variant {
          id
          sku
          title
          product { id title handle }
        }
      }
    }
    """
    vars = {'inventoryItemId': f"gid://shopify/InventoryItem/{inventory_item_id}"}
    d = gql(q, vars)
    item = (d.get('data') or {}).get('inventoryItem')
    if not item:
        return None
    v = item.get('variant') or {}
    p = v.get('product') or {}
    product_gid = p.get('id', '')
    product_num = product_gid.rsplit('/', 1)[-1] if product_gid else ''
    return {
        'sku': v.get('sku'),
        'product_title': p.get('title'),
        'variant_title': v.get('title'),
        'admin_url': f"https://{SHOPIFY_STORE}/admin/products/{product_num}",
        'inventory_url': f"https://{SHOPIFY_STORE}/admin/products/{product_num}/inventory",
    }


def set_on_hand(inventory_item_id: int, location_id: str, quantity: int) -> dict:
    """Kald inventorySetOnHandQuantities for EN SKU."""
    m = """
    mutation setOnHand($input: InventorySetOnHandQuantitiesInput!) {
      inventorySetOnHandQuantities(input: $input) {
        userErrors { field message }
        inventoryAdjustmentGroup { createdAt reason }
      }
    }
    """
    vars = {
        'input': {
            'reason': 'correction',
            'referenceDocumentUri': f'logistics://boligretning/canary-{datetime.utcnow().date()}',
            'setQuantities': [{
                'inventoryItemId': f"gid://shopify/InventoryItem/{inventory_item_id}",
                'locationId': f"gid://shopify/Location/{location_id}",
                'quantity': quantity,
            }],
        }
    }
    return gql(m, vars)


def main():
    if not SHOPIFY_TOKEN:
        sys.exit("❌ SHOPIFY_ACCESS_TOKEN mangler")

    if not os.path.exists(DRYRUN_CSV):
        sys.exit(f"❌ {DRYRUN_CSV} findes ikke — koer 'python scripts/sync_inventory_v2.py' foerst")

    with open(CACHE_PATH, encoding='utf-8') as f:
        cache = json.load(f)
    location_id = cache['location_id']
    sku_to_inv = cache['inventory_items']
    print(f"📦 Cache: location={location_id}, {len(sku_to_inv)} SKUs")

    # Laes dry-run-rows
    with open(DRYRUN_CSV, encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print("✅ Dry-run var tom (ingen aendringer) — intet at validere")
        return
    print(f"📄 Dry-run CSV: {len(rows)} kandidater\n")

    # Find en SKU hvor (a) inventory_item_id eksisterer i cachen,
    # og (b) Shopifys nuvaerende vaerdi reelt afviger fra den nye
    chosen = None
    for row in rows[:30]:                          # tjek op til 30 for at finde en god kandidat
        sku = row['Variant SKU'].strip()
        new_qty = int(row['Inventory Available: Shop location'])
        inv_id = sku_to_inv.get(sku)
        if not inv_id:
            continue
        current = query_on_hand(inv_id, location_id)
        if current is None:
            continue
        if current == new_qty:
            continue                               # ville vaere no-op — find en med reel forskel
        chosen = {'sku': sku, 'inv_id': inv_id, 'new_qty': new_qty, 'current': current}
        break

    if not chosen:
        print("⚠ Kunne ikke finde en SKU hvor Shopify-vaerdi afviger fra v2-output blandt foerste 30")
        print("  Det betyder enten at Matrixify allerede har anvendt aendringerne,")
        print("  eller at alle de foerste 30 rows har samme vaerdi i forvejen.")
        return

    sku = chosen['sku']; inv_id = chosen['inv_id']
    current = chosen['current']; new_qty = chosen['new_qty']
    print(f"🎯 Valgt SKU: {sku}")
    print(f"   inventory_item_id: {inv_id}")
    print(f"   Shopify on-hand FOER: {current}")
    print(f"   v2 vil saette til:    {new_qty}\n")

    # Hent variant-info til spot-check
    info = get_variant_admin_url(inv_id)
    if info:
        print(f"📋 Product: {info['product_title']}")
        if info['variant_title'] and info['variant_title'] != 'Default Title':
            print(f"   Variant: {info['variant_title']}")
        print(f"   Admin: {info['admin_url']}")
        print(f"   Inventory: {info['inventory_url']}\n")

    # Anvend aendringen
    print("🚀 Kalder inventorySetOnHandQuantities...")
    result = set_on_hand(inv_id, location_id, new_qty)
    set_result = result['data']['inventorySetOnHandQuantities']
    if set_result['userErrors']:
        print(f"❌ FEJL — userErrors: {set_result['userErrors']}")
        sys.exit(1)
    print(f"   ✅ Mutation accepteret. inventoryAdjustmentGroup: {set_result.get('inventoryAdjustmentGroup')}")

    # Verificér via re-query
    time.sleep(2)
    after = query_on_hand(inv_id, location_id)
    print(f"\n🔬 Shopify on-hand EFTER: {after}")

    if after == new_qty:
        print(f"\n✅ END-TO-END BEKRAEFTET")
        print(f"   {current} → {after} (forventet {new_qty})")
        print(f"   Direct-API path virker for inventory.")
    else:
        print(f"\n❌ MISMATCH — forventet {new_qty}, fik {after}")
        print(f"   Noget i kaeden virker ikke som forventet. Spot-check via Admin URL.")
        sys.exit(1)


if __name__ == "__main__":
    main()
