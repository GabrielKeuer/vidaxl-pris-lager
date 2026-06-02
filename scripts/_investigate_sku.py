"""Dybde-tjek af en enkelt SKU.

Henter status fra 4 kilder:
  1. VidaXL feed — er SKU stadig i deres katalog? B2B-pris? Status?
  2. Shopify — current pris, compareAt, cost, status, inventory
  3. Supabase vidaxl_pricing_state — vores state-data
  4. Daily delete-script's threshold-check (hvad ville den gøre)

Brug: python scripts/_investigate_sku.py <SKU>
"""
import json
import os
import sys
from io import StringIO

import pandas as pd
import requests

SKU = sys.argv[1] if len(sys.argv) > 1 else None
if not SKU:
    sys.exit("Brug: python _investigate_sku.py <SKU>")

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE_URL') or 'b7916a-38.myshopify.com'
SHOPIFY_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json"

VIDAXL_URL = ("https://feed.vidaxl.io/api/v1/feeds/download/"
              "f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping_offer.csv")

FEED_HEADERS = {
    "User-Agent": "Mozilla/5.0", "Accept": "text/csv,*/*",
    "Accept-Language": "da-DK,da;q=0.9,en;q=0.8",
}


def gql(query, variables=None):
    r = requests.post(GRAPHQL,
                      headers={'X-Shopify-Access-Token': SHOPIFY_TOKEN,
                               'Content-Type': 'application/json'},
                      json={'query': query, 'variables': variables or {}}, timeout=60)
    r.raise_for_status()
    return r.json()


print(f"\n══════════════════════════════════════════════════════════")
print(f" SKU: {SKU}")
print(f"══════════════════════════════════════════════════════════\n")

# === 1. VidaXL feed ===
print("【1】 VidaXL FEED (kilde til alle b2b-priser)")
print("    Henter feed...")
r = requests.get(VIDAXL_URL, headers=FEED_HEADERS, timeout=180)
r.raise_for_status()
df = pd.read_csv(StringIO(r.text))
df['SKU'] = df['SKU'].astype(str)
row = df[df['SKU'] == SKU]
if row.empty:
    print(f"    ❌ SKU {SKU} FINDES IKKE i VidaXL feed lige nu")
    print(f"    → Det betyder produktet er UDGÅET fra VidaXL's katalog.")
    print(f"    → daily_delete.py vil flagge det til sletning ved næste kørsel.")
else:
    r0 = row.iloc[0]
    print(f"    ✅ SKU {SKU} findes i VidaXL feed.")
    print(f"    Article name: {r0.get('Article name')}")
    print(f"    B2B price: {r0.get('B2B price')} EUR (RAW)")
    print(f"    Stock: {r0.get('Stock')}")
    avail_keys = [c for c in df.columns if 'available' in c.lower() or 'status' in c.lower()]
    for k in avail_keys:
        print(f"    {k}: {r0.get(k)}")

# === 2. Shopify ===
print("\n【2】 SHOPIFY (current state)")
with open('output/shop_skus.json', encoding='utf-8') as f:
    cache = json.load(f)
vm = cache.get('variants', {}).get(SKU)
if not vm:
    print(f"    ❌ SKU {SKU} ikke i shop_skus.json cache — eksisterer ikke i Shopify")
else:
    var_id, prod_id = vm
    q = """
    query($id: ID!) {
      productVariant(id: $id) {
        id sku price compareAtPrice
        product { id title status onlineStoreUrl handle }
        inventoryItem { id unitCost { amount } }
      }
    }
    """
    d = gql(q, {'id': f"gid://shopify/ProductVariant/{var_id}"})
    v = d.get('data', {}).get('productVariant') or {}
    p = v.get('product') or {}
    cost = (v.get('inventoryItem') or {}).get('unitCost') or {}
    print(f"    Product title: {p.get('title')}")
    print(f"    Product status: {p.get('status')}")
    print(f"    Product handle: {p.get('handle')}")
    print(f"    Online store URL: {p.get('onlineStoreUrl')}")
    print(f"    Variant SKU: {v.get('sku')}")
    print(f"    Price: {v.get('price')} kr")
    print(f"    CompareAtPrice: {v.get('compareAtPrice')}")
    print(f"    Cost: {cost.get('amount')} kr")

# === 3. Supabase pricing state ===
print("\n【3】 SUPABASE vidaxl_pricing_state (vores tracker)")
url = os.environ.get("SUPABASE_URL"); key = os.environ.get("SUPABASE_SERVICE_KEY")
if not url or not key:
    print(f"    ⚠ SUPABASE_URL/KEY mangler — kan ikke tjekke")
else:
    from supabase import create_client
    sb = create_client(url, key)
    res = sb.table("vidaxl_pricing_state").select("*").eq("sku", SKU).execute()
    if not res.data:
        print(f"    ❌ SKU {SKU} ikke i vidaxl_pricing_state")
    else:
        s = res.data[0]
        for k in ['sku', 'pricing_group', 'status', 'b2b_cost', 'normal_price',
                  'sale_price', 'warmup_complete_at', 'last_normal_period_started_at']:
            if k in s:
                print(f"    {k}: {s[k]}")

print(f"\n══════════════════════════════════════════════════════════\n")
