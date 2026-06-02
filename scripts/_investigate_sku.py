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

# === 1. Shopify (rækkefølge ændret: pull denne FØRST, så vi har data selv hvis feed = 403) ===
print("【1】 SHOPIFY (current state)")
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

# === 2. Supabase pricing state ===
print("\n【2】 SUPABASE vidaxl_pricing_state (vores tracker)")
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

# === 3. VidaXL feeds — TJEK BEGGE (main + offer) ===
import time as _time
import zipfile
import io as _io

OFFER_URL = ("https://feed.vidaxl.io/api/v1/feeds/download/"
             "f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping_offer.csv")
MAIN_URL = ("https://feed.vidaxl.io/api/v1/feeds/download/"
            "f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping.csv.zip")


def _fetch_with_retry(url, label, max_attempts=6):
    print(f"\n    Henter {label}...")
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, headers=FEED_HEADERS, timeout=300)
            if r.status_code in (403, 429, 500, 502, 503, 504):
                wait = 10 * attempt
                print(f"    {r.status_code} (attempt {attempt}/{max_attempts}) — wait {wait}s")
                _time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"    fejl: {str(e)[:120]}")
            _time.sleep(10 * attempt)
    return None


def _show_row(label, df, sku):
    print(f"\n    {label}: {len(df):,} total rows")
    row = df[df['SKU'].astype(str) == sku]
    if row.empty:
        print(f"    ❌ SKU {sku} IKKE i {label}")
        return False
    r0 = row.iloc[0]
    print(f"    ✅ SKU {sku} findes i {label}")
    for c in df.columns:
        val = r0.get(c)
        if pd.notna(val) and str(val).strip():
            v_str = str(val)
            if len(v_str) > 100: v_str = v_str[:100] + '...'
            print(f"      {c}: {v_str}")
    return True


# Offer feed (stock + price — bruges af sync_inventory, sync_prices)
print("\n【3a】 OFFER FEED (vidaXL_dk_dropshipping_offer.csv) — Stock+Price kilde")
r_offer = _fetch_with_retry(OFFER_URL, "offer feed")
in_offer = False
if r_offer is not None:
    df_offer = pd.read_csv(StringIO(r_offer.text))
    in_offer = _show_row("offer feed", df_offer, SKU)
else:
    print(f"    ⚠ Offer feed ikke tilgaengelig")

# Main feed (full catalog — bruges af daily_create, daily_delete)
print("\n【3b】 MAIN FEED (vidaXL_dk_dropshipping.csv.zip) — Full catalog, source-of-truth")
r_main = _fetch_with_retry(MAIN_URL, "main feed (.zip)")
in_main = False
if r_main is not None:
    try:
        with zipfile.ZipFile(_io.BytesIO(r_main.content)) as zf:
            csv_name = next((n for n in zf.namelist() if n.endswith('.csv')), None)
            if not csv_name:
                print(f"    ⚠ Kunne ikke finde CSV i ZIP — entries: {zf.namelist()[:3]}")
            else:
                with zf.open(csv_name) as f:
                    df_main = pd.read_csv(f, sep=None, engine='python')
                    in_main = _show_row("main feed", df_main, SKU)
    except Exception as e:
        print(f"    ⚠ Kunne ikke unzip main feed: {str(e)[:200]}")
else:
    print(f"    ⚠ Main feed ikke tilgaengelig")

# === KONKLUSION ===
print(f"\n【KONKLUSION】")
if in_main and in_offer:
    print(f"    ✅ SKU {SKU} er AKTIV hos VidaXL (i begge feeds)")
elif in_offer and not in_main:
    print(f"    ⚠ SKU {SKU} er KUN i offer-feed, IKKE i main-feed")
    print(f"    → VidaXL har formentlig fjernet det fra hovedkataloget")
    print(f"    → daily_delete.py vil flagge det til sletning ved naeste koersel")
    print(f"    → BEKRAEFTER din mistanke om at det er udgaaet")
elif not in_offer and in_main:
    print(f"    ⚠ SKU {SKU} er KUN i main, IKKE i offer — mystisk, men ikke udgaaet")
else:
    print(f"    ❌ SKU {SKU} er FJERNET fra begge feeds — DEFINITIVT udgaaet")

print(f"\n══════════════════════════════════════════════════════════\n")
