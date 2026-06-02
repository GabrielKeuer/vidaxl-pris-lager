"""Direct-API replacement for sync_inventory.py.

Genbruger 100% af den eksisterende diff-logik fra sync_inventory.py.
Forskellen er KUN output-laget:
  - sync_inventory.py    skriver CSV → Matrixify læser → Shopify
  - sync_inventory_v2.py (denne)  kalder direkte inventorySetQuantities

Modes:
  --dry-run (default): skriver output/new_inventory_updates.csv i SAMME format
                       som det gamle script. Bruges til CSV-diff validering
                       under migration.
  --live:              kalder Shopify GraphQL direkte i batches af 100.
                       Skriver IKKE CSV (er ikke længere nødvendigt).

Begge modes skriver state/last_inventory.csv så næste run kan diff'e korrekt.

Forward-compat (Kayoom/HUB):
  - fetch_supplier_data() er adskilt. Når Kayoom kommer på SFTP, skiftes kun denne
  - CONFIG-dict øverst gør config-flytning til Supabase trivial senere
"""
import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime
from io import StringIO

import pandas as pd
import requests


CONFIG = {
    # Senere: load fra Supabase hub_settings.product_automation_inventory
    "vidaxl_feed_url": (
        "https://feed.vidaxl.io/api/v1/feeds/download/"
        "f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping_offer.csv"
    ),
    "shop_cache_path": "output/shop_skus.json",
    "state_file": "state/last_inventory.csv",
    "dry_run_csv": "output/new_inventory_updates.csv",
    "batch_size": 100,           # inventorySetQuantities tager op til 250, vi bruger 100 for sikkerhed
    "max_retries": 4,
    "request_timeout": 60,
    "csv_headers": [
        'Variant SKU',
        'Inventory Available: Shop location',
        'Variant Command',
    ],
}


SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE_URL') or 'b7916a-38.myshopify.com'
SHOPIFY_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')


# === FETCH ===========================================================
# Adskilt så Kayoom (SFTP) senere kan plugge en anden henter ind uden
# at røre transform/push-lagene.

def fetch_supplier_data() -> pd.DataFrame:
    """Hent VidaXL CSV-feed. Returner DataFrame med mindst SKU + Stock kolonner."""
    print(f"📥 Fetching VidaXL feed...")
    r = requests.get(CONFIG["vidaxl_feed_url"], timeout=CONFIG["request_timeout"])
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))
    df['SKU'] = df['SKU'].astype(str)
    print(f"✅ {len(df)} produkter fra VidaXL")
    return df


def load_shop_cache() -> dict:
    """Load shop_skus.json med skus + inventory_items + location_id."""
    with open(CONFIG["shop_cache_path"], 'r', encoding='utf-8') as f:
        data = json.load(f)
    if 'inventory_items' not in data or 'location_id' not in data:
        sys.exit(
            "❌ shop_skus.json mangler 'inventory_items' eller 'location_id'.\n"
            "   Kør update_shop_cache.py først for at populate dem."
        )
    return data


# === TRANSFORM =======================================================
# Genbruger samme logik som sync_inventory.py — beregner kun ÆNDRINGER
# siden sidste run (smart-skip via state-fil).

def compute_inventory_changes(supplier_df: pd.DataFrame, shop_skus: set, state_file: str) -> pd.DataFrame:
    """Returner DataFrame med kolonner SKU, Stock for SKUs hvor lager er ændret siden sidste run.

    Identisk diff-logik med sync_inventory.py:
      - Filter VidaXL til kun produkter der findes i shop
      - Merge mod state, find Stock-ændringer eller nye produkter
    """
    shop_products = supplier_df[supplier_df['SKU'].isin(shop_skus)].copy()
    print(f"🎯 Filtreret til {len(shop_products)} produkter i shoppen")

    if os.path.exists(state_file):
        last_state = pd.read_csv(state_file, dtype={'SKU': str})
        merged = shop_products.merge(
            last_state[['SKU', 'Stock']],
            on='SKU', how='left', suffixes=('_new', '_old')
        )
        changes = merged[
            (merged['Stock_new'] != merged['Stock_old']) | (merged['Stock_old'].isna())
        ].copy()
        changes['Stock'] = changes['Stock_new']
        return changes[['SKU', 'Stock']]
    else:
        # First run
        return shop_products[['SKU', 'Stock']].copy()


# === PUSH ============================================================
# Niveau 2: inventorySetQuantities tager array af op til 250 items pr. kald.
# Idempotent (set absolute target), ingen risiko ved re-run.

GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json"


def gql(query: str, variables: dict | None = None) -> dict:
    """GraphQL med throttle-aware retry."""
    if not SHOPIFY_TOKEN:
        sys.exit("❌ SHOPIFY_ACCESS_TOKEN mangler")
    payload = {'query': query}
    if variables:
        payload['variables'] = variables
    headers = {'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json'}
    for attempt in range(1, CONFIG["max_retries"] + 1):
        r = requests.post(GRAPHQL, headers=headers, json=payload, timeout=CONFIG["request_timeout"])
        if r.status_code != 200:
            raise Exception(f"HTTP {r.status_code}: {r.text[:300]}")
        data = r.json()
        if 'errors' in data:
            throttled = any('Throttled' in str(e) or 'THROTTLED' in str(e) for e in data['errors'])
            if throttled and attempt < CONFIG["max_retries"]:
                wait = 2 ** attempt
                print(f"  ⏳ Throttled, retry {attempt}/{CONFIG['max_retries']} in {wait}s")
                time.sleep(wait)
                continue
            raise Exception(f"GraphQL errors: {data['errors']}")
        cost = data.get('extensions', {}).get('cost', {}).get('throttleStatus', {})
        if cost.get('currentlyAvailable', 1000) < 200:
            time.sleep(0.5)
        return data
    raise Exception("Max retries exceeded")


def push_to_shopify(changes: pd.DataFrame, location_id: str, sku_to_inv: dict) -> dict:
    """Kald inventorySetQuantities i batches. Returner stats-dict."""
    print(f"🚀 Pushing {len(changes)} inventory updates til Shopify (location {location_id})")
    stats = {'updated': 0, 'skipped_no_inv_id': 0, 'errors': 0}

    # Byg quantities-array; skip SKUs uden mapping
    quantities = []
    for _, row in changes.iterrows():
        sku = row['SKU']
        inv_id = sku_to_inv.get(sku)
        if not inv_id:
            stats['skipped_no_inv_id'] += 1
            continue
        try:
            qty = int(row['Stock']) if pd.notna(row['Stock']) else 0
        except (ValueError, TypeError):
            qty = 0
        quantities.append({
            'inventoryItemId': f"gid://shopify/InventoryItem/{inv_id}",
            'locationId': f"gid://shopify/Location/{location_id}",
            'quantity': qty,
        })

    # Send i batches
    mutation = """
    mutation setOnHand($input: InventorySetOnHandQuantitiesInput!) {
      inventorySetOnHandQuantities(input: $input) {
        userErrors { field message }
        inventoryAdjustmentGroup { createdAt reason }
      }
    }
    """
    batch_size = CONFIG["batch_size"]
    total_batches = (len(quantities) + batch_size - 1) // batch_size
    for i in range(0, len(quantities), batch_size):
        batch = quantities[i:i + batch_size]
        batch_num = i // batch_size + 1
        input_var = {
            'reason': 'correction',
            'referenceDocumentUri': f'logistics://boligretning/vidaxl-sync/{datetime.utcnow().date()}',
            'setQuantities': batch,
        }
        try:
            data = gql(mutation, {'input': input_var})
            errs = data.get('data', {}).get('inventorySetOnHandQuantities', {}).get('userErrors', [])
            if errs:
                stats['errors'] += len(errs)
                print(f"  ⚠ Batch {batch_num}/{total_batches}: {len(errs)} userErrors — første: {errs[0]}")
            else:
                stats['updated'] += len(batch)
                if batch_num % 10 == 0 or batch_num == total_batches:
                    print(f"  ✅ Batch {batch_num}/{total_batches} OK ({stats['updated']} cumulative)")
        except Exception as e:
            stats['errors'] += len(batch)
            print(f"  ❌ Batch {batch_num}/{total_batches} fejlede: {str(e)[:200]}")

    return stats


# === OUTPUT WRITERS ==================================================

def write_dry_run_csv(changes: pd.DataFrame, path: str):
    """Skriv samme format som sync_inventory.py producerer, så CSV-diff bliver tom ved match."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CONFIG["csv_headers"])
        writer.writeheader()
        for _, row in changes.iterrows():
            writer.writerow({
                'Variant SKU': row['SKU'],
                'Inventory Available: Shop location': row['Stock'],
                'Variant Command': 'UPDATE',
            })
    print(f"📄 Dry-run CSV: {path} ({len(changes)} rækker)")


def save_state(supplier_df: pd.DataFrame, shop_skus: set, state_file: str):
    """Gem state ENS med sync_inventory.py (samme rækkefølge + format)."""
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    shop_products = supplier_df[supplier_df['SKU'].isin(shop_skus)]
    shop_products[['SKU', 'Stock']].to_csv(state_file, index=False)
    print(f"💾 State gemt → {state_file}")


# === MAIN ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--live', action='store_true',
                        help="Kald Shopify GraphQL direkte. Default er dry-run der skriver CSV til sammenligning.")
    parser.add_argument('--skip-state-save', action='store_true',
                        help="Spring state-save over (vigtigt under dry-run så vi ikke ødelægger gammelt scripts state-source)")
    args = parser.parse_args()

    mode = "LIVE (kalder Shopify direkte)" if args.live else "DRY-RUN (skriver CSV til diff)"
    print(f"🚀 sync_inventory_v2 — {mode}")

    cache = load_shop_cache()
    shop_skus = set(cache['skus'])
    sku_to_inv = cache['inventory_items']
    location_id = cache['location_id']
    print(f"📦 Cache: {len(shop_skus)} SKUs, {len(sku_to_inv)} m. inventory_item_id, location={location_id}")

    supplier_df = fetch_supplier_data()
    changes = compute_inventory_changes(supplier_df, shop_skus, CONFIG["state_file"])
    print(f"📝 {len(changes)} inventory-ændringer fundet")

    if args.live:
        stats = push_to_shopify(changes, location_id, sku_to_inv)
        print(f"\n📊 STATS: updated={stats['updated']}, skipped_no_inv_id={stats['skipped_no_inv_id']}, errors={stats['errors']}")
        if stats['errors']:
            sys.exit(1)
    else:
        write_dry_run_csv(changes, CONFIG["dry_run_csv"])

    if not args.skip_state_save:
        save_state(supplier_df, shop_skus, CONFIG["state_file"])

    print("✅ Færdig")


if __name__ == "__main__":
    main()
