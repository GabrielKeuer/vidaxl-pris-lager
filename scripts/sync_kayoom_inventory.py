#!/usr/bin/env python3
"""
Kayoom Inventory Sync til Shopify
Henter den daglige stock-CSV fra Kayoom FTP (/stocks/Kayoomstock_*.csv) og
synkroniserer lager til Shopify. Samme downstream som Benuta/Sollux —
kun "fetch"-laget er FTP i stedet for HTTPS.

CSV-format: semikolon-separeret, header "sku;stock". SKU matcher Shopify variant-SKU 1:1.
"""

import os
import sys
import time
import csv
import io
import ftplib
from datetime import datetime
import requests

SHOPIFY_STORE_URL = os.environ.get('SHOPIFY_STORE_URL')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
FTP_HOST = os.environ.get('KAYOOM_FTP_HOST', 'kayoom-dropshipping.com')
FTP_PORT = int(os.environ.get('KAYOOM_FTP_PORT', '64721'))
FTP_USER = os.environ.get('KAYOOM_FTP_USER')
FTP_PASS = os.environ.get('KAYOOM_FTP_PASSWORD')

GRAPHQL_URL = f"https://{SHOPIFY_STORE_URL}/admin/api/2024-01/graphql.json"

stats = {'total_products': 0, 'updated': 0, 'not_found': 0, 'errors': 0}


def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")


def download_kayoom_csv():
    """Hent nyeste Kayoomstock_*.csv fra FTP med retry (3 forsøg, backoff 60s/120s)."""
    log("📥 Forbinder til Kayoom FTP...")
    last_err = None
    for attempt in range(1, 4):
        try:
            ftp = ftplib.FTP()
            ftp.connect(FTP_HOST, FTP_PORT, timeout=60)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd('stocks')
            files = [f for f in ftp.nlst() if f.lower().startswith('kayoomstock') and f.lower().endswith('.csv')]
            if not files:
                ftp.quit()
                raise RuntimeError("Ingen Kayoomstock-fil i /stocks")
            # Filnavnet indeholder timestamp (Kayoomstock_YYYYMMDDHHMM) -> leksikografisk = nyeste
            latest = sorted(files)[-1]
            log(f"📄 Henter {latest}")
            buf = io.BytesIO()
            ftp.retrbinary(f"RETR {latest}", buf.write)
            ftp.quit()
            text = buf.getvalue().decode('utf-8-sig')
            log(f"✅ Downloadet ({len(text)} bytes, forsøg {attempt}/3)")
            return text
        except Exception as e:
            last_err = e
            if attempt < 3:
                wait = 60 * attempt
                log(f"⚠️  Forsøg {attempt}/3 fejlede ({type(e).__name__}). Prøver igen om {wait}s...")
                time.sleep(wait)
    log(f"❌ Kunne ikke hente fra FTP efter 3 forsøg. Sidste fejl: {last_err}")
    sys.exit(1)


def parse_csv(text):
    """Parse semikolon-CSV (header sku;stock)."""
    log("📋 Parser CSV...")
    products = []
    reader = csv.reader(io.StringIO(text), delimiter=';')
    next(reader, None)  # spring header over
    for row in reader:
        if len(row) >= 2 and row[0].strip():
            st = row[1].strip()
            qty = int(st) if st.lstrip('-').isdigit() else 0
            products.append({'sku': row[0].strip(), 'quantity': max(0, qty)})
    log(f"✅ Parsed {len(products)} produkter")
    stats['total_products'] = len(products)
    return products


def execute_graphql(query, variables=None):
    headers = {'Content-Type': 'application/json', 'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN}
    payload = {'query': query}
    if variables:
        payload['variables'] = variables
    response = requests.post(GRAPHQL_URL, json=payload, headers=headers, timeout=60)
    response.raise_for_status()
    data = response.json()
    if 'errors' in data:
        raise Exception(f"GraphQL errors: {data['errors']}")
    return data['data']


def get_location_id():
    log("📍 Henter location ID...")
    query = """
    query { locations(first: 10) { edges { node { id name } } } }
    """
    data = execute_graphql(query)
    for edge in data['locations']['edges']:
        if edge['node']['name'] == 'Shop location':
            log(f"✅ Bruger location: {edge['node']['name']}")
            return edge['node']['id']
    location = data['locations']['edges'][0]['node']
    log(f"⚠️  'Shop location' ikke fundet, bruger: {location['name']}")
    return location['id']


def get_inventory_item_ids(skus):
    log(f"🔍 Henter inventory IDs for {len(skus)} SKUs...")
    query = """
    query getInventoryItems($query: String!) {
      productVariants(first: 250, query: $query) {
        edges { node { sku inventoryItem { id } } }
        pageInfo { hasNextPage endCursor }
      }
    }
    """
    mapping = {}
    batch_size = 250
    total_batches = (len(skus) + batch_size - 1) // batch_size
    for i in range(0, len(skus), batch_size):
        batch_skus = skus[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        log(f"  Batch {batch_num}/{total_batches}: søger {len(batch_skus)} SKUs...")
        sku_queries = ' OR '.join([f'sku:"{sku}"' for sku in batch_skus])
        has_next = True
        cursor = None
        while has_next:
            variables = {'query': sku_queries}
            if cursor:
                q = query.replace('first: 250', f'first: 250, after: "{cursor}"')
                data = execute_graphql(q, variables)
            else:
                data = execute_graphql(query, variables)
            for edge in data['productVariants']['edges']:
                node = edge['node']
                if node['sku'] and node['inventoryItem']:
                    mapping[node['sku']] = node['inventoryItem']['id']
            has_next = data['productVariants']['pageInfo']['hasNextPage']
            cursor = data['productVariants']['pageInfo'].get('endCursor')
    log(f"✅ Fandt {len(mapping)} produkter i Shopify")
    return mapping


def update_inventory(products, location_id):
    log("🔄 Opdaterer lager...")
    skus = [p['sku'] for p in products]
    inventory_mapping = get_inventory_item_ids(skus)
    updates = []
    for product in products:
        inv_id = inventory_mapping.get(product['sku'])
        if not inv_id:
            stats['not_found'] += 1
            continue
        updates.append({'inventory_item_id': inv_id, 'quantity': product['quantity']})
    batch_size = 100
    total_batches = (len(updates) + batch_size - 1) // batch_size
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        log(f"📦 Batch {batch_num}/{total_batches} ({len(batch)} items)...")
        try:
            update_batch(batch, location_id)
            stats['updated'] += len(batch)
        except Exception as e:
            log(f"❌ Batch {batch_num} fejlede: {e}")
            stats['errors'] += len(batch)


def update_batch(batch, location_id):
    mutation = """
    mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) {
      inventorySetOnHandQuantities(input: $input) {
        userErrors { field message }
        inventoryAdjustmentGroup { createdAt reason }
      }
    }
    """
    set_quantities = [
        {'inventoryItemId': item['inventory_item_id'], 'locationId': location_id, 'quantity': item['quantity']}
        for item in batch
    ]
    variables = {'input': {'reason': 'correction', 'setQuantities': set_quantities}}
    data = execute_graphql(mutation, variables)
    if data['inventorySetOnHandQuantities']['userErrors']:
        raise Exception(f"Update errors: {data['inventorySetOnHandQuantities']['userErrors']}")


def print_stats():
    log("\n" + "=" * 50)
    log("📊 KAYOOM SYNC SUMMARY")
    log("=" * 50)
    log(f"Produkter i CSV:           {stats['total_products']}")
    log(f"✅ Opdateret:              {stats['updated']}")
    log(f"⚠️  Ikke i Shopify:        {stats['not_found']}")
    log(f"❌ Fejl:                   {stats['errors']}")
    log("=" * 50 + "\n")


def main():
    log("🚀 Starting Kayoom → Shopify Inventory Sync\n")
    if not SHOPIFY_STORE_URL or not SHOPIFY_ACCESS_TOKEN:
        log("❌ Mangler Shopify-env (SHOPIFY_STORE_URL / SHOPIFY_ACCESS_TOKEN)")
        sys.exit(1)
    if not FTP_USER or not FTP_PASS:
        log("❌ Mangler FTP-credentials (KAYOOM_FTP_USER / KAYOOM_FTP_PASSWORD)")
        sys.exit(1)
    try:
        csv_content = download_kayoom_csv()
        products = parse_csv(csv_content)
        location_id = get_location_id()
        update_inventory(products, location_id)
        print_stats()
        log("✅ Kayoom sync completed!\n")
    except Exception as e:
        log(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
