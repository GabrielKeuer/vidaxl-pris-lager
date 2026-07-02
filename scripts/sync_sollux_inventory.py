#!/usr/bin/env python3
"""
Sollux Inventory Sync til Shopify
Synkroniserer lager fra Sollux CSV til Shopify produkter
"""

import os
import sys
import time
import random
import requests
import csv
from datetime import datetime
from io import StringIO

# Shopify configuration
SHOPIFY_STORE_URL = os.environ.get('SHOPIFY_STORE_URL')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
SOLLUX_CSV_URL = 'https://apps.sollux-lighting.com/stock/products_availability.csv'

# Supabase Edge Function-proxy — fallback naar Sollux' firewall blokerer
# GitHub-runnerens IP (rodaarsag verificeret 2026-06-09: IP-blok af dele af
# Azure-intervallerne, ikke kvote/UA — retries fra SAMME runner-IP hjaelper
# derfor ikke). Proxyen (functions/v1/sollux-stock-proxy) henter CSV'en fra
# Supabase-infrastrukturens IP i stedet (verificeret naaelig 2026-07-02:
# 200 OK, 45 KB, ~1s) og kraever service-key som Bearer-token.
SUPABASE_URL = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')
SOLLUX_PROXY_URL = (f"{SUPABASE_URL}/functions/v1/sollux-stock-proxy"
                    if SUPABASE_URL else None)

# GraphQL endpoint
GRAPHQL_URL = f"https://{SHOPIFY_STORE_URL}/admin/api/2024-01/graphql.json"

# Statistics
stats = {
    'total_products': 0,
    'updated': 0,
    'not_found': 0,
    'errors': 0
}


def log(message):
    """Print timestamped log message"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")


# Retry-konfiguration for Sollux-download
# 3 forsoeg pr. kilde (direkte + proxy) — retries fra samme runner-IP slaar
# alligevel ikke en IP-blok, saa vi skifter hurtigere til proxyen i stedet
# for at braende 7 min af paa 6 direkte forsoeg.
ATTEMPTS_PER_SOURCE = 3
# (connect, read): fejl hurtigt på connect (15s) frem for at sidde fast i 60s,
# men giv serveren god tid til at levere selve CSV'en (120s).
REQUEST_TIMEOUT = (15, 120)
# Browser-agtig User-Agent — nogle WAF'er afviser default python-requests-UA.
REQUEST_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/124.0.0.0 Safari/537.36'),
    'Accept': 'text/csv,*/*',
}


def download_sollux_csv():
    """Download CSV — direkte fra Sollux foerst, Supabase-proxy som fallback.

    Sollux' firewall blokerer dele af GitHub-runnernes IP-intervaller
    (ConnectTimeout paa ~25% af koerslerne; jaevnt spredt over doegnet, ikke
    kvote/UA). Retries fra samme runner hjaelper kun mod korte blip — ved en
    IP-blok er eneste redning en anden afsender-IP. Derfor: 3 direkte forsoeg,
    derefter 3 forsoeg via sollux-stock-proxy (Supabase Edge Function), som
    henter fra Supabase-infrastrukturens IP.
    """
    log("📥 Downloading Sollux CSV...")
    sources = [('direkte', SOLLUX_CSV_URL, REQUEST_HEADERS)]
    if SOLLUX_PROXY_URL and SUPABASE_SERVICE_KEY:
        sources.append(('proxy', SOLLUX_PROXY_URL,
                        {'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
                         'Accept': 'text/csv,*/*'}))
    else:
        log("⚠️  SUPABASE_URL/SUPABASE_SERVICE_KEY ikke sat — kun direkte forsøg")

    last_err = None
    for src_label, url, headers in sources:
        for attempt in range(1, ATTEMPTS_PER_SOURCE + 1):
            try:
                response = requests.get(url, timeout=REQUEST_TIMEOUT,
                                        headers=headers)
                response.raise_for_status()
                body = response.text
                # Sanity-check: proxy/WAF-fejlsider maa ikke parses som lager
                if not body.lstrip().upper().startswith('SKU'):
                    raise requests.exceptions.HTTPError(
                        f"Uventet indhold fra {src_label}: {body[:60]!r}")
                log(f"✅ Downloaded CSV via {src_label} ({len(body)} bytes, "
                    f"attempt {attempt}/{ATTEMPTS_PER_SOURCE})")
                return body
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.HTTPError) as e:
                last_err = e
                err_type = type(e).__name__
                if attempt < ATTEMPTS_PER_SOURCE:
                    # Backoff (20, 35s) + jitter — kort, for naeste kilde venter
                    wait = 20 + 15 * (attempt - 1) + random.uniform(0, 10)
                    log(f"⚠️  {src_label} attempt {attempt}/{ATTEMPTS_PER_SOURCE} "
                        f"failed ({err_type}). Retrying in {wait:.0f}s...")
                    time.sleep(wait)
                else:
                    log(f"⚠️  {src_label}: alle {ATTEMPTS_PER_SOURCE} forsøg "
                        f"fejlede ({err_type}): {str(e)[:160]}")

    log(f"❌ Could not download Sollux CSV (direkte + proxy). "
        f"Last error: {type(last_err).__name__}: {str(last_err)[:160]}")
    sys.exit(1)


def parse_sollux_csv(csv_content):
    """Parse semicolon-delimited CSV"""
    log("📋 Parsing CSV...")
    
    products = []
    csv_reader = csv.reader(StringIO(csv_content), delimiter=';')
    
    # Skip header
    next(csv_reader)
    
    for row in csv_reader:
        if len(row) >= 3:
            sku = row[0].strip()
            ean = row[1].strip()
            quantity = int(row[2]) if row[2].strip().isdigit() else 0
            
            products.append({
                'sku': sku,
                'ean': ean,
                'quantity': quantity
            })
    
    log(f"✅ Parsed {len(products)} products")
    stats['total_products'] = len(products)
    return products


def execute_graphql(query, variables=None):
    """Execute GraphQL query"""
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
    }
    
    payload = {'query': query}
    if variables:
        payload['variables'] = variables
    
    response = requests.post(GRAPHQL_URL, json=payload, headers=headers)
    response.raise_for_status()
    
    data = response.json()
    
    if 'errors' in data:
        raise Exception(f"GraphQL errors: {data['errors']}")
    
    return data['data']


def get_location_id():
    """Get Shop location ID"""
    log("📍 Getting location ID...")
    
    query = """
    query {
      locations(first: 10) {
        edges {
          node {
            id
            name
          }
        }
      }
    }
    """
    
    data = execute_graphql(query)
    
    # Find "Shop location"
    for edge in data['locations']['edges']:
        if edge['node']['name'] == 'Shop location':
            log(f"✅ Using location: {edge['node']['name']}")
            return edge['node']['id']
    
    # Fallback to first location if Shop location not found
    location = data['locations']['edges'][0]['node']
    log(f"⚠️  'Shop location' not found, using: {location['name']}")
    return location['id']


def get_inventory_item_ids(skus):
    """Get inventory item IDs for SKUs"""
    log(f"🔍 Fetching inventory IDs for {len(skus)} SKUs...")
    
    # Build query string
    sku_queries = ' OR '.join([f'sku:"{sku}"' for sku in skus])
    
    query = """
    query getInventoryItems($query: String!) {
      productVariants(first: 250, query: $query) {
        edges {
          node {
            sku
            inventoryItem {
              id
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """
    
    mapping = {}
    has_next = True
    cursor = None
    
    while has_next:
        variables = {'query': sku_queries}
        if cursor:
            query_with_cursor = query.replace('first: 250', f'first: 250, after: "{cursor}"')
            data = execute_graphql(query_with_cursor, variables)
        else:
            data = execute_graphql(query, variables)
        
        for edge in data['productVariants']['edges']:
            node = edge['node']
            if node['sku'] and node['inventoryItem']:
                mapping[node['sku']] = node['inventoryItem']['id']
        
        has_next = data['productVariants']['pageInfo']['hasNextPage']
        cursor = data['productVariants']['pageInfo'].get('endCursor')
        
        if has_next:
            log(f"  Fetching more... ({len(mapping)} found so far)")
    
    log(f"✅ Found {len(mapping)} products in Shopify")
    return mapping


def update_inventory(products, location_id):
    """Update inventory in batches"""
    log("🔄 Updating inventory...")
    
    # Get all SKUs
    skus = [p['sku'] for p in products]
    
    # Get inventory item IDs
    inventory_mapping = get_inventory_item_ids(skus)
    
    # Prepare updates
    updates = []
    for product in products:
        inventory_item_id = inventory_mapping.get(product['sku'])
        
        if not inventory_item_id:
            log(f"⚠️  SKU not found: {product['sku']}")
            stats['not_found'] += 1
            continue
        
        updates.append({
            'sku': product['sku'],
            'inventory_item_id': inventory_item_id,
            'quantity': product['quantity']
        })
    
    # Update in batches of 100
    batch_size = 100
    total_batches = (len(updates) + batch_size - 1) // batch_size
    
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        
        log(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} items)...")
        
        try:
            update_batch(batch, location_id)
            stats['updated'] += len(batch)
            log(f"✅ Batch {batch_num} completed")
        except Exception as e:
            log(f"❌ Batch {batch_num} failed: {e}")
            stats['errors'] += len(batch)


def update_batch(batch, location_id):
    """Update a batch of inventory items"""
    mutation = """
    mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) {
      inventorySetOnHandQuantities(input: $input) {
        userErrors {
          field
          message
        }
        inventoryAdjustmentGroup {
          createdAt
          reason
        }
      }
    }
    """
    
    set_quantities = [
        {
            'inventoryItemId': item['inventory_item_id'],
            'locationId': location_id,
            'quantity': item['quantity']
        }
        for item in batch
    ]
    
    variables = {
        'input': {
            'reason': 'correction',
            'setQuantities': set_quantities
        }
    }
    
    data = execute_graphql(mutation, variables)
    
    if data['inventorySetOnHandQuantities']['userErrors']:
        errors = data['inventorySetOnHandQuantities']['userErrors']
        raise Exception(f"Update errors: {errors}")


def print_stats():
    """Print final statistics"""
    log("\n" + "="*50)
    log("📊 SOLLUX SYNC SUMMARY")
    log("="*50)
    log(f"Total products in CSV:     {stats['total_products']}")
    log(f"✅ Successfully updated:   {stats['updated']}")
    log(f"⚠️  Not found in Shopify:  {stats['not_found']}")
    log(f"❌ Errors:                 {stats['errors']}")
    log("="*50 + "\n")


def main():
    """Main execution"""
    log("🚀 Starting Sollux → Shopify Inventory Sync\n")
    
    # Validate environment
    if not SHOPIFY_STORE_URL or not SHOPIFY_ACCESS_TOKEN:
        log("❌ Missing environment variables!")
        sys.exit(1)
    
    try:
        # Download and parse CSV
        csv_content = download_sollux_csv()
        products = parse_sollux_csv(csv_content)
        
        # Get location
        location_id = get_location_id()
        
        # Update inventory
        update_inventory(products, location_id)
        
        # Print stats
        print_stats()
        
        log("✅ Sollux sync completed successfully!\n")
        
    except Exception as e:
        log(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
