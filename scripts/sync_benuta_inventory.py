#!/usr/bin/env python3
"""
Benuta Inventory Sync til Shopify
Synkroniserer lager fra Benuta CSV til Shopify produkter
"""

import os
import sys
import requests
import csv
from datetime import datetime
from io import StringIO

# Shopify configuration
SHOPIFY_STORE_URL = os.environ.get('SHOPIFY_STORE_URL')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
BENUTA_CSV_URL = 'https://get.cpexp.de/Uggcv3RwDVE9jJWsvqfzLqu4nVIul2y_W02CD3UXPTJWGqJK24Jyjfh5VKzV_lBF/benutab2b_b2bkundende.csv'

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


def download_benuta_csv():
    """Download CSV fra Benuta"""
    log("📥 Downloading Benuta CSV...")
    
    try:
        response = requests.get(BENUTA_CSV_URL, timeout=30)
        response.raise_for_status()
        log(f"✅ Downloaded CSV ({len(response.text)} bytes)")
        return response.text
    except Exception as e:
        log(f"❌ Error downloading CSV: {e}")
        sys.exit(1)


def parse_benuta_csv(csv_content):
    """Parse pipe-delimited CSV"""
    log("📋 Parsing CSV...")
    
    products = []
    csv_reader = csv.reader(StringIO(csv_content), delimiter='|')
    
    # Skip header
    next(csv_reader)
    
    for row in csv_reader:
        if len(row) >= 15:
            sku = row[14].strip()  # SKU er kolonne 15 (index 14)
            stock = row[6].strip()  # Stock er kolonne 7 (index 6)
            quantity = int(stock) if stock.isdigit() else 0
            
            if sku:  # Kun hvis SKU findes
                products.append({
                    'sku': sku,
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
    """Get inventory item IDs for SKUs in batches"""
    log(f"🔍 Fetching inventory IDs for {len(skus)} SKUs...")
    
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
    batch_size = 250  # Process 250 SKUs per query to avoid too long query strings
    total_batches = (len(skus) + batch_size - 1) // batch_size
    
    for i in range(0, len(skus), batch_size):
        batch_skus = skus[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        
        log(f"  Batch {batch_num}/{total_batches}: Searching {len(batch_skus)} SKUs...")
        
        # Build query string for this batch
        sku_queries = ' OR '.join([f'sku:"{sku}"' for sku in batch_skus])
        
        # Fetch all pages for this batch
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
    log("📊 BENUTA SYNC SUMMARY")
    log("="*50)
    log(f"Total products in CSV:     {stats['total_products']}")
    log(f"✅ Successfully updated:   {stats['updated']}")
    log(f"⚠️  Not found in Shopify:  {stats['not_found']}")
    log(f"❌ Errors:                 {stats['errors']}")
    log("="*50 + "\n")


def main():
    """Main execution"""
    log("🚀 Starting Benuta → Shopify Inventory Sync\n")
    
    # Validate environment
    if not SHOPIFY_STORE_URL or not SHOPIFY_ACCESS_TOKEN:
        log("❌ Missing environment variables!")
        sys.exit(1)
    
    try:
        # Download and parse CSV
        csv_content = download_benuta_csv()
        products = parse_benuta_csv(csv_content)
        
        # Get location
        location_id = get_location_id()
        
        # Update inventory
        update_inventory(products, location_id)
        
        # Print stats
        print_stats()
        
        log("✅ Benuta sync completed successfully!\n")
        
    except Exception as e:
        log(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
