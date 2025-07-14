#!/usr/bin/env python3
"""
Sync inventory from VidaXL to Shopify via Matrixify CSV
Compares current Shopify inventory with VidaXL and outputs differences
"""

import csv
import requests
import pandas as pd
from datetime import datetime
from io import StringIO
import os
import time
from typing import Dict, List, Set

# Configuration
SHOPIFY_STORE = os.getenv('SHOPIFY_STORE_URL', 'boligretning.myshopify.com')
SHOPIFY_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')
LOCATION_ID = 97768178013  # Shop location
VIDAXL_URL = "https://transport.productsup.io/de8254c69e698a08e904/channel/188044/vidaXL_dk_dropshipping.csv"

# Test mode - set to False for full sync
TEST_MODE = True
TEST_LIMIT = 10

def fetch_shopify_inventory() -> Dict[str, int]:
    """Fetch all inventory levels from Shopify for specific location"""
    print(f"🔍 Fetching inventory from Shopify location {LOCATION_ID}...")
    
    inventory_by_sku = {}
    cursor = None
    total_fetched = 0
    
    while True:
        # GraphQL query for inventory - RETTET VERSION
        query = """
        query getInventory($cursor: String, $locationId: ID!) {
            location(id: $locationId) {
                inventoryLevels(first: 250, after: $cursor) {
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    edges {
                        node {
                            id
                            quantities(names: ["available"]) {
                                name
                                quantity
                            }
                            item {
                                sku
                                variant {
                                    sku
                                    id
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        
        variables = {
            "locationId": f"gid://shopify/Location/{LOCATION_ID}",
            "cursor": cursor
        }
        
        response = requests.post(
            f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json",
            json={"query": query, "variables": variables},
            headers={
                "X-Shopify-Access-Token": SHOPIFY_TOKEN,
                "Content-Type": "application/json"
            }
        )
        
        if response.status_code != 200:
            print(f"❌ Error fetching inventory: {response.status_code}")
            print(response.text)
            break
            
        data = response.json()
        
        if 'errors' in data:
            print(f"❌ GraphQL errors: {data['errors']}")
            break
            
        inventory_levels = data['data']['location']['inventoryLevels']
        
        for edge in inventory_levels['edges']:
            node = edge['node']
            
            # Få SKU fra enten item.sku eller item.variant.sku
            sku = None
            if node['item']:
                if node['item']['sku']:
                    sku = node['item']['sku']
                elif node['item']['variant'] and node['item']['variant']['sku']:
                    sku = node['item']['variant']['sku']
            
            # Få available quantity fra quantities array
            available_qty = 0
            if 'quantities' in node and node['quantities']:
                for qty in node['quantities']:
                    if qty['name'] == 'available':
                        available_qty = qty['quantity']
                        break
            
            if sku:
                inventory_by_sku[sku] = available_qty
                total_fetched += 1
        
        print(f"   Fetched {total_fetched} inventory levels...")
        
        if not inventory_levels['pageInfo']['hasNextPage']:
            break
            
        cursor = inventory_levels['pageInfo']['endCursor']
        time.sleep(0.5)  # Rate limiting
        
        if TEST_MODE and total_fetched >= TEST_LIMIT:
            print(f"   TEST MODE: Stopping at {TEST_LIMIT} products")
            break
    
    print(f"✅ Fetched inventory for {len(inventory_by_sku)} SKUs")
    return inventory_by_sku

def fetch_vidaxl_inventory() -> pd.DataFrame:
    """Fetch VidaXL inventory from CSV"""
    print("📥 Fetching VidaXL inventory...")
    
    try:
        response = requests.get(VIDAXL_URL)
        response.raise_for_status()
        
        # Read CSV with proper encoding
        vidaxl_data = pd.read_csv(
            StringIO(response.text),
            dtype={'SKU': str, 'Stock': int}
        )
        
        print(f"✅ Loaded {len(vidaxl_data)} products from VidaXL")
        return vidaxl_data
        
    except Exception as e:
        print(f"❌ Failed to fetch VidaXL data: {e}")
        raise

def compare_inventory(shopify_inv: Dict[str, int], vidaxl_df: pd.DataFrame) -> List[Dict]:
    """Compare Shopify and VidaXL inventory, return differences"""
    print("🔄 Comparing inventory...")
    
    differences = []
    checked = 0
    
    # Convert VidaXL data to dict for faster lookup
    vidaxl_inv = dict(zip(vidaxl_df['SKU'].astype(str), vidaxl_df['Stock']))
    
    # ONLY compare SKUs that exist in BOTH systems
    shopify_skus = set(shopify_inv.keys())
    vidaxl_skus = set(vidaxl_inv.keys())
    common_skus = shopify_skus & vidaxl_skus  # Intersection - kun fælles SKUs
    
    print(f"   Shopify SKUs: {len(shopify_skus)}")
    print(f"   VidaXL SKUs: {len(vidaxl_skus)}")
    print(f"   Common SKUs: {len(common_skus)}")
    
    for sku in common_skus:
        shopify_qty = shopify_inv[sku]
        vidaxl_qty = vidaxl_inv[sku]
        
        # Only include if quantities are different
        if shopify_qty != vidaxl_qty:
            differences.append({
                'sku': sku,
                'shopify_qty': shopify_qty,
                'vidaxl_qty': vidaxl_qty,
                'difference': vidaxl_qty - shopify_qty
            })
        
        checked += 1
        if checked % 1000 == 0:
            print(f"   Checked {checked} SKUs...")
            
        if TEST_MODE and len(differences) >= TEST_LIMIT:
            print(f"   TEST MODE: Stopping at {TEST_LIMIT} differences")
            break
    
    print(f"✅ Found {len(differences)} inventory differences (out of {len(common_skus)} common SKUs)")
    return differences

def generate_matrixify_csv(differences: List[Dict], output_file: str):
    """Generate Matrixify-compatible CSV"""
    print(f"📝 Generating Matrixify CSV...")
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Matrixify header
        writer.writerow([
            'Variant SKU',
            'Inventory Available: Shop location',
            'Variant Command'
        ])
        
        # Write differences
        for diff in differences:
            writer.writerow([
                diff['sku'],
                diff['vidaxl_qty'],  # New quantity from VidaXL
                'UPDATE'
            ])
    
    print(f"✅ Written {len(differences)} updates to {output_file}")
    
    # Also save detailed report
    report_file = output_file.replace('.csv', '_report.csv')
    with open(report_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'sku', 'shopify_qty', 'vidaxl_qty', 'difference'
        ])
        writer.writeheader()
        writer.writerows(differences)
    
    print(f"📊 Detailed report saved to {report_file}")

def main():
    """Main sync process"""
    print(f"🚀 Starting Inventory Sync - {datetime.now()}")
    print(f"📍 Location ID: {LOCATION_ID}")
    print(f"🧪 TEST MODE: {'ON' if TEST_MODE else 'OFF'}")
    
    if not SHOPIFY_TOKEN:
        print("❌ SHOPIFY_ACCESS_TOKEN not set!")
        return
    
    try:
        # Step 1: Fetch Shopify inventory
        shopify_inventory = fetch_shopify_inventory()
        
        # Step 2: Fetch VidaXL inventory
        vidaxl_inventory = fetch_vidaxl_inventory()
        
        # Step 3: Compare inventories
        differences = compare_inventory(shopify_inventory, vidaxl_inventory)
        
        # Step 4: Generate output
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'output/matrixify_inventory_update_{timestamp}.csv'
        
        if differences:
            generate_matrixify_csv(differences, output_file)
            
            # Summary statistics
            total_to_add = sum(d['difference'] for d in differences if d['difference'] > 0)
            total_to_remove = sum(d['difference'] for d in differences if d['difference'] < 0)
            
            print("\n📊 Summary:")
            print(f"   Products to update: {len(differences)}")
            print(f"   Total units to add: {total_to_add}")
            print(f"   Total units to remove: {abs(total_to_remove)}")
            print(f"\n✅ Ready for Matrixify import: {output_file}")
        else:
            print("\n✅ No inventory differences found - all in sync!")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()
