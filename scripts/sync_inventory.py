import csv
import requests
import pandas as pd
from datetime import datetime
from io import StringIO
import os
import json

VIDAXL_URL = "https://transport.productsup.io/de8254c69e698a08e904/channel/188044/vidaXL_dk_dropshipping.csv"

def load_shop_skus():
    """Load SKUs fra cache"""
    try:
        with open('output/shop_skus.json', 'r') as f:
            data = json.load(f)
            return set(str(sku) for sku in data['skus'])
    except:
        print("❌ Could not load shop SKUs")
        return set()

def main():
    print(f"🚀 Starting Inventory Sync - {datetime.now()}")
    
    # Load shop SKUs
    shop_skus = load_shop_skus()
    if not shop_skus:
        print("❌ No shop SKUs found - exiting")
        exit(1)
    print(f"✅ Loaded {len(shop_skus)} shop SKUs")
    
    # Fetch VidaXL data
    try:
        response = requests.get(VIDAXL_URL)
        response.raise_for_status()
        vidaxl_data = pd.read_csv(StringIO(response.text))
        print(f"✅ Loaded {len(vidaxl_data)} products from VidaXL")
    except Exception as e:
        print(f"❌ Failed to fetch VidaXL data: {e}")
        exit(1)
    
    # Filter to only shop products
    vidaxl_data['SKU'] = vidaxl_data['SKU'].astype(str)
    shop_products = vidaxl_data[vidaxl_data['SKU'].isin(shop_skus)].copy()
    print(f"🎯 Filtered to {len(shop_products)} products in shop")
    
    # Load last state
    os.makedirs('state', exist_ok=True)
    state_file = 'state/last_inventory.csv'
    
    if os.path.exists(state_file):
        last_state = pd.read_csv(state_file, dtype={'SKU': str})
        
        # Find changes
        merged = shop_products.merge(
            last_state[['SKU', 'Stock']],
            on='SKU',
            how='left',
            suffixes=('_new', '_old')
        )
        
        # Products with stock changes or new products
        changes = merged[
            (merged['Stock_new'] != merged['Stock_old']) | 
            (merged['Stock_old'].isna())
        ].copy()
    else:
        # First run - all products are "changes"
        changes = shop_products.copy()
        changes['Stock_new'] = changes['Stock']
    
    # Create output
    os.makedirs('output', exist_ok=True)
    output_rows = []
    
    if len(changes) > 0:
        print(f"📝 Found {len(changes)} inventory changes")
        for _, row in changes.iterrows():
            output_rows.append({
                'Variant SKU': row['SKU'],
                'Variant Inventory Qty': row.get('Stock_new', row.get('Stock')),
                'Variant Command': 'UPDATE'
            })
    
    # Write output (even if empty)
    with open('output/inventory_updates.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'Variant SKU', 'Variant Inventory Qty', 'Variant Command'
        ])
        writer.writeheader()
        writer.writerows(output_rows)
    
    print(f"✅ Written {len(output_rows)} updates to output/inventory_updates.csv")
    
    # Save current state
    shop_products[['SKU', 'Stock']].to_csv(state_file, index=False)
    print("💾 State saved")

if __name__ == "__main__":
    main()
