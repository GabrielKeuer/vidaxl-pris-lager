import csv
import requests
import pandas as pd
from datetime import datetime
from io import StringIO
import os
import json

VIDAXL_URL = "https://transport.productsup.io/de8254c69e698a08e904/channel/188044/vidaXL_dk_dropshipping.csv"
PRICE_MARKUP = 1.60

def calculate_retail_price(b2b_price):
    """Beregn retail pris med markup og afrunding"""
    try:
        import math
        price = float(b2b_price) * PRICE_MARKUP
        return int(10 * math.ceil(price / 10) - 1)
    except:
        return 0

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
    print(f"🚀 Starting Price Sync - {datetime.now()}")
    
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
    
    # Calculate retail prices
    shop_products['Retail_Price'] = shop_products['B2B price'].apply(calculate_retail_price)
    
    # Load last state
    os.makedirs('state', exist_ok=True)
    state_file = 'state/last_prices.csv'
    
    if os.path.exists(state_file):
        last_state = pd.read_csv(state_file, dtype={'SKU': str})
        
        # Find changes
        merged = shop_products.merge(
            last_state[['SKU', 'Retail_Price']],
            on='SKU',
            how='left',
            suffixes=('_new', '_old')
        )
        
        # Products with price changes or new products
        changes = merged[
            (merged['Retail_Price_new'] != merged['Retail_Price_old']) | 
            (merged['Retail_Price_old'].isna())
        ].copy()
    else:
        # First run - all products are "changes"
        changes = shop_products.copy()
        changes['Retail_Price_new'] = changes['Retail_Price']
    
    # Create output
    os.makedirs('output', exist_ok=True)
    output_rows = []
    
    if len(changes) > 0:
        print(f"📝 Found {len(changes)} price changes")
        for _, row in changes.iterrows():
            output_rows.append({
                'Variant SKU': row['SKU'],
                'Variant Price': row.get('Retail_Price_new', row.get('Retail_Price')),
                'Variant Cost': row['B2B price'],
                'Variant Command': 'UPDATE'
            })
    
    # Write output (even if empty)
    with open('output/price_updates.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'Variant SKU', 'Variant Price', 'Variant Cost', 'Variant Command'
        ])
        writer.writeheader()
        writer.writerows(output_rows)
    
    print(f"✅ Written {len(output_rows)} updates to output/price_updates.csv")
    
    # Save current state
    shop_products[['SKU', 'Retail_Price']].to_csv(state_file, index=False)
    print("💾 State saved")

if __name__ == "__main__":
    main()
