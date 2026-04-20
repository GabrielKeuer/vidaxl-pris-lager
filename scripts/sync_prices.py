import csv
import requests
import pandas as pd
from datetime import datetime
from io import StringIO
import os
import json
import math
import random

VIDAXL_URL = "https://feed.vidaxl.io/api/v1/feeds/download/f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping_offer.csv"
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'Kategori_Config.xlsx')


def load_pris_config():
    """Load pristabel fra Pris_Config sheet"""
    try:
        pris_df = pd.read_excel(CONFIG_PATH, sheet_name='Pris_Config')
        pris_df['Indkøb'] = pd.to_numeric(pris_df['Indkøb'], errors='coerce')
        pris_df['Markup'] = pd.to_numeric(pris_df['Markup'], errors='coerce')
        pris_df = pris_df.sort_values('Indkøb').reset_index(drop=True)
        print(f"✅ Loaded {len(pris_df)} pristrin fra config")
        return pris_df
    except Exception as e:
        print(f"⚠️ Kunne ikke loade Pris_Config: {e} — bruger fallback 1.7x")
        return pd.DataFrame()


def get_markup(b2b_price, pris_config):
    """Slå markup-multiplikator op baseret på indkøbspris."""
    if pris_config.empty:
        return 1.7
    b2b = float(b2b_price)
    matching = pris_config[pris_config['Indkøb'] >= b2b]
    if len(matching) > 0:
        return float(matching.iloc[0]['Markup'])
    return float(pris_config.iloc[-1]['Markup'])


def calculate_retail_price(b2b_price, pris_config):
    """Beregn retail pris: markup fra pristabel, afrund ceil(50)-1"""
    try:
        price = float(b2b_price)
        markup = get_markup(price, pris_config)
        raw = price * markup
        return int(math.ceil(raw / 50) * 50 - 1)
    except:
        return 0


def calculate_compare_price(selling_price):
    """Sammenligningspris: random 15-35% rabat, ceil(50)-1, max 9999"""
    discount_pct = random.uniform(0.15, 0.35)
    raw = selling_price / (1 - discount_pct)
    compare = int(math.ceil(raw / 50) * 50 - 1)
    if selling_price <= 9800 and compare > 9999:
        compare = 9999
    return compare

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

    # Load pricing config
    pris_config = load_pris_config()

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
    shop_products['Retail_Price'] = shop_products['B2B price'].apply(
        lambda x: calculate_retail_price(x, pris_config)
    )

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
            retail = row.get('Retail_Price_new', row.get('Retail_Price'))
            compare = calculate_compare_price(retail)
            output_rows.append({
                'Variant SKU': row['SKU'],
                'Variant Price': retail,
                'Variant Compare At Price': compare,
                'Variant Cost': row['B2B price'],
                'Variant Command': 'UPDATE'
            })

    # Write output (even if empty)
    with open('output/price_updates.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'Variant SKU', 'Variant Price', 'Variant Compare At Price',
            'Variant Cost', 'Variant Command'
        ])
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"✅ Written {len(output_rows)} updates to output/price_updates.csv")

    # Save current state
    shop_products[['SKU', 'Retail_Price']].to_csv(state_file, index=False)
    print("💾 State saved")

if __name__ == "__main__":
    main()
