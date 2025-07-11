import requests
import json
import os
from datetime import datetime

# Shopify credentials
SHOPIFY_STORE = 'b7916a-38.myshopify.com'
SHOPIFY_TOKEN = os.environ['SHOPIFY_ACCESS_TOKEN']

def fetch_all_skus():
    """Hent alle SKUs fra Shopify via GraphQL"""
    print(f"🚀 Fetching SKUs from Shopify...")
    
    all_skus = set()
    cursor = None
    page = 0
    
    while True:
        query = """
        query getVariants($cursor: String) {
          productVariants(first: 250, after: $cursor) {
            edges {
              node {
                sku
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        """
        
        response = requests.post(
            f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json",
            headers={
                'X-Shopify-Access-Token': SHOPIFY_TOKEN,
                'Content-Type': 'application/json'
            },
            json={'query': query, 'variables': {'cursor': cursor}}
        )
        
        if response.status_code != 200:
            raise Exception(f"API Error: {response.status_code}")
            
        data = response.json()
        variants = data['data']['productVariants']
        
        # Extract SKUs
        for edge in variants['edges']:
            sku = edge['node'].get('sku')
            if sku and sku.strip():
                all_skus.add(str(sku).strip())
        
        # Check for more pages
        if not variants['pageInfo']['hasNextPage']:
            break
            
        cursor = variants['pageInfo']['endCursor']
        page += 1
        print(f"  Page {page}: {len(all_skus)} SKUs...")
    
    return sorted(list(all_skus))

def main():
    try:
        skus = fetch_all_skus()
        print(f"✅ Found {len(skus)} SKUs")
        
        # Save to output folder
        output = {
            'skus': skus,
            'count': len(skus),
            'updated': datetime.now().isoformat()
        }
        
        os.makedirs('output', exist_ok=True)
        with open('output/shop_skus.json', 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"💾 Saved to output/shop_skus.json")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        exit(1)

if __name__ == "__main__":
    main()
