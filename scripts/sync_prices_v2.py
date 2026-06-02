"""Direct-API replacement for sync_prices.py.

GENBRUGER 100% af eksisterende pricing.py (tier-baseret markup, sale_discount,
campaign, A/B/C-grupper). Forskellen er KUN output-laget:
  - sync_prices.py     skriver merged CSV → Matrixify læser → Shopify
  - sync_prices_v2.py  kalder direkte productVariantsBulkUpdate +
                       inventoryItemUpdate i Shopify GraphQL Admin API

Modes:
  --dry-run (default): emulerer sync_prices.py's exact output (merge med
                       eksisterende output/price_updates.csv) til
                       output/new_price_updates.csv. Bruges til CSV-diff.
  --live:              pusher today's delta direkte til Shopify. Skriver tom
                       price_updates.csv (Matrixify-neutralisering). Updates
                       Supabase state.
  --skip-state-save:   Spring upsert til vidaxl_pricing_state over (vigtigt
                       under dry-run så vi ikke fjernerer gammelt scripts
                       state-source under parallel-køring).

HUB-fremtid:
  Hele pricing-logikken er allerede i pricing.py (config fra Supabase
  hub_settings.product_automation_pricing). HUB bygges senere som UI ovenpå
  samme config-tabel — denne migration ændrer intet ved logik-laget,
  kun ved output-laget.

Forward-compat (Kayoom/SFTP):
  fetch_supplier_feed() er adskilt. Når Kayoom kommer på SFTP, swappes kun
  denne funktion. Resten genbruges.
"""
import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from io import StringIO

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pricing


CONFIG = {
    "vidaxl_feed_url": (
        "https://feed.vidaxl.io/api/v1/feeds/download/"
        "f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping_offer.csv"
    ),
    "warmup_days": 60,
    "shop_cache_path": "output/shop_skus.json",
    "live_csv_path": "output/price_updates.csv",
    "dry_run_csv": "output/new_price_updates.csv",
    "csv_headers": [
        "Variant SKU", "Variant Price", "Variant Compare At Price",
        "Variant Cost", "Variant Command",
    ],
    "max_retries": 4,
    "request_timeout": 180,
    "supabase_state_table": "vidaxl_pricing_state",
    "supabase_batch_size": 500,
}


SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE_URL') or 'b7916a-38.myshopify.com'
SHOPIFY_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json"


FEED_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/csv,application/octet-stream,*/*",
    "Accept-Language": "da-DK,da;q=0.9,en;q=0.8",
}


# === FETCH ===========================================================

def fetch_supplier_feed() -> pd.DataFrame:
    """Hent VidaXL B2B-feed med retry på 403/429/5xx."""
    print(f"📥 Fetching VidaXL feed...")
    last_err = None
    for attempt in range(1, CONFIG["max_retries"] + 1):
        try:
            r = requests.get(CONFIG["vidaxl_feed_url"],
                             headers=FEED_HEADERS, timeout=CONFIG["request_timeout"])
            if r.status_code in (403, 429, 500, 502, 503, 504):
                wait = 5 * attempt
                print(f"  feed responded {r.status_code} (attempt {attempt}) — retrying in {wait}s")
                time.sleep(wait)
                last_err = r
                continue
            r.raise_for_status()
            df = pd.read_csv(StringIO(r.text))
            df["SKU"] = df["SKU"].astype(str)
            df["B2B price"] = pd.to_numeric(df["B2B price"], errors="coerce")
            print(f"✅ {len(df)} rows fra VidaXL")
            return df
        except requests.HTTPError as e:
            last_err = e
            time.sleep(5 * attempt)
    raise RuntimeError(f"Feed fetch failed after {CONFIG['max_retries']} attempts: {last_err}")


def load_shop_cache() -> dict:
    with open(CONFIG["shop_cache_path"], 'r', encoding='utf-8') as f:
        data = json.load(f)
    if 'variants' not in data:
        sys.exit("❌ shop_skus.json mangler 'variants' — kør update_shop_cache.py")
    return data


def get_supabase_client():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        return None
    from supabase import create_client
    return create_client(url, key)


def load_pricing_state(sb) -> dict:
    """Hent alle rows fra vidaxl_pricing_state, paginer 1000 ad gangen."""
    all_rows, page, page_size = [], 0, 1000
    while True:
        res = (sb.table(CONFIG["supabase_state_table"])
               .select("sku,pricing_group,status,b2b_cost,normal_price,sale_price,warmup_complete_at")
               .range(page * page_size, (page + 1) * page_size - 1)
               .execute())
        if not res.data: break
        all_rows.extend(res.data)
        if len(res.data) < page_size: break
        page += 1
    return {r["sku"]: r for r in all_rows}


# === TRANSFORM =======================================================
# Identisk logik som sync_prices.py — vi GENBRUGER pricing.py uændret.

def compute_price_diffs(feed_df, state, pricing_cfg, shop_skus):
    """Returnér (today_rows, state_updates, counters).

    today_rows = liste af dicts klar til CSV-emit (sync_prices format)
    state_updates = liste af dicts til Supabase upsert
    counters = stats-dict for logging
    """
    shop_products = feed_df[feed_df["SKU"].isin(shop_skus)]
    print(f"🎯 {len(shop_products)} feed-rows matcher shop")

    today_rows = []
    state_updates = []
    counters = {
        "skip_no_state": 0, "skip_on_sale_frozen": 0, "skip_unchanged": 0,
        "skip_invalid_b2b": 0, "update_normal": 0, "update_warmup": 0,
        "warmup_reset": 0,
    }

    new_warmup_at = (datetime.now(timezone.utc) +
                     timedelta(days=CONFIG["warmup_days"])).isoformat()

    for _, row in shop_products.iterrows():
        sku = str(row["SKU"]).strip()
        b2b = row["B2B price"]
        if pd.isna(b2b) or b2b <= 0:
            counters["skip_invalid_b2b"] += 1
            continue
        b2b = float(b2b)

        product_state = state.get(sku)
        if product_state is None:
            counters["skip_no_state"] += 1
            continue

        status = product_state["status"]
        if status == "on_sale":
            counters["skip_on_sale_frozen"] += 1
            continue

        new_normal = pricing.calculate_normal_price(b2b, pricing_cfg)
        new_sale = pricing.calculate_sale_price(b2b, pricing_cfg)

        old_normal = int(float(product_state.get("normal_price") or 0))
        old_b2b = float(product_state.get("b2b_cost") or 0)

        normal_unchanged = int(new_normal) == old_normal
        b2b_unchanged = abs(b2b - old_b2b) < 0.01
        if normal_unchanged and b2b_unchanged:
            counters["skip_unchanged"] += 1
            continue

        today_rows.append({
            "Variant SKU": sku,
            "Variant Price": new_normal,
            "Variant Compare At Price": "",
            "Variant Cost": b2b,
            "Variant Command": "UPDATE",
        })

        update = {
            "sku": sku, "pricing_group": product_state["pricing_group"],
            "status": status, "b2b_cost": b2b,
            "normal_price": new_normal, "sale_price": new_sale,
        }
        if status == "warmup" and not normal_unchanged:
            update["warmup_complete_at"] = new_warmup_at
            counters["warmup_reset"] += 1
        state_updates.append(update)
        if status == "warmup":
            counters["update_warmup"] += 1
        else:
            counters["update_normal"] += 1

    print(f"📊 Counters: {counters}")
    return today_rows, state_updates, counters


# === PUSH (Niveau 2 direct API) =====================================

def gql(query, variables=None):
    if not SHOPIFY_TOKEN:
        sys.exit("❌ SHOPIFY_ACCESS_TOKEN mangler")
    payload = {'query': query}
    if variables: payload['variables'] = variables
    headers = {'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json'}
    for attempt in range(1, CONFIG["max_retries"] + 1):
        r = requests.post(GRAPHQL, headers=headers, json=payload, timeout=CONFIG["request_timeout"])
        if r.status_code != 200:
            raise Exception(f"HTTP {r.status_code}: {r.text[:300]}")
        d = r.json()
        if 'errors' in d:
            throttled = any('Throttled' in str(e) or 'THROTTLED' in str(e) for e in d['errors'])
            if throttled and attempt < CONFIG["max_retries"]:
                time.sleep(2 ** attempt)
                continue
            raise Exception(f"GraphQL errors: {d['errors']}")
        cost = d.get('extensions', {}).get('cost', {}).get('throttleStatus', {})
        if cost.get('currentlyAvailable', 1000) < 200:
            time.sleep(0.5)
        return d
    raise Exception("Max retries exceeded")


PRICE_MUTATION = """
mutation updPrices($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    userErrors { field message }
    productVariants { id }
  }
}
"""

COST_MUTATION = """
mutation updCost($id: ID!, $input: InventoryItemInput!) {
  inventoryItemUpdate(id: $id, input: $input) {
    userErrors { field message }
    inventoryItem { id }
  }
}
"""


def push_to_shopify(today_rows, variants_map, sku_to_inv):
    """Push price+compareAt via productVariantsBulkUpdate (gruppér pr. produkt)
    og cost via inventoryItemUpdate.
    """
    print(f"🚀 Pushing {len(today_rows)} price-changes til Shopify")
    stats = {"price_updated": 0, "cost_updated": 0,
             "skipped_no_variant": 0, "errors": 0}

    # Gruppér per product_id for productVariantsBulkUpdate
    by_product = defaultdict(list)        # product_id -> [(variant_id, row), ...]
    cost_ops = []                         # [(inventory_item_id, b2b), ...]
    for row in today_rows:
        sku = row["Variant SKU"]
        vm = variants_map.get(sku)
        if not vm:
            stats["skipped_no_variant"] += 1
            continue
        variant_id, product_id = vm
        by_product[product_id].append((variant_id, row))
        inv_id = sku_to_inv.get(sku)
        if inv_id and row.get("Variant Cost"):
            cost_ops.append((inv_id, float(row["Variant Cost"])))

    print(f"  {len(by_product)} unikke produkter at opdatere")
    print(f"  {len(cost_ops)} cost-opdateringer")

    # 1. Price + compareAtPrice — én mutation pr. produkt
    for n, (product_id, items) in enumerate(by_product.items(), 1):
        variants_payload = []
        for variant_id, row in items:
            v = {
                "id": f"gid://shopify/ProductVariant/{variant_id}",
                "price": str(row["Variant Price"]),
            }
            cap = row.get("Variant Compare At Price")
            if cap not in (None, "", "nan"):
                try: v["compareAtPrice"] = str(int(float(cap)))
                except (ValueError, TypeError): v["compareAtPrice"] = None
            else:
                v["compareAtPrice"] = None
            variants_payload.append(v)
        try:
            d = gql(PRICE_MUTATION, {
                "productId": f"gid://shopify/Product/{product_id}",
                "variants": variants_payload,
            })
            errs = d['data']['productVariantsBulkUpdate']['userErrors']
            if errs:
                stats["errors"] += len(errs)
                print(f"  ⚠ product {product_id}: userErrors {errs[:2]}")
            else:
                stats["price_updated"] += len(items)
                if n % 50 == 0 or n == len(by_product):
                    print(f"  Price progress: {n}/{len(by_product)} products ({stats['price_updated']} variants)")
        except Exception as e:
            stats["errors"] += len(items)
            print(f"  ❌ product {product_id} fejlede: {str(e)[:150]}")

    # 2. Cost — én mutation pr. SKU (kan ikke bulk'es direkte)
    for n, (inv_id, cost) in enumerate(cost_ops, 1):
        try:
            d = gql(COST_MUTATION, {
                "id": f"gid://shopify/InventoryItem/{inv_id}",
                "input": {"cost": str(cost)},
            })
            errs = d['data']['inventoryItemUpdate']['userErrors']
            if errs:
                stats["errors"] += 1
                print(f"  ⚠ cost inv_id {inv_id}: userErrors {errs[:1]}")
            else:
                stats["cost_updated"] += 1
                if n % 100 == 0 or n == len(cost_ops):
                    print(f"  Cost progress: {n}/{len(cost_ops)}")
        except Exception as e:
            stats["errors"] += 1
            print(f"  ❌ cost inv_id {inv_id} fejlede: {str(e)[:150]}")

    return stats


# === STATE / OUTPUT =================================================

def write_merged_csv(today_rows, shop_skus, output_path):
    """Match sync_prices.py merge-logik EXACT for byte-for-byte sammenligning.

    Læser eksisterende output/price_updates.csv (158k rows), layer today's
    diffs ovenpå, prune til current shop_skus, sortér by SKU.
    """
    existing = {}
    if os.path.exists(CONFIG["live_csv_path"]):
        with open(CONFIG["live_csv_path"], "r", newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                sku = r.get("Variant SKU")
                if sku: existing[sku] = r
    print(f"📂 Existing CSV: {len(existing)} rows")

    merged = dict(existing)
    for r in today_rows:
        merged[r["Variant SKU"]] = r

    merged = {sku: row for sku, row in merged.items() if sku in shop_skus}

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CONFIG["csv_headers"])
        writer.writeheader()
        for sku in sorted(merged.keys()):
            writer.writerow({k: merged[sku].get(k, "") for k in CONFIG["csv_headers"]})
    print(f"📄 Wrote {len(merged)} rows ({len(existing)} existing + {len(today_rows)} new diffs merged) → {output_path}")


def upsert_state(sb, state_updates):
    if not state_updates:
        print("💾 No state updates needed")
        return
    bs = CONFIG["supabase_batch_size"]
    total = 0
    for i in range(0, len(state_updates), bs):
        batch = state_updates[i:i + bs]
        res = sb.table(CONFIG["supabase_state_table"]).upsert(
            batch, on_conflict="sku"
        ).execute()
        total += len(res.data) if res.data else 0
    print(f"💾 Updated {total} rows in vidaxl_pricing_state")


# === MAIN ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--live', action='store_true',
                        help="Kald Shopify direkte. Default er dry-run der skriver merged CSV.")
    parser.add_argument('--skip-state-save', action='store_true',
                        help="Skip Supabase upsert (vigtigt under dry-run parallel-køring).")
    args = parser.parse_args()

    mode = "LIVE" if args.live else "DRY-RUN"
    print(f"🚀 sync_prices_v2 — {mode}")

    sb = get_supabase_client()
    if sb is None:
        sys.exit("❌ SUPABASE_URL / SUPABASE_SERVICE_KEY mangler")

    pricing_cfg = pricing.load_pricing_config(sb)
    if not pricing_cfg or not pricing_cfg.get("tiers"):
        sys.exit("❌ Pricing tiers ikke loaded fra Supabase — afviser at koere med fallback")
    print(f"✅ {len(pricing_cfg['tiers'])} pricing tiers")

    state = load_pricing_state(sb)
    print(f"✅ {len(state)} rows fra vidaxl_pricing_state")

    cache = load_shop_cache()
    shop_skus = set(cache['skus'])
    variants_map = {k: v for k, v in cache['variants'].items()}
    sku_to_inv = cache['inventory_items']
    print(f"📦 Cache: {len(shop_skus)} SKUs")

    feed_df = fetch_supplier_feed()
    today_rows, state_updates, counters = compute_price_diffs(feed_df, state, pricing_cfg, shop_skus)

    if args.live:
        stats = push_to_shopify(today_rows, variants_map, sku_to_inv)
        print(f"\n📊 STATS: {stats}")
        if stats["errors"]:
            sys.exit(1)
        # Neutraliser eksisterende CSV (Matrixify læser intet at gøre)
        with open(CONFIG["live_csv_path"], "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CONFIG["csv_headers"]).writeheader()
        print(f"🧹 Tømte {CONFIG['live_csv_path']} (Matrixify-neutralisering)")
    else:
        # Dry-run: emit samme merged-format som det gamle script
        write_merged_csv(today_rows, shop_skus, CONFIG["dry_run_csv"])

    if not args.skip_state_save and args.live:
        upsert_state(sb, state_updates)

    print("✅ Færdig")


if __name__ == "__main__":
    main()
