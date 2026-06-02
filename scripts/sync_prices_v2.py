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
    "on_sale_diffs_csv": "output/on_sale_diffs.csv",
    "csv_headers": [
        "Variant SKU", "Variant Price", "Variant Compare At Price",
        "Variant Cost", "Variant Command",
    ],
    "on_sale_headers": [
        "Variant SKU", "Variant Price", "Variant Cost",
        "Compare At Action", "Locked Compare At", "Variant Command",
    ],
    "max_retries": 4,
    "request_timeout": 180,
    "supabase_state_table": "vidaxl_pricing_state",
    "supabase_batch_size": 500,

    # Niveau 2 vs Niveau 3 (Bulk Operations) threshold.
    # < threshold: Niveau 2 (regular GraphQL) — hurtigere for små runs pga.
    #              ingen submit/poll-overhead. Begrænset af 50 pts/s rate.
    # >= threshold: Niveau 3 (bulkOperationRunMutation) — async server-side.
    #              Skalerer til 100k+ mutations, ingen rate limit under run.
    "bulk_threshold": 1000,
    "bulk_poll_interval_seconds": 15,
    "bulk_max_wait_minutes": 45,
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
    """Returnér (today_rows, on_sale_rows, state_updates, counters).

    today_rows    = warmup/normal product changes (matchet med OLD scripts output)
    on_sale_rows  = NY: on_sale produkter hvor b2b/sale ændres. compareAt LÅST,
                    medmindre edge case (new_sale >= compareAt) hvor compareAt ryddes.
    state_updates = liste af dicts til Supabase upsert
    counters      = stats-dict for logging

    Omnibus-tolkning (rettet 2026-06-02):
      - on_sale: KUN compareAtPrice er låst. price + cost MÅ ændres ved b2b-drift.
      - Edge case: hvis ny computed sale_price >= låst compareAt → meningsløst sale
        (sale ≥ ref-pris). Vi rydder compareAt og sætter price = new_sale.
        Status forbliver 'on_sale' i state (rotate_groups håndterer reset til normal).
    """
    shop_products = feed_df[feed_df["SKU"].isin(shop_skus)]
    print(f"🎯 {len(shop_products)} feed-rows matcher shop")

    today_rows = []
    on_sale_rows = []
    state_updates = []
    counters = {
        "skip_no_state": 0, "skip_invalid_b2b": 0,
        "skip_unchanged_normal": 0, "skip_unchanged_on_sale": 0,
        "update_normal": 0, "update_warmup": 0, "warmup_reset": 0,
        "update_on_sale": 0, "on_sale_edge_case_cleared": 0,
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
        new_normal = pricing.calculate_normal_price(b2b, pricing_cfg)
        new_sale = pricing.calculate_sale_price(b2b, pricing_cfg)
        old_normal = int(float(product_state.get("normal_price") or 0))
        old_sale = int(float(product_state.get("sale_price") or 0))
        old_b2b = float(product_state.get("b2b_cost") or 0)
        b2b_unchanged = abs(b2b - old_b2b) < 0.01

        # === ON_SALE: price + cost MÅ ændres. compareAt låst (medmindre edge case). ===
        if status == "on_sale":
            new_sale_int = int(new_sale) if new_sale else 0
            sale_unchanged = new_sale_int == old_sale
            if sale_unchanged and b2b_unchanged:
                counters["skip_unchanged_on_sale"] += 1
                continue

            # locked_compare_at = den compareAt der blev sat ved rotation-start
            # (= state.normal_price som var current normal da rotation startede).
            locked_compare_at = old_normal

            # Edge case: ny sale-pris >= låst compareAt → meningsløst sale, ryd compareAt
            edge_case = (new_sale_int > 0 and new_sale_int >= locked_compare_at)

            on_sale_rows.append({
                "Variant SKU": sku,
                "Variant Price": new_sale_int,
                "Variant Cost": b2b,
                "Compare At Action": "CLEAR" if edge_case else "KEEP",
                "Locked Compare At": locked_compare_at,
                "Variant Command": "UPDATE",
            })

            # State: behold normal_price FROZEN (= reference til original compareAt
            # ved rotation start). Opdater b2b og sale_price.
            state_updates.append({
                "sku": sku,
                "pricing_group": product_state["pricing_group"],
                "status": "on_sale",
                "b2b_cost": b2b,
                "normal_price": old_normal,   # FROZEN — bevar reference
                "sale_price": new_sale_int,
            })
            counters["update_on_sale"] += 1
            if edge_case:
                counters["on_sale_edge_case_cleared"] += 1
            continue

        # === NORMAL / WARMUP: uændret logik fra v1 ===
        normal_unchanged = int(new_normal) == old_normal
        if normal_unchanged and b2b_unchanged:
            counters["skip_unchanged_normal"] += 1
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
    return today_rows, on_sale_rows, state_updates, counters


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
mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    userErrors { field message }
    productVariants { id }
  }
}
"""


def _build_by_product(today_rows, on_sale_rows, variants_map, stats):
    """Konvertér rows → {product_id: [variant_input, ...]} payload.

    Cost er nu inkluderet i variant-input via inventoryItem.cost — en
    productVariantsBulkUpdate kan opdatere price + compareAt + cost i
    EN mutation (vs. de gamle to mutations pr. variant).
    """
    by_product = defaultdict(list)

    def _add(sku, vm, vinput, cost):
        variant_id, product_id = vm
        v = {"id": f"gid://shopify/ProductVariant/{variant_id}"}
        v.update(vinput)
        if cost is not None:
            v["inventoryItem"] = {"cost": str(cost)}
        by_product[product_id].append(v)

    # Normal/warmup: clear compareAt
    for row in today_rows:
        sku = row["Variant SKU"]
        vm = variants_map.get(sku)
        if not vm:
            stats["skipped_no_variant"] += 1
            continue
        cost = None
        if row.get("Variant Cost") not in (None, "", "nan"):
            try: cost = float(row["Variant Cost"])
            except (ValueError, TypeError): pass
        _add(sku, vm, {
            "price": str(row["Variant Price"]),
            "compareAtPrice": None,
        }, cost)

    # On_sale: KEEP (omit compareAt) eller CLEAR (eksplicit null)
    for row in on_sale_rows:
        sku = row["Variant SKU"]
        vm = variants_map.get(sku)
        if not vm:
            stats["skipped_no_variant"] += 1
            continue
        vinput = {"price": str(row["Variant Price"])}
        if row["Compare At Action"] == "CLEAR":
            vinput["compareAtPrice"] = None
            stats["on_sale_compare_at_cleared"] += 1
        cost = None
        if row.get("Variant Cost") not in (None, "", "nan"):
            try: cost = float(row["Variant Cost"])
            except (ValueError, TypeError): pass
        _add(sku, vm, vinput, cost)

    return by_product


def push_to_shopify_graphql(today_rows, on_sale_rows, variants_map):
    """Niveau 2: regular GraphQL Admin API. En productVariantsBulkUpdate
    mutation pr. produkt. Hurtig for små batches (<1000 changes).
    """
    print(f"🚀 NIVEAU 2 (GraphQL): {len(today_rows)} normal + {len(on_sale_rows)} on_sale changes")
    stats = {"variants_updated": 0, "products_processed": 0,
             "skipped_no_variant": 0, "errors": 0,
             "on_sale_compare_at_cleared": 0}

    by_product = _build_by_product(today_rows, on_sale_rows, variants_map, stats)
    print(f"  {len(by_product)} unikke produkter at opdatere")
    if stats["on_sale_compare_at_cleared"]:
        print(f"  ⚠ {stats['on_sale_compare_at_cleared']} on_sale ryddes compareAt (edge case)")

    for n, (product_id, variants_payload) in enumerate(by_product.items(), 1):
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
                stats["variants_updated"] += len(variants_payload)
                stats["products_processed"] += 1
                if n % 50 == 0 or n == len(by_product):
                    print(f"  Progress: {n}/{len(by_product)} products ({stats['variants_updated']} variants)")
        except Exception as e:
            stats["errors"] += len(variants_payload)
            print(f"  ❌ product {product_id} fejlede: {str(e)[:150]}")

    return stats


# === NIVEAU 3: Bulk Operations =====================================

BULK_MUTATION_TEMPLATE = '''
mutation call($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    userErrors { field message }
    productVariants { id }
  }
}
'''

STAGED_UPLOAD_MUTATION = """
mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
  stagedUploadsCreate(input: $input) {
    userErrors { field message }
    stagedTargets {
      url
      resourceUrl
      parameters { name value }
    }
  }
}
"""

BULK_RUN_MUTATION = """
mutation bulkOperationRunMutation($mutation: String!, $stagedUploadPath: String!) {
  bulkOperationRunMutation(mutation: $mutation, stagedUploadPath: $stagedUploadPath) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
"""

BULK_STATUS_QUERY = """
query {
  currentBulkOperation(type: MUTATION) {
    id status errorCode createdAt completedAt objectCount rootObjectCount
    fileSize url partialDataUrl
  }
}
"""


def push_to_shopify_bulk(today_rows, on_sale_rows, variants_map):
    """Niveau 3: bulkOperationRunMutation. Skalérer til 100k+ mutations.
    Submit JSONL → server-side async processing → poll → resultater.

    Flow:
      1. Build by_product dict
      2. Skriv lokal JSONL fil
      3. stagedUploadsCreate → få signed S3-URL
      4. Upload JSONL til S3
      5. bulkOperationRunMutation(template, stagedUploadPath)
      6. Poll currentBulkOperation hvert N sek til status=COMPLETED|FAILED
      7. Parse resultat (download fra url-feltet hvis success)
    """
    import tempfile

    print(f"🚀 NIVEAU 3 (Bulk Operations): {len(today_rows)} normal + {len(on_sale_rows)} on_sale changes")
    stats = {"variants_updated": 0, "products_processed": 0,
             "skipped_no_variant": 0, "errors": 0,
             "on_sale_compare_at_cleared": 0,
             "bulk_operation_id": None, "object_count": 0}

    by_product = _build_by_product(today_rows, on_sale_rows, variants_map, stats)
    if not by_product:
        print("  Ingen mutations at sende. Færdig.")
        return stats
    print(f"  {len(by_product)} unikke produkter i bulk")
    if stats["on_sale_compare_at_cleared"]:
        print(f"  ⚠ {stats['on_sale_compare_at_cleared']} on_sale ryddes compareAt (edge case)")

    # ---- Step 1: skriv JSONL fil ----
    jsonl_path = tempfile.mktemp(suffix='.jsonl')
    with open(jsonl_path, 'w', encoding='utf-8') as f:
        for product_id, variants_payload in by_product.items():
            line = json.dumps({
                "productId": f"gid://shopify/Product/{product_id}",
                "variants": variants_payload,
            }, separators=(',', ':'))
            f.write(line + '\n')
    file_size = os.path.getsize(jsonl_path)
    print(f"  📄 JSONL fil: {file_size:,} bytes, {len(by_product)} lines → {jsonl_path}")

    # ---- Step 2: stagedUploadsCreate (få signed S3-URL) ----
    print(f"  ⬆ Anmoder om upload-URL...")
    d = gql(STAGED_UPLOAD_MUTATION, {
        "input": [{
            "filename": "bulk_price_updates.jsonl",
            "mimeType": "text/jsonl",
            "httpMethod": "POST",
            "resource": "BULK_MUTATION_VARIABLES",
        }]
    })
    errs = d['data']['stagedUploadsCreate'].get('userErrors') or []
    if errs:
        raise Exception(f"stagedUploadsCreate failed: {errs}")
    target = d['data']['stagedUploadsCreate']['stagedTargets'][0]
    upload_url = target['url']
    resource_url = target['resourceUrl']
    parameters = {p['name']: p['value'] for p in target['parameters']}
    staged_upload_path = parameters.get('key', '')
    print(f"     staged upload path: {staged_upload_path}")

    # ---- Step 3: Upload JSONL til S3 ----
    print(f"  ⬆ Uploader JSONL til Shopify's S3...")
    with open(jsonl_path, 'rb') as f:
        files = {'file': ('bulk_price_updates.jsonl', f, 'text/jsonl')}
        # Build multipart form: parameters first, then file last
        data = list(parameters.items())
        r = requests.post(upload_url, data=data, files=files, timeout=120)
    if r.status_code not in (200, 201, 204):
        raise Exception(f"S3 upload failed: HTTP {r.status_code}: {r.text[:300]}")
    print(f"     ✅ Uploaded (status {r.status_code})")
    os.unlink(jsonl_path)

    # ---- Step 4: bulkOperationRunMutation ----
    print(f"  🚀 Starter bulk-mutation...")
    d = gql(BULK_RUN_MUTATION, {
        "mutation": BULK_MUTATION_TEMPLATE,
        "stagedUploadPath": staged_upload_path,
    })
    bulk = d['data']['bulkOperationRunMutation']
    if bulk.get('userErrors'):
        raise Exception(f"bulkOperationRunMutation failed: {bulk['userErrors']}")
    op = bulk['bulkOperation']
    stats["bulk_operation_id"] = op['id']
    print(f"     ✅ Bulk operation startet: {op['id']} (status={op['status']})")

    # ---- Step 5: Poll til færdig ----
    print(f"  ⏳ Poller status hvert {CONFIG['bulk_poll_interval_seconds']}s...")
    start = time.time()
    last_status = None
    last_count = None
    max_wait = CONFIG["bulk_max_wait_minutes"] * 60
    while True:
        time.sleep(CONFIG["bulk_poll_interval_seconds"])
        d = gql(BULK_STATUS_QUERY)
        cur = d['data']['currentBulkOperation']
        if cur is None:
            print(f"  ⚠ currentBulkOperation returnerede null — antager færdig")
            break
        status = cur['status']
        count = cur.get('objectCount') or 0
        elapsed = int(time.time() - start)
        if status != last_status or count != last_count:
            print(f"     [{elapsed:>4}s] status={status} objectCount={count}")
            last_status = status; last_count = count
        if status in ('COMPLETED', 'FAILED', 'CANCELED', 'EXPIRED'):
            break
        if time.time() - start > max_wait:
            raise Exception(f"Bulk operation timeout efter {max_wait}s — status={status}")

    # ---- Step 6: Parse resultat ----
    if cur['status'] != 'COMPLETED':
        raise Exception(f"Bulk operation endte med status={cur['status']}, errorCode={cur.get('errorCode')}")

    stats["object_count"] = cur.get('objectCount') or 0
    print(f"  ✅ Bulk completed: {stats['object_count']} mutations executed")

    # Download result JSONL og count errors
    result_url = cur.get('url')
    if result_url:
        try:
            r = requests.get(result_url, timeout=120)
            r.raise_for_status()
            result_lines = r.text.strip().split('\n')
            for line in result_lines:
                if not line: continue
                try:
                    res = json.loads(line)
                    user_errors = (res.get('data', {}) or {}).get(
                        'productVariantsBulkUpdate', {}).get('userErrors') or []
                    if user_errors:
                        stats["errors"] += len(user_errors)
                    else:
                        # Count successful productVariants
                        pv = (res.get('data', {}) or {}).get(
                            'productVariantsBulkUpdate', {}).get('productVariants') or []
                        stats["variants_updated"] += len(pv)
                        stats["products_processed"] += 1
                except json.JSONDecodeError:
                    continue
            if stats["errors"]:
                print(f"  ⚠ {stats['errors']} userErrors i bulk-resultat")
        except Exception as e:
            print(f"  ⚠ Kunne ikke parse resultat: {str(e)[:200]}")

    return stats


def push_to_shopify(today_rows, on_sale_rows, variants_map):
    """Auto-vælg Niveau 2 eller Niveau 3 baseret på batch-størrelse."""
    total = len(today_rows) + len(on_sale_rows)
    threshold = CONFIG["bulk_threshold"]
    print(f"📦 Total changes: {total} (threshold for bulk: {threshold})")
    if total >= threshold:
        return push_to_shopify_bulk(today_rows, on_sale_rows, variants_map)
    else:
        return push_to_shopify_graphql(today_rows, on_sale_rows, variants_map)


# === STATE / OUTPUT =================================================

def write_on_sale_csv(on_sale_rows, output_path):
    """Skriv on_sale-ændringer til SEPARAT fil (ikke samme format som main CSV).

    Bruges til dry-run-validering: viser hvilke on_sale-ændringer v2 ville
    foretage i --live. Indeholder ekstra kolonner som main CSV ikke har
    ('Compare At Action', 'Locked Compare At').
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CONFIG["on_sale_headers"])
        writer.writeheader()
        for row in on_sale_rows:
            writer.writerow({k: row.get(k, "") for k in CONFIG["on_sale_headers"]})
    print(f"📄 On-sale diffs: {len(on_sale_rows)} rækker → {output_path}")


def write_merged_csv(today_rows, shop_skus, output_path):
    """Match sync_prices.py merge-logik EXACT for byte-for-byte sammenligning.

    Læser eksisterende output/price_updates.csv (158k rows), layer today's
    diffs ovenpå, prune til current shop_skus, sortér by SKU.
    KUN non-sale rows — on_sale håndteres separat via write_on_sale_csv.
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
    print(f"📦 Cache: {len(shop_skus)} SKUs")

    feed_df = fetch_supplier_feed()
    today_rows, on_sale_rows, state_updates, counters = compute_price_diffs(
        feed_df, state, pricing_cfg, shop_skus)

    if args.live:
        stats = push_to_shopify(today_rows, on_sale_rows, variants_map)
        print(f"\n📊 STATS: {stats}")
        if stats["errors"]:
            sys.exit(1)
        # Neutraliser eksisterende Matrixify-CSV (skriv tom så hvis schedule
        # stadig kører, læser den intet at gøre)
        with open(CONFIG["live_csv_path"], "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CONFIG["csv_headers"]).writeheader()
        print(f"🧹 Tømte {CONFIG['live_csv_path']} (Matrixify-neutralisering)")
        # Audit trail: skriv on_sale_diffs.csv også i --live så vi har historie
        # over hvilke aendringer der blev pushed (specielt edge cases).
        write_on_sale_csv(on_sale_rows, CONFIG["on_sale_diffs_csv"])
    else:
        # Dry-run: main CSV matcher OLD bit-for-bit (KUN non-sale rows).
        # on_sale ændringer skrives til separat fil — synlig dokumentation
        # af det NYE on_sale-behavior FØR cutover.
        write_merged_csv(today_rows, shop_skus, CONFIG["dry_run_csv"])
        write_on_sale_csv(on_sale_rows, CONFIG["on_sale_diffs_csv"])

    if not args.skip_state_save and args.live:
        upsert_state(sb, state_updates)

    print("✅ Færdig")


if __name__ == "__main__":
    main()
