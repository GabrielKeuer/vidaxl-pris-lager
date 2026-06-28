"""Direct-API replacement for rotate_groups.py.

GENBRUGER 100% af eksisterende compute-logik (A→B→C-rotation, Omnibus
25-dages gate, warmup-handling, state-tracking i Supabase). Forskel:
  - OLD: writes Matrixify CSV → Matrixify importerer → Shopify
  - NEW: writes pre-flight snapshot + pusher via Bulk Operations
         (50k+ SKU-mutations i én rotation).

Modes:
  --dry-run     : computes + writes CSV i samme format som OLD til
                  output/new_rotation_updates.csv. Bruges til CSV-diff.
                  Ingen Shopify, ingen state, ingen rotation-advance.
  --live        : Pusher til Shopify direkte. Skriver snapshot FØR ændringer.
                  Updates Supabase state + rotation-state.
  --force       : Skip 25-dages Omnibus-gate.
  --skip-snapshot : Skip pre-flight snapshot (ikke anbefalet i live mode).

Snapshot:
  Gemmer current Supabase state for alle affected SKUs FØR mutation til
  output/rotation_snapshot_<rotation_id>.json. Bruges som rollback-grundlag
  hvis noget gaar galt.

Reuse:
  Bulk Operations push-logik er dupliceret fra sync_prices_v2.py for at
  undgaa kobling. Refactor til shared lib senere under cleanup.
"""
import argparse
import csv
import json
import os
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pricing  # noqa: F401  (parity m. OLD)


CONFIG = {
    "rotation_state_key": "vidaxl_rotation_state",
    "rotation_min_days": 25,                # Omnibus 4.4-rule gate
    "group_cycle": ["A", "B", "C"],
    "supabase_state_table": "vidaxl_pricing_state",
    "supabase_batch_size": 500,
    "shop_cache_path": "output/shop_skus.json",
    "live_csv_path": "output/price_updates.csv",
    "dry_run_csv": "output/new_rotation_updates.csv",
    "snapshot_dir": "output/rotation_snapshots",
    "csv_headers": [
        "Variant SKU", "Variant Price", "Variant Compare At Price",
        "Variant Cost", "Variant Command",
    ],
    # Bulk Operations (Niveau 3) — rotation rammer altid >1000 mutations
    "bulk_threshold": 1000,
    "bulk_poll_interval_seconds": 15,
    "bulk_max_wait_minutes": 60,
    "max_retries": 4,
    "request_timeout": 180,
}


SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE_URL') or 'b7916a-38.myshopify.com'
SHOPIFY_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json"


# === SUPABASE HELPERS ================================================

def get_supabase_client():
    url = os.environ.get("SUPABASE_URL"); key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        return None
    from supabase import create_client
    return create_client(url, key)


def load_rotation_state(sb):
    res = sb.table("hub_settings").select("value").eq("key", CONFIG["rotation_state_key"]).execute()
    return res.data[0]["value"] if res.data else None


def save_rotation_state(sb, state):
    sb.table("hub_settings").upsert({
        "key": CONFIG["rotation_state_key"],
        "value": state,
        "description": "VidaXL group rotation state — managed by rotate_groups_v2.py",
    }, on_conflict="key").execute()


def next_group_in_cycle(prev):
    cycle = CONFIG["group_cycle"]
    if prev not in cycle:
        return cycle[0]
    return cycle[(cycle.index(prev) + 1) % len(cycle)]


def fetch_state_paginated(sb, group, statuses):
    rows = []
    page = 0; page_size = 1000
    while True:
        res = (sb.table(CONFIG["supabase_state_table"])
               .select("sku,pricing_group,status,b2b_cost,normal_price,sale_price,warmup_complete_at,last_normal_period_started_at")
               .eq("pricing_group", group).in_("status", statuses)
               .range(page * page_size, (page + 1) * page_size - 1)
               .execute())
        if not res.data: break
        rows.extend(res.data)
        if len(res.data) < page_size: break
        page += 1
    return rows


def parse_iso(s):
    """Tolerant ISO-parser. Python's fromisoformat kraver 0/3/6-cifret microseconds —
    Supabase ROW i dag har en med 5 cifre. Normaliser til 6 cifre."""
    if not s: return None
    import re
    s = s.replace("Z", "+00:00")
    # Pad/truncate fractional seconds til 6 cifre
    s = re.sub(r'\.(\d{1,9})(?=[+\-T ])', lambda m: '.' + m.group(1).ljust(6, '0')[:6], s)
    return datetime.fromisoformat(s)


def upsert_state_batches(sb, state_updates):
    if not state_updates:
        print("💾 No state updates needed"); return
    bs = CONFIG["supabase_batch_size"]; total = 0
    for i in range(0, len(state_updates), bs):
        batch = state_updates[i:i + bs]
        res = sb.table(CONFIG["supabase_state_table"]).upsert(batch, on_conflict="sku").execute()
        total += len(res.data) if res.data else 0
    print(f"💾 Updated {total} state rows")


# === COMPUTE (identisk med v1) ======================================

def compute_rotation(sb, force=False):
    """Returner (end_rows, start_rows, state_updates, meta) eller exit hvis gate.

    end_rows: SKUs der gaar FRA on_sale TIL normal (i forrige aktive gruppe)
    start_rows: SKUs der gaar FRA normal/warmup TIL on_sale (i ny aktive gruppe)
    """
    rotation_state = load_rotation_state(sb) or {}
    prev_active = rotation_state.get("active_group")
    last_rotated_at = parse_iso(rotation_state.get("last_rotated_at"))

    if last_rotated_at and not force:
        days_since = (datetime.now(timezone.utc) - last_rotated_at).days
        if days_since < CONFIG["rotation_min_days"]:
            print(f"❌ Last rotation {days_since} days ago (min {CONFIG['rotation_min_days']}). Use --force.")
            sys.exit(2)

    next_active = next_group_in_cycle(prev_active)
    now_iso = datetime.now(timezone.utc).isoformat()
    rotation_id = (f"rot-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-"
                   f"{prev_active or 'INIT'}-to-{next_active}")
    print(f"🔄 {prev_active or '(none)'} → {next_active}  ({rotation_id})")

    end_rows = []; start_rows = []; state_updates = []

    # 1) END previous sale
    end_count = 0
    if prev_active:
        prev_sale = fetch_state_paginated(sb, prev_active, ["on_sale"])
        for r in prev_sale:
            end_rows.append({
                "sku": r["sku"],
                "normal_price": r["normal_price"],
                "sale_price": r["sale_price"],     # for snapshot
                "b2b_cost": r.get("b2b_cost") or "",
                "pricing_group": r["pricing_group"],
            })
            state_updates.append({
                "sku": r["sku"],
                "pricing_group": r["pricing_group"],
                "status": "normal",
                "last_normal_period_started_at": now_iso,
                "last_status_change_at": now_iso,
            })
            end_count += 1
    print(f"   - Ended sale on {end_count} products i group {prev_active}")

    # 2) START next group's sale
    start_count = 0; skipped_warmup = 0; skipped_no_prices = 0
    next_eligible = fetch_state_paginated(sb, next_active, ["normal", "warmup"])
    now = datetime.now(timezone.utc)
    for r in next_eligible:
        warmup_at = parse_iso(r.get("warmup_complete_at"))
        if warmup_at is not None and warmup_at > now:
            skipped_warmup += 1; continue
        if r.get("normal_price") is None or r.get("sale_price") is None:
            skipped_no_prices += 1; continue
        start_rows.append({
            "sku": r["sku"],
            "normal_price": r["normal_price"],
            "sale_price": r["sale_price"],
            "b2b_cost": r.get("b2b_cost") or "",
            "pricing_group": r["pricing_group"],
        })
        state_updates.append({
            "sku": r["sku"],
            "pricing_group": r["pricing_group"],
            "status": "on_sale",
            # Bevar eksisterende vaerdi: produktet FORLADER normal-perioden nu,
            # men kolonnen er NOT NULL og upsert sender en INSERT-row (selv ved
            # ON CONFLICT UPDATE) -> uden denne fejler den med 23502.
            "last_normal_period_started_at": r.get("last_normal_period_started_at"),
            "last_status_change_at": now_iso,
        })
        start_count += 1
    print(f"   + Started sale on {start_count} products i group {next_active} "
          f"(skipped {skipped_warmup} warmup-not-ready, {skipped_no_prices} no-prices)")

    meta = {
        "rotation_id": rotation_id,
        "prev_active": prev_active,
        "next_active": next_active,
        "now_iso": now_iso,
        "end_count": end_count,
        "start_count": start_count,
        "skipped_warmup": skipped_warmup,
        "skipped_no_prices": skipped_no_prices,
        "rotation_state": rotation_state,
    }
    return end_rows, start_rows, state_updates, meta


# === SNAPSHOT (pre-flight rollback-grundlag) ========================

def write_snapshot(end_rows, start_rows, meta):
    """Gem BEFORE-state (fra Supabase pricing-state) til JSON for rollback."""
    os.makedirs(CONFIG["snapshot_dir"], exist_ok=True)
    path = f"{CONFIG['snapshot_dir']}/snapshot_{meta['rotation_id']}.json"
    snap = {
        "rotation_id": meta["rotation_id"],
        "timestamp": meta["now_iso"],
        "prev_active": meta["prev_active"],
        "next_active": meta["next_active"],
        "counts": {
            "end_count": meta["end_count"],
            "start_count": meta["start_count"],
        },
        # END rows: WAS on_sale (price=sale_price, compareAt=normal_price)
        "ended_skus_before": [{
            "sku": r["sku"],
            "before_price": r["sale_price"],
            "before_compare_at": r["normal_price"],
            "before_cost": r["b2b_cost"],
        } for r in end_rows],
        # START rows: WAS normal (price=normal_price, compareAt=null)
        "started_skus_before": [{
            "sku": r["sku"],
            "before_price": r["normal_price"],
            "before_compare_at": None,
            "before_cost": r["b2b_cost"],
        } for r in start_rows],
    }
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(snap, f, separators=(',', ':'))
    size_kb = os.path.getsize(path) / 1024
    print(f"📸 Snapshot: {path} ({size_kb:.0f} KB)")
    return path


# === CSV OUTPUT (til dry-run validering mod OLD) ====================

def write_dry_run_csv(end_rows, start_rows, output_path):
    """Match OLD's exact format så CSV-diff er meningsfuld."""
    rows = []
    for r in end_rows:
        rows.append({
            "Variant SKU": r["sku"],
            "Variant Price": r["normal_price"],
            "Variant Compare At Price": "",
            "Variant Cost": r["b2b_cost"] or "",
            "Variant Command": "UPDATE",
        })
    for r in start_rows:
        rows.append({
            "Variant SKU": r["sku"],
            "Variant Price": r["sale_price"],
            "Variant Compare At Price": r["normal_price"],
            "Variant Cost": r["b2b_cost"] or "",
            "Variant Command": "UPDATE",
        })
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CONFIG["csv_headers"])
        w.writeheader(); w.writerows(rows)
    print(f"📄 Dry-run CSV: {len(rows)} rækker → {output_path}")


# === BULK OPERATIONS (Niveau 3 push) ================================

BULK_TEMPLATE = '''
mutation call($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    userErrors { field message }
    productVariants { id }
  }
}
'''

STAGED_UPLOAD = """
mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
  stagedUploadsCreate(input: $input) {
    userErrors { field message }
    stagedTargets { url resourceUrl parameters { name value } }
  }
}
"""

BULK_RUN = """
mutation bulkOperationRunMutation($mutation: String!, $stagedUploadPath: String!) {
  bulkOperationRunMutation(mutation: $mutation, stagedUploadPath: $stagedUploadPath) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
"""

BULK_STATUS = """
query { currentBulkOperation(type: MUTATION) {
  id status errorCode createdAt completedAt objectCount url
} }
"""


def gql(query, variables=None):
    if not SHOPIFY_TOKEN: sys.exit("❌ SHOPIFY_ACCESS_TOKEN mangler")
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
                time.sleep(2 ** attempt); continue
            raise Exception(f"GraphQL errors: {d['errors']}")
        cost = d.get('extensions', {}).get('cost', {}).get('throttleStatus', {})
        if cost.get('currentlyAvailable', 1000) < 200:
            time.sleep(0.5)
        return d
    raise Exception("Max retries exceeded")


def push_to_shopify_bulk(end_rows, start_rows, variants_map):
    """Push rotation til Shopify via Bulk Operations."""
    print(f"🚀 NIVEAU 3 (Bulk Operations): {len(end_rows)} end + {len(start_rows)} start")
    stats = {"variants_updated": 0, "products_processed": 0,
             "skipped_no_variant": 0, "errors": 0, "bulk_operation_id": None}

    by_product = defaultdict(list)

    def _add(sku, vinput, cost):
        vm = variants_map.get(sku)
        if not vm:
            stats["skipped_no_variant"] += 1; return
        variant_id, product_id = vm
        v = {"id": f"gid://shopify/ProductVariant/{variant_id}"}
        v.update(vinput)
        if cost not in (None, "", "nan") and cost != 0:
            try: v["inventoryItem"] = {"cost": str(float(cost))}
            except (ValueError, TypeError): pass
        by_product[product_id].append(v)

    # END: price=normal_price, compareAt=null (clear)
    for r in end_rows:
        _add(r["sku"], {
            "price": str(r["normal_price"]),
            "compareAtPrice": None,
        }, r.get("b2b_cost"))

    # START: price=sale_price, compareAt=normal_price (NEW sale-display)
    for r in start_rows:
        _add(r["sku"], {
            "price": str(r["sale_price"]),
            "compareAtPrice": str(r["normal_price"]),
        }, r.get("b2b_cost"))

    if not by_product:
        print("  Ingen mutations at sende. Færdig."); return stats
    total_variants = sum(len(v) for v in by_product.values())
    print(f"  {len(by_product)} unikke produkter, {total_variants} variants")

    # Generate JSONL
    jsonl_path = tempfile.mktemp(suffix='.jsonl')
    with open(jsonl_path, 'w', encoding='utf-8') as f:
        for product_id, variants_payload in by_product.items():
            line = json.dumps({
                "productId": f"gid://shopify/Product/{product_id}",
                "variants": variants_payload,
            }, separators=(',', ':'))
            f.write(line + '\n')
    print(f"  📄 JSONL: {os.path.getsize(jsonl_path):,} bytes")

    # stagedUploadsCreate
    d = gql(STAGED_UPLOAD, {"input": [{
        "filename": "rotation_updates.jsonl",
        "mimeType": "text/jsonl",
        "httpMethod": "POST",
        "resource": "BULK_MUTATION_VARIABLES",
    }]})
    target = d['data']['stagedUploadsCreate']['stagedTargets'][0]
    parameters = {p['name']: p['value'] for p in target['parameters']}
    path = parameters.get('key', '')

    # Upload til S3
    with open(jsonl_path, 'rb') as f:
        r = requests.post(target['url'], data=list(parameters.items()),
                          files={'file': ('rotation_updates.jsonl', f, 'text/jsonl')},
                          timeout=120)
    if r.status_code not in (200, 201, 204):
        raise Exception(f"S3 upload failed: {r.status_code}: {r.text[:300]}")
    os.unlink(jsonl_path)
    print(f"  ⬆ Uploaded (status {r.status_code})")

    # bulkOperationRunMutation
    d = gql(BULK_RUN, {"mutation": BULK_TEMPLATE, "stagedUploadPath": path})
    bulk = d['data']['bulkOperationRunMutation']
    if bulk.get('userErrors'):
        raise Exception(f"bulkOperationRunMutation failed: {bulk['userErrors']}")
    op = bulk['bulkOperation']
    stats["bulk_operation_id"] = op['id']
    print(f"  🚀 Bulk operation: {op['id']}")

    # Poll til færdig
    start = time.time(); last = None
    max_wait = CONFIG["bulk_max_wait_minutes"] * 60
    while True:
        time.sleep(CONFIG["bulk_poll_interval_seconds"])
        d = gql(BULK_STATUS)
        cur = d['data']['currentBulkOperation']
        if cur is None:
            print(f"  ⚠ currentBulkOperation=null — antager faerdig"); break
        elapsed = int(time.time() - start)
        key = (cur['status'], cur.get('objectCount'))
        if key != last:
            print(f"     [{elapsed:>4}s] status={cur['status']} count={cur.get('objectCount')}")
            last = key
        if cur['status'] in ('COMPLETED', 'FAILED', 'CANCELED', 'EXPIRED'):
            break
        if elapsed > max_wait:
            raise Exception(f"Bulk operation timeout efter {max_wait}s — status={cur['status']}")

    if cur['status'] != 'COMPLETED':
        raise Exception(f"Bulk operation endte med status={cur['status']}, errorCode={cur.get('errorCode')}")
    stats["object_count"] = cur.get('objectCount') or 0
    print(f"  ✅ Bulk completed: {stats['object_count']} mutations executed")

    # Parse resultat
    if cur.get('url'):
        r = requests.get(cur['url'], timeout=120)
        for line in r.text.strip().split('\n'):
            if not line: continue
            try:
                res = json.loads(line)
                ue = (res.get('data', {}) or {}).get('productVariantsBulkUpdate', {}).get('userErrors') or []
                if ue:
                    stats["errors"] += len(ue)
                else:
                    pv = (res.get('data', {}) or {}).get('productVariantsBulkUpdate', {}).get('productVariants') or []
                    stats["variants_updated"] += len(pv)
                    stats["products_processed"] += 1
            except json.JSONDecodeError:
                continue

    return stats


# === MAIN ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute + writes CSV til diff. Ingen Shopify, ingen state, ingen rotation-advance.")
    parser.add_argument("--live", action="store_true",
                        help="Push til Shopify + opdater state. Default er --dry-run.")
    parser.add_argument("--force", action="store_true", help="Skip 25-dages gate")
    parser.add_argument("--skip-snapshot", action="store_true",
                        help="Skip pre-flight snapshot (ikke anbefalet)")
    args = parser.parse_args()

    if not args.live and not args.dry_run:
        args.dry_run = True  # default safe-mode

    mode = "LIVE" if args.live else "DRY-RUN"
    print(f"🔄 rotate_groups_v2 — {mode}{' [FORCE]' if args.force else ''}")

    sb = get_supabase_client()
    if sb is None:
        sys.exit("❌ SUPABASE_URL / SUPABASE_SERVICE_KEY mangler")

    end_rows, start_rows, state_updates, meta = compute_rotation(sb, force=args.force)
    total = len(end_rows) + len(start_rows)
    print(f"📦 Total mutations: {total}")

    if args.dry_run:
        write_dry_run_csv(end_rows, start_rows, CONFIG["dry_run_csv"])
        print("✅ Dry-run færdig (intet pushet til Shopify, ingen state-aendringer)")
        return

    # LIVE mode

    # Pre-flight snapshot
    if not args.skip_snapshot:
        write_snapshot(end_rows, start_rows, meta)

    # Load variants_map fra cache
    with open(CONFIG["shop_cache_path"], 'r', encoding='utf-8') as f:
        cache = json.load(f)
    variants_map = cache['variants']
    print(f"📦 Cache: {len(variants_map)} SKU→variant mappings")

    # Push til Shopify
    stats = push_to_shopify_bulk(end_rows, start_rows, variants_map)
    print(f"\n📊 STATS: {stats}")
    if stats["errors"]:
        print(f"⚠ {stats['errors']} errors — afbryder før state-update for sikker rollback-grundlag")
        sys.exit(1)

    # Update Supabase pricing-state
    upsert_state_batches(sb, state_updates)

    # Save rotation state
    new_state = {
        "active_group": meta["next_active"],
        "previous_group": meta["prev_active"],
        "last_rotated_at": meta["now_iso"],
        "rotation_id": meta["rotation_id"],
        "rotation_count": (meta["rotation_state"].get("rotation_count") or 0) + 1,
        "ended_count": meta["end_count"],
        "started_count": meta["start_count"],
        "skipped_warmup": meta["skipped_warmup"],
    }
    save_rotation_state(sb, new_state)
    print(f"💾 Rotation state saved: active={meta['next_active']}, count={new_state['rotation_count']}")

    # Neutraliser eksisterende Matrixify-CSV
    with open(CONFIG["live_csv_path"], "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=CONFIG["csv_headers"]).writeheader()
    print(f"🧹 Tømte {CONFIG['live_csv_path']} (Matrixify-neutralisering)")

    print("\n✅ Rotation gennemført")


if __name__ == "__main__":
    main()
