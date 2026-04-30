"""
Daily VidaXL price sync.

Reads tier-based pricing config from Supabase (hub_settings.product_automation_pricing).
Reads per-SKU state from vidaxl_pricing_state.

Per-SKU logic:
  - SKU not in vidaxl_pricing_state    -> SKIP (waiting for migration; do not push old behaviour)
  - status='on_sale'                    -> SKIP (FROZEN — Omnibus 30/60-day rule means we cannot
                                                 change price or compare_at_price during sale)
  - status='warmup' or 'normal'         -> recompute normal_price; emit UPDATE if it changed.
                                           Variant Compare At Price always '' (empty).
                                           For 'warmup', if normal_price changes, reset
                                           warmup_complete_at — visible price change starts a
                                           new reference period.

Exits non-zero if Supabase config cannot be loaded — no silent fallback to flat 1.7x.
"""
import csv
import os
import sys
import json
from datetime import datetime, timedelta, timezone
from io import StringIO

import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pricing

VIDAXL_URL = "https://feed.vidaxl.io/api/v1/feeds/download/f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping_offer.csv"
WARMUP_DAYS = 60
SHOP_SKUS_PATH = "output/shop_skus.json"
OUTPUT_PATH = "output/price_updates.csv"


def get_supabase_client():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        return None
    from supabase import create_client
    return create_client(url, key)


def load_shop_skus():
    try:
        with open(SHOP_SKUS_PATH, "r") as f:
            data = json.load(f)
        return set(str(s) for s in data["skus"])
    except Exception as e:
        print(f"❌ Could not load shop SKUs from {SHOP_SKUS_PATH}: {e}")
        return set()


def load_pricing_state(sb):
    """Fetch all rows from vidaxl_pricing_state. Returns dict keyed by sku.

    Paginates by 1000-row batches because Supabase REST has a default cap.
    """
    all_rows = []
    page = 0
    page_size = 1000
    while True:
        res = (
            sb.table("vidaxl_pricing_state")
            .select("sku,pricing_group,status,b2b_cost,normal_price,sale_price,warmup_complete_at")
            .range(page * page_size, (page + 1) * page_size - 1)
            .execute()
        )
        if not res.data:
            break
        all_rows.extend(res.data)
        if len(res.data) < page_size:
            break
        page += 1
    return {r["sku"]: r for r in all_rows}


FEED_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/csv,application/octet-stream,*/*",
    "Accept-Language": "da-DK,da;q=0.9,en;q=0.8",
}


def fetch_vidaxl_feed():
    """Fetch the VidaXL B2B offer feed. Retries with backoff on 403/429/5xx."""
    import time
    last_err = None
    for attempt in range(1, 5):
        try:
            response = requests.get(VIDAXL_URL, headers=FEED_HEADERS, timeout=180)
            if response.status_code in (403, 429, 500, 502, 503, 504):
                wait = 5 * attempt
                print(f"   feed responded {response.status_code} (attempt {attempt}/4) — retrying in {wait}s")
                time.sleep(wait)
                last_err = response
                continue
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text))
            df["SKU"] = df["SKU"].astype(str)
            df["B2B price"] = pd.to_numeric(df["B2B price"], errors="coerce")
            return df
        except requests.HTTPError as e:
            last_err = e
            wait = 5 * attempt
            print(f"   feed fetch failed: {e} (attempt {attempt}/4) — retrying in {wait}s")
            time.sleep(wait)
    if hasattr(last_err, "raise_for_status"):
        last_err.raise_for_status()
    raise RuntimeError(f"Feed fetch failed after 4 attempts: {last_err}")


def main():
    print(f"🚀 Price Sync started at {datetime.now(timezone.utc).isoformat()}")

    sb = get_supabase_client()
    if sb is None:
        print("❌ SUPABASE_URL or SUPABASE_SERVICE_KEY missing — cannot run.")
        sys.exit(1)

    pricing_cfg = pricing.load_pricing_config(sb)
    if not pricing_cfg or not pricing_cfg.get("tiers"):
        print("❌ Pricing tiers not loaded from Supabase — refusing to run with fallback.")
        sys.exit(1)
    print(f"✅ Loaded {len(pricing_cfg['tiers'])} pricing tiers")

    state = load_pricing_state(sb)
    status_counts = {"warmup": 0, "normal": 0, "on_sale": 0}
    for r in state.values():
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1
    print(f"✅ Loaded {len(state)} rows fra vidaxl_pricing_state — status: {status_counts}")

    shop_skus = load_shop_skus()
    if not shop_skus:
        print("❌ No shop SKUs found.")
        sys.exit(1)
    print(f"✅ Loaded {len(shop_skus)} shop SKUs")

    feed_df = fetch_vidaxl_feed()
    shop_products = feed_df[feed_df["SKU"].isin(shop_skus)].copy()
    print(f"🎯 {len(shop_products)} feed-rows match shop SKUs")

    output_rows = []
    state_updates = []
    counters = {
        "skip_no_state": 0,
        "skip_on_sale_frozen": 0,
        "skip_unchanged": 0,
        "skip_invalid_b2b": 0,
        "update_normal": 0,
        "update_warmup": 0,
        "warmup_reset": 0,
    }

    now_iso = datetime.now(timezone.utc).isoformat()
    new_warmup_at = (datetime.now(timezone.utc) + timedelta(days=WARMUP_DAYS)).isoformat()

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
            continue  # Wait for migration to populate vidaxl_pricing_state

        status = product_state["status"]
        if status == "on_sale":
            counters["skip_on_sale_frozen"] += 1
            continue  # Frozen — must not change price or compare_at_price during sale

        new_normal = pricing.calculate_normal_price(b2b, pricing_cfg)
        new_sale = pricing.calculate_sale_price(b2b, pricing_cfg)

        old_normal = int(float(product_state.get("normal_price") or 0))
        old_b2b = float(product_state.get("b2b_cost") or 0)

        normal_unchanged = int(new_normal) == old_normal
        b2b_unchanged = abs(b2b - old_b2b) < 0.01
        if normal_unchanged and b2b_unchanged:
            counters["skip_unchanged"] += 1
            continue

        output_rows.append({
            "Variant SKU": sku,
            "Variant Price": new_normal,
            "Variant Compare At Price": "",
            "Variant Cost": b2b,
            "Variant Command": "UPDATE",
        })

        update = {
            "sku": sku,
            "pricing_group": product_state["pricing_group"],
            "status": status,
            "b2b_cost": b2b,
            "normal_price": new_normal,
            "sale_price": new_sale,
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
    # MERGE strategy: read existing CSV, layer today's diffs on top, write back.
    # This protects migration / rotation rows from being clobbered by a small
    # daily diff before Matrixify has had time to import. Matrixify treats
    # repeat UPDATE rows as idempotent no-ops, so a stable full-catalog snapshot
    # in the CSV is safe.
    fieldnames = [
        "Variant SKU", "Variant Price", "Variant Compare At Price",
        "Variant Cost", "Variant Command",
    ]
    existing_rows = {}
    os.makedirs("output", exist_ok=True)
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                sku = r.get("Variant SKU")
                if sku:
                    existing_rows[sku] = r
    print(f"📂 Existing CSV: {len(existing_rows)} rows")

    merged = dict(existing_rows)
    for r in output_rows:
        merged[r["Variant SKU"]] = r

    # Prune SKUs that no longer exist in the shop (deleted products)
    merged = {sku: row for sku, row in merged.items() if sku in shop_skus}

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for sku in sorted(merged.keys()):
            writer.writerow({k: merged[sku].get(k, "") for k in fieldnames})
    print(f"✅ Wrote {len(merged)} rows ({len(existing_rows)} existing + {len(output_rows)} new diffs merged, "
          f"after pruning to current shop_skus)")

    if state_updates:
        # Upsert in batches of 500 to stay within Supabase API limits
        batch_size = 500
        total_upserted = 0
        for i in range(0, len(state_updates), batch_size):
            batch = state_updates[i:i + batch_size]
            res = sb.table("vidaxl_pricing_state").upsert(
                batch, on_conflict="sku"
            ).execute()
            total_upserted += len(res.data) if res.data else 0
        print(f"💾 Updated {total_upserted} rows in vidaxl_pricing_state")
    else:
        print("💾 No state updates needed")


if __name__ == "__main__":
    main()
