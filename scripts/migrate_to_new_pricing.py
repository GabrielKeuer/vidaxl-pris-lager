"""
Day 0 migration: populate vidaxl_pricing_state for ALL shop VidaXL products and
activate group A's first sale.

This is the one-time migration that takes Shopify from "buggy 1.7x flat
pricing with fake compare_at_price on everything" to:
  - All VidaXL products: status='normal', new tier-based normal_price,
    compare_at_price cleared.
  - Random 33% (group A): status='on_sale', price=sale_price,
    compare_at_price=normal_price.

Idempotent — safe to re-run:
  - Existing state rows are PRESERVED. warmup/on_sale stay as-is; normal gets
    prices refreshed if changed.
  - Group A activation only flips products currently in 'normal' status.
  - Repeated runs do not reset warmup_complete_at or
    last_normal_period_started_at on existing rows.

Required env: SUPABASE_URL, SUPABASE_SERVICE_KEY.
Required file: output/shop_skus.json (run "Update Shop Cache" workflow first).
"""
import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pricing

VIDAXL_URL = "https://feed.vidaxl.io/api/v1/feeds/download/f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping_offer.csv"
SHOP_SKUS_PATH = "output/shop_skus.json"
OUTPUT_PATH = "output/migration_updates.csv"
ROTATION_STATE_KEY = "vidaxl_rotation_state"
INITIAL_ACTIVE_GROUP = "A"


def get_supabase_client():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        return None
    from supabase import create_client
    return create_client(url, key)


def load_shop_skus():
    with open(SHOP_SKUS_PATH, "r") as f:
        data = json.load(f)
    return set(str(s) for s in data["skus"])


FEED_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/csv,application/octet-stream,*/*",
    "Accept-Language": "da-DK,da;q=0.9,en;q=0.8",
}


def fetch_vidaxl_feed():
    """Fetch the VidaXL B2B offer feed. Retries with backoff on 403/429/5xx."""
    import time
    print("📥 Henter VidaXL feed...")
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


def load_existing_state(sb):
    """Load all existing rows from vidaxl_pricing_state, paginated."""
    rows = []
    page_size = 1000
    page = 0
    while True:
        res = (
            sb.table("vidaxl_pricing_state")
            .select("sku,pricing_group,status,b2b_cost,normal_price,sale_price,warmup_complete_at")
            .range(page * page_size, (page + 1) * page_size - 1)
            .execute()
        )
        if not res.data:
            break
        rows.extend(res.data)
        if len(res.data) < page_size:
            break
        page += 1
    return {r["sku"]: r for r in rows}


def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute and write CSV but do not update Supabase or save rotation state")
    parser.add_argument("--limit", type=int, default=0,
                        help="Process only the first N matching SKUs (for smoke testing)")
    parser.add_argument("--skip-activate", action="store_true",
                        help="Populate state but do NOT flip group A to on_sale (Day 0 in two stages)")
    args = parser.parse_args(argv)

    print(f"🚚 Migration started at {datetime.now(timezone.utc).isoformat()}"
          f"{' [DRY-RUN]' if args.dry_run else ''}")
    if args.limit:
        print(f"   --limit {args.limit} (smoke test mode)")

    sb = get_supabase_client()
    if sb is None:
        print("❌ SUPABASE_URL / SUPABASE_SERVICE_KEY missing.")
        sys.exit(1)

    pricing_cfg = pricing.load_pricing_config(sb)
    if not pricing_cfg or not pricing_cfg.get("tiers"):
        print("❌ Pricing tiers ikke loaded fra Supabase.")
        sys.exit(1)
    print(f"✅ {len(pricing_cfg['tiers'])} pricing tiers loaded")

    shop_skus = load_shop_skus()
    print(f"✅ {len(shop_skus)} shop SKUs loaded")

    feed_df = fetch_vidaxl_feed()
    print(f"✅ {len(feed_df)} feed rows fetched")

    feed_df = feed_df[feed_df["SKU"].isin(shop_skus)].copy()
    feed_df = feed_df[(feed_df["B2B price"].notna()) & (feed_df["B2B price"] > 0)]
    print(f"🎯 {len(feed_df)} VidaXL products in shop with valid B2B price")

    if args.limit:
        feed_df = feed_df.head(args.limit)
        print(f"   → trimmed to {len(feed_df)} (--limit)")

    existing = load_existing_state(sb)
    print(f"📋 {len(existing)} rows already in vidaxl_pricing_state — they will be PRESERVED")

    now_iso = datetime.now(timezone.utc).isoformat()

    insert_records = []   # status='normal' rows for SKUs not yet in state
    update_records = []   # price refreshes for SKUs already in state (status preserved)
    csv_rows = []         # what Matrixify will push to Shopify

    counters = {
        "new_normal": 0, "preserved_warmup": 0, "preserved_on_sale": 0,
        "preserved_normal_refreshed": 0, "preserved_normal_unchanged": 0,
    }

    for _, row in feed_df.iterrows():
        sku = str(row["SKU"]).strip()
        b2b = float(row["B2B price"])
        normal = pricing.calculate_normal_price(b2b, pricing_cfg)
        sale = pricing.calculate_sale_price(b2b, pricing_cfg)
        group = pricing.assign_group(sku)

        prior = existing.get(sku)

        if prior is None:
            # New row: status='normal', no warmup (existing Shopify product)
            insert_records.append({
                "sku": sku,
                "pricing_group": group,
                "status": "normal",
                "b2b_cost": b2b,
                "normal_price": normal,
                "sale_price": sale,
                "warmup_complete_at": None,
                "last_normal_period_started_at": now_iso,
                "last_status_change_at": now_iso,
            })
            counters["new_normal"] += 1
        else:
            status = prior["status"]
            old_normal = int(float(prior.get("normal_price") or 0))
            old_b2b = float(prior.get("b2b_cost") or 0)
            normal_changed = int(normal) != old_normal
            b2b_changed = abs(b2b - old_b2b) > 0.01

            if status in ("warmup", "on_sale"):
                # Preserve status; refresh prices ONLY if changed (and only outside frozen on_sale)
                if status == "warmup" and (normal_changed or b2b_changed):
                    update_records.append({
                        "sku": sku,
                        "pricing_group": prior["pricing_group"],
                        "status": prior["status"],
                        "b2b_cost": b2b,
                        "normal_price": normal,
                        "sale_price": sale,
                    })
                if status == "warmup":
                    counters["preserved_warmup"] += 1
                else:
                    counters["preserved_on_sale"] += 1
                # Do NOT emit CSV row for these (warmup keeps current Shopify state;
                # on_sale must remain frozen during sale period)
                continue
            else:
                # status='normal': refresh prices if changed
                if normal_changed or b2b_changed:
                    update_records.append({
                        "sku": sku,
                        "pricing_group": prior["pricing_group"],
                        "status": prior["status"],
                        "b2b_cost": b2b,
                        "normal_price": normal,
                        "sale_price": sale,
                    })
                    counters["preserved_normal_refreshed"] += 1
                else:
                    counters["preserved_normal_unchanged"] += 1

        # Emit CSV row for new + preserved-normal SKUs (push to Shopify)
        csv_rows.append({
            "Variant SKU": sku,
            "Variant Price": normal,
            "Variant Compare At Price": "",
            "Variant Cost": b2b,
            "Variant Command": "UPDATE",
            "_pricing_group": group,
            "_normal": normal,
            "_sale": sale,
        })

    print(f"📊 Status counts: {counters}")
    print(f"   → {len(insert_records)} new INSERT rows")
    print(f"   → {len(update_records)} price-refresh UPDATE rows (status preserved)")
    print(f"   → {len(csv_rows)} CSV rows for Matrixify (excluding warmup/on_sale)")

    # ---- Write Supabase state (unless dry-run) ----
    if not args.dry_run:
        if insert_records:
            for batch in chunked(insert_records, 500):
                sb.table("vidaxl_pricing_state").upsert(
                    batch, on_conflict="sku", ignore_duplicates=True
                ).execute()
            print(f"💾 Inserted {len(insert_records)} new state rows")
        if update_records:
            for batch in chunked(update_records, 500):
                sb.table("vidaxl_pricing_state").upsert(
                    batch, on_conflict="sku"
                ).execute()
            print(f"💾 Refreshed prices on {len(update_records)} existing rows")

    # ---- Activate group A (flip status='normal' → 'on_sale' and rewrite their CSV row) ----
    activated = 0
    if not args.skip_activate:
        # Re-fetch state (post-insert) to find current 'normal' rows in group A
        if not args.dry_run:
            current_state = load_existing_state(sb)
        else:
            # In dry-run, simulate from in-memory data
            current_state = dict(existing)
            for r in insert_records:
                current_state[r["sku"]] = r

        # Filter: in CSV (eligible), pricing_group=A, status='normal' (not warmup/on_sale)
        csv_skus = {r["Variant SKU"] for r in csv_rows}
        flip_records = []
        for sku, st in current_state.items():
            if sku not in csv_skus:
                continue
            if st.get("pricing_group") != INITIAL_ACTIVE_GROUP:
                continue
            if st.get("status") != "normal":
                continue
            flip_records.append({
                "sku": sku,
                "pricing_group": st["pricing_group"],
                "status": "on_sale",
                "last_status_change_at": now_iso,
            })

        # Replace CSV rows for flipped SKUs to use sale pricing
        flip_skus = {r["sku"] for r in flip_records}
        for r in csv_rows:
            if r["Variant SKU"] in flip_skus:
                r["Variant Price"] = r["_sale"]
                r["Variant Compare At Price"] = r["_normal"]

        activated = len(flip_records)
        print(f"🅰️  Activating group {INITIAL_ACTIVE_GROUP}: {activated} products flipped to on_sale")

        if not args.dry_run and flip_records:
            for batch in chunked(flip_records, 500):
                sb.table("vidaxl_pricing_state").upsert(batch, on_conflict="sku").execute()
            print(f"💾 Flipped {activated} products to on_sale in state")

            # Save rotation_state so future rotations know A is active
            rotation_state = {
                "active_group": INITIAL_ACTIVE_GROUP,
                "previous_group": None,
                "last_rotated_at": now_iso,
                "rotation_id": f"day0-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}",
                "rotation_count": 1,
                "ended_count": 0,
                "started_count": activated,
                "skipped_warmup": 0,
            }
            sb.table("hub_settings").upsert({
                "key": ROTATION_STATE_KEY,
                "value": rotation_state,
                "description": "VidaXL group rotation state — managed by rotate_groups.py",
            }, on_conflict="key").execute()
            print(f"💾 Rotation state saved: active_group={INITIAL_ACTIVE_GROUP}")

    # ---- Write CSV ----
    os.makedirs("output", exist_ok=True)
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "Variant SKU", "Variant Price", "Variant Compare At Price",
            "Variant Cost", "Variant Command",
        ])
        writer.writeheader()
        for r in csv_rows:
            writer.writerow({k: r[k] for k in [
                "Variant SKU", "Variant Price", "Variant Compare At Price",
                "Variant Cost", "Variant Command",
            ]})
    print(f"📝 Wrote {len(csv_rows)} rows to {OUTPUT_PATH}")

    # ---- Summary ----
    print("─" * 60)
    print("MIGRATION SUMMARY")
    print(f"  Total CSV rows pushed:    {len(csv_rows)}")
    print(f"  Group A activated:        {activated}")
    print(f"  Preserved warmup:         {counters['preserved_warmup']}")
    print(f"  Preserved on_sale:        {counters['preserved_on_sale']}")
    if args.dry_run:
        print("  [DRY-RUN] No Supabase writes; no rotation state saved")


if __name__ == "__main__":
    main()
