"""Bulk-repricing operation triggered fra hub Katalog Engine.

Bruges når en pricing-regel er ændret OG bruger klikker "Opdater eksisterende
produkter" i UI'et — for at presse de nye priser ud NU i stedet for at vente
på næste daglige sync_prices_v2-run.

Designvalg (låst med Gabriel 2026-06-09):
  - Påvirker ALLE produkter for vendor/(type) — også dem der er on_sale NU.
  - Ændrer IKKE rotation (hvilke produkter der er på tilbud). Status bevares.
  - on_sale: genberegner BÅDE tilbudsprisen OG "før"-prisen (compareAt = ny
    normal) — "genberegn begge frit" (ingen Omnibus-guard; OK fordi markup blev
    sænket. Hvis markup senere hæves, hæves før-prisen — tilføj sænk-kun-guard).
  - Edge case: hvis ny sale >= ny normal → meningsløst tilbud, ryd compareAt.

Performance: GENBRUGER sync_prices_v2's hurtige datasti i stedet for at
paginere ~320 GraphQL-kald (gammel version brugte 53 min bare på at hente):
  - load_shop_cache()      → sku→variant-map (forudbygget, sekunder)
  - fetch_supplier_feed()  → b2b-kostpriser
  - push_to_shopify()      → Shopify Bulk Operations (skalerer til 100k+)

Status-tracking: pricing_bulk_jobs opdateres undervejs. (Tidligere bug:
_update_job skrev kolonnen `updated_at` som ikke findes → hver skrivning
fejlede → jobbet hang på "pending". Fjernet her.)

Argumenter:
  --job-id UUID       pricing_bulk_jobs.id der eksekveres
  --vendor STR        Shopify vendor filter (fx "vidaXL")
  --product-type STR  Optional Shopify productType filter (alle types hvis tom)
  --dry-run           Beregn + rapportér uden at pushe (preview)
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

from pricing import (
    calculate_normal_price,
    calculate_sale_price,
    load_pricing_config,
)
from sync_prices_v2 import (
    fetch_supplier_feed,
    get_supabase_client,
    load_pricing_state,
    load_shop_cache,
    push_to_shopify,
    upsert_state,
)


def _now():
    return datetime.now(timezone.utc).isoformat()


def _update_job(sb, job_id, **fields):
    """Opdater pricing_bulk_jobs. BEMÆRK: ingen `updated_at` — den kolonne
    findes ikke i tabellen, og at sende den fik hele opdateringen til at fejle."""
    try:
        sb.table("pricing_bulk_jobs").update(fields).eq("id", job_id).execute()
    except Exception as e:
        print(f"⚠ Could not update job {job_id}: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--vendor", required=True)
    parser.add_argument("--product-type", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    target_vendor = args.vendor
    target_type = args.product_type or None
    mode = "DRY-RUN" if args.dry_run else "LIVE"
    print(f"🚀 bulk_repricing — {mode} — vendor={target_vendor} type={target_type or '(alle)'}")

    sb = get_supabase_client()
    if sb is None:
        sys.exit("❌ Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")

    job_id = args.job_id
    _update_job(sb, job_id,
                status="running",
                started_at=_now(),
                github_run_id=int(os.environ.get("GITHUB_RUN_ID", "0")) or None,
                github_run_url=os.environ.get("GITHUB_RUN_URL") or None,
                log_summary="Henter cache + feed...")

    try:
        # 1. Load data (samme hurtige datasti som dagssyncen)
        state = load_pricing_state(sb)
        print(f"✅ {len(state)} rows fra vidaxl_pricing_state")
        cache = load_shop_cache()
        shop_skus = set(cache["skus"])
        variants_map = cache["variants"]
        product_types_by_sku = cache.get("product_types", {})
        vendors_by_sku = cache.get("vendors", {})
        print(f"📦 Cache: {len(shop_skus)} SKUs")
        feed_df = fetch_supplier_feed()

        # 2. Config-resolver pr. (vendor, product_type) — hierarki via pricing_rules
        default_cfg = load_pricing_config(sb)
        if not default_cfg or not default_cfg.get("tiers"):
            sys.exit("❌ Default pricing-config ikke loaded fra Supabase")
        _cfg_cache = {}

        def resolve_cfg(vendor, ptype):
            key = (vendor, ptype or "__none__")
            if key not in _cfg_cache:
                _cfg_cache[key] = (
                    load_pricing_config(sb, vendor=vendor, product_type=ptype)
                    or default_cfg
                )
            return _cfg_cache[key]

        # 3. Beregn ændringer (kun SKUs for valgt vendor + evt. product_type)
        today_rows, on_sale_rows, state_updates = [], [], []
        c = {"normal": 0, "warmup": 0, "on_sale": 0, "on_sale_edge_cleared": 0,
             "skip_unchanged": 0, "skip_no_state": 0, "skip_no_cost": 0,
             "skip_filter": 0, "skip_no_variant": 0}

        shop_feed = feed_df[feed_df["SKU"].astype(str).isin(shop_skus)]
        print(f"🎯 {len(shop_feed)} feed-rows matcher shop")

        for _, row in shop_feed.iterrows():
            sku = str(row["SKU"]).strip()
            b2b = row["B2B price"]
            if pd.isna(b2b) or b2b <= 0:
                c["skip_no_cost"] += 1
                continue
            b2b = float(b2b)

            vendor = vendors_by_sku.get(sku) or "vidaXL"
            ptype = product_types_by_sku.get(sku) or None
            if vendor != target_vendor:
                c["skip_filter"] += 1
                continue
            if target_type is not None and ptype != target_type:
                c["skip_filter"] += 1
                continue

            st = state.get(sku)
            if st is None:
                c["skip_no_state"] += 1
                continue
            if sku not in variants_map:
                c["skip_no_variant"] += 1
                continue

            cfg = resolve_cfg(vendor, ptype)
            new_normal = calculate_normal_price(b2b, cfg)
            if not new_normal or new_normal <= 0:
                c["skip_no_cost"] += 1
                continue

            status = st["status"]
            old_normal = int(float(st.get("normal_price") or 0))
            old_sale = int(float(st.get("sale_price") or 0))

            if status == "on_sale":
                new_sale = calculate_sale_price(b2b, cfg)
                new_sale_int = int(new_sale) if new_sale else 0
                if new_sale_int == old_sale and new_normal == old_normal:
                    c["skip_unchanged"] += 1
                    continue
                # Rule-change: genberegn BÅDE sale OG før-pris (compareAt = ny normal).
                # Edge case: sale >= normal → meningsløst tilbud, ryd compareAt.
                edge = new_sale_int > 0 and new_sale_int >= new_normal
                on_sale_rows.append({
                    "Variant SKU": sku,
                    "Variant Price": new_sale_int,
                    "Variant Cost": b2b,
                    "Compare At Action": "CLEAR" if edge else "SET",
                    "Set Compare At": new_normal,
                    "Locked Compare At": new_normal,
                    "Variant Command": "UPDATE",
                })
                state_updates.append({
                    "sku": sku,
                    "pricing_group": st["pricing_group"],
                    "status": "on_sale",
                    "b2b_cost": b2b,
                    "normal_price": new_normal,
                    "sale_price": new_sale_int,
                })
                c["on_sale"] += 1
                if edge:
                    c["on_sale_edge_cleared"] += 1
            else:
                # normal / warmup
                if new_normal == old_normal:
                    c["skip_unchanged"] += 1
                    continue
                new_sale = calculate_sale_price(b2b, cfg)
                today_rows.append({
                    "Variant SKU": sku,
                    "Variant Price": new_normal,
                    "Variant Compare At Price": "",
                    "Variant Cost": b2b,
                    "Variant Command": "UPDATE",
                })
                state_updates.append({
                    "sku": sku,
                    "pricing_group": st["pricing_group"],
                    "status": status,
                    "b2b_cost": b2b,
                    "normal_price": new_normal,
                    "sale_price": new_sale,
                })
                c["warmup" if status == "warmup" else "normal"] += 1

        total_changes = len(today_rows) + len(on_sale_rows)
        summary = (f"normal={c['normal']} warmup={c['warmup']} on_sale={c['on_sale']} "
                   f"(edge_cleared={c['on_sale_edge_cleared']}) "
                   f"skip_unchanged={c['skip_unchanged']} skip_filter={c['skip_filter']} "
                   f"skip_no_state={c['skip_no_state']} skip_no_cost={c['skip_no_cost']} "
                   f"skip_no_variant={c['skip_no_variant']}")
        print(f"📊 {total_changes} ændringer planlagt — {summary}")
        print(f"📋 Config-cache: {len(_cfg_cache)} unikke (vendor, type)")
        _update_job(sb, job_id, preview_count=total_changes,
                    log_summary=f"Planlagt {total_changes}: {summary}")

        # 4. DRY-RUN: stop med preview-stats
        if args.dry_run:
            _update_job(sb, job_id,
                        status="completed",
                        actual_count=0,
                        log_summary=f"DRY-RUN: {total_changes} ville ændres — {summary}",
                        completed_at=_now())
            print(f"✅ DRY-RUN done. {total_changes} ændringer ville blive pushed.")
            return 0

        # 5. LIVE: ingen ændringer?
        if total_changes == 0:
            _update_job(sb, job_id,
                        status="completed",
                        actual_count=0,
                        log_summary=f"Ingen ændringer — {summary}",
                        completed_at=_now())
            print("✅ Ingen ændringer at pushe.")
            return 0

        # 6. LIVE: push via Shopify (auto Niveau 2/3) + gem state
        stats = push_to_shopify(today_rows, on_sale_rows, variants_map)
        print(f"📊 STATS: {stats}")
        applied = stats.get("variants_updated", 0)
        errors = stats.get("errors", 0)

        # Gem state EFTER push (så en fejlet push ikke efterlader state foran Shopify)
        upsert_state(sb, state_updates)

        # Et par enkelte produkt-fejl (fx en slettet variant mellem cache-bygning
        # og nu) skal IKKE markere hele kørslen som fejlet — bulk-operationen
        # fuldførte. Vi tolererer op til 1% produkt-fejl som "completed" (med
        # failed_count registreret); derover = systemisk problem → failed.
        # Catastrofale fejl (submit/timeout/bulk FAILED) kastes og fanges nedenfor.
        total_attempted = applied + errors
        error_rate = errors / total_attempted if total_attempted else 0
        ok = error_rate <= 0.01
        _update_job(sb, job_id,
                    status="completed" if ok else "failed",
                    actual_count=applied,
                    failed_count=errors,
                    log_summary=f"{'Done' if ok else 'FAILED'}. {applied} opdateret, "
                                f"{errors} fejl ({error_rate:.2%}) — {summary}",
                    completed_at=_now())
        print(f"{'✅ DONE' if ok else '❌ FAILED'}. Applied={applied}, Errors={errors} ({error_rate:.2%})")
        return 0 if ok else 1

    except Exception as e:
        import traceback
        err_msg = f"FATAL: {str(e)[:500]}"
        print(f"❌ {err_msg}")
        print(traceback.format_exc())
        _update_job(sb, job_id, status="failed", log_summary=err_msg, completed_at=_now())
        return 1


if __name__ == "__main__":
    sys.exit(main())
