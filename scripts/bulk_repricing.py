"""Bulk-repricing operation triggered fra hub Katalog Engine.

Bruges når en pricing-regel er aendret OG bruger klikker
"Opdater eksisterende produkter" i UI'et — for at presse ud nu i stedet for
at vente på næste daglige sync_prices_v2-run.

Flow:
  1. Hent job-row fra pricing_bulk_jobs (skal vaere status='pending')
  2. Opdater til status='running' + started_at + github_run_id
  3. Hent alle Shopify-variants matchende vendor + product_type
  4. For hver: beregn nye priser via pricing-engine (med hierarki-match)
  5. Apply via Shopify Bulk Operations (samme mønster som sync_prices_v2)
  6. Opdater status='completed' + actual_count + completed_at

Respekterer on_sale-frozen logic: status='on_sale' SKUs i vidaxl_pricing_state
opdateres IKKE (samme som sync_prices_v2 — tilbudspriser fryses indtil
rotation flytter dem væk fra on_sale).

Argumenter:
  --job-id UUID     pricing_bulk_jobs.id der eksekveres
  --vendor STR      Shopify vendor filter
  --product-type STR  Optional Shopify productType filter (alle types hvis ikke sat)
  --dry-run         Kør uden at apply (preview-mode — actual_count = preview_count)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

# Import vores opdaterede pricing-modul
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pricing import (
    calculate_normal_price,
    calculate_sale_price,
    load_pricing_config,
    normalize_sku,
)

# === CONFIG ============================================================
SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE") or os.environ.get("SHOPIFY_STORE_URL", "")
SHOPIFY_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2024-10/graphql.json"
HEADERS = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}

BULK_THRESHOLD = 200      # >=200 changes -> bulk operation; else direct mutations
BATCH_SIZE = 100          # variants per bulk-update batch


# === SUPABASE CLIENT ===================================================

def _supabase():
    from supabase import create_client
    if not SUPABASE_URL or not SUPABASE_KEY:
        sys.exit("❌ Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _update_job(sb, job_id, **fields):
    fields.setdefault("updated_at", datetime.now(timezone.utc).isoformat())
    try:
        sb.table("pricing_bulk_jobs").update(fields).eq("id", job_id).execute()
    except Exception as e:
        print(f"⚠ Could not update job {job_id}: {e}")


# === SHOPIFY GRAPHQL ===================================================

def gql(query, variables=None, retries=4):
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    for attempt in range(1, retries + 1):
        r = requests.post(GRAPHQL, headers=HEADERS, json=payload, timeout=120)
        r.raise_for_status()
        d = r.json()
        if "errors" in d:
            throttled = any("Throttled" in str(e) or "THROTTLED" in str(e) for e in d["errors"])
            if throttled and attempt < retries:
                time.sleep(2 ** attempt)
                continue
            raise Exception(f"GraphQL errors: {d['errors']}")
        cost = d.get("extensions", {}).get("cost", {}).get("throttleStatus", {})
        if cost.get("currentlyAvailable", 1000) < 200:
            time.sleep(0.5)
        return d
    raise Exception("Max retries exceeded")


def fetch_products_for_vendor_type(vendor: str, product_type: str | None) -> list[dict]:
    """Henter alle produkter + variants matching filter.

    Returnerer list[{product_id, variants: [{id, sku, price, compareAtPrice, cost}, ...]}]
    """
    query = f'vendor:"{vendor}"'
    if product_type:
        query += f' AND product_type:"{product_type}"'

    products = []
    cursor = None
    while True:
        q = """
        query($cursor: String, $query: String!) {
          products(first: 100, after: $cursor, query: $query) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                id title productType vendor
                variants(first: 100) {
                  edges {
                    node {
                      id sku price compareAtPrice
                      inventoryItem { unitCost { amount } }
                    }
                  }
                }
              }
            }
          }
        }
        """
        d = gql(q, {"cursor": cursor, "query": query})
        page = d["data"]["products"]
        for e in page["edges"]:
            p = e["node"]
            variants = []
            for ve in p["variants"]["edges"]:
                v = ve["node"]
                cost_amount = (v.get("inventoryItem") or {}).get("unitCost") or {}
                variants.append({
                    "id": v["id"],
                    "sku": normalize_sku(v["sku"]),
                    "price": float(v["price"]) if v["price"] else 0,
                    "compareAtPrice": float(v["compareAtPrice"]) if v["compareAtPrice"] else None,
                    "cost": float(cost_amount.get("amount", 0)) if cost_amount else 0,
                })
            products.append({
                "id": p["id"],
                "title": p["title"],
                "vendor": p["vendor"],
                "product_type": p["productType"],
                "variants": variants,
            })
        if not page["pageInfo"]["hasNextPage"]:
            break
        cursor = page["pageInfo"]["endCursor"]
        time.sleep(0.2)
    return products


def load_pricing_states(sb, skus: list[str]) -> dict:
    """Hent on_sale/warmup state for SKUs fra vidaxl_pricing_state."""
    state_by_sku = {}
    BATCH = 500
    for i in range(0, len(skus), BATCH):
        batch = skus[i:i + BATCH]
        try:
            res = (
                sb.table("vidaxl_pricing_state")
                .select("sku,status")
                .in_("sku", batch)
                .execute()
            )
            for row in res.data or []:
                state_by_sku[row["sku"]] = row.get("status", "normal")
        except Exception as e:
            print(f"⚠ Failed to load pricing_state batch {i}: {e}")
    return state_by_sku


# === MAIN ==============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--vendor", required=True)
    parser.add_argument("--product-type", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = _supabase()

    # 1. Marker job som running
    _update_job(sb, args.job_id,
                status="running",
                started_at=datetime.now(timezone.utc).isoformat(),
                github_run_id=int(os.environ.get("GITHUB_RUN_ID", "0")) or None,
                github_run_url=os.environ.get("GITHUB_RUN_URL") or None,
                log_summary="Henter produkter fra Shopify...")

    try:
        # 2. Hent matching produkter (med deres productType per produkt)
        products = fetch_products_for_vendor_type(args.vendor, args.product_type)
        all_variants = []
        for p in products:
            for v in p["variants"]:
                if not v["sku"]:
                    continue
                # Vedhaeft product_type til hver variant saa vi kan loade
                # hierarki-korrekt config per produkt
                v = {**v, "product_type": p["product_type"]}
                all_variants.append(v)
        print(f"✅ {len(products)} produkter, {len(all_variants)} variants")
        _update_job(sb, args.job_id,
                    preview_count=len(all_variants),
                    log_summary=f"Behandler {len(all_variants)} variants...")

        # 3. Cache pricing config PR product_type — undgaar 50k Supabase-calls
        config_cache = {}  # product_type (string|None) -> config dict
        def cfg_for(product_type):
            key = product_type or "__VENDOR_GENERAL__"
            if key in config_cache:
                return config_cache[key]
            cfg = load_pricing_config(sb, vendor=args.vendor, product_type=product_type)
            config_cache[key] = cfg
            return cfg

        # 4. Hent on_sale-state
        skus = [v["sku"] for v in all_variants]
        state_by_sku = load_pricing_states(sb, skus)

        # 5. Beregn changes — hver variant bruger sin egen hierarki-match
        # Dette sikrer at hvis brugeren har en override paa fx Havemoebler,
        # vil disse produkter bruge override-config selvom vi koerer en bulk-update
        # for hele vendor'en.
        changes = []
        skipped_on_sale = 0
        skipped_no_cost = 0
        skipped_no_config = 0
        skipped_unchanged = 0
        for v in all_variants:
            sku = v["sku"]
            state = state_by_sku.get(sku, "normal")
            if state == "on_sale":
                skipped_on_sale += 1
                continue
            if not v["cost"] or v["cost"] <= 0:
                skipped_no_cost += 1
                continue

            # Per-variant config-match — KEY FIX: type-specific override respekteres
            variant_config = cfg_for(v.get("product_type"))
            if not variant_config:
                skipped_no_config += 1
                continue

            new_price = calculate_normal_price(v["cost"], variant_config)
            if new_price <= 0:
                continue
            current_price = int(v["price"])
            if new_price == current_price:
                skipped_unchanged += 1
                continue
            changes.append({
                "variant_id": v["id"],
                "sku": sku,
                "old_price": current_price,
                "new_price": new_price,
                "compare_at_price": None,
            })

        print(f"📊 {len(changes)} pris-aendringer planlagt (skip on_sale={skipped_on_sale}, no_cost={skipped_no_cost}, no_config={skipped_no_config}, unchanged={skipped_unchanged})")
        print(f"📋 Config-cache rammet for {len(config_cache)} unikke product_types")

        # 6. DRY-RUN: stop her med preview-stats
        if args.dry_run:
            _update_job(sb, args.job_id,
                        status="completed",
                        actual_count=0,
                        log_summary=f"DRY-RUN: {len(changes)} aendringer ville blive applied",
                        completed_at=datetime.now(timezone.utc).isoformat())
            print(f"✅ DRY-RUN done. {len(changes)} changes would be applied.")
            return 0

        # 7. APPLY changes — bulk update via productVariantsBulkUpdate
        # Vi grupperer per produkt (mutation kraever productId + variants).
        applied = 0
        failed = 0
        changes_by_product = {}
        for c in changes:
            # Find product id for variant
            for p in products:
                if any(v["id"] == c["variant_id"] for v in p["variants"]):
                    changes_by_product.setdefault(p["id"], []).append(c)
                    break

        total_products = len(changes_by_product)
        print(f"🚀 Applying {len(changes)} aendringer paa {total_products} produkter")

        for idx, (product_id, prod_changes) in enumerate(changes_by_product.items(), 1):
            mutation = """
            mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
              productVariantsBulkUpdate(productId: $productId, variants: $variants) {
                productVariants { id sku price }
                userErrors { field message }
              }
            }
            """
            variants_input = [
                {"id": c["variant_id"], "price": str(c["new_price"]), "compareAtPrice": None}
                for c in prod_changes
            ]
            try:
                d = gql(mutation, {"productId": product_id, "variants": variants_input})
                res = d["data"]["productVariantsBulkUpdate"]
                errs = res.get("userErrors") or []
                if errs:
                    failed += len(prod_changes)
                    print(f"  [{idx}/{total_products}] ❌ {product_id}: {errs[:1]}")
                else:
                    applied += len(prod_changes)
            except Exception as e:
                failed += len(prod_changes)
                print(f"  [{idx}/{total_products}] ❌ {product_id}: {str(e)[:100]}")

            # Periodically update job status
            if idx % 50 == 0:
                _update_job(sb, args.job_id,
                            actual_count=applied,
                            failed_count=failed,
                            log_summary=f"Behandlet {idx}/{total_products} produkter ({applied} OK, {failed} fejl)")

        # 8. Final job update
        _update_job(sb, args.job_id,
                    status="completed",
                    actual_count=applied,
                    failed_count=failed,
                    log_summary=f"Done. {applied} opdateret, {failed} fejl, {skipped_on_sale} on_sale, {skipped_no_cost} no_cost, {skipped_no_config} no_config, {skipped_unchanged} unchanged",
                    completed_at=datetime.now(timezone.utc).isoformat())

        print(f"✅ DONE. Applied={applied}, Failed={failed}")
        return 0 if failed == 0 else 1

    except Exception as e:
        import traceback
        err_msg = f"FATAL: {str(e)[:500]}"
        print(f"❌ {err_msg}")
        print(traceback.format_exc())
        _update_job(sb, args.job_id,
                    status="failed",
                    log_summary=err_msg,
                    completed_at=datetime.now(timezone.utc).isoformat())
        return 1


if __name__ == "__main__":
    sys.exit(main())
