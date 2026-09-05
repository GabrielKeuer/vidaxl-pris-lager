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
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

import pricing
from pricing import (
    calculate_normal_price,
    calculate_sale_price,
    load_pricing_config,
    resolve_variant_pricing,
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


# =============================================================================
# FICTIVE-MODE BULK (Benuta/Sollux/Kayoom) — ikke vidaXL-feed/rotation-baseret.
# Henter vendorens produkter direkte fra Shopify; cost = Variant Cost (matcher
# markup-basen pr. vendor); pris/førpris via resolve_variant_pricing(seed=handle).
# =============================================================================

def _shop_gql(query, variables=None):
    # Accepter begge env-navne (hub/cron bruger SHOPIFY_STORE_URL) + strip evt. scheme/slash.
    store = (os.environ.get("SHOPIFY_STORE_URL") or os.environ.get("SHOPIFY_STORE", "")).strip()
    store = store.replace("https://", "").replace("http://", "").rstrip("/")
    token = os.environ.get("SHOPIFY_ACCESS_TOKEN", "")
    if not store:
        raise RuntimeError("SHOPIFY_STORE_URL/SHOPIFY_STORE env mangler - kan ikke bygge Shopify-URL")
    url = f"https://{store}/admin/api/2024-10/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    data = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
    for attempt in range(1, 5):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers), timeout=120) as r:
                d = json.loads(r.read().decode())
        except Exception:
            if attempt < 4:
                time.sleep(2 ** attempt); continue
            raise
        if "errors" in d:
            if any("hrottl" in str(e).lower() for e in d["errors"]) and attempt < 4:
                time.sleep(2 ** attempt); continue
            raise Exception(f"GraphQL: {d['errors']}")
        return d
    raise Exception("Max retries")


def _gid_num(gid):
    """gid://shopify/.../123 -> 123 (int) eller None."""
    try:
        return int(str(gid).rsplit("/", 1)[-1])
    except (ValueError, TypeError, AttributeError):
        return None


# === BULK OPERATION QUERY (server-side katalog-eksport) =====================
# I stedet for at paginere ~3000 sider sekventielt (rate-limit-følsomt) beder vi
# Shopify eksportere ALLE vendor-produkter+varianter til én JSONL-fil server-side.
# Samme mekanik som Matrixify. Bulk QUERY er en separat type fra bulk MUTATION,
# så den daglige syncs mutation-bulk konflikter ikke med vores read-bulk.

_BULK_Q_RUN = """
mutation bulkOperationRunQuery($query: String!) {
  bulkOperationRunQuery(query: $query) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
"""
_BULK_Q_STATUS = """
query { currentBulkOperation(type: QUERY) { id status errorCode objectCount url } }
"""


def _wait_current_query_done(poll, max_wait_min):
    """Vent til en evt. kørende QUERY-bulk-op er færdig (kun én ad gangen pr. app+shop)."""
    waited = 0
    while waited <= max_wait_min * 60:
        s = _shop_gql(_BULK_Q_STATUS)["data"]["currentBulkOperation"]
        if not s or s["status"] not in ("CREATED", "RUNNING"):
            return
        time.sleep(poll); waited += poll
    raise Exception("Tidligere bulk-query blev ved med at køre — gav op")


def _bulk_export_vendor_products(vendor, ptype, poll=15, max_wait_min=45):
    """Eksportér alle (vendor[, product_type])-varianter via Bulk Operation Query.

    Returnerer en flad liste af variant-dicts: {id, sku, price, compareAtPrice,
    cost, handle, pid}. Robust: venter på ledig query-slot, poller til COMPLETED,
    streamer JSONL-resultatet linje for linje (parent-produkt før child-variant).
    """
    q_filter = f"vendor:'{vendor}'" + (f" AND product_type:'{ptype}'" if ptype else "")
    inner = (
        "{ products(query: %s) { edges { node { id handle "
        "variants { edges { node { id sku price compareAtPrice "
        "inventoryItem { unitCost { amount } } } } } } } } }" % json.dumps(q_filter)
    )

    # Submit (retry hvis en query-bulk allerede kører)
    res = None
    for attempt in range(1, 13):
        d = _shop_gql(_BULK_Q_RUN, {"query": inner})
        res = d["data"]["bulkOperationRunQuery"]
        errs = res.get("userErrors") or []
        if not errs:
            break
        if any("already in progress" in str(e).lower() for e in errs) and attempt < 12:
            print(f"  ⏳ En bulk-query kører allerede — venter ({attempt})...")
            _wait_current_query_done(poll, max_wait_min); continue
        raise Exception(f"bulkOperationRunQuery: {errs}")
    op_id = res["bulkOperation"]["id"]
    print(f"  🚀 Bulk-export startet: {op_id}")

    # Poll til færdig
    start = time.time(); url = None; last = None
    while True:
        time.sleep(poll)
        s = _shop_gql(_BULK_Q_STATUS)["data"]["currentBulkOperation"]
        if not s:
            continue
        if s["status"] != last:
            print(f"     [{int(time.time()-start):>4}s] status={s['status']} objectCount={s.get('objectCount')}")
            last = s["status"]
        if s["status"] == "COMPLETED":
            url = s.get("url"); break
        if s["status"] in ("FAILED", "CANCELED", "EXPIRED"):
            raise Exception(f"Bulk-export endte {s['status']} errorCode={s.get('errorCode')}")
        if time.time() - start > max_wait_min * 60:
            raise Exception(f"Bulk-export timeout efter {max_wait_min} min")

    if not url:
        print("  📦 Bulk-export: tomt resultat (ingen produkter)")
        return []

    # Download + parse JSONL (parent-produkt-linje før child-variant-linjer)
    handle_by_pid = {}
    variants = []
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(urllib.request.Request(url), timeout=300) as resp:
                for raw in resp:
                    line = raw.decode("utf-8").strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    oid = obj.get("id", "") or ""
                    if "/Product/" in oid:
                        handle_by_pid[oid] = obj.get("handle") or ""
                    elif "/ProductVariant/" in oid:
                        inv = (obj.get("inventoryItem") or {}).get("unitCost") or {}
                        variants.append({
                            "id": oid,
                            "sku": (obj.get("sku") or "").strip(),
                            "price": obj.get("price"),
                            "compareAtPrice": obj.get("compareAtPrice"),
                            "cost": float(inv.get("amount") or 0),
                            "pid": obj.get("__parentId"),
                        })
            break
        except Exception as e:
            if attempt < 3:
                print(f"  ⚠ Download-forsøg {attempt} fejlede ({str(e)[:80]}) — prøver igen")
                time.sleep(2 ** attempt); handle_by_pid = {}; variants = []
            else:
                raise
    for v in variants:
        v["handle"] = handle_by_pid.get(v["pid"], "")
    print(f"  📦 Bulk-export: {len(handle_by_pid)} produkter, {len(variants)} varianter")
    return variants


def run_fictive_bulk(sb, job_id, vendor, ptype, cfg, dry_run):
    """Fictive-mode bulk (Benuta/Sollux/Kayoom + vidaXL-fictive).

    Henter vendorens produkter fra Shopify, beregner (pris, fiktiv førpris) pr.
    variant via resolve_variant_pricing(seed=handle), og pusher KUN ændringer.

    FART: push'es gennem push_to_shopify (auto Niveau 2/3). Store batches kører
    via Shopify Bulk Operations (server-side, ét async job, skalerer til 100k+)
    i stedet for sekventielle 25-ad-gangen mutations — derfor minutter, ikke
    timer. Rows bygges i samme format som den daglige sync (_build_by_product):
    hver fictive variant er "altid på tilbud" → on_sale-row med Compare At
    Action SET (fiktiv førpris) eller CLEAR (ingen rabat).
    """
    # === KOST-SANDHED: feed-b2b, IKKE Shopify-cost ===
    # Rod-årsag fundet 2026-07-05: Shopify-cost var forældet (daily rotation-sync
    # skipper fictive; denne funktion læste den forældede cost → 'Ingen ændringer'
    # mens vidaXL hævede b2b ~15% → 93,6% af kataloget underprissat, 1,44× realiseret).
    # Nu: b2b hentes fra offer-feedet hver kørsel; pris+førpris+COST pushes sammen.
    feed_b2b = {}
    if (vendor or "").lower() == "vidaxl":
        print("📥 Henter vidaXL offer-feed (b2b = kost-sandhed)...")
        _fdf = fetch_supplier_feed()
        for _sku, _b2b in zip(_fdf["SKU"].astype(str), _fdf["B2B price"]):
            try:
                _b = float(_b2b)
                if _b > 0: feed_b2b[_sku.strip().replace(".0", "")] = _b
            except (TypeError, ValueError):
                pass
        if len(feed_b2b) < 1000:
            _update_job(sb, job_id, status="failed", completed_at=_now(),
                        log_summary=f"AFVIST: offer-feed gav kun {len(feed_b2b)} b2b-priser — kører ikke på forældet cost")
            sys.exit("❌ Offer-feed utilgængeligt/tomt — afviser at reprise på forældet Shopify-cost")
        print(f"   ✅ {len(feed_b2b)} b2b-priser fra feed")

    on_sale_rows = []      # rows til push_to_shopify
    variants_map = {}      # sku -> [variant_id, product_id] (numeriske)
    state_rows = []        # spejl til vidaxl_pricing_state (b2b/pris/førpris)
    checked = 0
    counters = {"feed_missing": 0, "cost_update": 0, "price_update": 0}
    print(f"🔎 Bulk-eksporterer '{vendor}'-produkter fra Shopify (fictive mode)...")
    for v in _bulk_export_vendor_products(vendor, ptype):
        checked += 1
        sku = v["sku"]
        if not sku:
            continue
        if feed_b2b:
            b2b = feed_b2b.get(str(sku).strip())
            if not b2b:
                counters["feed_missing"] += 1  # udgået hos vidaXL → delete-flow ejer den
                continue
        else:
            b2b = v["cost"]                     # ikke-vidaXL fictive vendors: uændret adfærd
            if not b2b or b2b <= 0:
                continue
        np_, nc_ = resolve_variant_pricing(b2b, cfg, seed=v["handle"], on_sale=True)
        np_ = int(np_); nc_ = int(nc_) if nc_ else None
        cur_p = int(round(float(v["price"]))) if v.get("price") else 0
        cur_c = int(round(float(v["compareAtPrice"]))) if v.get("compareAtPrice") else None
        cur_cost = float(v["cost"]) if v.get("cost") else 0.0
        cost_changed = abs(b2b - cur_cost) >= 0.01
        if np_ == cur_p and nc_ == cur_c and not cost_changed:
            continue
        if np_ != cur_p or nc_ != cur_c: counters["price_update"] += 1
        if cost_changed: counters["cost_update"] += 1
        vid_num = _gid_num(v["id"]); pid_num = _gid_num(v["pid"])
        if vid_num is None or pid_num is None:
            continue
        variants_map[sku] = [vid_num, pid_num]
        on_sale_rows.append({
            "Variant SKU": sku,
            "Variant Price": np_,
            "Variant Cost": b2b,                 # cost pushes i SAMME mutation
            "Compare At Action": "SET" if nc_ else "CLEAR",
            "Set Compare At": nc_ if nc_ else "",
            "Variant Command": "UPDATE",
        })
        state_rows.append({"sku": sku, "b2b_cost": b2b,
                           "normal_price": nc_ or np_, "sale_price": np_})
    print(f"   counters: {counters}")

    total_changes = len(on_sale_rows)
    _update_job(sb, job_id, preview_count=total_changes,
                log_summary=f"Planlagt {total_changes} variant-ændringer ({checked} tjekket)")
    print(f"📊 fictive: {total_changes} ændringer planlagt ({checked} varianter tjekket)")

    if dry_run:
        _update_job(sb, job_id, status="completed", actual_count=0, completed_at=_now(),
                    log_summary=f"DRY-RUN: {total_changes} ville ændres")
        print(f"✅ DRY-RUN done. {total_changes} ville ændres.")
        return 0
    if total_changes == 0:
        _update_job(sb, job_id, status="completed", actual_count=0, completed_at=_now(), log_summary="Ingen ændringer")
        print("✅ Ingen ændringer.")
        return 0

    # Push via Bulk Operations (auto Niveau 2/3 i push_to_shopify). progress_cb
    # kaldes med (produkter_behandlet, produkter_total) — vi skalerer til
    # variant-total så hubbens progressbar (actual_count/preview_count) passer.
    def _progress(count, tp):
        if tp:
            est = min(int(count / tp * total_changes), total_changes)
            _update_job(sb, job_id, actual_count=est,
                        log_summary=f"Pusher til Shopify… {count}/{tp} produkter")

    stats = push_to_shopify([], on_sale_rows, variants_map, progress_cb=_progress)
    print(f"📊 STATS: {stats}")
    applied = stats.get("variants_updated", 0)
    errors = stats.get("errors", 0)
    error_rate = errors / (applied + errors) if (applied + errors) else 0
    ok = error_rate <= 0.01
    samples = stats.get("error_samples") or []
    sample_str = "; ".join(f"{c}× {m}" for m, c in samples[:3])
    dups = stats.get("skipped_duplicate", 0)

    # === STATE-SPEJL: hold vidaxl_pricing_state i sync med det vi netop pushede ===
    # (hub-visning + fremtidig diagnostik; KUN eksisterende rækker, bevar group/status)
    if ok and state_rows and (vendor or "").lower() == "vidaxl":
        try:
            st = load_pricing_state(sb)
            payload = []
            for r in state_rows:
                cur = st.get(r["sku"])
                if not cur:
                    continue
                payload.append({"sku": r["sku"], "pricing_group": cur["pricing_group"],
                                "status": cur["status"], "b2b_cost": r["b2b_cost"],
                                "normal_price": r["normal_price"], "sale_price": r["sale_price"]})
            upsert_state(sb, payload)
            print(f"🗄 state-spejl opdateret: {len(payload)} rækker")
        except Exception as e:
            print(f"⚠ state-spejl fejlede (ikke kritisk): {e}")

    # === VERIFIKATIONS-PAS: genberegn mod frisk Shopify-eksport — fanger silent drift ===
    verify_note = ""
    if ok and total_changes > 0:
        try:
            drift = 0
            for v in _bulk_export_vendor_products(vendor, ptype):
                sku = v["sku"]
                b2b = feed_b2b.get(str(sku).strip()) if feed_b2b else (v["cost"] or 0)
                if not sku or not b2b or b2b <= 0:
                    continue
                np_, nc_ = resolve_variant_pricing(b2b, cfg, seed=v["handle"], on_sale=True)
                cur_p = int(round(float(v["price"]))) if v.get("price") else 0
                if int(np_) != cur_p:
                    drift += 1
            pct = drift / max(checked, 1)
            verify_note = f" | VERIFIKATION: {drift} rest-afvigelser ({pct:.2%})"
            if pct > 0.02:
                ok = False
                verify_note += " — OVER GRÆNSE, marker FAILED"
        except Exception as e:
            verify_note = f" | verifikation fejlede: {e}"

    log = f"{'Done' if ok else 'FAILED'}. {applied} opdateret, {errors} fejl ({error_rate:.2%})"
    if dups:
        log += f", {dups} dup-SKU sprunget over"
    log += f", counters={counters}"
    if sample_str:
        log += f" — {sample_str}"
    log += verify_note
    _update_job(sb, job_id, status="completed" if ok else "failed", actual_count=applied,
                failed_count=errors, completed_at=_now(), log_summary=log[:500])
    print(f"{'✅ DONE' if ok else '❌ FAILED'}. Applied={applied}, Errors={errors}{verify_note}")
    return 0 if ok else 1


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
        # 0. Mode-check: fictive-vendors (Benuta/Sollux/Kayoom) har hverken vidaXL-feed
        #    eller rotation-state → kør den Shopify-baserede fictive-gren i stedet.
        target_cfg = load_pricing_config(sb, vendor=target_vendor, product_type=target_type)
        if target_cfg and target_cfg.get("mode") == "fictive_discount":
            print(f"ℹ️  {target_vendor} = fictive_discount → Shopify-baseret bulk")
            return run_fictive_bulk(sb, job_id, target_vendor, target_type, target_cfg, args.dry_run)

        # 1. Load data (samme hurtige datasti som dagssyncen) — real_discount (vidaXL)
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
        # Løbende fremgang → pricing_bulk_jobs.actual_count, så hubben kan vise
        # en progressbar (actual_count / preview_count). count = produkter
        # behandlet indtil nu, tp = produkter i alt i denne push.
        def _progress(count, tp):
            if tp:
                est = min(int(count / tp * total_changes), total_changes)
                _update_job(sb, job_id, actual_count=est,
                            log_summary=f"Pusher til Shopify… {count}/{tp} produkter")

        stats = push_to_shopify(today_rows, on_sale_rows, variants_map, progress_cb=_progress)
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
