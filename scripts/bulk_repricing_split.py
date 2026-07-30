"""Pris-SPLITTEST bulk-repricing — ADDITIVT. Rører IKKE bulk_repricing.py/pricing.py/sync_prices_v2.py.

Tro kopi af bulk_repricing.run_fictive_bulk MED ÉN ændring: markup slås op pr. produkts gruppe
(pricing_experiment_assignment: handle -> 1.6/1.7) i stedet for config'ens faste 1.65. Alt andet —
offer-feed-b2b (kost-sandhed), fiktiv førpris seedet på handle (=> samme rabat-% => prisen isoleret),
push via Bulk Operations, verifikations-pas — genbruges 1:1 fra de originale moduler.

Gensidig eksklusivitet: --live (fuld) kræver hub_settings.pricing_mode = 'split_test'. Den normale
daglige sync_prices_v2 springer allerede vidaXL-fictive over, så den rører ikke split-priserne.

Args:
  --experiment-id UUID   pricing_experiment.id (default: seneste vidaXL-eksperiment)
  --vendor STR           default vidaXL
  --dry-run              beregn + rapportér, skriv INTET (default hvis hverken --dry-run/--live)
  --live                 push til Shopify
  --limit N              canary: behandl kun de N første produkter (til superviseret test)
  --force                spring pricing_mode-guarden over (kun til bevidste canary-live-kørsler)
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pricing
from pricing import resolve_variant_pricing, load_pricing_config
from sync_prices_v2 import (
    fetch_supplier_feed, get_supabase_client, load_pricing_state,
    push_to_shopify, upsert_state,
)
from bulk_repricing import _bulk_export_vendor_products, _gid_num, _now


def _load_assignment(sb, experiment_id, full=False):
    """handle -> markup (float). full=True: også product_type/price/gruppe (til canary-sampling)."""
    out = {}
    rows_full = []
    cols = "handle, markup, product_type, price, gruppe" if full else "handle, markup"
    frm = 0
    while True:
        res = sb.table("pricing_experiment_assignment").select(cols) \
            .eq("experiment_id", experiment_id).range(frm, frm + 999).execute()
        rows = res.data or []
        for r in rows:
            if r.get("handle"):
                out[r["handle"]] = float(r["markup"])
                if full:
                    rows_full.append(r)
        if len(rows) < 1000:
            break
        frm += 1000
    return (out, rows_full) if full else out


def _select_canary(rows_full, n):
    """DETERMINISTISK stratificeret udvalg af ~n produkter: sortér på (produkttype, pris, handle)
    og tag hver k'te → spreder over typer + prisklasser + begge grupper. Samme sæt hver gang
    (så --revert rammer præcis de samme produkter)."""
    rows = sorted(rows_full, key=lambda r: (r.get("product_type") or "", float(r.get("price") or 0), r["handle"]))
    if n >= len(rows):
        return {r["handle"] for r in rows}
    step = len(rows) / float(n)
    picked = {rows[int(i * step)]["handle"] for i in range(n)}
    return picked


def run_split_bulk(sb, vendor, cfg, assignment, dry_run, limit=None, canary_handles=None, revert=False):
    """Fictive bulk MED per-gruppe markup. Kopi af bulk_repricing.run_fictive_bulk,
    ændring markeret med  # SPLIT.
      canary_handles: behandl KUN disse produkter (superviseret canary).
      revert=True: brug config'ens basis-markup (1,65) i stedet for gruppens → rul canary tilbage."""
    feed_b2b = {}
    if (vendor or "").lower() == "vidaxl":
        print("📥 Henter vidaXL offer-feed (b2b = kost-sandhed)...")
        _fdf = fetch_supplier_feed()
        for _sku, _b2b in zip(_fdf["SKU"].astype(str), _fdf["B2B price"]):
            try:
                _b = float(_b2b)
                if _b > 0:
                    feed_b2b[_sku.strip().replace(".0", "")] = _b
            except (TypeError, ValueError):
                pass
        if len(feed_b2b) < 1000:
            sys.exit(f"❌ Offer-feed gav kun {len(feed_b2b)} b2b-priser — afviser at reprise på forældet cost")
        print(f"   ✅ {len(feed_b2b)} b2b-priser fra feed")

    on_sale_rows, variants_map, state_rows = [], {}, []
    checked = 0
    counters = {"feed_missing": 0, "price_update": 0, "cost_update": 0,
                "ikke_i_split": 0, "A": 0, "B": 0}
    seen_products = set()
    print(f"🔎 Bulk-eksporterer '{vendor}'-produkter (SPLIT fictive mode)...")
    for v in _bulk_export_vendor_products(vendor, None):
        # SPLIT: --limit tælles på PRODUKTER (ikke varianter)
        if limit is not None:
            seen_products.add(v["handle"])
            if len(seen_products) > limit:
                break
        # SPLIT: canary — behandl KUN de udvalgte produkter
        if canary_handles is not None and v["handle"] not in canary_handles:
            continue
        checked += 1
        sku = v["sku"]
        if not sku:
            continue
        # SPLIT: markup pr. produkts gruppe. Ikke i split → rør IKKE (spring over).
        grp_markup = assignment.get(v["handle"])
        if grp_markup is None:
            counters["ikke_i_split"] += 1
            continue
        counters["A" if abs(grp_markup - 1.6) < 1e-6 else "B"] += 1
        # SPLIT: revert → basis-markup (config'ens 1,65); ellers gruppens markup
        cfg_grp = cfg if revert else {**cfg, "fixed_markup": grp_markup}

        if feed_b2b:
            b2b = feed_b2b.get(str(sku).strip())
            if not b2b:
                counters["feed_missing"] += 1
                continue
        else:
            b2b = v["cost"]
            if not b2b or b2b <= 0:
                continue
        np_, nc_ = resolve_variant_pricing(b2b, cfg_grp, seed=v["handle"], on_sale=True)  # SPLIT: cfg_grp
        np_ = int(np_); nc_ = int(nc_) if nc_ else None
        cur_p = int(round(float(v["price"]))) if v.get("price") else 0
        cur_c = int(round(float(v["compareAtPrice"]))) if v.get("compareAtPrice") else None
        cur_cost = float(v["cost"]) if v.get("cost") else 0.0
        cost_changed = abs(b2b - cur_cost) >= 0.01
        if np_ == cur_p and nc_ == cur_c and not cost_changed:
            continue
        if np_ != cur_p or nc_ != cur_c:
            counters["price_update"] += 1
        if cost_changed:
            counters["cost_update"] += 1
        vid_num = _gid_num(v["id"]); pid_num = _gid_num(v["pid"])
        if vid_num is None or pid_num is None:
            continue
        variants_map[sku] = [vid_num, pid_num]
        on_sale_rows.append({
            "Variant SKU": sku, "Variant Price": np_, "Variant Cost": b2b,
            "Compare At Action": "SET" if nc_ else "CLEAR",
            "Set Compare At": nc_ if nc_ else "", "Variant Command": "UPDATE",
        })
        state_rows.append({"sku": sku, "b2b_cost": b2b, "normal_price": nc_ or np_, "sale_price": np_})

    total = len(on_sale_rows)
    print(f"📊 SPLIT fictive: {total} ændringer planlagt ({checked} varianter tjekket) · counters={counters}")

    if dry_run:
        # Skriv diff-CSV så vi kan diffe mod den lokale hub-dry-run (paritets-tjek)
        os.makedirs("output", exist_ok=True)
        with open("output/split-bulk-dryrun.csv", "w") as f:
            f.write("sku,ny_pris,ny_compareat,cost\n")
            for r in on_sale_rows:
                f.write(f"{r['Variant SKU']},{r['Variant Price']},{r['Set Compare At']},{r['Variant Cost']}\n")
        print(f"✅ DRY-RUN: {total} ville ændres. Skrevet output/split-bulk-dryrun.csv")
        return 0
    if total == 0:
        print("✅ Ingen ændringer.")
        return 0

    stats = push_to_shopify([], on_sale_rows, variants_map)
    applied = stats.get("variants_updated", 0); errors = stats.get("errors", 0)
    error_rate = errors / (applied + errors) if (applied + errors) else 0
    ok = error_rate <= 0.01
    print(f"{'✅ DONE' if ok else '❌ FAILED'}. Applied={applied}, Errors={errors} ({error_rate:.2%})")

    # State-spejl (bevar group/status som originalen)
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
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--experiment-id", default=None)
    ap.add_argument("--vendor", default="vidaXL")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--canary", type=int, default=None,
                    help="Stratificeret canary: kun ~N produkter (spredt over typer+priser+A/B)")
    ap.add_argument("--revert", action="store_true",
                    help="Rul (canary) tilbage til config'ens basis-markup (1,65)")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    dry_run = not args.live  # default = dry-run medmindre --live

    sb = get_supabase_client()

    # Eksperiment
    if args.experiment_id:
        exp = sb.table("pricing_experiment").select("id, navn, status").eq("id", args.experiment_id).single().execute().data
    else:
        r = sb.table("pricing_experiment").select("id, navn, status").eq("vendor", args.vendor) \
            .order("created_at", desc=True).limit(1).execute().data
        exp = r[0] if r else None
    if not exp:
        sys.exit("❌ Intet pricing_experiment fundet")
    print(f"🧪 Eksperiment: {exp['navn']} ({exp['id']}) status={exp['status']}")

    er_delmaengde = (args.limit is not None) or (args.canary is not None)
    # Gensidig eksklusivitet: FULD --live (hele kataloget) kræver pricing_mode=split_test.
    # Canary/limit-delmængder er superviserede test → tillades uden flag.
    if args.live and not er_delmaengde and not args.force:
        pm = sb.table("hub_settings").select("value").eq("key", "pricing_mode").maybe_single().execute().data
        mode = (pm or {}).get("value", {})
        mode = mode.get("mode") if isinstance(mode, dict) else mode
        if mode != "split_test":
            sys.exit(f"❌ pricing_mode={mode!r} — sæt hub_settings.pricing_mode.mode='split_test' før fuld --live (eller brug --canary til test)")

    cfg = load_pricing_config(sb, vendor=args.vendor, product_type=None)
    if not cfg or cfg.get("mode") != "fictive_discount":
        sys.exit(f"❌ {args.vendor}-config er ikke fictive_discount")

    assignment, rows_full = _load_assignment(sb, exp["id"], full=True)
    print(f"📋 Tildeling: {len(assignment)} produkter")
    if not assignment:
        sys.exit("❌ Tom tildeling")

    canary_handles = None
    if args.canary:
        canary_handles = _select_canary(rows_full, args.canary)
        print(f"🐤 Canary: {len(canary_handles)} produkter (stratificeret){' · REVERT' if args.revert else ''}")

    mode_str = "DRY-RUN" if dry_run else ("CANARY-LIVE" if er_delmaengde else "FULD LIVE")
    print(f"🚀 {mode_str} — vendor={args.vendor} canary={args.canary} revert={args.revert}")
    rc = run_split_bulk(sb, args.vendor, cfg, assignment, dry_run,
                        limit=args.limit, canary_handles=canary_handles, revert=args.revert)
    sys.exit(rc)


if __name__ == "__main__":
    main()
