"""
Monthly VidaXL group rotation: end previous sale group, start next group.

Reads rotation state from hub_settings.vidaxl_rotation_state.
Rotates A -> B -> C -> A. Per rotation:

  1. Previous active group: every product in status='on_sale' is reverted to
     'normal' with Variant Price=normal_price, Variant Compare At Price=''.
     last_normal_period_started_at is reset to now.

  2. Next active group: every product in status IN ('normal','warmup') with
     warmup_complete_at expired is moved to 'on_sale' with Variant Price=
     sale_price, Variant Compare At Price=normal_price.

Writes a Matrixify CSV with the combined updates. Updates Supabase state.
Audit trail: each rotation is one git commit with the CSV — git history shows
exactly when and which SKUs changed status.

Safety: if last rotation was less than ROTATION_MIN_DAYS days ago, the script
refuses to run unless --force is passed. This prevents Omnibus 4.4-rule
violation (sale max half of normalpris-period).
"""
import argparse
import csv
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pricing  # noqa: F401  (loaded for parity with other scripts and future use)

OUTPUT_PATH = "output/rotation_updates.csv"
ROTATION_STATE_KEY = "vidaxl_rotation_state"
GROUP_CYCLE = ["A", "B", "C"]
ROTATION_MIN_DAYS = 25  # Refuse to rotate again sooner than this without --force


def get_supabase_client():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        return None
    from supabase import create_client
    return create_client(url, key)


def load_rotation_state(sb):
    res = sb.table("hub_settings").select("value").eq("key", ROTATION_STATE_KEY).execute()
    if not res.data:
        return None
    return res.data[0]["value"]


def save_rotation_state(sb, state):
    payload = {
        "key": ROTATION_STATE_KEY,
        "value": state,
        "description": "VidaXL group rotation state — managed by rotate_groups.py",
    }
    sb.table("hub_settings").upsert(payload, on_conflict="key").execute()


def next_group_in_cycle(prev):
    if prev is None or prev not in GROUP_CYCLE:
        return GROUP_CYCLE[0]
    idx = GROUP_CYCLE.index(prev)
    return GROUP_CYCLE[(idx + 1) % len(GROUP_CYCLE)]


def fetch_state_paginated(sb, group, statuses):
    rows = []
    page_size = 1000
    page = 0
    while True:
        q = (
            sb.table("vidaxl_pricing_state")
            .select("sku,pricing_group,status,b2b_cost,normal_price,sale_price,warmup_complete_at")
            .eq("pricing_group", group)
            .in_("status", statuses)
            .range(page * page_size, (page + 1) * page_size - 1)
        )
        res = q.execute()
        if not res.data:
            break
        rows.extend(res.data)
        if len(res.data) < page_size:
            break
        page += 1
    return rows


def parse_iso(s):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true",
                        help="Skip the ROTATION_MIN_DAYS gate")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute changes and write CSV but don't update Supabase or save rotation state")
    args = parser.parse_args(argv)

    print(f"🔄 Rotation started at {datetime.now(timezone.utc).isoformat()}"
          f"{' [DRY-RUN]' if args.dry_run else ''}")

    sb = get_supabase_client()
    if sb is None:
        print("❌ SUPABASE_URL / SUPABASE_SERVICE_KEY missing — cannot run.")
        sys.exit(1)

    rotation_state = load_rotation_state(sb) or {}
    prev_active = rotation_state.get("active_group")
    last_rotated_at = parse_iso(rotation_state.get("last_rotated_at"))

    if last_rotated_at and not args.force:
        days_since = (datetime.now(timezone.utc) - last_rotated_at).days
        if days_since < ROTATION_MIN_DAYS:
            print(f"❌ Last rotation was only {days_since} days ago "
                  f"(min {ROTATION_MIN_DAYS}). Use --force to override.")
            sys.exit(2)

    next_active = next_group_in_cycle(prev_active)
    rotation_id = (
        f"rot-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-"
        f"{prev_active or 'INIT'}-to-{next_active}"
    )
    now_iso = datetime.now(timezone.utc).isoformat()

    print(f"🔄 {prev_active or '(none)'} -> {next_active}  ({rotation_id})")

    output_rows = []
    state_updates = []

    # ----- 1) End previous sale -----
    end_count = 0
    if prev_active:
        prev_sale = fetch_state_paginated(sb, prev_active, ["on_sale"])
        for r in prev_sale:
            output_rows.append({
                "Variant SKU": r["sku"],
                "Variant Price": r["normal_price"],
                "Variant Compare At Price": "",
                "Variant Cost": r.get("b2b_cost") or "",
                "Variant Command": "UPDATE",
            })
            state_updates.append({
                "sku": r["sku"],
                "status": "normal",
                "last_normal_period_started_at": now_iso,
                "last_status_change_at": now_iso,
            })
            end_count += 1
    print(f"   - Ended sale on {end_count} products in group {prev_active}")

    # ----- 2) Start next group's sale -----
    start_count = 0
    skipped_warmup = 0
    skipped_no_prices = 0
    next_eligible = fetch_state_paginated(sb, next_active, ["normal", "warmup"])
    for r in next_eligible:
        warmup_at = parse_iso(r.get("warmup_complete_at"))
        if warmup_at is not None and warmup_at > datetime.now(timezone.utc):
            skipped_warmup += 1
            continue
        if r.get("normal_price") is None or r.get("sale_price") is None:
            skipped_no_prices += 1
            continue
        output_rows.append({
            "Variant SKU": r["sku"],
            "Variant Price": r["sale_price"],
            "Variant Compare At Price": r["normal_price"],
            "Variant Cost": r.get("b2b_cost") or "",
            "Variant Command": "UPDATE",
        })
        state_updates.append({
            "sku": r["sku"],
            "status": "on_sale",
            "last_status_change_at": now_iso,
        })
        start_count += 1
    print(f"   + Started sale on {start_count} products in group {next_active} "
          f"(skipped {skipped_warmup} warmup-not-ready, {skipped_no_prices} no-prices)")

    # ----- 3) Write Matrixify CSV -----
    os.makedirs("output", exist_ok=True)
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "Variant SKU", "Variant Price", "Variant Compare At Price",
            "Variant Cost", "Variant Command",
        ])
        w.writeheader()
        w.writerows(output_rows)
    print(f"📝 Wrote {len(output_rows)} rows to {OUTPUT_PATH}")

    if args.dry_run:
        print("[DRY-RUN] Skipping Supabase updates.")
        return

    # ----- 4) Update vidaxl_pricing_state in batches -----
    if state_updates:
        batch_size = 500
        total = 0
        for i in range(0, len(state_updates), batch_size):
            batch = state_updates[i:i + batch_size]
            res = sb.table("vidaxl_pricing_state").upsert(batch, on_conflict="sku").execute()
            total += len(res.data) if res.data else 0
        print(f"💾 Updated {total} state rows")

    # ----- 5) Save rotation state -----
    new_state = {
        "active_group": next_active,
        "previous_group": prev_active,
        "last_rotated_at": now_iso,
        "rotation_id": rotation_id,
        "rotation_count": (rotation_state.get("rotation_count") or 0) + 1,
        "ended_count": end_count,
        "started_count": start_count,
        "skipped_warmup": skipped_warmup,
    }
    save_rotation_state(sb, new_state)
    print(f"💾 Rotation state saved: active_group={next_active}, count={new_state['rotation_count']}")


if __name__ == "__main__":
    main()
