"""Sammenlign output/inventory_updates.csv (gammelt sync_inventory) vs
output/new_inventory_updates.csv (nyt sync_inventory_v2 --dry-run).

Bruges som validation-step i workflow: hvis CSV'erne adskiller sig efter
normalisering (sortering, numerisk format) → exit 1 = workflow fejler =
vi får ikke flippet til --live før logikken er bevist identisk.

Edge-cases mitigeret:
  - Række-sortering kan variere → sortér efter Variant SKU før diff
  - Numerisk format (3 vs 3.0) → normalisér til int
  - Header-rækkefølge → normalisér med eksplicit field-list
"""
import csv
import sys
from pathlib import Path


OLD_PATH = "output/inventory_updates.csv"
NEW_PATH = "output/new_inventory_updates.csv"

EXPECTED_HEADERS = [
    'Variant SKU',
    'Inventory Available: Shop location',
    'Variant Command',
]


def normalize_row(row: dict) -> tuple:
    """Returnér hashable tuple af de tre kolonner, normaliseret."""
    sku = str(row.get('Variant SKU', '')).strip()
    raw_qty = row.get('Inventory Available: Shop location', '')
    # Normalisér numerisk: "5", "5.0", "5.00" → 5
    try:
        qty = int(float(str(raw_qty).strip())) if str(raw_qty).strip() else 0
    except ValueError:
        qty = 0
    cmd = str(row.get('Variant Command', '')).strip().upper()
    return (sku, qty, cmd)


def load_normalized(path: str) -> set:
    p = Path(path)
    if not p.exists():
        print(f"❌ Mangler {path}", file=sys.stderr)
        sys.exit(2)
    with p.open(encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        # Verificér headers
        if set(rdr.fieldnames or []) != set(EXPECTED_HEADERS):
            print(f"❌ {path} har uventede headers: {rdr.fieldnames}", file=sys.stderr)
            sys.exit(2)
        return {normalize_row(r) for r in rdr}


def main():
    old_set = load_normalized(OLD_PATH)
    new_set = load_normalized(NEW_PATH)

    only_in_old = old_set - new_set
    only_in_new = new_set - old_set

    print(f"📊 OLD ({OLD_PATH}): {len(old_set)} unique rows")
    print(f"📊 NEW ({NEW_PATH}): {len(new_set)} unique rows")

    if not only_in_old and not only_in_new:
        print("\n✅ IDENTICAL — dry-run match perfekt. Klar til --live cutover.")
        sys.exit(0)

    print(f"\n❌ DIFF FUNDET:")
    print(f"   Kun i OLD: {len(only_in_old)} rows")
    print(f"   Kun i NEW: {len(only_in_new)} rows")

    # Vis op til 10 eksempler fra hver side til debug
    for label, diff_set in [('OLD only', only_in_old), ('NEW only', only_in_new)]:
        if not diff_set:
            continue
        print(f"\n   --- {label} (max 10) ---")
        for row in sorted(diff_set)[:10]:
            print(f"     SKU={row[0]:<25} qty={row[1]:<6} cmd={row[2]}")

    # SKU-overlap der har forskellig qty/cmd (mest sandsynlige bug-pattern)
    old_by_sku = {r[0]: r for r in old_set}
    new_by_sku = {r[0]: r for r in new_set}
    diff_qty = []
    for sku in set(old_by_sku) & set(new_by_sku):
        if old_by_sku[sku] != new_by_sku[sku]:
            diff_qty.append((sku, old_by_sku[sku], new_by_sku[sku]))
    if diff_qty:
        print(f"\n   --- Samme SKU men forskellig værdi: {len(diff_qty)} (max 10) ---")
        for sku, old, new in diff_qty[:10]:
            print(f"     {sku}: OLD={old[1]}/{old[2]} NEW={new[1]}/{new[2]}")

    sys.exit(1)


if __name__ == "__main__":
    main()
