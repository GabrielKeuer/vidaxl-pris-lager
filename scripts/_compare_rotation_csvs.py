"""Sammenlign rotation CSV-output: gammelt rotate_groups vs v2 --dry-run.

OLD skriver til output/price_updates.csv (Matrixify-format).
NEW skriver til output/new_rotation_updates.csv (samme format).

For at få en meningsfuld diff må vi køre OLD i --dry-run mode (ellers
ville den opdatere Supabase state og gøre næste NEW-kørsel tom).

Format begge bruger:
  Variant SKU, Variant Price, Variant Compare At Price, Variant Cost, Variant Command
"""
import csv
import sys
from pathlib import Path

OLD_PATH = "output/price_updates.csv"
NEW_PATH = "output/new_rotation_updates.csv"
HEADERS = ["Variant SKU", "Variant Price", "Variant Compare At Price",
           "Variant Cost", "Variant Command"]


def _norm_price(v):
    s = str(v).strip() if v is not None else ""
    if s in ("", "nan", "NaN", "None"): return None
    try: return int(float(s))
    except ValueError: return None


def _norm_cost(v):
    s = str(v).strip() if v is not None else ""
    if s in ("", "nan", "NaN", "None"): return None
    try: return round(float(s), 2)
    except ValueError: return None


def normalize_row(row):
    return (
        str(row.get("Variant SKU", "")).strip(),
        _norm_price(row.get("Variant Price")),
        _norm_price(row.get("Variant Compare At Price")),
        _norm_cost(row.get("Variant Cost")),
        str(row.get("Variant Command", "")).strip().upper(),
    )


def load_normalized(path):
    p = Path(path)
    if not p.exists():
        print(f"❌ Mangler {path}", file=sys.stderr); sys.exit(2)
    with p.open(encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        return {normalize_row(r)[0]: normalize_row(r) for r in rdr}


def main():
    old = load_normalized(OLD_PATH)
    new = load_normalized(NEW_PATH)
    print(f"📊 OLD: {len(old)} rows")
    print(f"📊 NEW: {len(new)} rows")

    only_old = set(old) - set(new)
    only_new = set(new) - set(old)
    both = set(old) & set(new)
    diff_values = [(sku, old[sku], new[sku]) for sku in both if old[sku] != new[sku]]

    if not only_old and not only_new and not diff_values:
        print("\n✅ IDENTICAL — rotation logic match perfekt")
        sys.exit(0)

    print(f"\n❌ DIFF FUNDET:")
    print(f"   Kun i OLD: {len(only_old)}")
    print(f"   Kun i NEW: {len(only_new)}")
    print(f"   Same SKU, forskellig vaerdi: {len(diff_values)}")
    for sku in sorted(only_old)[:5]: print(f"   OLD only: {old[sku]}")
    for sku in sorted(only_new)[:5]: print(f"   NEW only: {new[sku]}")
    for sku, o, n in diff_values[:5]:
        print(f"   {sku}: OLD price={o[1]} cap={o[2]} cost={o[3]}")
        print(f"         NEW price={n[1]} cap={n[2]} cost={n[3]}")
    sys.exit(1)


if __name__ == "__main__":
    main()
