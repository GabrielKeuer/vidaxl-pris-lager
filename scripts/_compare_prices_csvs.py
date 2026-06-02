"""Sammenlign output/price_updates.csv (sync_prices) vs
output/new_price_updates.csv (sync_prices_v2 --dry-run).

Begge filer indeholder merged-full-catalog (158k rows). Normaliserer:
  - Variant Cost: float-strings (341, 341.0, "341.00") → samme
  - Compare At Price: empty, NaN, "" → ensartet tom-streng
  - Variant Price: int-strings
  - Sortering: efter SKU
  - Headers: forventet match
"""
import csv
import sys
from pathlib import Path

OLD_PATH = "output/price_updates.csv"
NEW_PATH = "output/new_price_updates.csv"
HEADERS = ["Variant SKU", "Variant Price", "Variant Compare At Price",
           "Variant Cost", "Variant Command"]


def _norm_price(v):
    """Returnér int eller None for tom."""
    s = str(v).strip() if v is not None else ""
    if s in ("", "nan", "NaN", "None"):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _norm_cost(v):
    """Returnér float afrundet til 2 decimaler, eller None."""
    s = str(v).strip() if v is not None else ""
    if s in ("", "nan", "NaN", "None"):
        return None
    try:
        return round(float(s), 2)
    except ValueError:
        return None


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
        print(f"❌ Mangler {path}", file=sys.stderr)
        sys.exit(2)
    with p.open(encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        if set(rdr.fieldnames or []) != set(HEADERS):
            print(f"❌ {path} har uventede headers: {rdr.fieldnames}", file=sys.stderr)
            sys.exit(2)
        # Returner som dict keyed by SKU for målrettet diff
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
        print("\n✅ IDENTICAL — sync_prices_v2 dry-run match perfekt")
        sys.exit(0)

    print(f"\n❌ DIFF FUNDET:")
    print(f"   Kun i OLD: {len(only_old)}")
    print(f"   Kun i NEW: {len(only_new)}")
    print(f"   Same SKU, forskellig vaerdi: {len(diff_values)}")

    for sku in sorted(only_old)[:10]:
        print(f"   OLD only: {old[sku]}")
    for sku in sorted(only_new)[:10]:
        print(f"   NEW only: {new[sku]}")
    print("\n   --- Forskellige vaerdier (max 10) ---")
    for sku, o, n in diff_values[:10]:
        print(f"   {sku}:")
        print(f"     OLD: price={o[1]} cap={o[2]} cost={o[3]}")
        print(f"     NEW: price={n[1]} cap={n[2]} cost={n[3]}")

    sys.exit(1)


if __name__ == "__main__":
    main()
