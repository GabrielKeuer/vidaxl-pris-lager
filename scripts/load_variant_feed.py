"""Indlæs det komplette variant-feed (output/complete_feed.json) i Supabase-tabellen br_variant_feed.
KUN struktur/beslutninger: gruppering, options, titel, status. INTET indhold (billede/beskrivelse/pris/
lager) — det hentes altid frisk fra vidaXL-feedet. Status pr. SKU: single / variant / udgaaede_soeskende."""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def main():
    P = json.load(open("output/complete_feed.json", encoding="utf-8"))
    sb = ME.get_supabase_client()
    rows = []
    from collections import Counter
    st = Counter()
    for p in P:
        specs = p["specs"]
        for v in p["variants"]:
            sku = v["sku"]
            iv = ME.OPTS.get(sku) or {}
            if p.get("orphan"):
                status = "udgaaede_soeskende"   # var variant, egen item_variant udgået → nu single-produkt
            elif not specs:
                status = "single"
            elif not iv:
                status = "udgaaede_soeskende"
            else:
                status = "variant"
            st[status] += 1
            r = {"sku": sku, "master_pid": p["mid"], "product_key": p["key"],
                 "product_title": p["title"], "item_variant": iv or None,
                 "status": status, "is_manual_fix": bool(p.get("manual")),
                 "variant_position": v.get("pos")}
            for i in range(3):
                nm = specs[i] if i < len(specs) else None
                r[f"option{i+1}_name"] = nm
                r[f"option{i+1}_value"] = (v["values"].get(nm) if nm else None) or None
            rows.append(r)
    print(f"rækker at indlæse: {len(rows)} | status: {dict(st)}")
    # batch-upsert
    B = 500; done = 0
    for i in range(0, len(rows), B):
        sb.table("br_variant_feed").upsert(rows[i:i+B], on_conflict="sku").execute()
        done += len(rows[i:i+B])
        if done % 10000 == 0 or done == len(rows):
            print(f"  …{done}/{len(rows)}", flush=True)
    print("✓ indlæst i br_variant_feed")

if __name__ == "__main__":
    main()
