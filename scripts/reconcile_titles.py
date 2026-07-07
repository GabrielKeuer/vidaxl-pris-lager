"""RECONCILIATION: kobl de godkendte titler (catalog_titles_simulation.csv) til STABILE nøgler
(SKU + master_pid) via frisk Shopify-eksport + feed. Output: per-SKU titel-orakel + drift-rapport.
READ-ONLY ift. Shopify."""
import csv, json, os, re, sys
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
from simulate_titles import get_data, load_mapping, load_bearing, prep_feed

CAT = r"C:\Users\APC\Desktop\catalog_titles_simulation.csv"
OUT = r"C:\Users\APC\vidaxl-pris-lager\output\approved_titles_by_sku.csv"

def main():
    mapping = load_mapping()
    clean_split, messy = load_bearing()
    prods, meta, ft = get_data()   # frisk eksport + feed (cacher også til fremtidige kørsler)
    print(f"📦 {len(prods)} produkter, {len(meta)} SKUs, {len(ft)} feed-titler")

    # 1) genskab grupperingen præcis som simulationen
    groups = defaultdict(list)
    for sk, mt in meta.items():
        mp = mapping.get(sk)
        if mp in messy: key = ("single", sk)
        elif mp in clean_split: key = ("split", mp, mt["opts"].get("Model") or mt["opts"].get("model") or "?")
        elif mp: key = ("merge", mp)
        else: key = ("nomaster", mt["pid"])
        groups[key].append(sk)

    # 2) indlæs godkendte rækker og byg match-indeks
    rows = list(csv.DictReader(open(CAT, encoding="utf-8-sig")))
    by_t1 = defaultdict(list)   # (src, keeper_handle, model, n_var, original) -> rows
    by_t2 = defaultdict(list)   # (src, keeper_handle, model) -> rows
    by_single = defaultdict(list)  # (keeper_handle, title_input) -> rows (singles)
    for r in rows:
        t1 = (r["source_type"], r["keeper_handle"], r["model_value"], r["n_variants"], r["original_title"])
        by_t1[t1].append(r)
        by_t2[(r["source_type"], r["keeper_handle"], r["model_value"])].append(r)
        if r["source_type"] == "single":
            by_single[(r["keeper_handle"], r["title_input"])].append(r)

    out, unmatched = [], []
    used = set()  # rid'er der er taget (mod dobbelt-tildeling ved kollision)
    stats = Counter()
    for key, skus in groups.items():
        typ = key[0]
        cur = Counter(meta[s]["pid"] for s in skus)
        keeper = cur.most_common(1)[0][0]
        kh = prods.get(keeper, {}).get("handle", "")
        orig = prods.get(keeper, {}).get("title", "")
        src = ("behold" if len(cur) <= 1 else "merge") if typ == "merge" else ("uændret" if typ == "nomaster" else typ)
        model = key[2] if typ == "split" else ""
        row = None; tier = ""
        if typ == "single":
            ti = prep_feed(ft.get(key[1], ("", ""))[0])
            for c in by_single.get((kh, ti), []):
                if c["rid"] not in used: row = c; tier = "single-feedtitel"; break
        else:
            for c in by_t1.get((src, kh, model, str(len(skus)), orig), []):
                if c["rid"] not in used: row = c; tier = "T1-eksakt"; break
            if row is None:
                for c in by_t2.get((src, kh, model), []):
                    if c["rid"] not in used: row = c; tier = "T2-keeper"; break
        if row is None:
            unmatched.append({"key": "|".join(map(str, key)), "src": src, "keeper_handle": kh,
                              "n_skus": len(skus), "orig": orig[:60]})
            stats["unmatched"] += 1
            continue
        used.add(row["rid"]); stats[tier] += 1
        mp = mapping.get(skus[0], "")
        for s in skus:
            out.append({"sku": s, "master_pid": mapping.get(s, ""), "group_key": "|".join(map(str, key)),
                        "source_type": src, "approved_title": row["generated_title"],
                        "keeper_handle": kh, "model_value": model, "match_tier": tier,
                        "issues": row["issues"], "rid": row["rid"]})

    # ubrugte godkendte rækker (grupper der er forsvundet siden = slettede produkter)
    stale = [r for r in rows if r["rid"] not in used]
    with open(OUT, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out[0].keys())); w.writeheader(); w.writerows(out)
    print(f"\n✅ {len(out)} SKUs → godkendt titel  ({OUT})")
    print(f"   match: {dict(stats)}")
    print(f"   grupper uden match (nye/drift siden sim): {len(unmatched)}")
    print(f"   godkendte rækker uden gruppe (slettet siden): {len(stale)}")
    for u in unmatched[:10]: print(f"     NY/DRIFT [{u['src']}] {u['keeper_handle'][:50]} ({u['n_skus']} SKUs)")
    json.dump(unmatched, open(r"C:\Users\APC\vidaxl-pris-lager\output\title_drift_groups.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)

if __name__ == "__main__":
    main()
