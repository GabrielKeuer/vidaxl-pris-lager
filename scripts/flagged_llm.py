"""Route de 61 flaggede merge-grupper gennem LLM-struktureringen (som de 86 atomize-keepers): gather
ALLE live-SKUs (keeper + donorer) pr. gruppe → Claude beslutter korrekt struktur (≤3 akser, dublet-
titler OK, singler hvor nødvendigt). Output: output/flagged_specs.json {keeper_handle: {products, delete_handles}}."""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import atomize_llm as AL
import csv

def main():
    n = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else None
    infile = sys.argv[sys.argv.index("--in") + 1] if "--in" in sys.argv else "output/flagged_groups.json"
    outfile = sys.argv[sys.argv.index("--out") + 1] if "--out" in sys.argv else "output/flagged_specs.json"
    flagged = json.load(open(infile, encoding="utf-8"))
    plans = {p["key"]: p for p in (json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8"))}
    oracle = {r["sku"]: r["approved_title"] for r in csv.DictReader(open("output/approved_titles_by_sku.csv", encoding="utf-8-sig")) if r["approved_title"]}
    groups = flagged[:n] if n else flagged
    out = {}
    for i, fg in enumerate(groups, 1):
        p = plans[fg["key"]]
        master = fg["key"].split("|")[1] if "|" in fg["key"] else ""
        keeper = fg["keeper_handle"]
        # gather live SKUs fra keeper + donorer + hvilke handles skal slettes
        handles = [keeper] + [d["handle"] for d in p["product_deletes"]]
        live_skus, del_handles = [], []
        for h in handles:
            d = ME.gql('query($h:String!){productByHandle(handle:$h){id variants(first:100){edges{node{sku}}}}}', {"h": h})
            pr = (d.get("data") or {}).get("productByHandle")
            if not pr:
                continue
            del_handles.append(h)
            for e in pr["variants"]["edges"]:
                s = (e["node"]["sku"] or "").strip()
                if s and s not in live_skus:
                    live_skus.append(s)
        if not live_skus:
            continue
        km = ME.build_keyname(live_skus, master)
        rows = []
        for s in live_skus:
            iv = ME.OPTS.get(s, {})
            cfg = " / ".join(f"{k}={v}" for k, v in iv.items() if k != "color" and v) or "(ingen)"
            rows.append({"sku": s, "config": cfg, "color": iv.get("color"), "title": oracle.get(s, "")})
        keeper_title = plans[fg["key"]].get("new_title") or keeper
        res = AL.call_llm(keeper_title, rows)
        if res and res.get("products"):
            out[keeper] = {"products": res["products"], "delete_handles": del_handles}
            print(f"[{i}/{len(groups)}] {fg['cat']} {keeper[:36]} → {len(res['products'])} produkter ({len(live_skus)} SKU, slet {len(del_handles)})")
        else:
            out[keeper] = {"products": [{"title": oracle.get(r["sku"]) or keeper_title, "variants": [{"sku": r["sku"]}]} for r in rows], "delete_handles": del_handles}
            print(f"[{i}/{len(groups)}] {fg['cat']} {keeper[:36]} → FALLBACK singler ({len(live_skus)})")
    json.dump(out, open(outfile, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    tp = sum(len(v["products"]) for v in out.values())
    print(f"\n=== {len(out)} grupper → {tp} produkter. Gemt output/flagged_specs.json ===")

if __name__ == "__main__":
    main()
