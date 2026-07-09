"""Synk plan.new_title fra de regenererede orakel-titler (eksekutoren bruger plan.new_title).
Udfyld manglende via keeperens feed-titel strippet for reelle akser. Gør planen til
den autoritative titel-kilde. --write for at gemme."""
import csv, json, os, re, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, __import__("os").environ.get("DROPXL_SCRIPTS", r"C:\Users\APC\dropxl-product-automation\scripts"))
sys.stdout.reconfigure(encoding="utf-8")
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
from title_engine import generate_title
from rebuild_plan_options import load_variants, axis_name

SP = r"C:\Users\APC\AppData\Local\Temp\claude\C--Users-APC\c0b60326-0d7f-46aa-bec2-7289b435d558\scratchpad"
IV = load_variants()
LABELS = json.load(open("output/axis_labels.json", encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}

def danish(sku, master):
    out = {}
    for k, v in IV.get(str(sku).strip(), {}).items():
        if v: out[LABELS.get(master, {}).get(k) or ("Farve" if k == "color" else (axis_name([v]) or "Model"))] = v
    return out

def main():
    write = "--write" in sys.argv
    plans = [json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8")]
    oracle = {r["sku"]: r["approved_title"] for r in csv.DictReader(open("output/approved_titles_by_sku.csv", encoding="utf-8-sig")) if r["approved_title"]}
    ft = json.load(open(SP + r"\sim_data_cache.json", encoding="utf-8"))["ft"]
    cache = json.load(open(SP + r"\plan_data_cache.json", encoding="utf-8"))
    varz, prods = cache["vars"], cache["prods"]
    h2pid = {pr["handle"]: pid for pid, pr in prods.items() if isinstance(pr, dict) and pr.get("handle")}
    pid2skus = defaultdict(list)
    for s, vv in varz.items():
        pid2skus[vv.get("pid")].append(s)

    synced = fallback = 0
    for p in plans:
        if p["action"] not in ("merge", "split", "atomize", "fix_mismerge_rest"):
            continue
        if p.get("unresolved_collisions") or p.get("dup_sku_quarantine"):
            continue
        skus = [m["sku"] for m in p["variant_creates"]]
        if not skus:
            continue
        master = p["key"].split("|")[1] if "|" in p["key"] else ""
        kskus = pid2skus.get(h2pid.get(p["keeper_handle"]), [])
        # titel = orakel for en SKU i gruppen (tilføjet el. keeper)
        title = next((oracle[s] for s in skus + kskus if s in oracle), None)
        if not title:  # fallback: keeperens feed-titel strippet for reelle akser
            full = list(set(skus + kskus))
            vals = defaultdict(set)
            for s in full:
                for k, v in danish(s, master).items():
                    if v: vals[k].add(v)
            axes = {k: sorted(v) for k, v in vals.items() if len(v) > 1}
            if p["action"] == "split":
                axes.pop("Model", None)
            ksku = kskus[0] if kskus else skus[0]
            src = (ft.get(ksku) or ft.get(skus[0]) or ["", ""])[0]
            title, _ = generate_title(src, axes, shared=True,
                                      feed_colors=[ft.get(s, ["", ""])[1] for s in full], n_variants=len(full))
            fallback += 1
        if title and title != p.get("new_title"):
            p["new_title"] = title
            p["title_changes"] = True
            synced += 1

    print(f"synket new_title: {synced} grupper ({fallback} via feed-fallback)")
    if write:
        with open("output/merge_plan.jsonl", "w", encoding="utf-8") as f:
            for p in plans:
                f.write(json.dumps(p, ensure_ascii=False) + "\n")
        print("✅ plan opdateret")
    else:
        print("(dry-run)")

if __name__ == "__main__":
    main()
