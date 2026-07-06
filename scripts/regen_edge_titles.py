"""Finpuds de sidste titel-kanttilfælde: grupper hvor keeper-titlen selv var variant-specifik
(fx 'Eldrevet Massagestol' på et produkt med El/Manuel + massage-akser) eller manglede titel.
Kilde = keeperens FEED-titel (generisk vidaXL-navn) strippet for reelle akser + LLM. --write for at gemme."""
import csv, json, os, re, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, r"C:\Users\APC\dropxl-product-automation\scripts")
sys.stdout.reconfigure(encoding="utf-8")
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
from title_engine import generate_title
import title_llm
from rebuild_plan_options import load_variants, axis_name

SP = r"C:\Users\APC\AppData\Local\Temp\claude\C--Users-APC\c0b60326-0d7f-46aa-bec2-7289b435d558\scratchpad"
ORACLE = "output/approved_titles_by_sku.csv"
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
    ft = json.load(open(SP + r"\sim_data_cache.json", encoding="utf-8"))["ft"]
    cache = json.load(open(SP + r"\plan_data_cache.json", encoding="utf-8"))
    varz, prods = cache["vars"], cache["prods"]
    h2pid = {pr["handle"]: pid for pid, pr in prods.items() if isinstance(pr, dict) and pr.get("handle")}
    pid2skus = defaultdict(list)
    for s, vv in varz.items():
        pid2skus[vv.get("pid")].append(s)
    orows = list(csv.DictReader(open(ORACLE, encoding="utf-8-sig")))
    otitle = {r["sku"]: r["approved_title"] for r in orows}

    cands = []
    for p in plans:
        if p["action"] not in ("merge", "fix_mismerge_rest"):
            continue
        if p.get("unresolved_collisions") or p.get("dup_sku_quarantine") or not p["variant_creates"]:
            continue
        skus = [m["sku"] for m in p["variant_creates"]]
        master = p["key"].split("|")[1] if "|" in p["key"] else ""
        kpid = h2pid.get(p["keeper_handle"]); kskus = pid2skus.get(kpid, [])
        full = list(set(skus + kskus))
        vals = defaultdict(set)
        for s in full:
            for k, v in danish(s, master).items():
                if v: vals[k].add(v)
        axes = {k: sorted(v) for k, v in vals.items() if len(v) > 1}
        title = otitle.get(skus[0], "")
        # flag: manglende titel ELLER titel indeholder en ikke-Farve akse-værdi (første ord)
        need = not title
        tl = title.lower()
        for k, vv in axes.items():
            if k == "Farve":
                continue
            for v in vv:
                w = str(v).split()[0].lower()
                if len(w) > 3 and not w.isdigit() and re.search(r"\b" + re.escape(w) + r"\b", tl):
                    need = True; break
            if need: break
        if not need:
            continue
        # kilde = keeperens egen feed-titel (generisk), ellers 1. variants
        ksku = kskus[0] if kskus else skus[0]
        src = (ft.get(ksku) or ft.get(skus[0]) or ["", ""])[0]
        fcolors = [ft.get(s, ["", ""])[1] for s in full]
        det, _ = generate_title(src, axes, shared=True, feed_colors=fcolors, n_variants=len(full))
        cands.append((skus, title, det, sorted(axes)))

    print(f"kanttilfælde at finpudse: {len(cands)}")
    recs = [{"i": i, "feed": c[2], "det": c[2], "axes": {k: [] for k in c[3]}} for i, c in enumerate(cands)]
    fixed = title_llm.repair_titles(recs) if recs else {}
    updates = {}
    for i, (skus, old, det, axes) in enumerate(cands):
        newt = fixed.get(i) or det
        print(f"  {old[:40]!r} → {newt[:40]!r}  [{'·'.join(axes)}]")
        for s in skus:
            updates[s] = newt

    if write:
        for r in orows:
            if r["sku"] in updates and updates[r["sku"]]:
                r["approved_title"] = updates[r["sku"]]
                r["issues"] = (r.get("issues", "") + "; " if r.get("issues") else "") + "regen_edge"
        with open(ORACLE, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(orows[0].keys())); w.writeheader(); w.writerows(orows)
        print(f"✅ orakel opdateret: {len(updates)} SKUs")
    else:
        print("(dry-run)")

if __name__ == "__main__":
    main()
