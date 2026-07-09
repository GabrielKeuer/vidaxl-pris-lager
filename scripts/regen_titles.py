"""Gen-generér titler på de REBUILT options (item_variant-komplette).
For hver eksekverbar merge/split/fix-gruppe: strip de nu-komplette akse-værdier fra orakel-titlen
via motoren → LLM-repair de ændrede → opdatér oracle-CSV. DRY-RUN default (--write for at gemme).
Titel-motor + LLM fra dropxl-repoet. READ-ONLY ift. Shopify."""
import csv, json, os, re, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, __import__("os").environ.get("DROPXL_SCRIPTS", r"C:\Users\APC\dropxl-product-automation\scripts"))
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

def danish_opts(sku, master):
    raw = IV.get(str(sku).strip(), {})
    lab = LABELS.get(master, {})
    out = {}
    for k, v in raw.items():
        if v:
            out[lab.get(k) or ("Farve" if k == "color" else (axis_name([v]) or "Model"))] = v
    return out

def main():
    write = "--write" in sys.argv
    plans = [json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8")]
    ft = json.load(open(SP + r"\sim_data_cache.json", encoding="utf-8"))["ft"]
    cache = json.load(open(SP + r"\plan_data_cache.json", encoding="utf-8"))
    varz = cache["vars"]; prods = cache["prods"]
    h2pid = {pr["handle"]: pid for pid, pr in prods.items() if isinstance(pr, dict) and pr.get("handle")}
    pid2skus = defaultdict(list)
    for s, vv in varz.items():
        pid2skus[vv.get("pid")].append(s)
    orows = list(csv.DictReader(open(ORACLE, encoding="utf-8-sig")))
    otitle = {}
    for r in orows:
        otitle[r["sku"]] = r["approved_title"]

    # deterministisk regen pr. gruppe
    cands = []   # (group, gsku_list, old_title, det_title, axes)
    for p in plans:
        if p["action"] not in ("merge", "split", "fix_mismerge_rest"):
            continue
        if p.get("unresolved_collisions") or p.get("dup_sku_quarantine"):
            continue
        skus = [m["sku"] for m in p["variant_creates"]]
        if not skus:
            continue
        old = otitle.get(skus[0])
        if not old:
            continue
        master = p["key"].split("|")[1] if "|" in p["key"] else ""
        # FULDE sæt: keeperens eksisterende varianter + de tilføjede (merge/fix).
        # Ellers ser en akse der kun varierer mod keeperen (fx Materiale mango vs akacie) enkelt-værdig ud.
        full_skus = list(skus)
        if p["action"] in ("merge", "fix_mismerge_rest"):
            full_skus += pid2skus.get(h2pid.get(p["keeper_handle"]), [])
        vals = defaultdict(set)
        for s in set(full_skus):
            for k, v in danish_opts(s, master).items():
                if v: vals[k].add(v)
        opts = {k: sorted(v) for k, v in vals.items() if len(v) > 1}   # kun REELLE akser
        if p["action"] == "split":
            opts.pop("Model", None)
        fcolors = [ft.get(s, ["", ""])[1] for s in skus]
        det, _ = generate_title(old, opts, shared=True, feed_colors=fcolors, n_variants=len(skus))
        if det and det != old:
            cands.append((p, skus, old, det, sorted(opts.keys())))
    print(f"deterministisk: {len(cands)} grupper får ændret titel (af komplette options)")

    # LLM-repair de ændrede (med akse-kontekst)
    recs = [{"i": i, "feed": c[2], "det": c[3], "axes": {k: [] for k in c[4]}} for i, c in enumerate(cands)]
    fixed = title_llm.repair_titles(recs) if recs else {}
    print(f"LLM-repair: {len(fixed)} yderligere rettet")

    # anvend: byg sku→ny titel
    updates = {}
    for i, (p, skus, old, det, axes) in enumerate(cands):
        newt = fixed.get(i) or det
        for s in skus:
            updates[s] = newt
    print(f"SKUs der får ny titel: {len(updates)}")
    print("\n— 15 eksempler —")
    seen = set()
    for i, (p, skus, old, det, axes) in enumerate(cands):
        if len(seen) >= 15: break
        newt = fixed.get(i) or det
        print(f"  {old[:46]!r}\n    → {newt[:46]!r}  [{'·'.join(axes)}]")
        seen.add(i)

    if write:
        for r in orows:
            if r["sku"] in updates:
                r["approved_title"] = updates[r["sku"]]
                r["issues"] = (r.get("issues", "") + "; " if r.get("issues") else "") + "regen_item_variant"
        with open(ORACLE, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(orows[0].keys())); w.writeheader(); w.writerows(orows)
        print(f"\n✅ orakel opdateret: {len(updates)} SKUs")
    else:
        print("\n(dry-run — kør med --write)")

if __name__ == "__main__":
    main()
