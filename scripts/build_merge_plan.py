"""MERGE-APPLY PLAN-BYGGER (DRY-RUN — ingen Shopify-writes).
Bygger den komplette eksekverings-plan for katalog-konsolideringen:
  pr. endelig gruppe: handling, keeper, titel (orakel), donorer, variant-flyt (SKU/pris/options/billede),
  301-redirects, advarsler. Fejl-merges håndteres naturligt (SKU følger sit eget master_pid).
Output: output/merge_plan.jsonl + output/merge_plan_summary.json"""
import csv, json, os, re, sys, time, urllib.request
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
from bulk_repricing import _shop_gql
from simulate_titles import load_mapping, load_bearing, get_data as _sim_data

CACHE2 = r"C:\Users\APC\AppData\Local\Temp\claude\C--Users-APC\c0b60326-0d7f-46aa-bec2-7289b435d558\scratchpad\plan_data_cache.json"
ORACLE = r"C:\Users\APC\vidaxl-pris-lager\output\approved_titles_by_sku.csv"
OUT = r"C:\Users\APC\vidaxl-pris-lager\output\merge_plan.jsonl"
SUM = r"C:\Users\APC\vidaxl-pris-lager\output\merge_plan_summary.json"

_RUN = "mutation($q:String!){bulkOperationRunQuery(query:$q){bulkOperation{id status} userErrors{message}}}"
_STAT = "query{currentBulkOperation(type:QUERY){id status errorCode objectCount url}}"

def export_full():
    """Bulk-eksport med ALT plan-relevant: produkt (handle/titel/status/billeder-count) +
    varianter (id/sku/pris/compareAt/barcode/options/billede)."""
    if os.path.exists(CACHE2):
        d = json.load(open(CACHE2, encoding="utf-8"))
        return d["prods"], d["vars"]
    inner = ('{ products(query: "vendor:\'vidaXL\'") { edges { node { id handle title status productType '
             'onlineStoreUrl featuredMedia { id } '
             'variants { edges { node { id sku barcode price compareAtPrice '
             'selectedOptions { name value } media(first: 1) { edges { node { id } } } } } } } } } }')
    for _ in range(60):
        s = _shop_gql(_STAT)["data"]["currentBulkOperation"]
        if not s or s["status"] not in ("CREATED", "RUNNING"): break
        time.sleep(10)
    r = _shop_gql(_RUN, {"q": inner})["data"]["bulkOperationRunQuery"]
    if r.get("userErrors"): raise SystemExit(r["userErrors"])
    url = None
    while True:
        time.sleep(8)
        s = _shop_gql(_STAT)["data"]["currentBulkOperation"]
        if not s: continue
        if s["status"] == "COMPLETED": url = s.get("url"); break
        if s["status"] in ("FAILED", "CANCELED", "EXPIRED"): raise SystemExit(s.get("errorCode"))
    prods, varz = {}, {}
    with urllib.request.urlopen(urllib.request.Request(url), timeout=600) as resp:
        for raw in resp:
            line = raw.decode("utf-8").strip()
            if not line: continue
            o = json.loads(line); oid = o.get("id", "") or ""
            if "/Product/" in oid and "__parentId" not in o:
                prods[oid] = {"handle": o.get("handle"), "title": o.get("title"), "status": o.get("status"),
                              "ptype": o.get("productType") or "", "url": o.get("onlineStoreUrl")}
            elif "/ProductVariant/" in oid:
                sk = (o.get("sku") or "").strip().replace(".0", "")
                if not sk: continue
                media = o.get("media", {}).get("edges", []) if isinstance(o.get("media"), dict) else []
                varz[sk] = {"vid": oid, "pid": o.get("__parentId"), "barcode": o.get("barcode"),
                            "price": o.get("price"), "cap": o.get("compareAtPrice"),
                            "opts": {so["name"]: so["value"] for so in (o.get("selectedOptions") or [])},
                            "has_img": bool(media)}
    json.dump({"prods": prods, "vars": varz}, open(CACHE2, "w", encoding="utf-8"))
    return prods, varz

def _residual(title, common):
    toks = [w for w in (title or "").split() if w.lower() not in common]
    return " ".join(toks)[:60].strip() or None

_AXIS_MAP = {"number of pieces": "Antal i pakke", "antal": "Antal i pakke", "color": "Farve", "colour": "Farve",
             "size": "Størrelse", "height": "Højde", "width": "Bredde", "length": "Længde", "depth": "Dybde",
             "material": "Materiale", "model": "Model", "type": "Model", "diameter": "Diameter"}

def normalize_axes(plan):
    """Flet engelske dublet-akser → dansk kanonisk navn; drop helt tomme akser;
    drop enkelt-værdi-akser hvis der er brug for slots (<jf. 3-akse-grænsen)."""
    moves = plan["variant_creates"]
    if not moves: return plan
    for m in moves:
        ov = {}
        for n, v in (m["option_values"] or {}).items():
            cn = _AXIS_MAP.get(n.lower().strip(), n)
            if cn not in ov or (not ov[cn] and v): ov[cn] = v
        m["option_values"] = ov
    axes = sorted({n for m in moves for n in m["option_values"]})
    # drop akser hvor ALLE værdier er tomme
    dead = [a for a in axes if not any(m["option_values"].get(a) for m in moves)]
    for a in dead:
        for m in moves: m["option_values"].pop(a, None)
    axes = [a for a in axes if a not in dead]
    # drop enkelt-værdi-akser KUN hvis vi er på 3 (frigør slot til afledning)
    if len(axes) >= 3:
        for a in list(axes):
            vals = {m["option_values"].get(a) for m in moves}
            if len(vals - {None, ""}) <= 1 and len(axes) >= 3:
                for m in moves: m["option_values"].pop(a, None)
                axes.remove(a)
                plan["warnings"].append(f"akse_droppet(én værdi): {a}")
    if dead: plan["warnings"].append(f"akse_droppet(tom): {dead}")
    plan["target_axes"] = axes
    return plan

def _ncoll(moves):
    c = Counter(tuple(sorted((m["option_values"] or {}).items())) for m in moves)
    return sum(v - 1 for v in c.values() if v > 1)

def resolve_collisions(plan, ft):
    """Manglende-akse-afledning (GRÅDIG/partiel): tilføj akse hvis den REDUCERER kollisioner.
    1) feed-Color → Farve  2) feed-titel-residual → Model.
    Rest-kollisioner med IDENTISK feed (titel+farve) = ægte dup-SKUs → karantæne (afventer keep-regler)."""
    moves = plan["variant_creates"]
    if _ncoll(moves) == 0: return plan
    axes = list(plan["target_axes"])
    # 1) Farve fra feed-Color
    if "Farve" not in axes and len(axes) < 3:
        cols = {m["sku"]: (ft.get(m["sku"], ("", ""))[1] or "").strip() for m in moves}
        if any(cols.values()):
            before = _ncoll(moves)
            trial = [dict(m, option_values=dict(m["option_values"], Farve=cols[m["sku"]] or None)) for m in moves]
            if _ncoll(trial) < before:
                for m in moves: m["option_values"]["Farve"] = cols[m["sku"]] or None
                axes = sorted(axes + ["Farve"]); plan["warnings"].append("akse_afledt: Farve fra feed-Color")
    # 2) Model fra titel-residual
    if _ncoll(moves) and "Model" not in axes and len(axes) < 3:
        titles = {m["sku"]: (ft.get(m["sku"], ("", ""))[0] or "") for m in moves}
        toklists = [set(t.lower().split()) for t in titles.values() if t]
        common = set.intersection(*toklists) if toklists else set()
        res = {s: _residual(t, common) for s, t in titles.items()}
        before = _ncoll(moves)
        trial = [dict(m, option_values=dict(m["option_values"], Model=res[m["sku"]])) for m in moves]
        if _ncoll(trial) < before:
            for m in moves: m["option_values"]["Model"] = res[m["sku"]]
            axes = sorted(axes + ["Model"]); plan["warnings"].append("akse_afledt: Model fra feed-titel-residual")
    plan["target_axes"] = axes
    # klassificér rest: dup-SKU (identisk feed) vs reelt uløst
    rest = defaultdict(list)
    for m in moves: rest[tuple(sorted((m["option_values"] or {}).items()))].append(m)
    dup_pairs, unresolved = [], []
    for sigv, ms in rest.items():
        if len(ms) <= 1: continue
        feeds = {(ft.get(m["sku"], ("", ""))[0], ft.get(m["sku"], ("", ""))[1]) for m in ms}
        if len(feeds) == 1: dup_pairs.append([m["sku"] for m in ms])
        else: unresolved.append([m["sku"] for m in ms])
    if dup_pairs:
        plan["dup_sku_quarantine"] = dup_pairs
        plan["warnings"].append(f"DUP_SKU_KARANTÆNE: {len(dup_pairs)} par (afventer Gabriels keep-regler)")
    if unresolved:
        plan["unresolved_collisions"] = unresolved
        plan["warnings"].append(f"ULØST_KOLLISION: {len(unresolved)} sæt → vidaxl.dk option-scrape")
    return plan

def main():
    mapping = load_mapping()
    clean_split, messy = load_bearing()
    oracle = {}
    for r in csv.DictReader(open(ORACLE, encoding="utf-8-sig")):
        oracle[r["sku"]] = r["approved_title"]
    prods, varz = export_full()
    _p2, _m2, ft = _sim_data()   # feed-titler+farver (cached) til akse-afledning
    print(f"📦 {len(prods)} produkter, {len(varz)} varianter (m. priser) | {len(ft)} feed-attributter")

    # endelig gruppering pr. SKU (fejl-merges følger automatisk deres eget master_pid)
    groups = defaultdict(list)
    for sk, v in varz.items():
        mp = mapping.get(sk)
        if mp in messy: key = ("atomize", sk)
        elif mp in clean_split: key = ("split", mp, v["opts"].get("Model") or v["opts"].get("model") or "?")
        elif mp: key = ("group", mp)
        else: key = ("keep", v["pid"])
        groups[key].append(sk)

    # hvilke produkter mister ALLE varianter (→ slet+redirect) vs. nogle (→ delvis donor)
    prod_final_home = defaultdict(set)   # pid -> set af gruppe-keys hvor produktets SKUs ender
    for key, skus in groups.items():
        for s in skus: prod_final_home[varz[s]["pid"]].add(key)

    plans, warn_ct = [], Counter()
    for key, skus in groups.items():
        typ = key[0]
        cur = Counter(varz[s]["pid"] for s in skus)
        keeper = cur.most_common(1)[0][0]
        kinfo = prods.get(keeper, {})
        donors = [p for p in cur if p != keeper]
        title = oracle.get(skus[0], "")
        warnings = []
        if not title: warnings.append("mangler_orakel_titel(ny gruppe → title_engine)"); warn_ct["ny_gruppe_titel"] += 1

        if typ == "group" and len(cur) == 1 and len(prod_final_home[keeper]) == 1:
            action = "ingen"   # allerede korrekt ét produkt med kun egne SKUs
        elif typ == "group": action = "merge"
        elif typ == "split": action = "split"
        elif typ == "atomize": action = "atomize"
        else:
            action = "ingen" if len(prod_final_home[keeper]) == 1 else "fix_mismerge_rest"

        # variant-planer: alt der IKKE allerede bor på keeper skal (gen)skabes dér
        moves = []
        target_axes = sorted({n for s in skus for n in varz[s]["opts"] if n.lower() != "title"} - ({"Model"} if typ == "split" else set()))
        for s in skus:
            v = varz[s]
            if v["pid"] == keeper and action in ("ingen", "merge", "fix_mismerge_rest"): continue
            ov = {n: v["opts"].get(n) for n in target_axes}
            missing = [n for n, val in ov.items() if not val]
            if missing:
                warnings.append(f"sku {s}: mangler option-værdi for {missing} (→ afled fra feed)")
                warn_ct["afled_option"] += 1
            moves.append({"sku": s, "src_variant": v["vid"], "src_product": prods.get(v["pid"], {}).get("handle"),
                          "price": v["price"], "compare_at": v["cap"], "barcode": v["barcode"],
                          "option_values": ov, "copy_variant_image": v["has_img"]})
        # sletninger + redirects: donor-produkter der mister ALLE deres varianter
        deletes, redirects = [], []
        for d in donors:
            if prod_final_home[d] <= {key}:   # alle d's SKUs ender i denne gruppe
                h = prods.get(d, {}).get("handle")
                deletes.append({"pid": d, "handle": h})
                redirects.append({"from": f"/products/{h}", "to": f"/products/{kinfo.get('handle')}"})
        if action != "ingen" or moves:
            pl = {"key": "|".join(map(str, key)), "action": action, "keeper": keeper,
                          "keeper_handle": kinfo.get("handle"), "keeper_status": kinfo.get("status"),
                          "new_title": title, "title_changes": title != kinfo.get("title"),
                          "target_axes": target_axes, "n_variants_final": len(skus),
                          "variant_creates": moves, "product_deletes": deletes, "redirects": redirects,
                          "reviews_todo": [d["handle"] for d in deletes], "warnings": warnings}
            if action == "merge":
                pl = normalize_axes(pl)
                pl = resolve_collisions(pl, ft)
            plans.append(pl)

    with open(OUT, "w", encoding="utf-8") as f:
        for p in plans: f.write(json.dumps(p, ensure_ascii=False) + "\n")
    creates = sum(len(p["variant_creates"]) for p in plans)
    dels = sum(len(p["product_deletes"]) for p in plans)
    reds = sum(len(p["redirects"]) for p in plans)
    by_action = Counter(p["action"] for p in plans)
    summary = {"grupper_med_handling": len(plans), "actions": dict(by_action),
               "variant_creates": creates, "product_deletes": dels, "redirects": reds,
               "est_dage_ved_1000_pr_dag": -(-creates // 1000), "advarsler": dict(warn_ct),
               "titel_ændres_på": sum(1 for p in plans if p["title_changes"])}
    json.dump(summary, open(SUM, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    print(f"\n✅ plan: {OUT}")

if __name__ == "__main__":
    main()
