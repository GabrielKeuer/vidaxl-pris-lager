"""KOMPLET FORUD-GENERERET + VALIDERET PLAN for HELE kataloget (som en Matrixify-fil).
Hver master_pid → ét færdigt produkt: titel (2-vejs, valideret), variant-akser, SKUs, status.
Alt defineres + valideres FØR eksekvering. Flager præcis det der kræver manuel stilling.
Output: output/final_catalog_plan.json (fuld) + Desktop/final_catalog_plan.csv (til gennemsyn) + summary."""
import sys, os, io, zipfile, csv, re, json
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def clean(t):
    return re.sub(r"\s+", " ", re.sub(r"(?i)\bvidaxl\b", "", t or "")).strip()

def housestyle(t):
    return " ".join(w[:1].upper() + w[1:] if w else w for w in clean(t).split())

def toks(t):
    return set(re.findall(r"[a-zæøå0-9.,/²]+", (t or "").lower()))

def main():
    sb = ME.get_supabase_client()
    bym = defaultdict(list); fr = 0
    while True:
        b = sb.table("vidaxl_sku_master").select("sku,master_pid").range(fr, fr + 999).execute().data or []
        for x in b:
            bym[x["master_pid"]].append(str(x["sku"]).strip())
        if len(b) < 1000:
            break
        fr += 1000
    z = zipfile.ZipFile(io.BytesIO(ME.get_feed_zip(os.environ["FEED_URL"])))
    name = [f for f in z.namelist() if f.endswith(".csv")][0]
    feed = {}
    for r in csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8")):
        s = str(r.get("SKU") or "").strip().replace(".0", "")
        if s:
            feed[s] = r.get("Title") or ""
    oracle = {r["sku"]: r["approved_title"] for r in csv.DictReader(open("output/approved_titles_by_sku.csv", encoding="utf-8-sig")) if r["approved_title"]}
    print(f"master_pids: {len(bym)} | feed: {len(feed)} | orakel: {len(oracle)}")

    plan = {}; cat = Counter()
    for mid, skus in bym.items():
        live = [s for s in skus if s in feed]
        if not live:
            cat["udgaaet"] += 1; continue
        opts = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        axisvals = defaultdict(set)
        for s in live:
            for k, v in opts[s].items():
                axisvals[k].add(v)
        axes = sorted({k for k, vv in axisvals.items() if len(vv) > 1})
        rec = {"master_pid": mid, "skus": live, "n": len(live)}
        if len(live) == 1:
            rec.update(type="single", title=housestyle(feed[live[0]]), axes=[], status="ok")
            cat["ok_single"] += 1
        elif not axes:
            rec.update(type="multi_no_axes", axes=[], status="manuel", reason="multi uden item_variant-akser",
                       title=housestyle(feed[live[0]]), titler=[feed[s][:38] for s in live[:3]])
            cat["MANUEL_no_axes"] += 1
        else:
            combos = [tuple(opts[s].get(a, "") for a in axes) for s in live]
            if len(combos) != len(set(combos)):
                rec.update(type="collision", axes=axes, status="manuel", reason="dublet variant-kombo",
                           title=housestyle(feed[live[0]]))
                cat["MANUEL_collision"] += 1
            else:
                # MULTI-titel = orakel. VALIDÉR: indeholder titlen et TAL der ikke findes i NOGEN
                # variants feed-titel? → orakel har en FORKERT attribut (fx "180" hvor alle er "280").
                title = next((oracle[s] for s in live if oracle.get(s)), None) or housestyle(feed[live[0]])
                all_feed = set().union(*[toks(feed[s]) for s in live])
                title_nums = {t for t in toks(title) if re.search(r"\d", t)}
                wrong = {t for t in title_nums if t not in all_feed}
                if wrong:
                    rec.update(type="multi", axes=axes, title=title, status="titel_manuel",
                               reason=f"orakel-titel har tal der ikke er i feed: {sorted(wrong)[:4]}",
                               feed_sample=clean(feed[live[0]])[:55])
                    cat["MANUEL_titel"] += 1
                else:
                    rec.update(type="multi", axes=axes, title=title, status="ok")
                    cat["ok_multi"] += 1
        plan[mid] = rec

    json.dump(plan, open("output/final_catalog_plan.json", "w", encoding="utf-8"), ensure_ascii=False)
    # CSV til gennemsyn
    out = r"C:\Users\APC\Desktop\final_catalog_plan.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f); w.writerow(["master_pid", "type", "status", "n_varianter", "titel", "akser", "reason"])
        for r in plan.values():
            w.writerow([r["master_pid"], r["type"], r["status"], r["n"], r.get("title", ""),
                        " · ".join(r.get("axes", [])), r.get("reason", "")])
    tot = len(plan)
    ok = cat["ok_single"] + cat["ok_multi"]
    man = sum(v for k, v in cat.items() if k.startswith("MANUEL"))
    print(f"\n=== KOMPLET PLAN: {tot} produkter ===")
    for k, n in cat.most_common():
        print(f"  {n:6d}  {k}")
    print(f"\n  ✅ AUTO-KORREKT (klar): {ok} ({100*ok/tot:.1f}%)")
    print(f"  ⚠ MANUEL STILLING: {man} ({100*man/tot:.1f}%)")
    print(f"\n  CSV til gennemsyn: {out}")

if __name__ == "__main__":
    main()
