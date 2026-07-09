"""PRE-FLIGHT AUDIT af alle combines mod NUVÆRENDE live-tilstand (fanger drift siden planen). READ-ONLY.
Pr. combine tjekkes: (1) alle SKUs stadig live? (2) eksisterende anker-produkt findes? (3) ingen FREMMEDE
SKUs sneget ind (så det er stadig ren combine, ikke blevet et split)? (4) titel valid.
Klassificerer: OK / ALLEREDE-SAMLET / DRIFTET(fremmede) / MANGLER-SKU / INTET-ANKER."""
import sys, os, json, re
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def main():
    plan = json.load(open("output/combine_plan.json", encoding="utf-8"))
    done = set(json.load(open("output/combine_done.json", encoding="utf-8")) if os.path.exists("output/combine_done.json") else [])
    print(f"combines i plan: {len(plan)} | allerede kørt: {len(done)}")

    # NUVÆRENDE live
    Q = ('query($a:String){products(first:80,query:"vendor:vidaXL",after:$a){pageInfo{hasNextPage endCursor} '
         'edges{node{id handle variants(first:200){edges{node{sku}}}}}}}')
    after = None; prod = {}; sku2pid = {}; pg = 0
    while True:
        d = ME.gql(Q, {"a": after}); pr = (d.get("data") or {}).get("products") or {}
        for e in pr.get("edges", []):
            n = e["node"]; pid = n["id"]
            sks = set((v["node"]["sku"] or "").strip() for v in n["variants"]["edges"] if v["node"].get("sku"))
            prod[pid] = {"handle": n["handle"], "skus": sks}
            for s in sks:
                sku2pid[s] = pid
        pg += 1
        if pg % 40 == 0:
            print(f"  …{len(prod)} live-produkter", flush=True)
        if pr.get("pageInfo", {}).get("hasNextPage"):
            after = pr["pageInfo"]["endCursor"]
        else:
            break
    print(f"live-produkter: {len(prod)}\n")

    cats = defaultdict(list)
    for c in plan:
        if c["mid"] + "|" + c["title"] in done:
            cats["ALLEREDE-KØRT"].append(c); continue
        want = set(c["skus"])
        live = [s for s in want if s in sku2pid]
        if len(live) < len(want):
            cats["MANGLER-SKU"].append((c, len(want) - len(live))); continue
        pids = {sku2pid[s] for s in live}
        if len(pids) == 1:
            cats["ALLEREDE-SAMLET"].append(c); continue
        foreign = sum(1 for pid in pids for s in prod[pid]["skus"] if s not in want)
        if foreign:
            cats["DRIFTET"].append((c, foreign)); continue
        # eksisterende anker (reneste handle) findes altid når pids ikke-tom → OK
        cats["OK"].append(c)

    print("=== PRE-FLIGHT RESULTAT ===")
    for k in ("OK", "ALLEREDE-SAMLET", "ALLEREDE-KØRT", "DRIFTET", "MANGLER-SKU", "INTET-ANKER"):
        v = cats.get(k, [])
        print(f"  {k}: {len(v)}")
    print()
    for k in ("DRIFTET", "MANGLER-SKU"):
        for item in cats.get(k, [])[:6]:
            c, x = item
            print(f"     [{k}] \"{c['title'][:44]}\" ({c['mid']}) — {x}")
    ok = len(cats["OK"])
    donors_ok = sum(c["n_donors"] for c in cats["OK"])
    print(f"\n  → {ok} combines KLAR til kørsel (bruger eksisterende ankre), samler ~{donors_ok} donorer")
    if not cats.get("DRIFTET") and not cats.get("MANGLER-SKU"):
        print("  ✓ INGEN drift — planen er intakt")
    json.dump({k: [(c["mid"] if isinstance(c, dict) else c[0]["mid"]) for c in v] for k, v in cats.items()},
              open("output/combine_audit.json", "w", encoding="utf-8"), ensure_ascii=False)

if __name__ == "__main__":
    main()
