"""Re-label + re-evaluér alle merge/fix-grupper med KORREKT per-nøgle-navngivning (build_keyname
fra eksekutoren). Retter fejl-splittede akser (fx daybed variationAttribute3 = ÉN 'Model'-akse, ikke
Model+Materiale), opdaterer gemte option_values + target_axes, og OPHÆVER falske >3-akse-karantæner.
Bevarer titel-review + dup-karantæne (separate beslutninger). --write for at gemme."""
import json, os, sys
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

SP = r"C:\Users\APC\AppData\Local\Temp\claude\C--Users-APC\c0b60326-0d7f-46aa-bec2-7289b435d558\scratchpad"

def main():
    write = "--write" in sys.argv
    plans = [json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8")]
    cache = json.load(open(SP + r"\plan_data_cache.json", encoding="utf-8"))
    varz, prods = cache["vars"], cache["prods"]
    h2pid = {pr["handle"]: pid for pid, pr in prods.items() if isinstance(pr, dict) and pr.get("handle")}
    pid2skus = defaultdict(list)
    for s, vv in varz.items():
        pid2skus[vv.get("pid")].append(s)

    relabeled = resolved = still_coll = still_over = 0
    for p in plans:
        if p["action"] not in ("merge", "fix_mismerge_rest"):
            continue
        if p.get("unresolved_collisions") == [["title_review"]]:
            continue  # titel-review = separat beslutning — rør ikke
        added = [m["sku"] for m in p["variant_creates"]]
        if not added:
            continue
        master = p["key"].split("|")[1] if "|" in p["key"] else ""
        kskus = pid2skus.get(h2pid.get(p["keeper_handle"]), [])
        km = ME.build_keyname(added + kskus, master)
        # relabel de tilføjede varianters options konsistent
        ch = 0
        for m in p["variant_creates"]:
            newov = ME.danish_opts(m["sku"], master, km)
            if newov and newov != m["option_values"]:
                m["option_values"] = newov; ch += 1
        if ch:
            relabeled += 1
        # reelle akser på HELE sættet
        vals = defaultdict(set)
        for s in set(added + kskus):
            for k, v in ME.danish_opts(s, master, km).items():
                if v: vals[k].add(v)
        real_axes = sorted(k for k, vv in vals.items() if len(vv) > 1)
        # drop glitch-akse(r) hvis >3 (domineret + redundant) → re-filtrér gemte options
        if len(real_axes) > 3:
            allopts = [ME.danish_opts(s, master, km) for s in set(added + kskus)]
            reduced, dropped = ME.reduce_to_3_axes(allopts, real_axes)
            if dropped:
                real_axes = sorted(reduced)
                for m in p["variant_creates"]:
                    m["option_values"] = {k: v for k, v in (m["option_values"] or {}).items() if k in real_axes}
        # kollision på tilføjede (efter relabel + evt. akse-drop)
        sig = Counter(tuple(sorted((m["option_values"] or {}).items())) for m in p["variant_creates"])
        coll = sum(v - 1 for v in sig.values() if v > 1)
        was_q = bool(p.get("unresolved_collisions") or p.get("dup_sku_quarantine"))
        if len(real_axes) > 3:
            still_over += 1
            p["unresolved_collisions"] = [["over_3_axes"]]
            p["needs_review"] = f">3 akser ({real_axes})"
            p.pop("dup_sku_quarantine", None)
            continue
        if coll > 0:
            # disambiguér (som eksekutoren): to varianter m. samme kombo → suffiks på sidste akse
            seen = set(); axis = real_axes[-1] if real_axes else None
            for m in p["variant_creates"]:
                ov = m["option_values"] or {}
                if frozenset(ov.items()) in seen and axis:
                    base = ov.get(axis, "—"); nn = 2
                    while frozenset({**ov, axis: f"{base} {nn}"}.items()) in seen:
                        nn += 1
                    ov[axis] = f"{base} {nn}"; m["option_values"] = ov
                seen.add(frozenset((m["option_values"] or {}).items()))
            still_coll += 1
        if was_q:
            p.pop("unresolved_collisions", None); p.pop("needs_review", None)
            p.pop("dup_sku_quarantine", None); resolved += 1
        p["target_axes"] = real_axes

    print(f"relabeled (options rettet): {relabeled}")
    print(f"OPHÆVET karantæne (falsk >3/kollision): {resolved}")
    print(f"stadig >3 akser: {still_over} | stadig ægte kollision: {still_coll}")
    if write:
        with open("output/merge_plan.jsonl", "w", encoding="utf-8") as f:
            for p in plans:
                f.write(json.dumps(p, ensure_ascii=False) + "\n")
        print("✅ plan opdateret")
    else:
        print("(dry-run)")

if __name__ == "__main__":
    main()
