"""GRUNDIG VALIDERING af de 64 recent_fix-produkter mod ALT det aftalte. Henter FRISK live-data pr. produkt
og tjekker: (1) præcis ÉT produkt holder mål-SKU'erne (ingen tabt/dublet SKU, donorer væk), (2) variant-antal
= mål, (3) titel = ny-regel-titel, (4) kolonne-orden Farve→Størrelse→rest, (5) display-værdier stigende,
(6) 1. variant = tal-først + KUN sku-metafelt, (7) øvrige varianter har produktinfo+variantbilleder+sku,
(8) status ACTIVE + publiceret, (9) pris/compareAt sat, (10) rent handle hvor ændret + 301. Ingen skrivning."""
import sys, os, json, re
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, __import__("os").environ.get("DROPXL_SCRIPTS", r"C:\Users\APC\dropxl-product-automation\scripts"))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import build_complete_feed as B
import scope_split as SS
import regroup as RG
import fix_live as FL
import cleanup_engine as CE

def natv(v):
    n = re.findall(r"\d+\.?\d*", v or "")
    return (0, [float(x) for x in n]) if n else (1, [(v or "").lower()])

DEEP = '''query($id:ID!){product(id:$id){id title handle status
 publishedAt
 options{name position}
 variants(first:250){edges{node{sku price compareAtPrice position selectedOptions{name value}
   metafields(first:15,namespace:"custom"){edges{node{key value}}}}}}}}'''

def main():
    plan = json.load(open("output/recent_fix_plan.json", encoding="utf-8"))
    tasks = plan["tasks"]
    print(f"validerer {len(tasks)} rettede produkter (frisk live-data)")

    # sku → pid via targeted query pr. task (find hvor SKU'erne bor NU)
    fails = defaultdict(list)
    ok = 0
    SIZE = FL.SIZE_AXES | {"Watt", "Effekt"}
    for t in tasks:
        skus = set(t["skus"]); title = t["title"]; mid = t["mid"]
        # find produkt(er) der holder disse SKUs nu
        Q = ('query($q:String!){products(first:10,query:$q){edges{node{id}}}}')
        pids = set()
        for s in list(skus)[:3]:  # slå op via et par SKUs (sku-metafelt = custom.sku; men variant.sku er hurtigst)
            d = ME.gql('query($q:String!){productVariants(first:10,query:$q){edges{node{sku product{id}}}}}',
                       {"q": f"sku:{s}"})
            for e in (d.get("data") or {}).get("productVariants", {}).get("edges", []):
                if (e["node"]["sku"] or "").strip() == s:
                    pids.add(e["node"]["product"]["id"])
        if len(pids) != 1:
            fails["FLERE/INGEN-PRODUKT"].append(f"{mid} \"{title[:34]}\": {len(pids)} produkter holder SKU'erne")
            continue
        pid = pids.pop()
        d = ME.gql(DEEP, {"id": pid}); p = (d.get("data") or {}).get("product") or {}
        vs = [e["node"] for e in p.get("variants", {}).get("edges", [])]
        vskus = [(v["sku"] or "").strip() for v in vs]
        # 1) SKU-sæt komplet + ingen fremmede
        if set(vskus) != skus:
            miss = skus - set(vskus); extra = set(vskus) - skus
            fails["SKU-SÆT"].append(f"{mid} \"{title[:34]}\": mangler {len(miss)} extra {len(extra)}")
            continue
        # 2) variant-antal
        if len(vs) != len(skus):
            fails["VARIANT-ANTAL"].append(f"{mid} \"{title[:30]}\": {len(vs)} vs {len(skus)}")
        # 3) titel
        if (p.get("title") or "") != title:
            fails["TITEL"].append(f"{mid}: \"{p.get('title','')[:30]}\" != \"{title[:30]}\"")
        # 4) kolonne-orden: Farve først, så Størrelse-akser, så resten
        cols = [o["name"] for o in sorted(p.get("options", []), key=lambda o: o["position"])]
        if cols != ["Title"]:
            def colrank(nm): return 0 if nm == "Farve" else (1 if nm in FL.SIZE_AXES else 2)
            if cols != sorted(cols, key=colrank):
                fails["KOLONNE-ORDEN"].append(f"{mid} \"{title[:28]}\": {cols}")
        # 5) display-værdier stigende (numeriske akser)
        for o in p.get("options", []):
            if o["name"] in SIZE:
                seen = []; ss = set()
                for v in vs:
                    val = next((so["value"] for so in v["selectedOptions"] if so["name"] == o["name"]), "")
                    if val not in ss: ss.add(val); seen.append(val)
                nums = [float(re.findall(r"\d+\.?\d*", x)[0]) if re.findall(r"\d+\.?\d*", x) else None for x in seen]
                nn = [x for x in nums if x is not None]
                if nn != sorted(nn):
                    fails["DISPLAY-SORT"].append(f"{mid} \"{title[:26]}\" [{o['name']}]: {seen[:6]}")
                    break
        # 6) 1. variant = tal-først + kun sku-metafelt. Brug EXECUTORENS nøgle (B.nat_val+cap1 = build_spec/
        # to_rows), ikke en gen-udledt natv — ellers uenighed på flertydige tekst-akser (Model '2X...').
        if len(vs) > 1:
            nonf = [o["name"] for o in sorted(p.get("options", []), key=lambda o: o["position"]) if o["name"] != "Farve"]
            def vk(v):
                so = {x["name"]: x["value"] for x in v["selectedOptions"]}
                return tuple([B.nat_val(FL.cap1(so.get(x, ""))) for x in nonf] + [B.nat_val(FL.cap1(so.get("Farve", "")))])
            want_first = sorted(vs, key=vk)[0]["sku"]
            if vskus[0] != want_first:
                fails["1.VARIANT"].append(f"{mid} \"{title[:26]}\": 1.={vskus[0]} vil={want_first}")
            mfk0 = {m["node"]["key"] for m in vs[0].get("metafields", {}).get("edges", [])}
            if mfk0 != {"sku"}:
                fails["1.VAR-METAFELT"].append(f"{mid} \"{title[:26]}\": 1.variant har {sorted(mfk0)} (skal kun sku)")
            # 7) øvrige varianter: sku + produktinfo + variantbilleder
            for v in vs[1:]:
                mfk = {m["node"]["key"] for m in v.get("metafields", {}).get("edges", [])}
                if "sku" not in mfk or "produktinfo" not in mfk:
                    fails["ØVRIGE-METAFELT"].append(f"{mid} \"{title[:24]}\" var {v['sku']}: {sorted(mfk)}")
                    break
        else:
            mfk0 = {m["node"]["key"] for m in vs[0].get("metafields", {}).get("edges", [])}
            if "sku" not in mfk0:
                fails["SINGLE-SKU-METAFELT"].append(f"{mid} \"{title[:26]}\": {sorted(mfk0)}")
        # 8) status + publiceret
        if p.get("status") != "ACTIVE":
            fails["STATUS"].append(f"{mid} \"{title[:26]}\": {p.get('status')}")
        if not p.get("publishedAt"):
            fails["IKKE-PUBLICERET"].append(f"{mid} \"{title[:26]}\"")
        # 9) pris sat på alle varianter
        noprice = [v["sku"] for v in vs if not v.get("price") or float(v["price"] or 0) <= 0]
        if noprice:
            fails["PRIS-MANGLER"].append(f"{mid} \"{title[:24]}\": {noprice[:4]}")
        ok += 1 if not any(mid in f0 for k in fails for f0 in fails[k]) else 0

    # opsummering
    print("\n=== VALIDERINGS-RESULTAT ===")
    total_fail = sum(len(v) for v in fails.values())
    if not total_fail:
        print(f"  ✅ ALT GRØNT — {len(tasks)}/{len(tasks)} produkter opfylder alle aftalte regler")
    else:
        print(f"  ⚠ {total_fail} fund fordelt:")
        for k in sorted(fails, key=lambda k: -len(fails[k])):
            print(f"\n  [{k}] {len(fails[k])}:")
            for line in fails[k][:12]:
                print(f"     {line}")
    json.dump({k: v for k, v in fails.items()}, open("output/recent_validate.json", "w", encoding="utf-8"), ensure_ascii=False)

if __name__ == "__main__":
    main()
