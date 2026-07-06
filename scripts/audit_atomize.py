"""SLAVISK AUDIT af atomize/split-resultatet: for hver SKU i atomize_specs.json — verificér at den nu
ligger på ét ACTIVE, publiceret produkt med forventet titel + Farve/Konfiguration-option, billede og
korrekte metafelter. Flag: manglende SKUs, forkert titel/option, ikke-publiceret, manglende billede,
overlevende keeper-produkter. READ-ONLY."""
import json, os, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def variant_by_sku(sku):
    d = ME.gql('query($q:String!){productVariants(first:5,query:$q){edges{node{sku '
               'selectedOptions{name value} image{url} '
               'metafields(first:6){edges{node{key}}} '
               'product{title status handle resourcePublicationsCount{count}}}}}}', {"q": f"sku:{sku}"})
    return [e["node"] for e in (((d.get("data") or {}).get("productVariants") or {}).get("edges") or [])
            if (e["node"]["sku"] or "").strip() == str(sku)]

def main():
    specs = json.load(open("output/atomize_specs.json", encoding="utf-8"))
    issues = defaultdict(list)
    n_sku = n_ok = 0
    # 1) keepers slettet?
    for h in specs:
        d = ME.gql("query($h:String!){productByHandle(handle:$h){id}}", {"h": h})
        if (d.get("data") or {}).get("productByHandle"):
            issues["keeper_overlever"].append(h)
    # 2) hver SKU verificeres mod forventet spec
    for h, prods in specs.items():
        for p in prods:
            multi_farve = len({v.get("Farve") for v in p["variants"] if v.get("Farve")}) > 1
            multi_konf = len({v.get("Konfiguration") for v in p["variants"] if v.get("Konfiguration")}) > 1
            for j, v in enumerate(p["variants"]):
                sku = v["sku"]; n_sku += 1
                found = variant_by_sku(sku)
                if not found:
                    issues["mangler_sku"].append(sku); continue
                if len(found) > 1:
                    issues["dup_sku"].append(sku)
                node = found[0]; prod = node["product"]
                ok = True
                if prod["title"] != p["title"]:
                    issues["forkert_titel"].append(f'{sku}: "{prod["title"]}" != "{p["title"]}"'); ok = False
                if prod["status"] != "ACTIVE":
                    issues["ikke_active"].append(f'{sku}: {prod["status"]}'); ok = False
                if (prod.get("resourcePublicationsCount") or {}).get("count", 0) == 0:
                    issues["ikke_publiceret"].append(sku); ok = False
                so = {o["name"]: o["value"] for o in node["selectedOptions"]}
                if multi_farve and v.get("Farve") and so.get("Farve") != v["Farve"]:
                    issues["forkert_farve"].append(f'{sku}: {so.get("Farve")} != {v["Farve"]}'); ok = False
                if multi_konf and v.get("Konfiguration") and so.get("Konfiguration") != v["Konfiguration"]:
                    issues["forkert_konfig"].append(f'{sku}: {so.get("Konfiguration")}'); ok = False
                if not node.get("image"):
                    issues["mangler_billede"].append(sku); ok = False
                mfk = {x["node"]["key"] for x in node["metafields"]["edges"]}
                if "sku" not in mfk:
                    issues["mangler_sku_metafelt"].append(sku); ok = False
                if j > 0 and (multi_farve or multi_konf) and "produktinfo" not in mfk:
                    issues["mangler_produktinfo"].append(sku); ok = False
                if ok:
                    n_ok += 1

    print(f"=== AUDIT: {n_sku} SKUs verificeret, {n_ok} rene ===")
    if not issues:
        print("✅ INGEN problemer — alle produkter korrekte")
    for k, v in issues.items():
        print(f"  ⚠ {k}: {len(v)} — {v[:5]}")

if __name__ == "__main__":
    main()
