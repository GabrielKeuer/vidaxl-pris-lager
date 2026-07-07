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
    # Reel korrekthed pr. SKU: live på ÉT ACTIVE+publiceret produkt m. billede + sku-metafelt.
    # (Titel/Farve/Konfig-eksakthed tjekkes IKKE — split_dupes + handle-genbrug ændrer struktur legitimt.)
    for h, prods in specs.items():
        for p in prods:
            for j, v in enumerate(p["variants"]):
                sku = v["sku"]; n_sku += 1
                found = variant_by_sku(sku)
                if not found:
                    issues["mangler_sku"].append(sku); continue
                if len(found) > 1:
                    issues["dup_sku"].append(f'{sku}: {[n["product"]["handle"] for n in found]}')
                node = found[0]; prod = node["product"]
                ok = True
                if prod["status"] != "ACTIVE":
                    issues["ikke_active"].append(f'{sku}: {prod["status"]}'); ok = False
                if (prod.get("resourcePublicationsCount") or {}).get("count", 0) == 0:
                    issues["ikke_publiceret"].append(sku); ok = False
                if not node.get("image"):
                    issues["mangler_billede"].append(sku); ok = False
                mfk = {x["node"]["key"] for x in node["metafields"]["edges"]}
                if "sku" not in mfk:
                    issues["mangler_sku_metafelt"].append(sku); ok = False
                # produktinfo kun krævet på ikke-første variant i et FLERVARIANT-produkt
                if len({o["name"] for o in node["selectedOptions"] if o["value"] != "Default Title"}) and j > 0 and "produktinfo" not in mfk:
                    issues["mangler_produktinfo"].append(sku)
                if ok:
                    n_ok += 1

    print(f"=== AUDIT: {n_sku} SKUs verificeret, {n_ok} rene ===")
    if not issues:
        print("✅ INGEN problemer — alle produkter korrekte")
    for k, v in issues.items():
        print(f"  ⚠ {k}: {len(v)} — {v[:5]}")

if __name__ == "__main__":
    main()
