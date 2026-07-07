"""FULD KATALOG-AUDIT: sampler aktive vidaXL-produkter og tjekker alle kvalitets-dimensioner:
body_html, tags, productType, Farve=option 1, variant-værdier natural-sorteret, priser, samt at hver
variant har native billede + sku-metafelt. Rapporterer sundhed% + flagger systematiske problemer.
--n <sample> (default 600); --all for hele kataloget."""
import sys, os, re
from collections import Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def nat(v):
    nums = re.findall(r"\d+\.?\d*", v or "")
    return (0, [float(n) for n in nums], (v or "").lower()) if nums else (1, [], (v or "").lower())

def main():
    do_all = "--all" in sys.argv
    limit = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else 600
    issues = Counter(); checked = 0; multi = 0; cursor = None
    while True:
        q = ('query($c:String){products(first:60,after:$c,query:"vendor:vidaXL status:active"){'
             'pageInfo{hasNextPage endCursor} edges{node{handle title descriptionHtml tags productType '
             'options{position name optionValues{name}} '
             'variants(first:120){edges{node{sku price image{url} metafields(first:6){edges{node{key}}}}}}}}}}')
        d = ME.gql(q, {"c": cursor})
        pr = (d.get("data") or {}).get("products") or {}
        for e in pr.get("edges", []):
            p = e["node"]; checked += 1
            if not (p.get("descriptionHtml") or "").strip():
                issues["mangler_body_html"] += 1
            if not (p.get("tags") or []):
                issues["mangler_tags"] += 1
            if not (p.get("productType") or "").strip():
                issues["mangler_product_type"] += 1
            opts = p["options"]
            farve = [o for o in opts if o["name"] == "Farve"]
            if farve and farve[0]["position"] != 1:
                issues["farve_ikke_option1"] += 1
            # variant-værdier sorteret?
            for o in opts:
                vals = [v["name"] for v in o["optionValues"]]
                if len(vals) > 2 and vals != sorted(vals, key=nat):
                    issues["variant_vaerdier_usorteret"] += 1
                    break
            vs = [x["node"] for x in p["variants"]["edges"]]
            if len(vs) > 1:
                multi += 1
            for n in vs:
                if not n.get("image"):
                    issues["variant_uden_billede"] += 1; break
            for n in vs:
                if "sku" not in {x["node"]["key"] for x in n["metafields"]["edges"]}:
                    issues["variant_uden_sku_metafelt"] += 1; break
            if any((n.get("price") in (None, "0.00", "0")) for n in vs):
                issues["variant_uden_pris"] += 1
        if not do_all and checked >= limit:
            break
        if not pr.get("pageInfo", {}).get("hasNextPage"):
            break
        cursor = pr["pageInfo"]["endCursor"]
    print(f"=== KATALOG-AUDIT: {checked} aktive vidaXL-produkter ({multi} multi-variant) ===")
    if not issues:
        print("✅ INGEN problemer på nogen dimension")
    for k, n in issues.most_common():
        print(f"  ⚠ {k}: {n} ({100*n/checked:.1f}%)")

if __name__ == "__main__":
    main()
