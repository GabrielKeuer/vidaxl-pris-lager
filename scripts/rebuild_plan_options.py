"""Genopbyg merge-planens variant-options fra item_variant (autoritativ kilde) + verifikations-delta.
color→Farve; attr2/attr3→akse via værdi-inferens (mål→Størrelse, materiale→Materiale, tal→Antal, ellers Model).
Rapporterer: hvor mange grupper får ændrede options (verifikation), hvor mange kollisioner opløses.
DRY-RUN som default (skriver kun med --write). READ-ONLY ift. Shopify."""
import json, os, re, sys
from collections import defaultdict, Counter
sys.stdout.reconfigure(encoding="utf-8")
PLAN = "output/merge_plan.jsonl"
VAR = "output/sku_variants.jsonl"

_MAT = ("træ", "stål", "læder", "stof", "polyrattan", "rattan", "metal", "glas", "aluminium", "bambus",
        "jern", "fløjl", "velour", "kunstlæder", "plastik", "mango", "fyrre", "akacie", "eg", "teak",
        "massiv", "textilene", "poly", "bomuld", "mikrofiber", "gummi", "douglas", "gran", "bøg", "birk")

def axis_name(values):
    vals = [str(v).strip() for v in values if v not in (None, "")]
    if not vals:
        return None
    n = len(vals)
    if sum(1 for v in vals if re.search(r"\d+\s*[x×]\s*\d+|\bcm\b|\bmm\b|\bm²|\bm\b|ø\d", v.lower())) >= n * 0.5:
        return "Størrelse"
    if sum(1 for v in vals if any(m in v.lower() for m in _MAT)) >= n * 0.5:
        return "Materiale"
    if all(re.fullmatch(r"\d+", v) for v in vals):
        return "Antal i pakke"
    return "Model"

def load_variants():
    """Returnerer {sku: {item_variant_nøgle: værdi}} — HELE dicten (color, variationAttribute1/2/3,
    numberOfNumber m.fl.)."""
    d = {}
    for l in open(VAR, encoding="utf-8"):
        try:
            r = json.loads(l)
            if not (r.get("note") == "ok" and r.get("opts")):
                continue
            # '_binary'-nøgler er samme akse-slot som ikke-binary (produkt bruger enten
            # liste- eller Ja/Nej-form) → flet ind i base-slot, ikke-binary vinder
            opts = {k: v for k, v in r["opts"].items() if not k.endswith("_binary")}
            for k, v in r["opts"].items():
                if k.endswith("_binary"):
                    opts.setdefault(k[:-len("_binary")], v)
            d[str(r["sku"]).strip()] = opts
        except Exception:
            pass
    return d

def main():
    write = "--write" in sys.argv
    iv = load_variants()
    labels = json.load(open("output/axis_labels.json", encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}
    print(f"📚 {len(iv)} SKUs med item_variant | {len(labels)} masters med eksakte akse-navne")
    plans = [json.loads(l) for l in open(PLAN, encoding="utf-8")]

    stats = Counter()
    delta_groups = 0; resolved = 0; still = 0; not_covered = 0
    ex = []
    for p in plans:
        if p["action"] not in ("merge", "split", "atomize", "fix_mismerge_rest"):
            continue
        skus = [m["sku"] for m in p["variant_creates"]]
        if not skus:
            continue
        cov = [s for s in skus if s in iv]
        if len(cov) < len(skus):
            not_covered += 1
            continue  # genopbyg KUN fuldt-scrapede grupper (ellers blandes gamle+nye akser → falske 4-akser)
        # dansk akse-navn pr. item_variant-nøgle: eksakt vidaXL-label (scrape) → ellers værdi-inferens
        master = p["key"].split("|")[1] if "|" in p["key"] else p["key"]
        mlabels = labels.get(master, {})
        keys = set()
        for s in cov:
            keys |= set(iv[s].keys())
        keyname = {}
        for k in sorted(keys):
            nm = mlabels.get(k) or ("Farve" if k == "color"
                                    else (axis_name([iv[s].get(k) for s in cov if iv[s].get(k)]) or "Model"))
            base, c = nm, 2
            while nm in keyname.values():   # undgå navne-kollision (to nøgler → samme navn)
                nm = f"{base} {c}"; c += 1
            keyname[k] = nm
        changed = 0
        for m in p["variant_creates"]:
            s = m["sku"]
            if s not in iv:
                continue
            newov = {keyname[k]: val for k, val in iv[s].items() if val and keyname.get(k)}
            if p["action"] == "split":       # split beholder Model-aksen som identitet
                newov.pop("Model", None)
            if newov and newov != m["option_values"]:
                changed += 1
                m["option_values"] = newov
        if changed:
            delta_groups += 1
        # re-detektér kollisioner på de dækkede SKUs
        sig = Counter(tuple(sorted((m["option_values"] or {}).items()))
                      for m in p["variant_creates"] if m["sku"] in iv)
        coll = sum(v - 1 for v in sig.values() if v > 1)
        axes = sorted({k for m in p["variant_creates"] for k in (m["option_values"] or {})})
        had_q = bool(p.get("unresolved_collisions")) or bool(p.get("dup_sku_quarantine"))
        if coll == 0 and len(axes) <= 3 and len(cov) == len(skus):
            if had_q:
                p.pop("unresolved_collisions", None); p.pop("dup_sku_quarantine", None); resolved += 1
            p["target_axes"] = axes
        elif coll > 0 or len(axes) > 3:
            # stadig ægte kollision/for mange akser efter item_variant → KARANTÆNE (merg ikke forkert)
            sig2 = defaultdict(list)
            for m in p["variant_creates"]:
                if m["sku"] in iv:
                    sig2[tuple(sorted((m["option_values"] or {}).items()))].append(m["sku"])
            p["unresolved_collisions"] = [v for v in sig2.values() if len(v) > 1] or [[m["sku"] for m in p["variant_creates"]]]
            p.pop("dup_sku_quarantine", None)   # superseret af unresolved_collisions
            p["target_axes"] = axes
            still += 1
        else:
            still += 1
            if len(ex) < 6 and coll > 0:
                ex.append((p["key"], coll, axes))

    print(f"\n=== VERIFIKATIONS-DELTA ===")
    print(f"  grupper hvor options ÆNDREDES (var forkerte): {delta_groups}")
    print(f"  karantæne-grupper OPLØST: {resolved}")
    print(f"  stadig uløst/ikke-fuldt-dækket: {still}")
    print(f"  grupper ikke fuldt scrapet endnu: {not_covered}")
    for k, c, a in ex:
        print(f"    rest {k}: {c} kollisioner, akser={a}")
    if write:
        with open(PLAN, "w", encoding="utf-8") as f:
            for p in plans:
                f.write(json.dumps(p, ensure_ascii=False) + "\n")
        print("\n✅ plan opdateret (--write)")
    else:
        print("\n(dry-run — kør med --write for at gemme)")

if __name__ == "__main__":
    main()
