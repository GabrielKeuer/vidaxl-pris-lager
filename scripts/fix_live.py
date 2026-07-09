"""FIX LIVE: ret de allerede-live forkert-oprettede vidaXL-produkter (de berørte master_pids) med den NYE
gruppering (per-SKU strip → gruppér-hvis-identisk → ellers single). Bygger fuld spec (titel + varianter +
billeder/beskrivelse fra feed + pris fra hub), opretter live, redirecter + sletter de gamle.
RESUMBAR (output/fix_live_done.json) + 1000-variant-limit-aware (--max-variants, stopper pænt, genoptag ved
gen-kørsel). Default = DRY-RUN; --live udfører. --limit N = kun første N produkter (test)."""
import sys, os, io, zipfile, json, argparse, re
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, r"C:\Users\APC\dropxl-product-automation\scripts")
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import build_complete_feed as B
import scope_split as SS
import regroup as RG
import cleanup_engine as CE
import pricing as PR

SIZE_AXES = {"Størrelse", "Højde", "Bredde", "Længde", "Dybde", "Bordlængde", "Diameter", "Størrelse 2", "Tykkelse"}
DONE_FILE = "output/fix_live_done.json"

def cap1(v):
    v = (v or "").strip()
    for i, ch in enumerate(v):
        if ch.isalpha():
            return v[:i] + ch.upper() + v[i+1:]
    return v

def regroup_master(mid, live, opts, titles, lbl):
    """→ liste af produkter: {key, title, specs:[(navn,[nøgle])], skus:[...] }"""
    axv = defaultdict(set)
    for s in live:
        for k, v in opts[s].items():
            axv[k].add(v)
    axes = sorted(k for k, vv in axv.items() if len(vv) > 1)
    namef = lambda k: ("Farve" if k == "color" else (lbl.get(mid, {}).get(k) or B.option_name(k, axv[k])))
    has_color = "color" in axes
    has_size = any(namef(k) in SIZE_AXES for k in axes)
    resid = {}
    for s in live:
        vals = [opts[s].get(k) for k in axes]
        resid[s] = B.strip_axes(B.clean(titles.get(s, "")), vals, strip_colors=has_color, strip_dims=has_size)
    groups = defaultdict(list)
    for s in live:
        groups[RG.canonical(resid[s])].append(s)
    out = []
    gi = 0
    for key, gsk in groups.items():
        gav = defaultdict(set)
        for s in gsk:
            for k in axes:
                gav[k].add(opts[s].get(k, ""))
        gaxes = [k for k in axes if len({x for x in gav[k] if x}) > 1]
        rep = Counter(resid[s] for s in gsk).most_common(1)[0][0]
        # variant-gruppe kræver IKKE-tom værdi for ALLE gaxes (Shopify afviser tomme option-værdier);
        # SKUs med tom akse-værdi bliver singles med deres fulde feed-titel.
        vskus = [s for s in gsk if gaxes and all(opts[s].get(k) for k in gaxes)]
        if len(vskus) >= 2 and RG.valid_group_title(rep):
            # KOLONNE-rækkefølge: Farve → Størrelse → resten (vilkårligt)
            def _colord(nm):
                return 0 if nm == "Farve" else (1 if nm in SIZE_AXES else 2)
            specs = sorted([(namef(k), [k]) for k in gaxes], key=lambda ns: _colord(ns[0]))
            gi += 1
            out.append({"key": f"{mid}_g{gi}", "title": rep, "specs": specs, "skus": vskus})
            singles = [s for s in gsk if s not in vskus]
        else:
            singles = gsk
        for s in singles:
            gi += 1
            out.append({"key": f"{mid}_g{gi}", "title": B.housestyle(B.clean(titles.get(s, ""))),
                        "specs": [], "skus": [s]})
    return out

def variant_sort_key(s, names, opts):
    """TAL-FØRST: ikke-Farve options (Størrelse/Watt/mål — naturlig rækkefølge) sorteres FØR Farve
    (vilkårlig rækkefølge). Delt nøgle mellem to_rows (variant_position) og reorder-steppet, så den
    variant der ender FØRST er den samme hvis indhold ligger på produkt-niveau."""
    nonf = [B.nat_val(cap1(" ".join(opts[s].get(k, "") for k in ks))) for nm, ks in names if nm != "Farve"]
    farve = [B.nat_val(cap1(" ".join(opts[s].get(k, "") for k in ks))) for nm, ks in names if nm == "Farve"]
    return tuple(nonf + farve)

def to_rows(prod, opts):
    """konvertér nyt produkt → br_variant_feed-format rækker (som build_spec forventer)."""
    names = prod["specs"][:3]
    skus = sorted(prod["skus"], key=lambda s: variant_sort_key(s, names, opts)) if names else sorted(prod["skus"])
    rows = []
    for pos, s in enumerate(skus, 1):
        row = {"sku": s, "product_key": prod["key"], "product_title": prod["title"],
               "variant_position": pos, "status": "variant" if names else "single"}
        for i in range(3):
            if i < len(names):
                nm, ks = names[i]
                row[f"option{i+1}_name"] = nm
                row[f"option{i+1}_value"] = cap1(" ".join(opts[s].get(k, "") for k in ks).strip())
            else:
                row[f"option{i+1}_name"] = None
                row[f"option{i+1}_value"] = None
        rows.append(row)
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="kun første N produkter (test)")
    ap.add_argument("--max-variants", type=int, default=1000, help="stop efter N oprettede varianter (genoptag ved gen-kørsel)")
    a = ap.parse_args()

    affected = json.load(open("output/pilot_check.json", encoding="utf-8"))["affected"]
    print(f"berørte master_pids: {len(affected)}")

    # feed som DataFrame (til build_spec) + titel-dict (til gruppering)
    feed = CE.load_feed_df()
    titles = feed["Title"].to_dict() if "Title" in feed.columns else {}
    SS.setup_universe(list(feed.index))
    for w in ("cremehvid", "cremehvide", "råhvid", "gråhvid", "offwhite", "sølvgrå", "koksgrå"):
        B.COLOR_UNIVERSE.add(w)
    B.build_color_re()
    lbl = json.load(open("output/axis_labels.json", encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}

    # master_pid → SKUs
    sb = ME.get_supabase_client()
    bym = defaultdict(list); fr = 0
    while True:
        b = sb.table("vidaxl_sku_master").select("sku,master_pid").range(fr, fr + 999).execute().data or []
        for x in b:
            if x["master_pid"] in set(affected):
                bym[x["master_pid"]].append(str(x["sku"]).strip())
        if len(b) < 1000:
            break
        fr += 1000

    # byg nye produkter
    all_prod = []
    for mid in affected:
        live = [s for s in bym.get(mid, []) if s in feed.index]
        if not live:
            continue
        opts = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        all_prod += regroup_master(mid, live, opts, titles, {mid: lbl.get(mid, {})})
    print(f"nye produkter: {len(all_prod)} (variant: {sum(1 for p in all_prod if p['specs'])}, single: {sum(1 for p in all_prod if not p['specs'])})")

    # byg specs
    cfg = PR.load_pricing_config(sb, vendor="vidaXL")
    rum = {}
    try:
        r = sb.table("hub_settings").select("value").eq("key", "vidaxl_rum_mapping").execute().data
        rum = (r[0]["value"] if r else {}) or {}
    except Exception:
        pass
    opts_by_mid = {}
    specs = []
    for prod in all_prod:
        mid = prod["key"].split("_g")[0]
        if mid not in opts_by_mid:
            opts_by_mid[mid] = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in bym.get(mid, [])}
        rows = to_rows(prod, opts_by_mid[mid])
        spec, flags = CE.build_spec(prod["key"], rows, feed, cfg, rum)
        if spec:
            spec["_flags"] = flags
            specs.append(spec)
    print(f"specs bygget: {len(specs)}")
    json.dump([{"key": s["product_key"], "title": s["title"], "n": len(s["variants"])} for s in specs],
              open("output/fix_live_specs.json", "w", encoding="utf-8"), ensure_ascii=False)

    if a.limit:
        specs = specs[:a.limit]

    done = set(json.load(open(DONE_FILE, encoding="utf-8")) if os.path.exists(DONE_FILE) else [])
    todo = [s for s in specs if s["product_key"] not in done]
    print(f"\n=== {'LIVE' if a.live else 'DRY-RUN'}: {len(specs)} produkter, {len(done)} allerede gjort, {len(todo)} tilbage ===")
    for s in todo[:5]:
        print(f"   {s['product_key']}: \"{s['title']}\" ({len(s['variants'])} var) {s['_flags'][:2]}")

    if not a.live:
        print("\n(dry-run — intet rørt. Kør med --live for at udføre.)")
        return

    import create_products_v2 as CP
    location_id = CP.get_primary_location_id()
    log = lambda m: print(m, flush=True)
    created = 0; vcount = 0; redir = 0; deleted = 0
    for s in todo:
        if vcount >= a.max_variants:
            print(f"\n⏸ nåede {a.max_variants}-variant-grænsen — stopper pænt. Kør igen for at fortsætte.")
            break
        skus = [v["sku"] for v in s["variants"]]
        ps = CE.to_product_spec(CP, s)
        res = CP.call_product_set(ps, location_id)
        errs = (res or {}).get("userErrors") or []
        if errs or not (res or {}).get("product"):
            log(f"   ✗ {s['product_key']}: {errs[:2] or 'intet produkt'}")
            continue
        prod = res["product"]; new_id = prod["id"]; new_handle = prod["handle"]
        try:
            CP.publish_to_all_channels(new_id)
        except Exception as e:
            log(f"     (publish-advarsel: {e})")
        created += 1; vcount += len(skus)
        ME.del_self_redirect(f"/products/{new_handle}", False, log)
        for oid, ohandle in CE.old_products_for_skus(skus, new_id).items():
            ME.create_redirect(f"/products/{ohandle}", f"/products/{new_handle}", False, log, sb)
            redir += 1
            ME.delete_product(oid, ohandle, False, log)
            deleted += 1
        done.add(s["product_key"])
        json.dump(sorted(done), open(DONE_FILE, "w"), ensure_ascii=False)
        if created % 10 == 0:
            print(f"   … {created} oprettet, {vcount} varianter, {redir} redirects, {deleted} slettet", flush=True)
    print(f"\n=== FÆRDIG (denne kørsel): {created} oprettet, {vcount} varianter, {redir} redirects, {deleted} gamle slettet ===")
    print(f"    total gjort: {len(done)}/{len(specs)}")

if __name__ == "__main__":
    main()
