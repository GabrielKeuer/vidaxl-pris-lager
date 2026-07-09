"""COMBINE-MOTOR (cutover for SAMLING). For hver combine-gruppe: opdatér ANKER-produktet in-place via
productSet (behold handle/URL/SEO) så det får ALLE gruppens SKUs som varianter + korrekt titel + sortering,
og slet donor-produkterne + redirect dem til ankeret. Splitter INTET.
Resumbar (output/combine_done.json) + 1000-variant/dag-limit (--max-variants). Default DRY-RUN; --live udfører.
--only <master_pid> og --limit N til test."""
import sys, os, json, argparse
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, r"C:\Users\APC\dropxl-product-automation\scripts")
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import build_complete_feed as B
import scope_split as SS
import cleanup_engine as CE
import fix_live as FL
import pricing as PR

DONE = "output/combine_done.json"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--only", default="")
    ap.add_argument("--max-variants", type=int, default=1000)
    a = ap.parse_args()

    plan = json.load(open("output/combine_plan.json", encoding="utf-8"))
    if a.only:
        plan = [c for c in plan if c["mid"] == a.only]
    print(f"combine-grupper i plan: {len(plan)}")

    feed = CE.load_feed_df(); titles = feed["Title"].to_dict()
    SS.setup_universe(list(feed.index))
    for w in ("cremehvid", "cremehvide", "råhvid", "gråhvid", "offwhite", "sølvgrå", "koksgrå"):
        B.COLOR_UNIVERSE.add(w)
    B.build_color_re()
    lbl = json.load(open("output/axis_labels.json", encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}
    sb = ME.get_supabase_client()
    cfg = PR.load_pricing_config(sb, vendor="vidaXL")
    rum = {}
    try:
        r = sb.table("hub_settings").select("value").eq("key", "vidaxl_rum_mapping").execute().data
        rum = (r[0]["value"] if r else {}) or {}
    except Exception:
        pass

    # byg spec pr. combine (find den matchende gruppe i masteren)
    opts_cache = {}
    def build(c):
        mid = c["mid"]
        if mid not in opts_cache:
            r = sb.table("vidaxl_sku_master").select("sku").eq("master_pid", mid).execute().data or []
            live = [str(x["sku"]) for x in r if str(x["sku"]) in feed.index]
            opts_cache[mid] = ({s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}, live)
        opts, live = opts_cache[mid]
        want = set(c["skus"])
        for p in FL.regroup_master(mid, live, opts, titles, {mid: lbl.get(mid, {})}):
            if set(p["skus"]) == want:
                rows = FL.to_rows(p, opts)
                spec, _ = CE.build_spec(p["key"], rows, feed, cfg, rum)
                return spec
        return None

    done = set(json.load(open(DONE, encoding="utf-8")) if os.path.exists(DONE) else [])
    todo = [c for c in plan if c["mid"] + "|" + c["title"] not in done]
    if a.limit:
        todo = todo[:a.limit]
    print(f"{'LIVE' if a.live else 'DRY-RUN'}: {len(plan)} i plan, {len(done)} gjort, {len(todo)} i denne kørsel")

    if not a.live:
        for c in todo[:5]:
            print(f"   ANKER {c['anchor_handle']} ← {c['n_donors']} donorer → \"{c['title'][:44]}\" ({c['n_skus']} SKU)")
        print("(dry-run — intet rørt)")
        return

    import create_products_v2 as CP
    import re as _re
    loc = CP.get_primary_location_id(); log = lambda m: print(m, flush=True)
    REORDER = ('mutation($p:ID!,$pos:[ProductVariantPositionInput!]!){'
               'productVariantsBulkReorder(productId:$p,positions:$pos){userErrors{message}}}')

    def natv(v):
        n = _re.findall(r"\d+\.?\d*", v or "")
        return (0, [float(x) for x in n]) if n else (1, [(v or "").lower()])

    def reorder(pid):
        """TAL-FØRST: sortér varianter (ikke-Farve options efter position, Farve sidst) → option-værdierne
        gen-sorteres. SAMME nøgle som fix_live.variant_sort_key, så første variant matcher produkt-indholdet."""
        d = ME.gql('query($id:ID!){product(id:$id){options{name position} '
                   'variants(first:250){edges{node{id selectedOptions{name value}}}}}}', {"id": pid})
        p = (d.get("data") or {}).get("product") or {}
        nonf = [o["name"] for o in sorted(p.get("options", []), key=lambda o: o["position"]) if o["name"] != "Farve"]
        vs = [e["node"] for e in p.get("variants", {}).get("edges", [])]
        def vk(node):
            so = {o["name"]: o["value"] for o in node["selectedOptions"]}
            return tuple([natv(so.get(n, "")) for n in nonf] + [natv(so.get("Farve", ""))])
        svs = sorted(vs, key=vk)
        if [v["id"] for v in svs] != [v["id"] for v in vs]:
            pos = [{"id": v["id"], "position": i + 1} for i, v in enumerate(svs)]
            for i in range(0, len(pos), 250):
                ME.gql(REORDER, {"p": pid, "pos": pos[i:i + 250]})

    merged = vcount = redir = deleted = 0
    for c in todo:
        if vcount >= a.max_variants:
            log(f"\n⏸ nåede {a.max_variants}-variant-grænsen — stopper. Kør igen (cron) for at fortsætte.")
            break
        spec = build(c)
        if not spec:
            log(f"   ✗ {c['mid']}: kunne ikke genskabe gruppe-spec"); continue
        # alle fragmenter der holder gruppens SKUs → anker = det med RENESTE handle (bedste SEO)
        frag = CE.old_products_for_skus(c["skus"], "none")   # {pid: handle}
        if not frag:
            log(f"   ✗ {c['mid']}: ingen live-fragmenter fundet"); continue
        anchor_pid = min(frag, key=lambda pid: (1 if _re.search(r"-\d+$", frag[pid]) else 0, len(frag[pid])))
        # IN-PLACE merge på ankeret (behold ID + handle + URL + SEO) — build_spec sætter produkt-indhold +
        # metafelter (første variant: kun SKU; øvrige: SKU + produktinfo + variantbilleder)
        res = CP.call_product_set(CE.to_product_spec(CP, spec), loc, product_id=anchor_pid)
        errs = (res or {}).get("userErrors") or []
        if errs or not (res or {}).get("product"):
            log(f"   ✗ {c['mid']} \"{spec['title'][:34]}\": {errs[:2] or 'intet produkt'}"); continue
        pr = res["product"]; new_id = pr["id"]; new_handle = pr["handle"]
        try:
            CP.publish_to_all_channels(new_id)
        except Exception:
            pass
        reorder(new_id)   # sortér option-værdier (tal-først)
        merged += 1; vcount += c["n_donors"]
        # redirect + slet donor-fragmenterne (≠ anker)
        for oid, oh in frag.items():
            if oid == anchor_pid:
                continue
            ME.create_redirect(f"/products/{oh}", f"/products/{new_handle}", False, lambda m: None, sb)
            redir += 1
            ME.delete_product(oid, oh, False, lambda m: None)
            deleted += 1
        done.add(c["mid"] + "|" + c["title"])
        json.dump(sorted(done), open(DONE, "w", encoding="utf-8"), ensure_ascii=False)
        if merged % 20 == 0:
            log(f"   … {merged} merges, ~{vcount} varianter, {deleted} donorer slettet", flush=True)
    log(f"\n=== FÆRDIG (denne kørsel): {merged} merges, ~{vcount} varianter tilføjet, {redir} redirects, {deleted} donorer slettet ===")
    log(f"    total gjort: {len(done)}/{len(plan)}")

if __name__ == "__main__":
    main()
