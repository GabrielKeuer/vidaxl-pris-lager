"""COMBINE-MOTOR (cutover for SAMLING). For hver combine-gruppe: opdatér ANKER-produktet in-place via
productSet (behold handle/URL/SEO) så det får ALLE gruppens SKUs som varianter + korrekt titel + sortering,
og slet donor-produkterne + redirect dem til ankeret. Splitter INTET.
Resumbar (output/combine_done.json) + 1000-variant/dag-limit (--max-variants). Default DRY-RUN; --live udfører.
--only <master_pid> og --limit N til test."""
import sys, os, json, argparse
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, __import__("os").environ.get("DROPXL_SCRIPTS", r"C:\Users\APC\dropxl-product-automation\scripts"))
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
    # SIKKERHEDSGUARD: spring masters recent_fix allerede ejer (undgå double-processing).
    # --only overstyrer (eksplicit reconcile af én master).
    if not a.only and os.path.exists("output/recent_masters.json"):
        rec = set(json.load(open("output/recent_masters.json", encoding="utf-8")))
        n0 = len(plan); plan = [c for c in plan if c["mid"] not in rec]
        if len(plan) != n0:
            print(f"  (guard: sprang {n0 - len(plan)} combines på {len(rec)} recent_fix-masters over)")
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
    from product_utils import generate_handle
    loc = CP.get_primary_location_id(); log = lambda m: print(m, flush=True)

    _BC = ("graa", "gra", "blaa", "bla", "groen", "gron", "hvid", "sort", "brun", "roed", "rod",
           "gul", "beige", "creme", "antracit", "natur", "eg")
    def ugly_handle(h):
        """variant-specifikt handle = koder mål (120x400, -cm, -2-dublet) eller farve i halen."""
        if not h:
            return False
        h = h.lower()
        if _re.search(r"\d+\s*[-x]\s*\d+|-cm(-|$)|-\d{2,}(-|$)", h):
            return True
        tail = "-".join(h.split("-")[-2:])
        return any(c in tail for c in _BC)
    REORDER = ('mutation($p:ID!,$pos:[ProductVariantPositionInput!]!){'
               'productVariantsBulkReorder(productId:$p,positions:$pos){userErrors{message}}}')

    def natv(v):
        n = _re.findall(r"\d+\.?\d*", v or "")
        return (0, [float(x) for x in n]) if n else (1, [(v or "").lower()])

    def reorder(pid, want):
        """Reorder varianter efter spec'ens KENDTE tal-først-rækkefølge (want = SKU i to_rows-orden), så pos-1
        GARANTERET = den variant build_spec gav produkt-indhold. Gen-udledt nøgle kan divergere fra build_spec
        når option-værdier har flertydige tal (fx Model '2X...' vs 'Hjørnedel + 2x...') → metafelt-mismatch."""
        d = ME.gql('query($id:ID!){product(id:$id){variants(first:250){edges{node{id sku}}}}}', {"id": pid})
        vs = [e["node"] for e in (d.get("data") or {}).get("product", {}).get("variants", {}).get("edges", [])]
        idx = {s: i for i, s in enumerate(want)}
        svs = sorted(vs, key=lambda v: idx.get((v["sku"] or "").strip(), 10**9))
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
        old_handle = frag[anchor_pid]
        # variant-specifikt anker-handle → sæt rent titel-baseret handle (Gabriels valg); ellers behold.
        want_handle = generate_handle(spec["title"], set()) if ugly_handle(old_handle) else None
        # IN-PLACE merge på ankeret (behold ID + SEO) — build_spec sætter produkt-indhold + metafelter
        # (første variant: kun SKU; øvrige: SKU + produktinfo + variantbilleder)
        ps = CE.to_product_spec(CP, spec)
        if want_handle:
            # eksplicit handle → Shopify auto-suffikser IKKE (fejler HANDLE_NOT_UNIQUE); prøv -2,-3… selv
            h = want_handle; n = 1
            while True:
                res = CP.call_product_set(ps, loc, product_id=anchor_pid, handle=h)
                errs = (res or {}).get("userErrors") or []
                if any(e.get("code") == "HANDLE_NOT_UNIQUE" for e in errs) and n < 9:
                    n += 1; h = f"{want_handle}-{n}"; continue
                break
        else:
            res = CP.call_product_set(ps, loc, product_id=anchor_pid)
            errs = (res or {}).get("userErrors") or []
        if errs or not (res or {}).get("product"):
            log(f"   ✗ {c['mid']} \"{spec['title'][:34]}\": {errs[:2] or 'intet produkt'}"); continue
        pr = res["product"]; new_id = pr["id"]; new_handle = pr["handle"]
        # fjern evt. gammel self-redirect på ankerets endelige path (rest fra 70k-oprydning) — ellers
        # skygger den produktet + donor→anker afvises ('can't redirect to another redirect')
        ME.del_self_redirect(f"/products/{new_handle}", False, lambda m: None)
        # 301 fra det gamle (grimme) anker-handle → det nye rene (Shopify kan have suffikset)
        if want_handle and new_handle != old_handle:
            ME.create_redirect(f"/products/{old_handle}", f"/products/{new_handle}", False, lambda m: None, sb)
            redir += 1
        try:
            CP.publish_to_all_channels(new_id)
        except Exception:
            pass
        reorder(new_id, [v["sku"] for v in spec["variants"]])   # sortér option-værdier (tal-først)
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
        # notér håndterede SKUs (så vi aldrig rører korrekte produkter igen)
        HS = "output/handled_skus.json"
        hs = set(json.load(open(HS, encoding="utf-8")) if os.path.exists(HS) else [])
        hs |= set(c["skus"])
        json.dump(sorted(hs), open(HS, "w", encoding="utf-8"), ensure_ascii=False)
        if merged % 20 == 0:
            log(f"   … {merged} merges, ~{vcount} varianter, {deleted} donorer slettet")
    log(f"\n=== FÆRDIG (denne kørsel): {merged} merges, ~{vcount} varianter tilføjet, {redir} redirects, {deleted} donorer slettet ===")
    log(f"    total gjort: {len(done)}/{len(plan)}")

if __name__ == "__main__":
    main()
