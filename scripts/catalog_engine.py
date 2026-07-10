"""DELT KATALOG-MOTOR: process_group() = behandl én mål-gruppe → OPRET nyt / MERGE in-place / PARK / SKIP.
Extraheret + generaliseret fra combine_exec's testede per-gruppe-logik (dedup, chunked-large, rent handle,
create_single-sikkerhedsnet, tal-först-reorder, 301+slet donorer). Bruges af det samlede create+merge-flow
(unified_sync). combine_exec migreres til denne motor SENERE (når combine-backlog er færdig). Se
UNIFIED_CREATE_MERGE_PLAN.md.

Ingen top-level side-effekter — kald process_group(group, opts, ctx, create_if_missing, dry_run, log)."""
import os, sys, json, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.environ.get("DROPXL_SCRIPTS", r"C:\Users\APC\dropxl-product-automation\scripts"))
import merge_executor as ME
import build_complete_feed as B
import fix_live as FL
import cleanup_engine as CE

MIN_STOCK_PRIMARY = 20
_BC = ("graa", "gra", "blaa", "bla", "groen", "gron", "hvid", "sort", "brun", "roed", "rod",
       "gul", "beige", "creme", "antracit", "natur", "eg")
VBULK = ('mutation($productId:ID!,$variants:[ProductVariantsBulkInput!]!){'
         'productVariantsBulkCreate(productId:$productId,variants:$variants){'
         'productVariants{id sku} userErrors{field message code}}}')
REORDER = ('mutation($p:ID!,$pos:[ProductVariantPositionInput!]!){'
           'productVariantsBulkReorder(productId:$p,positions:$pos){userErrors{message}}}')


class Ctx:
    """Runtime-kontekst: feed + config + Shopify-adgang. Byg én gang pr. kørsel."""
    def __init__(self, feed, titles, cfg, rum, sb, stock=None):
        self.feed = feed; self.titles = titles; self.cfg = cfg; self.rum = rum; self.sb = sb
        self.stock = stock or {}
        import create_products_v2 as CP
        from product_utils import convert_danish_chars, generate_handle
        self.CP = CP; self.gh = generate_handle; self.cdc = convert_danish_chars
        self.loc = CP.get_primary_location_id()


def _slug(v, cdc):
    return re.sub(r"[^a-z0-9]+", "-", cdc((v or "").lower())).strip("-")

def ugly_handle_axis(handle, rows, names, cdc):
    """Handle er variant-specifikt (grimt) KUN hvis det indeholder en værdi der VARIERER som akse."""
    if not handle or not rows:
        return False
    h = handle.lower()
    for i in range(len(names)):
        vals = {r.get(f"option{i+1}_value") for r in rows if r.get(f"option{i+1}_value")}
        if len(vals) <= 1:
            continue
        for v in vals:
            sl = _slug(v, cdc)
            if sl and len(sl) >= 2 and sl in h:
                return True
    return False

def _natv(v):
    n = re.findall(r"\d+\.?\d*", v or "")
    return (0, [float(x) for x in n]) if n else (1, [(v or "").lower()])

def _bulk_variant_input(v, opt_names, loc):
    vin = {
        "optionValues": [{"optionName": nm, "name": val} for nm, val in v.option_values],
        "price": str(v.price), "barcode": v.barcode or None,
        "inventoryItem": {"sku": v.sku, "cost": str(v.cost), "tracked": True, "requiresShipping": True,
                          "measurement": {"weight": {"value": v.weight_grams / 1000.0, "unit": "KILOGRAMS"}}},
        "inventoryPolicy": "DENY",
        "inventoryQuantities": [{"locationId": loc, "availableQuantity": v.inventory_quantity}],
        "metafields": v.metafields, "taxable": True,
    }
    if v.compare_at_price is not None:
        vin["compareAtPrice"] = str(v.compare_at_price)
    return {k: vv for k, vv in vin.items() if vv is not None}

def _reorder(pid, want):
    vs = []; after = None
    while True:
        d = ME.gql('query($id:ID!,$a:String){product(id:$id){variants(first:250,after:$a){'
                   'pageInfo{hasNextPage endCursor} edges{node{id sku}}}}}', {"id": pid, "a": after})
        pv = (d.get("data") or {}).get("product", {}).get("variants", {})
        vs += [e["node"] for e in pv.get("edges", [])]
        if pv.get("pageInfo", {}).get("hasNextPage"): after = pv["pageInfo"]["endCursor"]
        else: break
    idx = {s: i for i, s in enumerate(want)}
    svs = sorted(vs, key=lambda v: idx.get((v["sku"] or "").strip(), 10**9))
    if [v["id"] for v in svs] != [v["id"] for v in vs]:
        pos = [{"id": v["id"], "position": i + 1} for i, v in enumerate(svs)]
        for i in range(0, len(pos), 250):
            ME.gql(REORDER, {"p": pid, "pos": pos[i:i + 250]})

def create_single(sku, ctx, log=lambda m: None):
    """Gen-opret en droppet dup-SKU som eget single-produkt → taber ALDRIG en SKU."""
    if sku not in ctx.feed.index:
        return False
    prod = {"key": f"{sku}_recover", "title": B.housestyle(B.clean(ctx.titles.get(sku, ""))), "specs": [], "skus": [sku]}
    o = {sku: {k: v for k, v in (ME.OPTS.get(sku) or {}).items() if v}}
    spec1, _ = CE.build_spec(prod["key"], FL.to_rows(prod, o), ctx.feed, ctx.cfg, ctx.rum)
    if not spec1:
        return False
    r = ctx.CP.call_product_set(CE.to_product_spec(ctx.CP, spec1), ctx.loc)
    if (r or {}).get("product"):
        try: ctx.CP.publish_to_all_channels(r["product"]["id"])
        except Exception: pass
        return True
    return False

def _apply(ps, anchor_pid, want_handle, ctx, log):
    """Dedup → productSet (OPRET hvis anchor_pid=None, ellers OPDATÉR) → chunked bulk-create hvis >250.
    Returnér (res, errs, want_handle, dropped)."""
    seen = set(); kept = []; dropped = []
    for v in ps.variants:
        key = tuple((nm, val) for nm, val in v.option_values)
        if key in seen:
            dropped.append(v.sku)
        else:
            seen.add(key); kept.append(v)
    ps.variants = kept
    large = len(kept) > 250
    ps.variants = kept[:250] if large else kept
    if want_handle:
        h = want_handle; n = 1
        while True:
            res = ctx.CP.call_product_set(ps, ctx.loc, product_id=anchor_pid, handle=h) if anchor_pid \
                else ctx.CP.call_product_set(ps, ctx.loc, handle=h)
            errs = (res or {}).get("userErrors") or []
            if any(e.get("code") == "HANDLE_NOT_UNIQUE" for e in errs):
                if n < 6:
                    n += 1; h = f"{want_handle}-{n}"; continue
                # fallback: anker → behold eksisterende handle; nyt → lad Shopify auto-generere
                want_handle = None
                res = ctx.CP.call_product_set(ps, ctx.loc, product_id=anchor_pid) if anchor_pid \
                    else ctx.CP.call_product_set(ps, ctx.loc)
                errs = (res or {}).get("userErrors") or []
            break
    else:
        res = ctx.CP.call_product_set(ps, ctx.loc, product_id=anchor_pid) if anchor_pid \
            else ctx.CP.call_product_set(ps, ctx.loc)
        errs = (res or {}).get("userErrors") or []
    ps.variants = kept
    if not (errs or not (res or {}).get("product")) and large:
        new_id = res["product"]["id"]
        rest = kept[250:]
        for i in range(0, len(rest), 250):
            chunk = [_bulk_variant_input(v, ps.options_definition, ctx.loc) for v in rest[i:i + 250]]
            d = ME.gql(VBULK, {"productId": new_id, "variants": chunk})
            berrs = ((d.get("data") or {}).get("productVariantsBulkCreate") or {}).get("userErrors") or []
            if berrs:
                log(f"      ⚠ bulk-create chunk {i // 250 + 2}: {berrs[:1]}")
        log(f"      + {len(rest)} overflow-varianter via bulk-create (i alt {len(kept)})")
    return res, errs, want_handle, dropped


def classify(g, sku2pid, prod_by_id, ctx):
    """READ-ONLY klassifikation af en gruppe (til dry-run): CREATE/MERGE/UNCHANGED/PARK/SKIP."""
    gskus = set(g["skus"])
    live_pids = {sku2pid[s] for s in gskus if s in sku2pid}
    covered = {s for s in gskus if s in sku2pid}
    if not live_pids:
        if any(ctx.stock.get(s, 0) >= MIN_STOCK_PRIMARY for s in gskus):
            return "CREATE", None
        return "SKIP", None
    allprod = set()
    for pid in live_pids: allprod |= prod_by_id[pid]["skuset"]
    if allprod - gskus:
        return "PARK", None
    if len(live_pids) == 1 and covered == gskus and next(iter(live_pids)) and prod_by_id[next(iter(live_pids))]["skuset"] == gskus:
        return "UNCHANGED", next(iter(live_pids))
    anchor = min(live_pids, key=lambda pid: (1 if re.search(r"-\d+$", prod_by_id[pid]["handle"]) else 0, len(prod_by_id[pid]["handle"])))
    return "MERGE", anchor


def process_group(g, opts, ctx, create_if_missing=True, dry_run=False, log=lambda m: None):
    """Behandl én mål-gruppe live. frag hentes FRISK (old_products_for_skus). Returnér result dict."""
    mid = g["key"].split("_g")[0]
    rows = FL.to_rows(g, opts)
    spec, _ = CE.build_spec(g["key"], rows, ctx.feed, ctx.cfg, ctx.rum)
    if not spec:
        return {"mid": mid, "title": g["title"], "action": "ERROR", "reason": "no-spec"}
    want = set(g["skus"])
    frag = CE.old_products_for_skus(list(want), "none")   # {pid: handle} FRISK
    # foreign-SKU-tjek pr. fragment → PARK hvis split kræves
    frag_skus = set()
    for pid in frag:
        d = ME.gql('query($id:ID!){product(id:$id){variants(first:250){edges{node{sku}}}}}', {"id": pid})
        frag_skus |= {(x["node"]["sku"] or "").strip() for x in (d.get("data") or {}).get("product", {}).get("variants", {}).get("edges", []) if x["node"].get("sku")}
    if frag and (frag_skus - want):
        return {"mid": mid, "title": g["title"], "action": "PARK", "n_skus": len(want)}
    if not frag:
        if not (create_if_missing and any(ctx.stock.get(s, 0) >= MIN_STOCK_PRIMARY for s in want)):
            return {"mid": mid, "title": g["title"], "action": "SKIP", "n_skus": len(want)}
        action = "CREATE"; anchor_pid = None; old_handle = None
    else:
        action = "MERGE"
        anchor_pid = min(frag, key=lambda pid: (1 if re.search(r"-\d+$", frag[pid]) else 0, len(frag[pid])))
        old_handle = frag[anchor_pid]
    if dry_run:
        return {"mid": mid, "title": g["title"], "action": action, "n_skus": len(want), "anchor": anchor_pid}

    # === APPLY ===
    ps = CE.to_product_spec(ctx.CP, spec)
    if action == "CREATE":
        want_handle = ctx.gh(spec["title"], set())
    else:
        want_handle = ctx.gh(spec["title"], set()) if ugly_handle_axis(old_handle, rows, g["specs"], ctx.cdc) else None
    res, errs, want_handle, dropped = _apply(ps, anchor_pid, want_handle, ctx, log)
    if errs or not (res or {}).get("product"):
        return {"mid": mid, "title": g["title"], "action": action, "ok": False, "errs": errs[:2]}
    pr = res["product"]; new_id = pr["id"]; new_handle = pr["handle"]
    ME.del_self_redirect(f"/products/{new_handle}", False, lambda m: None)
    if action == "MERGE" and want_handle and new_handle != old_handle:
        ME.create_redirect(f"/products/{old_handle}", f"/products/{new_handle}", False, lambda m: None, ctx.sb)
    try: ctx.CP.publish_to_all_channels(new_id)
    except Exception: pass
    _reorder(new_id, [v.sku for v in ps.variants])
    if action == "MERGE":
        keep_pids = set(CE.old_products_for_skus(dropped, new_id).keys()) if dropped else set()
        for oid, oh in frag.items():
            if oid == anchor_pid or oid in keep_pids:
                continue
            ME.create_redirect(f"/products/{oh}", f"/products/{new_handle}", False, lambda m: None, ctx.sb)
            ME.delete_product(oid, oh, False, lambda m: None)
    # sikkerhedsnet: droppede SKU der endte ikke-live → gen-opret som single
    for dsku in dropped:
        dd = ME.gql('query($q:String!){productVariants(first:3,query:$q){edges{node{sku}}}}', {"q": f"sku:{dsku}"})
        if not any((e["node"]["sku"] or "").strip() == dsku for e in (dd.get("data") or {}).get("productVariants", {}).get("edges", [])):
            create_single(dsku, ctx, log)
    return {"mid": mid, "title": g["title"], "action": action, "ok": True, "product_id": new_id,
            "handle": new_handle, "n_skus": len(want), "dropped": len(dropped)}
