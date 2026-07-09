"""RECONCILE de vidaXL-produkter VI har oprettet/pillet ved live de sidste dage → bring dem på linje med de
NYESTE regler (fuzzy ental/flertal + mørk-strip-gruppering, korrekt 1. variant/metafelter, tal-først, rent
handle for multi-variant). IN-PLACE på eksisterende anker (behold ID/SEO), 301+slet donorer. SPLITTER INTET
(hvis et produkt rummer SKUs reglerne vil dele = MÅL-MINDRE → parkeres, logges). Resumbar (output/
recent_fix_done.json) + max-variants. Default DRY-RUN; --live udfører. --limit N til test."""
import sys, os, json, argparse, re
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, __import__("os").environ.get("DROPXL_SCRIPTS", r"C:\Users\APC\dropxl-product-automation\scripts"))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import build_complete_feed as B
import scope_split as SS
import regroup as RG
import fix_live as FL
import cleanup_engine as CE
import pricing as PR

DONE = "output/recent_fix_done.json"
SINCE = os.environ.get("SINCE", "2026-07-04")
_BC = ("graa","gra","blaa","bla","groen","gron","hvid","sort","brun","roed","rod","gul","beige","creme","antracit","natur","eg")

try:
    from product_utils import convert_danish_chars
except Exception:
    def convert_danish_chars(s): return s

def _slug(v):
    s = convert_danish_chars((v or "").lower())
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")

def ugly_handle_axis(handle, rows, names):
    """AKSE-BEVIDST: et handle er 'grimt' (variant-specifikt) KUN hvis det indeholder en værdi der faktisk
    VARIERER som variant-akse (fx størrelse 120x400 eller farve grå der er en akse) — IKKE en fast dimension
    i titlen (Pavillontopdække 3x3 M, hvor kun farve varierer). Undgår falske positiver på single-akse-mål."""
    if not handle or not rows:
        return False
    h = handle.lower()
    for i in range(len(names)):
        vals = {r.get(f"option{i+1}_value") for r in rows if r.get(f"option{i+1}_value")}
        if len(vals) <= 1:
            continue  # fast værdi = identitet, ikke grim
        for v in vals:
            sl = _slug(v)
            if sl and len(sl) >= 2 and sl in h:
                return True
    return False

def natv(v):
    n = re.findall(r"\d+\.?\d*", v or "")
    return (0, [float(x) for x in n]) if n else (1, [(v or "").lower()])

def value_order_ok(vs, options):
    """er hvert numerisk dropdown vist stigende (først-set rækkefølge)?"""
    SIZE = FL.SIZE_AXES | {"Watt","Effekt"}
    for o in options:
        if o["name"] in SIZE:
            seen=[]; s=set()
            for sv in vs:
                val=sv.get(o["name"],"")
                if val not in s: s.add(val); seen.append(val)
            nums=[float(re.findall(r"\d+\.?\d*",v)[0]) if re.findall(r"\d+\.?\d*",v) else None for v in seen]
            nn=[x for x in nums if x is not None]
            if nn != sorted(nn): return False
    return True

def load_live_snapshot(force=False):
    """ÉT sweep af alle live vidaXL-produkter (id,handle,title,createdAt,options,variant-SKUs+options) →
    cache output/live_vidaxl.json. Genbrug til recent-detektion + fragment-opslag (in-memory, ingen per-frag-query)."""
    cache="output/live_vidaxl.json"
    if not force and os.path.exists(cache):
        d=json.load(open(cache,encoding="utf-8"))
        for p in d: p["skuset"]=set(p["skus"])
        print(f"live-snapshot (cache): {len(d)} produkter")
        return d
    Q=('query($a:String){products(first:80,query:"vendor:vidaXL",after:$a){pageInfo{hasNextPage endCursor} '
       'edges{node{id handle title createdAt options{name position} '
       'variants(first:200){edges{node{sku selectedOptions{name value}}}}}}}}')
    after=None; out=[]; pg=0
    while True:
        d=ME.gql(Q,{"a":after}); pr=(d.get("data") or {}).get("products") or {}
        for e in pr.get("edges",[]):
            n=e["node"]
            n["skus"]=[v["node"]["sku"] for v in n["variants"]["edges"] if v["node"].get("sku")]
            n["vs"]=[{o["name"]:o["value"] for o in v["node"]["selectedOptions"]} for v in n["variants"]["edges"]]
            del n["variants"]; out.append(n)
        pg+=1
        if pg%40==0: print(f"  …{len(out)} live",flush=True)
        if pr.get("pageInfo",{}).get("hasNextPage"): after=pr["pageInfo"]["endCursor"]
        else: break
    json.dump(out,open(cache,"w",encoding="utf-8"),ensure_ascii=False)
    for p in out: p["skuset"]=set(p["skus"])
    print(f"live-snapshot (frisk): {len(out)} produkter → {cache}")
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--max-variants", type=int, default=1000)
    ap.add_argument("--refresh", action="store_true", help="frisk live-snapshot (ellers cache)")
    a = ap.parse_args()

    # 1) live-snapshot (ét sweep, cached) → recent + fragment-opslag in-memory
    snap=load_live_snapshot(force=a.refresh)
    prod_by_id={p["id"]:p for p in snap}
    sku2pid={}
    for p in snap:
        for s in p["skuset"]: sku2pid[s]=p["id"]
    recent=[p for p in snap if (p.get("createdAt") or "")[:10] >= SINCE]
    live_by_id=prod_by_id
    print(f"recent vidaXL-produkter (createdAt≥{SINCE}): {len(recent)}")

    # 2) berørte masters
    allsku=sorted({s for p in recent for s in p["skus"]})
    sb=ME.get_supabase_client()
    sku2m={}
    for i in range(0,len(allsku),500):
        r=sb.table("vidaxl_sku_master").select("sku,master_pid").in_("sku",allsku[i:i+500]).execute().data or []
        for x in r: sku2m[str(x["sku"])]=x["master_pid"]
    masters=sorted({sku2m[s] for p in recent for s in p["skus"] if s in sku2m})
    print(f"berørte masters: {len(masters)}")

    # 3) feed + universer + pricing
    feed=CE.load_feed_df(); titles=feed["Title"].to_dict()
    SS.setup_universe(list(feed.index))
    for w in ("cremehvid","cremehvide","råhvid","gråhvid","offwhite","sølvgrå","koksgrå"): B.COLOR_UNIVERSE.add(w)
    B.build_color_re()
    lbl=json.load(open("output/axis_labels.json",encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}
    cfg=PR.load_pricing_config(sb,vendor="vidaXL")
    rum={}
    try:
        r=sb.table("hub_settings").select("value").eq("key","vidaxl_rum_mapping").execute().data
        rum=(r[0]["value"] if r else {}) or {}
    except Exception: pass

    bym=defaultdict(list); fr=0; mset=set(masters)
    while True:
        b=sb.table("vidaxl_sku_master").select("sku,master_pid").range(fr,fr+999).execute().data or []
        for x in b:
            if x["master_pid"] in mset: bym[x["master_pid"]].append(str(x["sku"]).strip())
        if len(b)<1000: break
        fr+=1000

    # 4) byg opgaver: pr. master, ny-regel-mål; find live-fragmenter; klassificér
    tasks=[]; park=[]; skip=0
    for mid in masters:
        live=[s for s in bym.get(mid,[]) if s in feed.index]
        if not live: continue
        opts={s:{k:v for k,v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        for p in FL.regroup_master(mid,live,opts,titles,{mid:lbl.get(mid,{})}):
            want=set(p["skus"])
            # fragment-produkter der holder mål-SKUs — in-memory via snapshot
            fpids={sku2pid[s] for s in want if s in sku2pid}
            frag={pid:prod_by_id[pid]["handle"] for pid in fpids}
            if not frag: continue
            frag_skus=set()
            for pid in fpids: frag_skus|=prod_by_id[pid]["skuset"]
            # rører kun dette produkt hvis fragmenterne PRÆCIST udgør målet (ingen fremmede SKUs = ingen split)
            if frag_skus!=want:
                if frag_skus>want:
                    park.append((mid,p["title"],len(want),len(frag_skus)));
                continue
            # er der behov for ændring? (merge, forkert 1.variant-orden, forkert display-sort, titel, handle)
            need=False; why=[]
            if len(frag)>1: need=True; why.append("merge")
            anchor_pid=min(frag,key=lambda pid:(1 if re.search(r"-\d+$",frag[pid]) else 0,len(frag[pid])))
            lp=live_by_id.get(anchor_pid)
            rows=FL.to_rows(p,opts); want_order=[r["sku"] for r in rows]
            want_cols=[nm for nm,_ in p["specs"]]
            if lp:
                cur_order=[s for s in lp["skus"]]
                cur_cols=[o["name"] for o in sorted(lp.get("options",[]),key=lambda o:o["position"])]
                # KUN første-variant-skift betyder noget (produkt-indhold følger 1. variant); ren mid-liste-
                # omrokering med samme 1. variant + stigende display er usynlig for kunden → ikke en opgave.
                if len(want)>1 and cur_order and want_order and cur_order[0]!=want_order[0]:
                    need=True; why.append("1.variant")
                if want_cols and cur_cols!=want_cols and cur_cols!=["Title"]: need=True; why.append("kolonne")
                if lp["title"]!=p["title"]: need=True; why.append("titel")
                if not value_order_ok(lp["vs"],lp.get("options",[])): need=True; why.append("display-sort")
            multi=len(want)>1
            if multi and ugly_handle_axis(frag[anchor_pid],rows,p["specs"]): need=True; why.append("handle")
            if not need:
                skip+=1; continue
            tasks.append({"mid":mid,"title":p["title"],"skus":sorted(want),"anchor":anchor_pid,
                          "anchor_handle":frag[anchor_pid],"frag":frag,"multi":multi,"why":why,
                          "spec_group":p})
    print(f"\nopgaver (skal rettes): {len(tasks)}  |  allerede-OK sprunget: {skip}  |  parkeret (split): {len(park)}")
    from collections import Counter
    wc=Counter(w for t in tasks for w in t["why"])
    print(f"  årsags-fordeling: {dict(wc)}")
    for t in tasks[:12]:
        print(f"   [{t['mid']}] \"{t['title'][:40]}\" ({len(t['skus'])} SKU, {len(t['frag'])} frag) {t['why']}")
    if park:
        print(f"\n  PARKERET (rummer SKUs reglerne vil splitte — rører ikke nu):")
        for mid,ti,nw,nf in park[:8]:
            print(f"     [{mid}] \"{ti[:40]}\" mål {nw} SKU vs fragment har {nf}")
    json.dump({"tasks":[{k:v for k,v in t.items() if k!='spec_group' and k!='frag'} for t in tasks],
               "park":park}, open("output/recent_fix_plan.json","w",encoding="utf-8"),ensure_ascii=False)

    if not a.live:
        print("\n(dry-run — intet rørt. --live for at udføre.)")
        return

    # 5) LIVE
    import create_products_v2 as CP
    from product_utils import generate_handle
    loc=CP.get_primary_location_id(); log=lambda m:print(m,flush=True)
    REORDER=('mutation($p:ID!,$pos:[ProductVariantPositionInput!]!){'
             'productVariantsBulkReorder(productId:$p,positions:$pos){userErrors{message}}}')
    def reorder(pid, want):
        """Reorder varianter efter spec'ens KENDTE tal-først-rækkefølge (want = liste af SKU i to_rows-orden),
        så pos-1 GARANTERET = den variant build_spec gav produkt-indhold (sku-only). Ingen gen-udledt nøgle
        der kan divergere fra build_spec (fixede M3007913-metafelt-mismatch)."""
        d=ME.gql('query($id:ID!){product(id:$id){variants(first:250){edges{node{id sku}}}}}',{"id":pid})
        vs=[e["node"] for e in (d.get("data") or {}).get("product",{}).get("variants",{}).get("edges",[])]
        idx={s:i for i,s in enumerate(want)}
        svs=sorted(vs,key=lambda v:idx.get((v["sku"] or "").strip(),10**9))
        if [v["id"] for v in svs]!=[v["id"] for v in vs]:
            pos=[{"id":v["id"],"position":i+1} for i,v in enumerate(svs)]
            for i in range(0,len(pos),250): ME.gql(REORDER,{"p":pid,"pos":pos[i:i+250]})

    done=set(json.load(open(DONE,encoding="utf-8")) if os.path.exists(DONE) else [])
    todo=[t for t in tasks if t["mid"]+"|"+t["title"] not in done]
    if a.limit: todo=todo[:a.limit]
    print(f"\n=== LIVE: {len(tasks)} opgaver, {len(done)} gjort, {len(todo)} i denne kørsel ===")
    fixed=vcount=redir=deleted=0
    for t in todo:
        if vcount>=a.max_variants:
            log(f"\n⏸ nåede {a.max_variants}-grænsen — stopper. Kør igen for at fortsætte."); break
        opts={s:{k:v for k,v in (ME.OPTS.get(s) or {}).items() if v} for s in bym.get(t["mid"],[])}
        rows=FL.to_rows(t["spec_group"],opts)
        spec,_=CE.build_spec(t["spec_group"]["key"],rows,feed,cfg,rum)
        if not spec: log(f"   ✗ {t['mid']}: ingen spec"); continue
        want_handle=generate_handle(t["title"],set()) if (t["multi"] and ugly_handle_axis(t["anchor_handle"],rows,t["spec_group"]["specs"])) else None
        ps=CE.to_product_spec(CP,spec)
        if want_handle:
            h=want_handle; n=1
            while True:
                res=CP.call_product_set(ps,loc,product_id=t["anchor"],handle=h)
                errs=(res or {}).get("userErrors") or []
                if any(e.get("code")=="HANDLE_NOT_UNIQUE" for e in errs) and n<9:
                    n+=1; h=f"{want_handle}-{n}"; continue
                break
        else:
            res=CP.call_product_set(ps,loc,product_id=t["anchor"]); errs=(res or {}).get("userErrors") or []
        if errs or not (res or {}).get("product"):
            log(f"   ✗ {t['mid']} \"{t['title'][:32]}\": {errs[:2] or 'intet produkt'}"); continue
        prod=res["product"]; nid=prod["id"]; nh=prod["handle"]
        try: CP.publish_to_all_channels(nid)
        except Exception: pass
        ME.del_self_redirect(f"/products/{nh}",False,lambda m:None)
        if want_handle and nh!=t["anchor_handle"]:
            ME.create_redirect(f"/products/{t['anchor_handle']}",f"/products/{nh}",False,lambda m:None,sb); redir+=1
        reorder(nid, [r["sku"] for r in rows])
        fixed+=1; vcount+=len(t["skus"])
        for oid,oh in t["frag"].items():
            if oid==t["anchor"]: continue
            ME.create_redirect(f"/products/{oh}",f"/products/{nh}",False,lambda m:None,sb); redir+=1
            ME.delete_product(oid,oh,False,lambda m:None); deleted+=1
        done.add(t["mid"]+"|"+t["title"])
        json.dump(sorted(done),open(DONE,"w",encoding="utf-8"),ensure_ascii=False)
        HS="output/handled_skus.json"
        hs=set(json.load(open(HS,encoding="utf-8")) if os.path.exists(HS) else [])
        hs|=set(t["skus"]); json.dump(sorted(hs),open(HS,"w",encoding="utf-8"),ensure_ascii=False)
        if fixed%10==0: log(f"   … {fixed} rettet, {vcount} var, {deleted} donorer slettet")
    print(f"\n=== FÆRDIG (denne kørsel): {fixed} rettet, {vcount} varianter, {redir} redirects, {deleted} slettet ===")
    print(f"    total gjort: {len(done)}/{len(tasks)}")

if __name__ == "__main__":
    main()
