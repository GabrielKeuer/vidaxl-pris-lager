"""SAMLET CREATE+MERGE — Fase 1b/1c: delta-detektion + live-wiring til catalog_engine + create-filter-paritet.
Én grupperings-logik (regroup) over feedet. Pr. mål-gruppe klassificeres og (i --live) behandles:
  CREATE          = ingen live-produkter holder gruppens SKU + opfylder create-filtre → opret nyt
  MERGE_nyvariant = live-produkt findes men gruppen har nye SKU → opdatér in-place (fuld spec-rebuild)
  MERGE_konsolidér= gruppens SKU spredt på flere produkter → samles (combine-adfærd)
  UNCHANGED       = live-produkt matcher gruppen præcist → rør ikke
  SKIP_filter     = ingen live + opfylder ikke create-filtre (lager/pris/kategori)
  PARK_split      = merge ville kræve SPLIT (fremmede SKU) → parkeret (split-backlog)

DELTA-DETEKTION (§3 i planen): state/last_catalog_skus.csv = sidste kendte feed-SKU-sæt pr. master.
Kun masters hvis feed-SKU-sæt har ÆNDRET sig (tilføjet/fjernet/ny master) processeres. --full = alle.
Snapshot opdateres KUN efter succesfuld LIVE-behandling (dry-run rører intet — heller ikke state).

CREATE-FILTRE (paritet m. dropxl daily_create, verificeret mod create_products_v2.py:1574-1580+1689):
  produkt:  mindst ét SKU m. Stock ≥ 20 OG B2B-pris > 0 OG aktiv hovedkategori (hub-config Import?=JA)
  variant:  kun SKU m. Stock ≥ 4 OG B2B-pris > 0 medtages ved CREATE (og som NYE varianter ved merge)
  merge:    eksisterende varianter fjernes ALDRIG pga. lavt lager (kun daily_delete fjerner)

Kør: python scripts/unified_sync.py [--full] [--refresh] [--limit N] [--only MID] [--live --budget N]
Default er DRY-RUN og rører INTET. --live kræver eksplicit flag. Se UNIFIED_CREATE_MERGE_PLAN.md."""
import sys, os, json, csv, argparse, datetime
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, __import__("os").environ.get("DROPXL_SCRIPTS", r"C:\Users\APC\dropxl-product-automation\scripts"))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import build_complete_feed as B
import scope_split as SS
import fix_live as FL
import cleanup_engine as CE
import pricing as PR
import catalog_engine as ENG

MIN_STOCK_PRIMARY = 20
MIN_STOCK_VARIANT = 4
STATE_PATH = "state/last_catalog_skus.csv"
JOURNAL = "output/unified_journal.jsonl"


def load_live_snapshot(force=False):
    """Alle live vidaXL-produkter (id,handle,title,skus) → cache output/live_vidaxl.json."""
    cache = "output/live_vidaxl.json"
    if not force and os.path.exists(cache):
        d = json.load(open(cache, encoding="utf-8"))
        for p in d: p["skuset"] = set(p.get("skus", []))
        print(f"live-snapshot (cache): {len(d)} produkter (brug --refresh for frisk)")
        return d
    Q = ('query($a:String){products(first:80,query:"vendor:vidaXL",after:$a){pageInfo{hasNextPage endCursor} '
         'edges{node{id handle title createdAt variantsCount{count} variants(first:250){edges{node{sku}}}}}}}')
    after = None; out = []; pg = 0; big = []
    while True:
        d = ME.gql(Q, {"a": after}); pr = (d.get("data") or {}).get("products") or {}
        for e in pr.get("edges", []):
            n = e["node"]
            n["skus"] = [v["node"]["sku"] for v in n["variants"]["edges"] if v["node"].get("sku")]
            if (n.get("variantsCount") or {}).get("count", 0) > 250:
                big.append(n["id"])
            del n["variants"]; out.append(n)
        pg += 1
        if pg % 40 == 0: print(f"  …{len(out)} live", flush=True)
        if pr.get("pageInfo", {}).get("hasNextPage"): after = pr["pageInfo"]["endCursor"]
        else: break
    bymap = {p["id"]: p for p in out}
    for pid in big:
        allsk = []; af = None
        while True:
            d = ME.gql('query($id:ID!,$a:String){product(id:$id){variants(first:250,after:$a){'
                       'pageInfo{hasNextPage endCursor} edges{node{sku}}}}}', {"id": pid, "a": af})
            pv = (d.get("data") or {}).get("product", {}).get("variants", {})
            allsk += [x["node"]["sku"] for x in pv.get("edges", []) if x["node"].get("sku")]
            if pv.get("pageInfo", {}).get("hasNextPage"): af = pv["pageInfo"]["endCursor"]
            else: break
        bymap[pid]["skus"] = allsk
    if big: print(f"  (paginerede {len(big)} store produkter >250 var fuldt)")
    json.dump(out, open(cache, "w", encoding="utf-8"), ensure_ascii=False)
    for p in out: p["skuset"] = set(p["skus"])
    print(f"live-snapshot (frisk): {len(out)} produkter → {cache}")
    return out


def load_state():
    """Sidste kendte feed-SKU-sæt pr. master (delta-basis). {} = intet snapshot (første kørsel = fuld scan)."""
    st = defaultdict(set)
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, encoding="utf-8") as f:
            for row in csv.reader(f):
                if len(row) == 2: st[row[0]].add(row[1])
    return st


def save_state(state):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for mid in sorted(state):
            for s in sorted(state[mid]): w.writerow([mid, s])


def journal(rec):
    os.makedirs(os.path.dirname(JOURNAL), exist_ok=True)
    rec["at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with open(JOURNAL, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="UDFØR (default: dry-run, rører intet)")
    ap.add_argument("--budget", type=int, default=500, help="max varianter at røre i live-kørsel")
    ap.add_argument("--full", action="store_true", help="ignorér delta-snapshot — scan alle masters")
    ap.add_argument("--refresh", action="store_true", help="frisk live-snapshot (ellers cache)")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--only", default="")
    a = ap.parse_args()

    # feed + universer + labels
    feed = CE.load_feed_df(); titles = feed["Title"].to_dict()
    stock = {}; price = {}
    if "Stock" in feed.columns:
        for s, v in feed["Stock"].items():
            try: stock[s] = int(float(v))
            except (ValueError, TypeError): stock[s] = 0
    pcol = "B2B price" if "B2B price" in feed.columns else None
    if pcol:
        for s, v in feed[pcol].items():
            try: price[s] = float(v)
            except (ValueError, TypeError): price[s] = 0.0
    SS.setup_universe(list(feed.index))
    for w in ("cremehvid", "cremehvide", "råhvid", "gråhvid", "offwhite", "sølvgrå", "koksgrå"):
        B.COLOR_UNIVERSE.add(w)
    B.build_color_re()
    lbl = json.load(open("output/axis_labels.json", encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}

    # aktive hovedkategorier (hub-config, Import?=JA) — PARITET m. daily_create (1c)
    aktive = set()
    kat = {}
    try:
        from product_utils import load_config
        import create_products_v2 as CPmod
        cfg_df, _, _, _ = load_config(CPmod.CONFIG_PATH)
        aktive = set(cfg_df[cfg_df["Import?"] == "JA"]["Kategori_Config"].tolist())
        print(f"aktive hovedkategorier (hub-config): {len(aktive)}")
    except Exception as e:
        print(f"⚠ kategori-config kunne ikke hentes ({e}) — kategori-filter er ÅBENT (alle kategorier)")
    if "Category" in feed.columns:
        for s, v in feed["Category"].items():
            kat[s] = str(v).split(" > ")[0] if v is not None else ""

    def create_primary_ok(skus):
        """Produkt-niveau create-filter: mindst ét SKU m. stock≥20 + pris>0 + aktiv kategori."""
        for s in skus:
            if stock.get(s, 0) >= MIN_STOCK_PRIMARY and price.get(s, 0) > 0 and (not aktive or kat.get(s, "") in aktive):
                return True
        return False

    def variant_ok(s):
        return stock.get(s, 0) >= MIN_STOCK_VARIANT and price.get(s, 0) > 0

    # live-snapshot → sku2pid
    snap = load_live_snapshot(force=a.refresh)
    prod_by_id = {p["id"]: p for p in snap}
    sku2pid = {}
    for p in snap:
        for s in p["skuset"]: sku2pid[s] = p["id"]

    # masters → feed-SKU (nutid) + DELTA mod state
    sb = ME.get_supabase_client()
    bym = defaultdict(list); fr = 0
    while True:
        b = sb.table("vidaxl_sku_master").select("sku,master_pid").range(fr, fr + 999).execute().data or []
        for x in b: bym[x["master_pid"]].append(str(x["sku"]).strip())
        if len(b) < 1000: break
        fr += 1000
    cur = {mid: {s for s in skus if s in feed.index} for mid, skus in bym.items()}
    cur = {mid: ss for mid, ss in cur.items() if ss}

    state = load_state()
    if a.only:
        masters = [a.only]
        print(f"--only {a.only}")
    elif a.full or not state:
        masters = sorted(cur)
        print(f"FULD SCAN ({'--full' if a.full else 'intet delta-snapshot endnu'}): {len(masters)} masters")
    else:
        masters = sorted(mid for mid in cur if cur[mid] != state.get(mid, set()))
        print(f"DELTA: {len(masters)} af {len(cur)} masters har ændret feed-SKU-sæt siden sidste snapshot")
    if a.limit: masters = masters[:a.limit]

    # ctx til motoren — kun i LIVE (dry-run klassificerer uden Shopify-write-afhængigheder)
    ctx = None
    if a.live:
        cfg = PR.load_pricing_config(sb, vendor="vidaXL")
        rum = {}
        try:
            r = sb.table("hub_settings").select("value").eq("key", "vidaxl_rum_mapping").execute().data
            rum = (r[0]["value"] if r else {}) or {}
        except Exception:
            pass
        ctx = ENG.Ctx(feed, titles, cfg, rum, sb, stock=stock)

    cats = Counter(); examples = defaultdict(list)
    create_variants = 0; merge_variants = 0
    spent = 0; processed_masters = set(); failed_masters = set()
    results = []

    for mid in masters:
        live = sorted(cur.get(mid, set()))
        if not live:
            processed_masters.add(mid); continue   # master helt ude af feed → daily_delete's domæne; snapshot opdateres
        opts = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        master_ok = True
        for g in FL.regroup_master(mid, live, opts, titles, {mid: lbl.get(mid, {})}):
            gskus = set(g["skus"])
            live_pids = {sku2pid[s] for s in gskus if s in sku2pid}
            covered = {s for s in gskus if s in sku2pid}

            # --- klassificér (rapport) ---
            if not live_pids:
                fskus = {s for s in gskus if variant_ok(s)}      # variant-filter (1c)
                if not create_primary_ok(gskus) or not fskus:
                    cats["SKIP_filter"] += 1; continue
                action = "CREATE"; touch = len(fskus)
                g2 = dict(g); g2["skus"] = [s for s in g["skus"] if s in fskus]
            else:
                allprod = set()
                for pid in live_pids: allprod |= prod_by_id[pid]["skuset"]
                if allprod - gskus:
                    cats["PARK_split"] += 1
                    if len(examples["PARK_split"]) < 6: examples["PARK_split"].append((mid, g["title"], len(gskus), len(allprod - gskus)))
                    continue
                new = gskus - covered
                fnew = {s for s in new if variant_ok(s)}          # nye varianter skal bestå variant-filter
                if len(live_pids) == 1 and not fnew and prod_by_id[next(iter(live_pids))]["skuset"] == gskus:
                    cats["UNCHANGED"] += 1; continue
                if len(live_pids) == 1 and not fnew and not new:
                    cats["UNCHANGED"] += 1; continue
                if len(live_pids) == 1 and new and not fnew:
                    cats["SKIP_nyvariant_filter"] += 1; continue  # ny SKU findes men under variant-tærskel → vent
                action = "MERGE_nyvariant" if len(live_pids) == 1 else "MERGE_konsolidér"
                touch = len(fnew) if len(live_pids) == 1 else len(gskus)
                g2 = dict(g); g2["skus"] = [s for s in g["skus"] if s in covered or s in fnew]
            cats[action] += 1
            if action == "CREATE": create_variants += touch
            else: merge_variants += len(set(g2["skus"]) - covered)
            if len(examples[action]) < 8: examples[action].append((mid, g["title"], len(g2["skus"])))

            # --- live-apply via delt motor ---
            if a.live:
                if spent + touch > a.budget:
                    print(f"⏸ budget ({a.budget} varianter) nået — stopper. Resten tages næste kørsel (delta-snapshot uændret for urørte masters).")
                    master_ok = False
                    break
                res = ENG.process_group(g2, opts, ctx, create_if_missing=True, dry_run=False, log=lambda m: print(m, flush=True))
                journal({"mid": mid, **{k: v for k, v in res.items() if k != "mid"}})
                results.append(res)
                if res.get("action") in ("CREATE", "MERGE") and res.get("ok"):
                    spent += touch
                    print(f"  ✓ {res['action']} {res.get('handle', '')} «{g['title'][:44]}» ({res.get('n_skus')} SKU)")
                elif res.get("action") in ("PARK", "SKIP"):
                    pass
                else:
                    master_ok = False
                    print(f"  ✗ {mid} «{g['title'][:40]}»: {res.get('errs') or res.get('reason')}")
        else:
            if a.live and master_ok:
                processed_masters.add(mid)
            elif a.live:
                failed_masters.add(mid)
            continue
        break   # budget nået (inner break) → stop ydre loop

    # --- delta-snapshot: opdatér KUN i live, KUN for fuldt behandlede masters ---
    if a.live and processed_masters:
        for mid in processed_masters:
            if mid in cur: state[mid] = set(cur[mid])
            else: state.pop(mid, None)
        save_state(state)
        print(f"delta-snapshot opdateret for {len(processed_masters)} masters → {STATE_PATH}"
              + (f" ({len(failed_masters)} fejlede — genoptages)" if failed_masters else ""))
    # (Intet snapshot-seed i dry-run: første LIVE-kørsel bygger snapshottet naturligt — UNCHANGED/PARK/SKIP-
    #  masters snapshottes gratis, kun CREATE/MERGE-arbejde koster budget → backloggen brænder ned kørsel for kørsel.)

    print(f"\n=== UNIFIED {'LIVE' if a.live else 'DRY-RUN'} ===")
    for k in ("CREATE", "MERGE_nyvariant", "MERGE_konsolidér", "UNCHANGED", "PARK_split", "SKIP_filter", "SKIP_nyvariant_filter"):
        print(f"  {k:22s} {cats.get(k, 0)}")
    print(f"  nye varianter (CREATE): ~{create_variants} | tilføjede varianter (MERGE): ~{merge_variants}")
    if a.live:
        okc = sum(1 for r in results if r.get("ok"))
        print(f"  LIVE: {okc} grupper udført, {spent} varianter rørt (budget {a.budget})")
    for k in ("CREATE", "MERGE_nyvariant", "MERGE_konsolidér", "PARK_split"):
        if examples.get(k):
            print(f"\n  [{k}] eksempler:")
            for ex in examples[k][:6]: print(f"     {ex}")
    json.dump({"cats": dict(cats), "create_variants": create_variants, "merge_variants": merge_variants,
               "examples": {k: v for k, v in examples.items()}, "live": a.live,
               "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat()},
              open("output/unified_dryrun.json", "w", encoding="utf-8"), ensure_ascii=False)
    print(f"\n  → output/unified_dryrun.json")


if __name__ == "__main__":
    main()
