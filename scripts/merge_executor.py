"""MERGE-EXECUTOR — eksekverer merge_plan.jsonl trin for trin. DRY-RUN er DEFAULT.

Modes:
  (ingen flag)      dry-run: simulér N grupper, print alle mutationer der VILLE køre
  --canary N --live kør N grupper live (fejl-merge-fix først), stop, rapportér
  --live            fuld kørsel m. dagsbudget (--budget, default 1000 variant-creates)

Design:
  - Journal i Supabase merge_exec_log: idempotent genoptagelse pr. gruppe/trin.
  - Trin pr. gruppe: [prep→] options → varianter (m. pris/cost/lager/billede) → titel →
    redirects → slet donorer → done. Sletning sker ALTID sidst og kun efter verifikation.
  - Kilder ved kørsel (aldrig cache): feed b2b+lager (offer-CSV), scraped option-matrix,
    titel-orakel (vidaxl_approved_titles), pricing-config (pricing_rules via pricing.py).
  - Keeperens EGNE varianter opdateres også (nye akse-værdier), ikke kun donor-flyt.
  - Reviews: donor-handles rapporteres FØR sletning (app-migrering afklares separat).
"""
import argparse, csv, io, json, os, re, sys, time, urllib.request, zipfile
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
# Lokal .env.local hvis den findes (dev); ellers env fra CI-secrets (GitHub Actions)
_ENVF = r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local"
if os.path.exists(_ENVF):
    for l in open(_ENVF, encoding="utf-8"):
        m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
        if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
os.environ.setdefault("SUPABASE_URL", os.environ.get("NEXT_PUBLIC_SUPABASE_URL", ""))
# CI: bulk_repricing/_shop_gql forventer disse navne
if os.environ.get("SHOPIFY_STORE_URL") and not os.environ.get("SHOPIFY_STORE"):
    os.environ["SHOPIFY_STORE"] = os.environ["SHOPIFY_STORE_URL"].replace("https://", "").rstrip("/")
import requests
import pricing
from pricing import resolve_variant_pricing
from sync_prices_v2 import get_supabase_client

STORE = os.environ["SHOPIFY_STORE_URL"].replace("https://", "").rstrip("/")
TOK = os.environ["SHOPIFY_ACCESS_TOKEN"]
GQL = f"https://{STORE}/admin/api/2024-10/graphql.json"
PLAN = "output/merge_plan.jsonl"
SCRAPED = "output/scraped_options.jsonl"
OFFER = ("https://feed.vidaxl.io/api/v1/feeds/download/"
         "f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping_offer.csv")

def gql(q, v=None):
    for a in range(4):
        r = requests.post(GQL, json={"query": q, "variables": v or {}},
                          headers={"X-Shopify-Access-Token": TOK}, timeout=(10, 120))
        d = r.json()
        if "errors" in d and any("THROTTLED" in str(e) for e in d["errors"]):
            time.sleep(2 ** a); continue
        return d
    return d

def load_sources():
    plans = [json.loads(l) for l in open(PLAN, encoding="utf-8")]
    scraped = {}
    if os.path.exists(SCRAPED):
        for l in open(SCRAPED, encoding="utf-8"):
            try:
                d = json.loads(l)
                if d.get("variant_map"): scraped[d["master"]] = d["variant_map"]
            except Exception: pass
    return plans, scraped

# item_variant-options (rå, _binary-normaliseret) + eksakte akse-navne pr. master.
# Bruges til at forene keeperens EKSISTERENDE varianter med nye akser (ellers får de
# tom/forkert option-værdi i Shopify når en merge introducerer en akse keeperen ikke havde).
def _load_opts():
    d = {}
    if os.path.exists("output/sku_variants.jsonl"):
        for l in open("output/sku_variants.jsonl", encoding="utf-8"):
            try:
                r = json.loads(l)
                if not (r.get("note") == "ok" and r.get("opts")):
                    continue
                o = {k: v for k, v in r["opts"].items() if not k.endswith("_binary")}
                for k, v in r["opts"].items():
                    if k.endswith("_binary"):
                        o.setdefault(k[:-len("_binary")], v)
                d[str(r["sku"]).strip()] = o
            except Exception:
                pass
    return d

OPTS = _load_opts()
LABELS = json.load(open("output/axis_labels.json", encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}
_MATW = ("træ", "stål", "læder", "stof", "rattan", "metal", "glas", "aluminium", "bambus", "jern",
         "fløjl", "velour", "mango", "fyrre", "akacie", "teak", "massiv", "poly", "bomuld", "gummi")

def _axis_one(k, v):
    if k == "color":
        return "Farve"
    vl = str(v).lower()
    if re.search(r"\d+\s*[x×]\s*\d+|\bcm\b|\bmm\b|ø\d", vl):
        return "Størrelse"
    if any(m in vl for m in _MATW):
        return "Materiale"
    if re.fullmatch(r"\d+", str(v).strip()):
        return "Antal i pakke"
    return "Model"

def _norm_val(v):
    # normalisér mål-separator (× → x) + whitespace; forbogstav pr. ord på ikke-mål-værdier
    # ('artisan eg' → 'Artisan Eg', men '200 cm bordlængde' urørt). Dedup'er casing-dubletter.
    v = re.sub(r"\s+", " ", str(v).replace("×", "x").replace(" ", " ")).strip()
    if v and not re.search(r"\d", v):
        v = " ".join(w[:1].upper() + w[1:] if w else w for w in v.split(" "))
    return v

def danish_opts(sku, master):
    """SKUs item_variant → {dansk_akse: værdi} (eksakt label pr. master → ellers inferens)."""
    raw = OPTS.get(str(sku).strip(), {})
    lab = LABELS.get(master, {})
    out = {}
    for k, v in raw.items():
        if v:
            out[lab.get(k) or _axis_one(k, v)] = _norm_val(v)
    return out

def load_feed():
    r = requests.get(OFFER, headers={"User-Agent": "Mozilla/5.0"}, timeout=300)
    if r.status_code != 200:
        sys.exit(f"❌ offer-feed {r.status_code} — eksekvering kræver frisk feed (pris/lager)")
    out = {}
    for row in csv.DictReader(io.StringIO(r.text)):
        s = (row.get("SKU") or "").strip().replace(".0", "")
        try: out[s] = (float(row.get("B2B price") or 0), int(float(row.get("Stock") or 0)))
        except Exception: pass
    return out

# ---------- enrichment: billeder + beskrivelse + EAN + vægt (fra hoved-feedet) ----------
# Merged varianter er ALTID 'ikke-første' → skal have samme variant-metafelter som normal-
# oprettelse (create_products_v2._row_to_variant_spec): custom.sku + custom.produktinfo +
# custom.variantbilleder (HELE feed-galleriet som permanente vidaXL-URLs) + native billede.
def _clean_vidaxl(t):
    if not t: return ""
    t = str(t)
    for v in ("vidaXL ", "vidaxl ", "VidaXL ", "VIDAXL ", "fra vidaXL", "vidaXL", "vidaxl"):
        t = t.replace(v, "")
    return t.strip()

def _all_images(row):
    imgs = []
    for i in range(1, 22):
        col = f"Image {i}" if i <= 12 else ("image 13" if i == 13 else ("Image 14" if i == 14 else f"image {i}"))
        v = (row.get(col) or "").strip()
        if v.startswith("http"):
            imgs.append(v)
    return imgs

def load_enrich(feed_url):
    """{sku: {images:[feed-urls], html:beskrivelse, ean:str, weight:gram}} fra hoved-feedet (ZIP)."""
    r = requests.get(feed_url, timeout=600)
    r.raise_for_status()
    out = {}
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        name = next(f for f in zf.namelist() if f.endswith(".csv"))
        with zf.open(name) as f:
            for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")):
                s = (row.get("SKU") or "").strip().replace(".0", "")
                if not s:
                    continue
                w = 0
                try: w = int(float(str(row.get("Weight") or 0).replace(",", ".")) * 1000)
                except Exception: pass
                out[s] = {"images": _all_images(row), "html": _clean_vidaxl(row.get("HTML_description", "")),
                          "ean": str(row.get("EAN") or "").strip(), "weight": w}
    return out

def oracle_title(sb, skus):
    for i in range(0, len(skus), 50):
        res = sb.table("vidaxl_approved_titles").select("approved_title").in_("sku", skus[i:i + 50]).limit(1).execute()
        if res.data: return res.data[0]["approved_title"]
    return None

def fetch_group_live(handles):
    """Hent gruppens produkter live: varianter (id/sku/options/billede-URL) + variant-metafelter."""
    out = {}
    for h in handles:
        d = gql("""query($h:String!){productByHandle(handle:$h){id handle title mediaCount{count}
          options{id name position optionValues{id name}}
          variants(first:250){edges{node{id sku selectedOptions{name value}
            image{url} metafields(first:5,namespace:"custom"){edges{node{key value type}}}}}}}}""", {"h": h})
        p = (d.get("data") or {}).get("productByHandle")
        if p: out[h] = p
    return out

# ---------- mutations (kun kaldt ved --live) ----------
def ensure_options(pid, target_axes, existing_options, dry, log):
    """Sørg for at keeper har præcis target-akserne (productOptionsCreate for manglende)."""
    have = {o["name"] for o in existing_options}
    missing = [a for a in target_axes if a not in have]
    for a in missing:
        log(f"    + option '{a}' på keeper")
        if not dry:
            d = gql("""mutation($pid:ID!,$opts:[OptionCreateInput!]!){
                productOptionsCreate(productId:$pid,options:$opts){userErrors{field message}}}""",
                {"pid": pid, "opts": [{"name": a, "values": [{"name": "—"}]}]})
            errs = d["data"]["productOptionsCreate"]["userErrors"]
            if errs: raise RuntimeError(f"optionsCreate: {errs}")
    return missing

def create_variants(pid, rows, dry, log, location_id=None, existing_media=0):
    """productVariantsBulkCreate i batches à 100. Sætter samme variant-data som normal-oprettelse:
    3 metafelter (custom.sku/produktinfo/variantbilleder), native billede + galleri fra PERMANENTE
    feed-URLs, barcode(EAN), vægt, cost, pris, lager."""
    media_map = upload_media(pid, rows, existing_media, dry, log)
    created = 0
    for i in range(0, len(rows), 100):
        chunk = rows[i:i + 100]
        vinputs = []
        for r in chunk:
            mf = [{"namespace": "custom", "key": "sku", "type": "single_line_text_field", "value": r["sku"]}]
            if r.get("html"):
                mf.append({"namespace": "custom", "key": "produktinfo",
                           "type": "multi_line_text_field", "value": r["html"]})
            if r.get("images"):
                mf.append({"namespace": "custom", "key": "variantbilleder",
                           "type": "list.single_line_text_field", "value": json.dumps(r["images"])})
            inv = {"sku": r["sku"], "cost": str(r["b2b"]), "tracked": True, "requiresShipping": True}
            if r.get("weight"):
                inv["measurement"] = {"weight": {"value": r["weight"] / 1000.0, "unit": "KILOGRAMS"}}
            vi = {"optionValues": [{"optionName": k, "name": v or "—"} for k, v in r["options"].items()],
                  "price": str(r["price"]), "inventoryItem": inv, "inventoryPolicy": "DENY",
                  "taxable": True, "metafields": mf}
            if r.get("ean"): vi["barcode"] = r["ean"]
            if r.get("cap"): vi["compareAtPrice"] = str(r["cap"])
            if location_id and r.get("stock") is not None:
                vi["inventoryQuantities"] = [{"locationId": location_id, "availableQuantity": int(r["stock"])}]
            # native billede via mediaId (dedup'et, permanent feed-URL) → vandtæt + under 250-grænsen
            mid = media_map.get((r.get("images") or [None])[0])
            if mid and not str(mid).startswith("dry-"): vi["mediaId"] = mid
            vinputs.append(vi)
        log(f"    + {len(chunk)} varianter (bulkCreate)")
        if not dry:
            d = gql("""mutation($pid:ID!,$v:[ProductVariantsBulkInput!]!){
                productVariantsBulkCreate(productId:$pid,variants:$v,strategy:REMOVE_STANDALONE_VARIANT){
                userErrors{field message} productVariants{id inventoryItem{id}}}}""", {"pid": pid, "v": vinputs})
            res = d["data"]["productVariantsBulkCreate"]
            if res["userErrors"]: raise RuntimeError(f"bulkCreate: {res['userErrors']}")
            created += len(res["productVariants"])
        else:
            created += len(chunk)
    return created

def upload_media(pid, rows, existing_media, dry, log):
    """KUN ét native billede pr. variant (dedup pr. URL) — IKKE hele galleriet. Galleriet ligger i
    variantbilleder-metafeltet (ubegrænset streng, ikke Shopify-media). Respekterer 250-media/produkt-
    grænsen: farve-DÆKNING først (1 pr. unik farve), så richness, cap ved budget. Overskydende varianter
    får intet native billede men viser galleri-strengen → intet produkt brækker."""
    budget = max(0, 245 - int(existing_media or 0))   # 5 buffer under 250
    firstimg = lambda r: (r.get("images") or [None])[0]
    ordered, seen, colors = [], set(), set()
    for r in rows:   # coverage: 1 billede pr. unik farve (vigtigst — hver farve får en thumbnail)
        u, c = firstimg(r), r["options"].get("Farve")
        if u and u not in seen and c and c not in colors:
            colors.add(c); seen.add(u); ordered.append(u)
    for r in rows:   # richness: resten
        u = firstimg(r)
        if u and u not in seen:
            seen.add(u); ordered.append(u)
    dropped = len(ordered) - budget
    ordered = ordered[:budget]
    if dropped > 0:
        log(f"    ⚠ {dropped} native billeder droppet (250-grænse) — de varianter bruger galleri-metafeltet")
    if not ordered:
        return {}
    log(f"    🖼 uploader {len(ordered)} native billeder (galleri-strengen dækker alle varianter)")
    if dry:
        return {u: f"dry-{i}" for i, u in enumerate(ordered)}
    out = {}
    for i in range(0, len(ordered), 50):
        chunk = ordered[i:i + 50]
        d = gql("""mutation($pid:ID!,$m:[CreateMediaInput!]!){
            productCreateMedia(productId:$pid,media:$m){media{id} mediaUserErrors{field message}}}""",
                {"pid": pid, "m": [{"originalSource": u, "mediaContentType": "IMAGE"} for u in chunk]})
        res = (d.get("data") or {}).get("productCreateMedia") or {}
        errs = res.get("mediaUserErrors") or []
        if errs:
            raise RuntimeError(f"productCreateMedia: {errs}")
        for u, m in zip(chunk, res.get("media") or []):   # productCreateMedia bevarer rækkefølge
            out[u] = m["id"]
    return out

def set_title(pid, title, dry, log):
    # opdatér BÅDE produkt-titel (H1) OG SEO-meta-titel (global.title_tag), så meta-titlen ikke
    # bliver stående på den gamle variant-specifikke titel. Ingen pris i titlen (JSON-LD viser live pris).
    seo_t = title if len(title) <= 70 else title[:67] + "..."
    log(f"    ✎ titel + SEO-meta → {title!r}")
    if not dry:
        d = gql("""mutation($i:ProductInput!){productUpdate(input:$i){userErrors{field message}}}""",
                {"i": {"id": pid, "title": title, "seo": {"title": seo_t}}})
        errs = d["data"]["productUpdate"]["userErrors"]
        if errs: raise RuntimeError(f"productUpdate: {errs}")

def create_redirect(frm, to, dry, log, sb):
    log(f"    ↪ redirect {frm} → {to}")
    if not dry:
        d = gql("""mutation($r:UrlRedirectInput!){urlRedirectCreate(urlRedirect:$r){userErrors{field message}}}""",
                {"r": {"path": frm, "target": to}})
        errs = (((d.get("data") or {}).get("urlRedirectCreate") or {}).get("userErrors")) or []
        # 'already exists' = idempotent OK (redirect ligger fra tidligere kørsel)
        if errs and not any("exist" in str(e).lower() or "taken" in str(e).lower() for e in errs):
            raise RuntimeError(f"urlRedirectCreate: {errs}")
        try: sb.table("deleted_redirects").insert({"path": frm, "target": to, "source": "merge_executor"}).execute()
        except Exception: pass

def delete_product(pid, handle, dry, log):
    log(f"    ✖ slet donor {handle}")
    if not dry:
        d = gql("""mutation($i:ProductDeleteInput!){productDelete(input:$i){deletedProductId userErrors{field message}}}""",
                {"i": {"id": pid}})
        res = (d.get("data") or {}).get("productDelete") or {}
        errs = res.get("userErrors") or []
        # allerede slettet ('not found'/'does not exist') = idempotent OK ved resume
        if errs and not any(("not" in str(e).lower() and ("found" in str(e).lower() or "exist" in str(e).lower()))
                            for e in errs):
            raise RuntimeError(f"productDelete: {errs}")

# ---------- gruppe-processor ----------
def backfill_existing(pid, existing_edges, target_axes, existing_opts, dry, log):
    """Sæt korrekte akse-værdier på keeperens EKSISTERENDE varianter for alle target-akser,
    så en nyintroduceret akse ikke efterlader tomme/forkerte værdier. Idempotent."""
    updates = []
    for e in existing_edges:
        s = e["node"]["sku"].strip()
        cur = {o["name"]: o["value"] for o in e["node"]["selectedOptions"] if o["name"] != "Title"}
        want = existing_opts.get(s, {})
        newvals = {a: (want.get(a) or cur.get(a)) for a in target_axes if (want.get(a) or cur.get(a))}
        if newvals and any(cur.get(a) != v for a, v in newvals.items()):
            updates.append({"id": e["node"]["id"],
                            "optionValues": [{"optionName": a, "name": v} for a, v in newvals.items()]})
    if not updates:
        return
    log(f"    ↻ backfill {len(updates)} eksisterende varianter (nye akse-værdier)")
    if dry:
        return
    for i in range(0, len(updates), 100):
        d = gql("""mutation($pid:ID!,$v:[ProductVariantsBulkInput!]!){
          productVariantsBulkUpdate(productId:$pid,variants:$v){userErrors{field message}}}""",
                {"pid": pid, "v": updates[i:i + 100]})
        errs = (((d.get("data") or {}).get("productVariantsBulkUpdate") or {}).get("userErrors")) or []
        if errs:
            raise RuntimeError(f"backfill: {errs}")

def process_group(p, scraped, feed, cfg, sb, dry, enrich=None, location_id=None):
    key = p["key"]; master = key.split("|")[1] if "|" in key else ""
    log_lines = []
    log = lambda s: (log_lines.append(s), print(s))
    log(f"▶ {key} [{p['action']}] keeper={p['keeper_handle']} ({p['n_variants_final']} var)")
    vm = scraped.get(master) or {}

    # 1) live-tilstand for keeper + donorer
    donor_handles = sorted({m["src_product"] for m in p["variant_creates"] if m.get("src_product")} - {p["keeper_handle"]})
    live = fetch_group_live([p["keeper_handle"]] + donor_handles)
    keeper = live.get(p["keeper_handle"])
    if not keeper:
        raise RuntimeError("keeper ikke fundet live (drift) — gruppe springes over")
    keeper_skus = {e["node"]["sku"].strip() for e in keeper["variants"]["edges"]}

    # 2) byg målmatrix pr. tilføjet SKU — options fra item_variant, pris fra feed×hub-regler
    rows = []
    for mv in p["variant_creates"]:
        sku = mv["sku"]
        if sku in keeper_skus: continue          # bor allerede på keeper
        opts = danish_opts(sku, master) or {k: v for k, v in (mv["option_values"] or {}).items()}
        b2b, stock = feed.get(sku, (0, 0))
        if b2b <= 0:
            log(f"    ⚠ {sku}: ingen b2b i feed — springes over"); continue
        price, cap = resolve_variant_pricing(b2b, cfg, seed=p["keeper_handle"], on_sale=True)
        img = None
        src = live.get(mv.get("src_product") or "")
        if src:
            for e in src["variants"]["edges"]:
                if e["node"]["sku"].strip() == sku and e["node"].get("image"):
                    img = e["node"]["image"]["url"]; break
        e = (enrich or {}).get(sku, {})
        rows.append({"sku": sku, "options": opts, "price": int(price), "cap": int(cap) if cap else None,
                     "b2b": b2b, "img_url": img, "stock": stock,
                     "images": e.get("images"), "html": e.get("html"), "ean": e.get("ean"), "weight": e.get("weight")})

    # keeperens EKSISTERENDE varianter → deres komplette options (live-valgte + item_variant)
    existing = {}
    for e in keeper["variants"]["edges"]:
        s = e["node"]["sku"].strip()
        cur = {o["name"]: o["value"] for o in e["node"]["selectedOptions"] if o["name"] != "Title"}
        for k, v in danish_opts(s, master).items():
            cur.setdefault(k, v)
        existing[s] = cur

    # REELLE akser = varierer på tværs af HELE sættet (eksisterende + tilføjede) ∪ keeperens nuværende
    axisvals = defaultdict(set)
    for o in list(existing.values()) + [r["options"] for r in rows]:
        for k, v in o.items():
            if v: axisvals[k].add(v)
    target_axes = sorted({k for k, vv in axisvals.items() if len(vv) > 1}
                         | {o["name"] for o in keeper["options"] if o["name"] != "Title"})
    for r in rows:   # enkelt-værdi-attributter hører til i titlen, ikke som akse
        r["options"] = {k: v for k, v in r["options"].items() if k in target_axes}

    # sikkerhed: >3 akser kan ikke lade sig gøre i Shopify → spring over (skulle være karantænet)
    if len(target_axes) > 3:
        log(f"    ⏭ >3 akser {target_axes} — springes over (manuel gennemgang)")
        return 0, 0
    # combo-dedup: spring en ny variant over hvis dens option-kombo allerede findes på keeper
    existing_combos = {frozenset(v.items()) for v in existing.values()}
    before = len(rows)
    rows = [r for r in rows if frozenset(r["options"].items()) not in existing_combos]
    if len(rows) < before:
        log(f"    ↷ {before - len(rows)} varianter har kombo der allerede findes på keeper — springes over")

    # 3) trin: opret akser → backfill eksisterende varianter → opret nye varianter
    ensure_options(keeper["id"], target_axes, keeper["options"], dry, log)
    backfill_existing(keeper["id"], keeper["variants"]["edges"], target_axes, existing, dry, log)
    n_created = create_variants(keeper["id"], rows, dry, log, location_id,
                                (keeper.get("mediaCount") or {}).get("count", 0))
    if p.get("new_title") and p["title_changes"]:
        set_title(keeper["id"], p["new_title"], dry, log)
    for dd in p["product_deletes"]:
        create_redirect(f"/products/{dd['handle']}", f"/products/{p['keeper_handle']}", dry, log, sb)
        delete_product(dd["pid"], dd["handle"], dry, log)
    return n_created, len(p["product_deletes"])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--canary", type=int, default=0)
    ap.add_argument("--budget", type=int, default=1000)
    ap.add_argument("--dry-groups", type=int, default=3)
    args = ap.parse_args()
    dry = not args.live
    sb = get_supabase_client()
    plans, scraped = load_sources()
    cfg = pricing.load_pricing_config(sb, vendor="vidaXL")
    class _MockFeed(dict):
        def get(self, k, d=None): return (100.0, 5)   # dry-run: .get() giver altid b2b
    feed = load_feed() if not dry else _MockFeed()
    if dry: print("⚠ DRY-RUN: feed mocked (b2b=100), ingen mutationer sendes\n")
    # enrichment (billeder/beskrivelse/EAN/vægt) + lager-lokation — kun ved live
    enrich, location_id = {}, None
    if not dry:
        fu = os.environ.get("FEED_URL")
        if not fu:
            sys.exit("❌ FEED_URL mangler — kræves for variant-metafelter/billeder")
        print("📥 Henter hoved-feed (billeder/beskrivelse)…")
        enrich = load_enrich(fu)
        print(f"   {len(enrich)} SKUs beriget")
        ld = gql("""{locations(first:1,query:"status:active"){edges{node{id}}}}""")
        location_id = ld["data"]["locations"]["edges"][0]["node"]["id"]

    # rækkefølge: fix_mismerge_rest først, så merges (mindste først = hurtig validérbar fremdrift)
    done = {r["group_key"] for r in (sb.table("merge_exec_log").select("group_key").eq("status", "done").execute().data or [])}
    todo = [p for p in plans if p["action"] in ("fix_mismerge_rest", "merge") and p["key"] not in done
            and not p.get("unresolved_collisions") and not p.get("dup_sku_quarantine")]
    todo.sort(key=lambda p: (p["action"] != "fix_mismerge_rest", len(p["variant_creates"])))
    limit = args.canary if args.canary else (args.dry_groups if dry else len(todo))
    spent = 0
    for p in todo[:limit] if not args.live or args.canary else todo:
        if spent + len(p["variant_creates"]) > args.budget and not dry:
            print(f"⏸ dagsbudget nået ({spent}/{args.budget})"); break
        try:
            if not dry:
                sb.table("merge_exec_log").upsert({"group_key": p["key"], "action": p["action"],
                    "status": "running", "keeper_pid": p["keeper"],
                    "n_variants_planned": len(p["variant_creates"]), "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}).execute()
            nc, nd = process_group(p, scraped, feed, cfg, sb, dry, enrich, location_id)
            spent += nc
            if not dry:
                sb.table("merge_exec_log").upsert({"group_key": p["key"], "status": "done",
                    "n_variants_created": nc, "donors_deleted": nd,
                    "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}).execute()
        except Exception as e:
            print(f"  ❌ {p['key']}: {e}")
            if not dry:
                sb.table("merge_exec_log").upsert({"group_key": p["key"], "status": "failed", "error": str(e)[:400]}).execute()
    print(f"\n{'DRY-RUN' if dry else 'LIVE'} slut. variant-creates: {spent}")

if __name__ == "__main__":
    main()
