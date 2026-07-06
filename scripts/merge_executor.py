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
from collections import defaultdict, Counter
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
        if d.get("errors"):   # top-level GraphQL-fejl (fx manglende scope) = HÅRD fejl, aldrig slug
            raise RuntimeError(f"GraphQL-fejl: {d['errors']}")
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

def _axis_name_multi(values):
    """Ét akse-navn ud fra ALLE en nøgles værdier (pr. nøgle, ikke pr. værdi)."""
    vals = [str(v).strip() for v in values if v]
    if not vals:
        return "Model"
    n = len(vals)
    if sum(1 for v in vals if re.search(r"\d+\s*[x×]\s*\d+|\bcm\b|\bmm\b|ø\d", v.lower())) >= n * 0.5:
        return "Størrelse"
    # materiale: kræv at ordet IKKE er del af 'udtræk'/'indtræk' (config, ikke træ-materiale)
    def is_mat(v):
        vl = v.lower()
        return any(m in vl for m in _MATW) and "træk" not in vl
    if sum(1 for v in vals if is_mat(v)) >= n * 0.5:
        return "Materiale"
    if all(re.fullmatch(r"\d+", v) for v in vals):
        return "Antal i pakke"
    return "Model"

def build_keyname(skus, master):
    """KONSISTENT akse-navn pr. item_variant-NØGLE for en gruppe (ikke pr. værdi) — så fx
    variationAttribute3 ('uden madras' + 'med udtræk') bliver ÉN akse, ikke Model+Materiale."""
    lab = LABELS.get(master, {})
    vals = defaultdict(list)
    for s in skus:
        for k, v in (OPTS.get(str(s).strip(), {}) or {}).items():
            if v: vals[k].append(v)
    km = {}
    for k in sorted(vals):
        nm = lab.get(k) or ("Farve" if k == "color" else _axis_name_multi(vals[k]))
        base, c = nm, 2
        while nm in km.values():
            nm = f"{base} {c}"; c += 1
        km[k] = nm
    return km

def reduce_to_3_axes(opts_list, axes):
    """Fjern GLITCH-akse(r) hvis >3 akser: en akse droppes kun hvis den er (a) domineret af ÉN
    værdi (≥60% af varianter — glitch-signal, fx Antal='1' på 24/27) OG (b) redundant (fjernelse
    skaber INGEN kollision — varianterne er stadig distinkte). Ægte akser (sengegavle/rumdeler hvor
    fjernelse ville kollidere) røres ikke. Returnerer (reducerede_akser, [droppede])."""
    axes = list(axes); dropped = []
    n = len(opts_list) or 1
    while len(axes) > 3:
        best = None
        for a in axes:
            rest = [x for x in axes if x != a]
            combos = [tuple(sorted((k, v) for k, v in o.items() if k in rest)) for o in opts_list]
            if len(combos) != len(set(combos)):
                continue  # fjernelse skaber kollision → ægte akse, ikke redundant
            dom = max(Counter(o.get(a) for o in opts_list).values()) / n
            if dom >= 0.6 and (best is None or dom > best[0]):
                best = (dom, a, rest)
        if not best:
            break
        dropped.append(best[1]); axes = best[2]
    return axes, dropped

def danish_opts(sku, master, keyname=None):
    """SKUs item_variant → {dansk_akse: værdi}. keyname (fra build_keyname) giver konsistent
    navn pr. nøgle; ellers eksakt label → per-værdi-inferens (fallback)."""
    raw = OPTS.get(str(sku).strip(), {})
    lab = LABELS.get(master, {})
    out = {}
    for k, v in raw.items():
        if v:
            name = (keyname or {}).get(k) or lab.get(k) or _axis_one(k, v)
            out[name] = _norm_val(v)
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
                productVariantsBulkCreate(productId:$pid,variants:$v){
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

def reorder_keeper_first(pid, keeper_skus, dry, log):
    """Sørg for at keeperens EGNE varianter står FØRST (bruger produkt-niveau beskrivelse/billeder;
    har kun custom.sku-metafelt). Ellers sorterer Shopify efter option-værdi og de kan havne sidst."""
    if dry:
        return
    d = gql("""query($id:ID!){product(id:$id){variants(first:250){edges{node{id sku}}}}}""", {"id": pid})
    vs = [(e["node"]["id"], (e["node"]["sku"] or "").strip()) for e in d["data"]["product"]["variants"]["edges"]]
    ks = set(keeper_skus)
    ordered = [v for v in vs if v[1] in ks] + [v for v in vs if v[1] not in ks]
    if [v[0] for v in ordered] != [v[0] for v in vs]:
        pos = [{"id": vid, "position": i + 1} for i, (vid, _) in enumerate(ordered)]
        for i in range(0, len(pos), 250):
            gql("""mutation($pid:ID!,$pos:[ProductVariantPositionInput!]!){
              productVariantsBulkReorder(productId:$pid,positions:$pos){userErrors{field message}}}""",
                {"pid": pid, "pos": pos[i:i + 250]})
        log("    ↕ keeper-variant(er) flyttet forrest")
    # 1. variant = sku-only (bruger produkt-niveau beskrivelse/billeder); fjern evt. arvede metafelter
    if ordered:
        gql("""mutation($m:[MetafieldIdentifierInput!]!){metafieldsDelete(metafields:$m){userErrors{message}}}""",
            {"m": [{"ownerId": ordered[0][0], "namespace": "custom", "key": k}
                   for k in ("produktinfo", "variantbilleder")]})

# option-prioritet: Farve altid nr. 1 (Shopify bruger option 1 til variant-thumbnail-swatch),
# derefter Form (2), Materiale (3), resten uændret. Rører KUN option-rækkefølge (ikke varianter).
_OPT_PRI = {"farve": 0, "form": 1, "materiale": 2}

def reorder_options_priority(pid, dry, log):
    d = gql("""query($id:ID!){product(id:$id){options{id name position}}}""", {"id": pid})
    opts = [o for o in (((d.get("data") or {}).get("product") or {}).get("options") or []) if o["name"] != "Title"]
    if len(opts) < 2:
        return
    cur = sorted(opts, key=lambda o: o["position"])
    want = sorted(opts, key=lambda o: (_OPT_PRI.get(o["name"].lower(), 9), o["position"]))
    if [o["id"] for o in want] == [o["id"] for o in cur]:
        return  # allerede korrekt
    if dry:
        log(f"    ↕ ville reordre options → {[o['name'] for o in want]}")
        return
    r = gql("""mutation($pid:ID!,$o:[OptionReorderInput!]!){
      productOptionsReorder(productId:$pid,options:$o){userErrors{field message}}}""",
            {"pid": pid, "o": [{"id": o["id"]} for o in want]})
    errs = (((r.get("data") or {}).get("productOptionsReorder") or {}).get("userErrors")) or []
    if errs:
        raise RuntimeError(f"reorder options: {errs}")
    log(f"    ↕ options → Farve først ({[o['name'] for o in want]})")

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

_REST = f"https://{STORE}/admin/api/2024-10"
_RESTH = {"X-Shopify-Access-Token": TOK, "Content-Type": "application/json"}

def _find_redirect(path):
    r = requests.get(f"{_REST}/redirects.json?path={path}", headers=_RESTH, timeout=30)
    for x in (r.json().get("redirects") or []):
        if x["path"] == path:
            return x
    return None

def del_self_redirect(path, dry, log):
    """Fjern ENHVER redirect på keeperens egen path (aktivt produkt skal ikke redirecte) — ellers
    afviser Shopify donor→keeper ('can't redirect to another redirect'). Target kan være fuld URL.
    Rester fra tidligere redirect-oprydning (~70k self-redirects path→fuld-URL-af-samme-path)."""
    if dry:
        return
    ex = _find_redirect(path)
    if ex:
        requests.delete(f"{_REST}/redirects/{ex['id']}.json", headers=_RESTH, timeout=30)
        log(f"    🧹 fjernet redirect på keeper-path {path}")

def create_redirect(frm, to, dry, log, sb):
    # REST (write_content dækker det — GraphQL urlRedirectCreate kræver write_online_store_navigation
    # som tokenet ikke har). Upsert: opdatér eksisterende self-redirect → keeper, ellers opret.
    log(f"    ↪ redirect {frm} → {to}")
    if not dry:
        ex = _find_redirect(frm)
        if ex:
            if ex["target"] != to:
                requests.put(f"{_REST}/redirects/{ex['id']}.json", headers=_RESTH,
                             json={"redirect": {"id": ex["id"], "path": frm, "target": to}}, timeout=30)
        else:
            rr = requests.post(f"{_REST}/redirects.json", headers=_RESTH,
                               json={"redirect": {"path": frm, "target": to}}, timeout=30)
            if rr.status_code not in (200, 201):
                raise RuntimeError(f"redirect POST {rr.status_code}: {rr.text[:200]}")
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
def reconcile_existing(pid, existing_edges, target_axes, existing_opts, feed, cfg, enrich, seed, dry, log):
    """Bring keeperens EKSISTERENDE varianter til FULDT KANONISK stand — vi kan ikke stole på at
    deres data stemmer (oprettet ad andre veje/tidspunkter). Sætter: kanoniske option-værdier
    (item_variant vinder), pris/compareAt/cost (feed×hub), barcode/vægt + metafelter (produktinfo+
    variantbilleder fra feed; sku altid). 1. variant strippes til sku-only senere af reorder."""
    ups = []
    for e in existing_edges:
        n = e["node"]; s = (n["sku"] or "").strip(); vid = n["id"]
        want = existing_opts.get(s, {})
        cur = {o["name"]: o["value"] for o in n["selectedOptions"] if o["name"] != "Title"}
        opts = {a: (want.get(a) or cur.get(a)) for a in target_axes if (want.get(a) or cur.get(a))}
        vin = {"id": vid, "optionValues": [{"optionName": a, "name": v} for a, v in opts.items()]}
        b2b, _ = feed.get(s, (0, 0))
        if b2b > 0:
            price, cap = resolve_variant_pricing(b2b, cfg, seed=seed, on_sale=True)
            vin["price"] = str(int(price))
            if cap:
                vin["compareAtPrice"] = str(int(cap))
            vin["inventoryItem"] = {"cost": str(b2b)}
        en = enrich.get(s, {})
        mf = [{"namespace": "custom", "key": "sku", "type": "single_line_text_field", "value": s}]
        if en.get("html"):
            mf.append({"namespace": "custom", "key": "produktinfo", "type": "multi_line_text_field", "value": en["html"]})
        if en.get("images"):
            mf.append({"namespace": "custom", "key": "variantbilleder", "type": "list.single_line_text_field", "value": json.dumps(en["images"])})
        vin["metafields"] = mf
        if en.get("ean"):
            vin["barcode"] = en["ean"]
        ups.append(vin)
    if not ups:
        return
    log(f"    ↻ reconcile {len(ups)} keeper-varianter (kanoniske options/pris/cost/metafelter)")
    if dry:
        return
    for i in range(0, len(ups), 100):
        d = gql("""mutation($pid:ID!,$v:[ProductVariantsBulkInput!]!){
          productVariantsBulkUpdate(productId:$pid,variants:$v){userErrors{field message}}}""",
                {"pid": pid, "v": ups[i:i + 100]})
        errs = (((d.get("data") or {}).get("productVariantsBulkUpdate") or {}).get("userErrors")) or []
        if errs:
            raise RuntimeError(f"reconcile: {errs}")

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
    # KONSISTENT akse-navn pr. nøgle for HELE gruppen (added + keeper) — så samme item_variant-
    # nøgle aldrig får to navne (fx variationAttribute3 = ÉN akse, ikke Model+Materiale)
    km = build_keyname([m["sku"] for m in p["variant_creates"]] + list(keeper_skus), master)

    # 2) byg målmatrix pr. tilføjet SKU — options fra item_variant, pris fra feed×hub-regler
    rows = []
    for mv in p["variant_creates"]:
        sku = mv["sku"]
        if sku in keeper_skus: continue          # bor allerede på keeper
        opts = danish_opts(sku, master, km) or {k: v for k, v in (mv["option_values"] or {}).items()}
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

    # keeperens EKSISTERENDE varianter → KANONISKE options. item_variant VINDER over keeperens
    # gamle Shopify-værdier (oprettet ad andre veje/tidspunkter, kan være forkerte: fx 'træ' i
    # stedet for 'Massivt Fyrretræ', eller 'Konstrueret træ' vs 'Konstrueret Træ' → dubletter).
    # Behold kun legacy-options som item_variant ikke dækker.
    existing = {}
    for e in keeper["variants"]["edges"]:
        s = e["node"]["sku"].strip()
        live_opts = {o["name"]: o["value"] for o in e["node"]["selectedOptions"] if o["name"] != "Title"}
        existing[s] = {**live_opts, **danish_opts(s, master, km)}

    # REELLE akser = varierer på tværs af HELE sættet (eksisterende + tilføjede) ∪ keeperens nuværende
    axisvals = defaultdict(set)
    for o in list(existing.values()) + [r["options"] for r in rows]:
        for k, v in o.items():
            if v: axisvals[k].add(v)
    target_axes = sorted({k for k, vv in axisvals.items() if len(vv) > 1}
                         | {o["name"] for o in keeper["options"] if o["name"] != "Title"})
    # drop glitch-akse(r) hvis >3 (domineret + redundant); ægte 4-akser røres ikke
    if len(target_axes) > 3:
        allopts = list(existing.values()) + [r["options"] for r in rows]
        target_axes, dropped = reduce_to_3_axes(allopts, target_axes)
        if dropped:
            log(f"    ✂ droppede glitch-akse(r) {dropped} → {len(target_axes)} akser")
    for r in rows:   # enkelt-værdi/droppede-akser hører til i titlen, ikke som akse
        r["options"] = {k: v for k, v in r["options"].items() if k in target_axes}
    # sikkerhed: stadig >3 akser → ægte, kan ikke i Shopify → spring over (skulle være karantænet)
    if len(target_axes) > 3:
        log(f"    ⏭ >3 akser {target_axes} — springes over (manuel gennemgang)")
        return 0, 0
    # combo-disambiguering: to varianter med SAMME option-kombo kan ikke begge være i Shopify.
    # I stedet for at droppe (datatab) omdøbes den 2./3. med suffiks ' 2',' 3'... på sidste akse,
    # så BEGGE overlever (fx to reelt forskellige varianter vidaXL ikke kan skelne via item_variant).
    seen = {frozenset((v or {}).items()) for v in existing.values()}
    axis = target_axes[-1] if target_axes else None
    for r in rows:
        if frozenset(r["options"].items()) in seen and axis:
            base = r["options"].get(axis, "—"); n = 2
            while frozenset({**r["options"], axis: f"{base} {n}"}.items()) in seen:
                n += 1
            r["options"][axis] = f"{base} {n}"
            log(f"    ↔ kombo-dublet omdøbt: {axis} → '{r['options'][axis]}'")
        seen.add(frozenset(r["options"].items()))

    # 3) trin: opret akser → reconcile keeperens eksisterende varianter (kanonisk) → opret nye
    ensure_options(keeper["id"], target_axes, keeper["options"], dry, log)
    reconcile_existing(keeper["id"], keeper["variants"]["edges"], target_axes, existing,
                       feed, cfg, enrich or {}, p["keeper_handle"], dry, log)
    n_created = create_variants(keeper["id"], rows, dry, log, location_id,
                                (keeper.get("mediaCount") or {}).get("count", 0))
    reorder_keeper_first(keeper["id"], keeper_skus, dry, log)
    reorder_options_priority(keeper["id"], dry, log)   # Farve = option 1 (variant-thumbnail-swatch)
    if p.get("new_title") and p["title_changes"]:
        set_title(keeper["id"], p["new_title"], dry, log)
    if p["product_deletes"]:
        del_self_redirect(f"/products/{p['keeper_handle']}", dry, log)   # så donor→keeper ikke afvises
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
    ap.add_argument("--group", default="", help="kør KUN denne gruppe (keeper-handle eller key)")
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
    if args.group:
        todo = [p for p in todo if args.group in (p["keeper_handle"], p["key"])]
        if not todo:
            sys.exit(f"❌ gruppe '{args.group}' ikke fundet i eksekverbare (måske done/karantæne)")
    limit = args.canary if args.canary else (len(todo) if args.group else (args.dry_groups if dry else len(todo)))
    spent = 0
    for p in todo[:limit] if not args.live or args.canary else todo:
        # Budget tjekkes KUN ved gruppe-grænse (grupper er ATOMISKE — process_group kører hele
        # gruppen eller intet, aldrig halvt). spent>0-guard: en enkelt gruppe der ALENE overskrider
        # budgettet kører altid (kan ikke deles), ellers ville den aldrig blive kørt. Journalen
        # (merge_exec_log status=done) sikrer at næste dags kørsel fortsætter hvor vi slap.
        if not dry and spent > 0 and spent + len(p["variant_creates"]) > args.budget:
            print(f"⏸ dagsbudget nået ({spent}/{args.budget}) — stopper ved gruppe-grænse, genoptager i morgen via journal")
            break
        try:
            if not dry:
                sb.table("merge_exec_log").upsert({"group_key": p["key"], "action": p["action"],
                    "status": "running", "keeper_pid": p["keeper"],
                    "n_variants_planned": len(p["variant_creates"]), "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}).execute()
            nc, nd = process_group(p, scraped, feed, cfg, sb, dry, enrich, location_id)
            spent += nc
            if not dry:
                sb.table("merge_exec_log").upsert({"group_key": p["key"], "action": p["action"], "status": "done",
                    "keeper_pid": p["keeper"], "n_variants_created": nc, "donors_deleted": nd,
                    "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}).execute()
        except Exception as e:
            print(f"  ❌ {p['key']}: {e}")
            if not dry:
                sb.table("merge_exec_log").upsert({"group_key": p["key"], "action": p["action"],
                    "status": "failed", "keeper_pid": p["keeper"], "error": str(e)[:400]}).execute()
    print(f"\n{'DRY-RUN' if dry else 'LIVE'} slut. variant-creates: {spent}")

if __name__ == "__main__":
    main()
