"""READ-ONLY scope-tjek: hvor mange master_pids ville splitte med KOORDINAT+BASIS-reglen?
Regel: for hver master_pid, strip variant-akse-værdier fra hver SKU's feed-titel → basis. Grupper SKU'er
efter option-KOMBO. Hvis SKU'er på SAMME kombo (eller ingen akser = fælles tom kombo) har DIVERGERENDE
basis (ikke delmængde, lavt token-overlap) → master_pid'en indeholder forskellige produkter → split.
Rapporterer antal + eksempler + tæt-på-tærsklen. INGEN ændringer."""
import sys, os, io, zipfile, csv, re, json
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, r"C:\Users\APC\dropxl-product-automation\scripts")
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME
import build_complete_feed as B
import title_rules as TR

MATERIAL_STOP = {"træ", "stof", "metal", "stål", "jern", "glas", "plast", "plastik", "beton", "læder",
                 "kunstlæder", "rattan", "polyrattan", "bambus", "aluminium", "polyester", "keramik",
                 "resin", "marmor", "gummi", "kork", "filt", "skum", "krydsfiner", "spånplade", "finér",
                 "velour", "fløjl", "bomuld", "jute", "sten", "kobber", "messing", "krom", "zink"}
SIZE_AXES = {"Størrelse", "Højde", "Bredde", "Længde", "Dybde", "Bordlængde", "Diameter"}

def setup_universe(feed):
    def addf(cl):
        if not cl or len(cl) < 3 or cl in MATERIAL_STOP:
            return
        forms = {cl, cl.replace("-", " "), cl.replace(" ", ""), cl.replace("-", "")}
        for p2, j in (("mørk ", "mørke"), ("lys ", "lyse")):
            if cl.startswith(p2):
                forms |= {j + cl[len(p2):], j + " " + cl[len(p2):]}
        for f in forms:
            if f and len(f) >= 2 and f not in MATERIAL_STOP:
                B.COLOR_UNIVERSE.add(f)
    for s in feed:
        addf((ME.OPTS.get(s) or {}).get("color", ""))
    for c in getattr(TR, "COLOR_LEX", set()):
        cl = c.lower().strip()
        if cl and len(cl) >= 2 and cl not in MATERIAL_STOP:
            B.COLOR_UNIVERSE.add(cl)
    for w in ("eg", "sonoma", "artisan", "røget", "skygrå", "himmelblå", "flaskegrøn", "nougat", "champagne"):
        B.COLOR_UNIVERSE.add(w)
    B.build_color_re()

def toks(s):
    return set(re.findall(r"[a-zæøå0-9]+", (s or "").lower()))

def same_product(a, b):
    """samme produkt hvis delmængde (ekstra ord) ELLER højt token-overlap (stavefejl)."""
    ta, tb = toks(a), toks(b)
    if not ta or not tb:
        return True
    if ta <= tb or tb <= ta:
        return True
    j = len(ta & tb) / len(ta | tb)
    return j >= 0.6

def main():
    sb = ME.get_supabase_client()
    bym = defaultdict(list); fr = 0
    while True:
        b = sb.table("vidaxl_sku_master").select("sku,master_pid").range(fr, fr + 999).execute().data or []
        for x in b:
            bym[x["master_pid"]].append(str(x["sku"]).strip())
        if len(b) < 1000:
            break
        fr += 1000
    z = zipfile.ZipFile(io.BytesIO(ME.get_feed_zip(os.environ["FEED_URL"])))
    name = [f for f in z.namelist() if f.endswith(".csv")][0]
    feed = {}
    for r in csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8")):
        s = str(r.get("SKU") or "").strip().replace(".0", "")
        if s:
            feed[s] = r.get("Title") or ""
    setup_universe(feed)
    lbl = json.load(open("output/axis_labels.json", encoding="utf-8")) if os.path.exists("output/axis_labels.json") else {}

    flagged = []; extra_products = 0
    for mid, skus in bym.items():
        live = [s for s in skus if s in feed]
        if len(live) < 2:
            continue
        opts = {s: {k: v for k, v in (ME.OPTS.get(s) or {}).items() if v} for s in live}
        axvals = defaultdict(set)
        for s in live:
            for k, v in opts[s].items():
                axvals[k].add(v)
        axes = sorted(k for k, vv in axvals.items() if len(vv) > 1)
        names = {k: ("Farve" if k == "color" else (lbl.get(mid, {}).get(k) or "x")) for k in axes}
        # SAMMENLIGNINGS-BASIS = produkt-NAVNET: strip ALLE farver+mål+materiale+antal AGGRESSIVT
        # (uanset akser), så "Sovesofa Velour Sort/Lysegrå" og "TV-skab Genbrugstræ/Mangotræ" bliver ens.
        # Kun ægte produkt-navns-forskel (Havestole vs Sidebord, Sofa vs Stol) overlever.
        base = {}
        for s in live:
            av = [opts[s].get(k) for k in axes]
            b = B.strip_axes(B.clean(feed[s]), av, strip_colors=True, strip_dims=True)
            b = re.sub(r"\b\d+\s*(?:stk|dele|pcs|sæt|pk|personers?|sæders?)\.?\b", " ", b.lower())
            b = " ".join(w for w in re.findall(r"[a-zæøå]+", b) if w not in MATERIAL_STOP and len(w) > 2)
            base[s] = b
        # PRÆCIS signal: to SKUs på SAMME variant-koordinat (samme kombo) med DIVERGERENDE produkt-navn
        # → akserne forklarer ikke forskellen → skjult produkt-forskel. (Bred alle-SKU-divergens over-flagger
        # 3000+ pga. feed-inkonsistens, så vi holder os til kollisions-signalet.)
        def cluster(sku_list):
            cl = []
            for s in sku_list:
                for grp in cl:
                    if same_product(base[s], base[grp[0]]):
                        grp.append(s); break
                else:
                    cl.append([s])
            return cl
        bycombo = defaultdict(list)
        for s in live:
            bycombo[tuple(opts[s].get(k, "") for k in axes)].append(s)
        divergent = any(len(cs) > 1 and len(cluster(cs)) > 1 for cs in bycombo.values())
        if divergent:
            allcl = cluster(live)
            flagged.append({"mid": mid, "n_skus": len(live), "n_produkter": len(allcl), "axes": [names[k] for k in axes],
                            "baser": list({base[s] for s in live})[:6]})
            extra_products += len(allcl) - 1

    print(f"=== SCOPE-TJEK (koordinat+basis-regel) ===")
    print(f"  master_pids der ville SPLITTE: {len(flagged)}")
    print(f"  ekstra produkter det giver: +{extra_products}")
    print(f"\n--- eksempler ---")
    for x in sorted(flagged, key=lambda z: -z["n_produkter"])[:15]:
        print(f"   {x['mid']} [{x['n_skus']} SKU → {x['n_produkter']} produkter] akser={x['axes']}")
        for b in x["baser"]:
            print(f"        \"{b[:52]}\"")
    json.dump(flagged, open("output/scope_split.json", "w", encoding="utf-8"), ensure_ascii=False)

if __name__ == "__main__":
    main()
