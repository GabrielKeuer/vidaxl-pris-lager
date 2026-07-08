"""READ-ONLY diagnostik af den nuværende plan (output/complete_feed.json). Identificerer OMFANGET af
problemer — ingen rettelser. Kategorier:
  A) multi_akse_i_titel : multi-produkt hvor titlen indeholder en variant-akse-værdi (inkl. KORTE som
     "2"/"1" + enhed stk/dele, størrelser, farver) — dvs. variant-info står fejlagtigt i titlen.
  B) single_faelles_titel: single-produkt der har fået en FÆLLES/strippet titel i stedet for SKU'ens EGEN
     feed-titel (fx tromlerne: forskellige produkter tvunget til samme titel).
  C) no_axes_multi       : master_pid med >1 SKU men INGEN item_variant → grupperet uden mulighed for varianter.
  D) brudt_titel         : titlen har intet produktnavn (kun farve/materiale/mål).
Output: konsol med tal + eksempler pr. kategori."""
import sys, os, io, zipfile, csv, re, json
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def clean(t):
    return re.sub(r"\s+", " ", re.sub(r"(?i)\bvidaxl\b", "", t or "")).strip()

def main():
    P = json.load(open("output/complete_feed.json", encoding="utf-8"))
    z = zipfile.ZipFile(io.BytesIO(ME.get_feed_zip(os.environ["FEED_URL"])))
    name = [f for f in z.namelist() if f.endswith(".csv")][0]
    feed = {}
    for r in csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8")):
        s = str(r.get("SKU") or "").strip().replace(".0", "")
        if s:
            feed[s] = r.get("Title") or ""
    print(f"produkter i plan: {len(P)} | feed: {len(feed)}")

    A, B, C, D = [], [], [], []
    by_mid = defaultdict(list)
    for p in P:
        by_mid[p["mid"]].append(p)
    for p in P:
        title = (p["title"] or "").lower()
        specs = p["specs"]
        if specs:  # MULTI
            vals = set()
            for v in p["variants"]:
                for nm in specs:
                    x = (v["values"].get(nm) or "").strip()
                    if x:
                        vals.add(x)
            # a) enhver akse-værdi i titlen (også korte)
            leak = [x for x in vals if x and (" " + x.lower() + " ") in (" " + title + " ")]
            # b) "N stk"/"N dele"/"N personers" i titel når det er en variant (kort tal-værdi findes som akse)
            numaxis = any(re.match(r"^\d+$", x) for x in vals)
            qty = re.search(r"\b\d+\s*(stk|dele|personers|ruller|pcs)\b", title) if numaxis else None
            if leak or qty:
                A.append({"key": p["key"], "title": p["title"], "leak": (leak[:3] or [qty.group(0)])})
        else:  # SINGLE
            sku = p["variants"][0]["sku"] if p["variants"] else None
            own = clean(feed.get(sku, "")) if sku else ""
            # b) kom denne single fra en multi-SKU-master (flere produkter fra samme mid)?
            sibs = by_mid[p["mid"]]
            from_multi = len(sibs) > 1 or p.get("orphan")
            if from_multi and own and p["title"].lower().strip() != own.lower().strip():
                B.append({"key": p["key"], "title": p["title"], "egen_feed": own[:46]})
        # d) brudt titel: intet ægte produktnavn (kun tal/farve/mål)? → første ord-token findes i farve/materiale
        toks = [w for w in re.findall(r"[a-zæøå]+", title) if len(w) > 2]
        if not toks:
            D.append({"key": p["key"], "title": p["title"]})
    # c) no_axes multi
    for mid, ps in by_mid.items():
        live_skus = sum(len(x["variants"]) for x in ps)
        if live_skus > 1 and all(not x["specs"] for x in ps):
            C.append({"mid": mid, "n_produkter": len(ps), "n_skus": live_skus,
                      "titler": list({x["title"] for x in ps})[:3]})

    print(f"\n=== DIAGNOSE (FULDT OMFANG) ===")
    print(f"  A) multi-akse-i-titel   : {len(A)}")
    print(f"  B) single-fælles-titel  : {len(B)}")
    print(f"  C) no-axes-multi-master : {len(C)}")
    print(f"  D) brudt-titel          : {len(D)}")
    for nm, L in (("A multi-akse-i-titel", A), ("B single-fælles-titel", B), ("C no-axes-multi", C), ("D brudt-titel", D)):
        print(f"\n--- {nm} (eks.) ---")
        for x in L[:8]:
            print(f"   {x}")
    json.dump({"A": A, "B": B, "C": C, "D": D}, open("output/plan_diagnose.json", "w", encoding="utf-8"), ensure_ascii=False)

if __name__ == "__main__":
    main()
