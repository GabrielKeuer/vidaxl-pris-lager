"""DUBLET-MASTER-DETEKTOR (read-only): find master_pids der reelt er SAMME produkt (vidaXL-dublet, fx
M3015580 vs M30155801 skænke). Verificeres via FEED-TITEL-signaturer: to master_pids er dubletter hvis
den mindstes SKU-produkter (rene feed-titler) er ~indeholdt i den størstes → falske par (280 vs 180 g/m²,
5 vs 3 Dele) har FORSKELLIGE signaturer og merges IKKE. Output: output/dup_master.json + Desktop-CSV."""
import sys, os, io, zipfile, csv, json, re
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def sig(t):
    """produkt-signatur: feed-titel uden vidaXL, lowercase, tal/dimensioner BEVARET (skelner g/m², Dele)."""
    t = re.sub(r"(?i)\bvidaxl\b", "", t or "").lower()
    t = re.sub(r"\bfarve\w*\b|\b(?:sort|hvid|grå|brun|blå|grøn|rød|beige|creme|antracit|natur\w*)\b", "", t)
    return " ".join(sorted(set(re.findall(r"[a-zæøå0-9/²]+", t)) - {"cm", "mm", "stk", "og", "med", "til"}))

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
    nm = [f for f in z.namelist() if f.endswith(".csv")][0]
    feedt = {}
    for r in csv.DictReader(io.TextIOWrapper(z.open(nm), encoding="utf-8")):
        s = str(r.get("SKU") or "").strip().replace(".0", "")
        if s:
            feedt[s] = r.get("Title") or ""

    # signatur-sæt pr. master_pid
    sigs = {}
    for mid, skus in bym.items():
        ss = {sig(feedt[s]) for s in skus if s in feedt}
        ss.discard("")
        if ss:
            sigs[mid] = ss
    # SIKKERT mønster: vidaXL-dublet er master_pid X og X+"1" (fx M3015580 → M30155801). Verificér med
    # signatur-overlap (mindstes SKU-produkter ~indeholdt i størstes) så tilfældige +1-par ikke merges.
    merges = {}   # keeper → [absorbed]
    for a in list(sigs):
        b = a + "1"
        if b not in sigs:
            continue
        inter = len(sigs[a] & sigs[b])
        small = min(len(sigs[a]), len(sigs[b]))
        if inter and inter / small >= 0.5:
            keeper, absorbed = (a, b) if len(bym.get(a, [])) >= len(bym.get(b, [])) else (b, a)
            merges.setdefault(keeper, []).append(absorbed)
    total_abs = sum(len(v) for v in merges.values())
    print(f"master_pids: {len(sigs)} | DUBLET-MASTER-par: {len(merges)} keepers absorberer {total_abs} dubletter")

    # flag hvilke der rører de 180 live-berørte
    aff = set(json.load(open("output/pilot_check.json", encoding="utf-8")).get("affected", []))
    touch = {k: v for k, v in merges.items() if k in aff or any(x in aff for x in v)}
    print(f"\ndubletter der rører de 180 live-produkter: {len(touch)}")
    for k, v in list(touch.items())[:25]:
        ex = min(sigs[k], key=len)
        print(f"   KEEP {k} [{len(bym[k])} SKU] ← {', '.join(v)}  ({ex[:44]})")

    json.dump({"merges": merges, "touch_180": touch}, open("output/dup_master.json", "w", encoding="utf-8"), ensure_ascii=False)
    out = r"C:\Users\APC\Desktop\dublet_mastere.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f); w.writerow(["keeper_master_pid", "keeper_skus", "absorberet_master_pid", "absorb_skus", "signatur", "rører_180"])
        for k, v in merges.items():
            for b in v:
                w.writerow([k, len(bym[k]), b, len(bym[b]), min(sigs[k], key=len)[:60], "JA" if (k in aff or b in aff) else ""])
    print(f"\n✓ {out}  +  output/dup_master.json")

if __name__ == "__main__":
    main()
