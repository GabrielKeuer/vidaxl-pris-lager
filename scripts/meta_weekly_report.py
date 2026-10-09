#!/usr/bin/env python3
"""
Meta Ads – ugentlig rapport til hubben (meta_weekly_reports) — deterministisk, uden AI.

Henter:
  * Meta Marketing API: konto 7d/30d, annoncesæt + annoncer (7d/30d/all-time), placeringer, alder.
  * Supabase order_attribution: FØRSTEPARTS-ordrer pr. kanal (br_sid-beacon) 7d/30d → "ægte" Meta-ROAS.
  * Shopify: DB % pr. leverandør (betalte ordrer 60 d, pris vs. unitCost) → break-even-ROAS pr. leverandør.
Regler (beslutningsgrundlag, IKKE automatisk handling):
  * break-even ROAS = 1 / DB-andel (inkl. moms) — vidaXL ~3,5×, Kayoom ~3,0×, Sollux ~2,9×, Ayyildiz 2,5×, Eurofirany ~1,8×.
  * annoncesæt vurderes mod break-even for sit produktsæt/brand (navn-match) + 1,15 sikkerhedsmargin.
Skriver én række i meta_weekly_reports (samme JSON-form som de tidligere rapporter) + Slack-resumé.
"""
import os, sys, json, time, datetime, urllib.request, urllib.parse, statistics, re
from collections import defaultdict

V = "v21.0"
TOK = os.environ["META_ACCESS_TOKEN"]; ACC = "act_" + os.environ["META_AD_ACCOUNT_ID"].replace("act_", "")
SB = os.environ["SUPABASE_URL"].rstrip("/"); SK = os.environ["SUPABASE_SERVICE_KEY"]
STORE = os.environ.get("SHOPIFY_STORE_URL", "").replace("https://", "").strip("/"); STOK = os.environ.get("SHOPIFY_ACCESS_TOKEN", "")
SLACK = os.environ.get("SLACK_WEBHOOK_URL", "")
VAT = 1.25

def log(m): print(f"[{datetime.datetime.now():%H:%M:%S}] {m}", flush=True)

def meta(path, **p):
    p["access_token"] = TOK
    url = f"https://graph.facebook.com/{V}/{path}?" + urllib.parse.urlencode(p)
    for a in range(5):
        try: return json.load(urllib.request.urlopen(url, timeout=120))
        except urllib.error.HTTPError as e:
            b = e.read()[:200]
            if e.code in (429, 500, 503) or b"request limit" in b: time.sleep(30 * (a + 1)); continue
            raise RuntimeError(f"Meta {e.code} {path}: {b}")
    raise RuntimeError("Meta: for mange forsøg")

def meta_all(path, **p):
    out = []; d = meta(path, **p)
    while True:
        out += d.get("data", []); nxt = d.get("paging", {}).get("next")
        if not nxt: return out
        d = json.load(urllib.request.urlopen(nxt, timeout=120))

F = "spend,impressions,clicks,ctr,cpc,cpm,actions,action_values,frequency"
def purch(r):
    a = {x["action_type"]: float(x["value"]) for x in r.get("actions", [])}; v = {x["action_type"]: float(x["value"]) for x in r.get("action_values", [])}
    return a.get("omni_purchase", a.get("purchase", 0)), v.get("omni_purchase", v.get("purchase", 0))

def sb_get(path):
    req = urllib.request.Request(f"{SB}/rest/v1/{path}", headers={"apikey": SK, "Authorization": f"Bearer {SK}"})
    return json.load(urllib.request.urlopen(req, timeout=120))

def sb_post(table, row):
    req = urllib.request.Request(f"{SB}/rest/v1/{table}", data=json.dumps(row).encode(), method="POST",
                                 headers={"apikey": SK, "Authorization": f"Bearer {SK}", "Content-Type": "application/json", "Prefer": "return=representation"})
    return json.load(urllib.request.urlopen(req, timeout=120))

def shopify_db_by_vendor(days=60):
    """DB % af pris inkl. moms pr. leverandør, betalte ordrer sidste N dage, kost = variant unitCost."""
    if not (STORE and STOK): return {}
    since = (datetime.date.today() - datetime.timedelta(days=days)).isoformat(); cur = None; acc = defaultdict(lambda: [0.0, 0.0])
    q = 'query($c:String,$q:String){orders(first:100,after:$c,query:$q){pageInfo{hasNextPage endCursor} nodes{lineItems(first:50){nodes{quantity vendor discountedTotalSet{shopMoney{amount}} variant{inventoryItem{unitCost{amount}}}}}}}}'
    while True:
        req = urllib.request.Request(f"https://{STORE}/admin/api/2024-10/graphql.json", data=json.dumps({"query": q, "variables": {"c": cur, "q": f"created_at:>={since} financial_status:paid"}}).encode(),
                                     headers={"X-Shopify-Access-Token": STOK, "Content-Type": "application/json"})
        d = json.load(urllib.request.urlopen(req, timeout=120))
        if "errors" in d:
            if "THROTTLED" in str(d["errors"]): time.sleep(5); continue
            log(f"Shopify: {d['errors'][:1]}"); break
        o = d["data"]["orders"]
        for n in o["nodes"]:
            for li in n["lineItems"]["nodes"]:
                c = (li.get("variant") or {}).get("inventoryItem", {}).get("unitCost")
                if not c or float(c["amount"]) <= 0: continue
                rev = float(li["discountedTotalSet"]["shopMoney"]["amount"]); acc[li["vendor"] or "?"][0] += rev; acc[li["vendor"] or "?"][1] += rev / VAT - float(c["amount"]) * li["quantity"]
        if not o["pageInfo"]["hasNextPage"]: break
        cur = o["pageInfo"]["endCursor"]
    return {v: (db / rev if rev else 0) for v, (rev, db) in acc.items() if rev > 0}

DEFAULT_DB = {"vidaxl": 0.28, "kayoom": 0.31, "sollux": 0.34, "ayyildiz": 0.40, "eurofirany": 0.55, "design": 0.35, "bestsellere": 0.28, "retargeting": 0.28}
def vendor_of(name):
    n = name.lower()
    for k in ("kayoom", "sollux", "ayyildiz", "eurofirany", "vidaxl", "design", "bestseller", "retargeting", "besøgt", "kurv", "købt"):
        if k in n: return {"bestseller": "bestsellere", "besøgt": "retargeting", "kurv": "retargeting", "købt": "retargeting"}.get(k, k)
    return "vidaxl"   # 'Alle møbler' = fuldt sortiment, 98 % vidaXL

def main():
    today = datetime.date.today(); week_start = (today - datetime.timedelta(days=today.weekday())).isoformat()
    log("Meta: konto 7d/30d")
    acct = {}
    for dp in ("last_7d", "last_30d"):
        r = (meta(f"{ACC}/insights", fields=F, date_preset=dp, level="account").get("data") or [{}])[0]
        p, v = purch(r) if r else (0, 0); sp = float(r.get("spend", 0) or 0)
        acct[dp[5:]] = {"spend": round(sp, 2), "revenue": round(v), "purchases": int(p), "roas": round(v / sp, 2) if sp else 0, "ctr": float(r.get("ctr", 0) or 0), "cpc": float(r.get("cpc", 0) or 0), "cpm": float(r.get("cpm", 0) or 0), "frequency": float(r.get("frequency", 0) or 0)}
    log("Meta: annoncesæt + annoncer")
    adsets = {a["id"]: a for a in meta_all(f"{ACC}/adsets", fields="id,name,effective_status,campaign{name},optimization_goal", limit=100)}
    ads = meta_all(f"{ACC}/ads", fields="id,name,effective_status,adset_id,created_time", limit=100)
    ins = {}
    for dp in ("last_7d", "last_30d", "maximum"):
        ins[dp] = {r["ad_id"]: r for r in meta_all(f"{ACC}/insights", fields="ad_id," + F, level="ad", date_preset=dp, limit=500)}
    adset_ins = {}
    for dp in ("last_7d", "last_30d"):
        adset_ins[dp] = {r["adset_id"]: r for r in meta_all(f"{ACC}/insights", fields="adset_id," + F, level="adset", date_preset=dp, limit=200)}
    log("Shopify: DB pr. leverandør")
    db = {k.lower(): v for k, v in shopify_db_by_vendor().items()}
    dbmap = dict(DEFAULT_DB); dbmap.update({k: v for k, v in db.items() if k in dbmap})
    log("Supabase: førsteparts-attribution")
    fp = {}
    for d, key in ((7, "7d"), (30, "30d")):
        since = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=d)).isoformat()
        rows = sb_get(f"order_attribution?select=channel,revenue,cancelled&order_created_at=gte.{since}&limit=5000")
        agg = defaultdict(lambda: [0, 0.0])
        for r in rows:
            if r.get("cancelled"): continue
            agg[r.get("channel") or "Ukendt"][0] += 1; agg[r.get("channel") or "Ukendt"][1] += float(r.get("revenue") or 0)
        fp[key] = {ch: {"orders": n, "revenue": round(rev)} for ch, (n, rev) in sorted(agg.items(), key=lambda kv: -kv[1][1])}
    # ---- annonce-performance (samme form som tidligere rapporter) ----
    perf = []; now = datetime.datetime.now(datetime.timezone.utc)
    for ad in ads:
        if ad["effective_status"] not in ("ACTIVE", "ADSET_PAUSED", "CAMPAIGN_PAUSED", "PAUSED"): continue
        r7 = ins["last_7d"].get(ad["id"]); r30 = ins["last_30d"].get(ad["id"]); ra = ins["maximum"].get(ad["id"])
        if not (r30 or ra): continue
        p7, v7 = purch(r7) if r7 else (0, 0); p30, v30 = purch(r30) if r30 else (0, 0); pa, va = purch(ra) if ra else (0, 0)
        s7 = float((r7 or {}).get("spend", 0) or 0); s30 = float((r30 or {}).get("spend", 0) or 0); sa = float((ra or {}).get("spend", 0) or 0)
        aset = adsets.get(ad["adset_id"], {}); days = (now - datetime.datetime.fromisoformat(ad["created_time"].replace("+0000", "+00:00"))).days
        vend = vendor_of(aset.get("name", "") + " " + ad["name"]); dbp = dbmap.get(vend, 0.28); be = round(1 / dbp, 2)
        perf.append({"id": ad["id"], "name": ad["name"], "status": ad["effective_status"], "adsetName": aset.get("name"), "campaignName": (aset.get("campaign") or {}).get("name"),
                     "spend7d": round(s7, 2), "revenue7d": round(v7), "purchases7d": int(p7), "roas7d": round(v7 / s7, 2) if s7 else 0, "ctr7d": float((r7 or {}).get("ctr", 0) or 0),
                     "spend30d": round(s30, 2), "revenue30d": round(v30), "purchases30d": int(p30), "roas30d": round(v30 / s30, 2) if s30 else 0, "ctr30d": float((r30 or {}).get("ctr", 0) or 0),
                     "spendAllTime": round(sa, 2), "revenueAllTime": round(va), "purchasesAllTime": int(pa), "roasAllTime": round(va / sa, 2) if sa else 0,
                     "daysActive": days, "isNew": days < 7, "vendor": vend, "dbPct": round(dbp * 100, 1), "breakEvenRoas": be,
                     "profitRoas30d": round((v30 * dbp) / s30, 2) if s30 else 0, "profit30d": round(v30 * dbp - s30)})
    # ---- forslag (regler) ----
    sugg = []; hi = []
    for p in perf:
        if p["status"] != "ACTIVE": continue
        if p["spend30d"] < 1500: sugg.append({"ad": p["name"], "adId": p["id"], "type": "watch", "reason": f"Kun {p['spend30d']:.0f} kr på 30 d — for lidt data til dom (min. 1.500 kr)", "priority": "lav"}); continue
        if p["roas30d"] >= p["breakEvenRoas"] * 1.5 and p["purchases30d"] >= 5: sugg.append({"ad": p["name"], "adId": p["id"], "type": "scale", "reason": f"ROAS {p['roas30d']}× (30 d) er 1,5× over break-even {p['breakEvenRoas']}× ({p['vendor']}, DB {p['dbPct']} %) — skru op", "priority": "medium"}); hi.append(p["name"])
        elif p["roas30d"] < p["breakEvenRoas"] and p["roas7d"] < p["breakEvenRoas"]: sugg.append({"ad": p["name"], "adId": p["id"], "type": "pause", "reason": f"ROAS {p['roas7d']}× (7 d) / {p['roas30d']}× (30 d) under break-even {p['breakEvenRoas']}× for {p['vendor']} (DB {p['dbPct']} %) — overvej pause", "priority": "høj"})
        elif p["roas30d"] > 0 and p["roas7d"] < p["roas30d"] * 0.5: sugg.append({"ad": p["name"], "adId": p["id"], "type": "watch", "reason": f"ROAS faldet fra {p['roas30d']}× (30 d) til {p['roas7d']}× (7 d)", "priority": "medium"})
        if p["ctr30d"] and p["ctr7d"] and p["ctr7d"] < p["ctr30d"] * 0.6: sugg.append({"ad": p["name"], "adId": p["id"], "type": "fatigue", "reason": f"CTR faldet {100 - p['ctr7d'] / p['ctr30d'] * 100:.0f} % (7 d vs 30 d) — kreativ træthed", "priority": "medium"})
    roas_trend = "stigende" if acct["7d"]["roas"] > acct["30d"]["roas"] * 1.1 else ("faldende" if acct["7d"]["roas"] < acct["30d"]["roas"] * 0.9 else "stabil")
    fp_meta7 = fp["7d"].get("Meta", {"orders": 0, "revenue": 0}); fp_meta30 = fp["30d"].get("Meta", {"orders": 0, "revenue": 0})
    report = {"metrics": {"7d": {**acct["7d"]}, "30d": {**acct["30d"]}, "roasTrend": roas_trend},
              "firstParty": {"7d": fp["7d"], "30d": fp["30d"], "metaRoas7d": round(fp_meta7["revenue"] / acct["7d"]["spend"], 2) if acct["7d"]["spend"] else 0, "metaRoas30d": round(fp_meta30["revenue"] / acct["30d"]["spend"], 2) if acct["30d"]["spend"] else 0},
              "dbByVendor": {k: round(v * 100, 1) for k, v in dbmap.items()}, "breakEvenByVendor": {k: round(1 / v, 2) for k, v in dbmap.items()},
              "adPerformance": sorted(perf, key=lambda x: -x["spend30d"]), "suggestions": sugg, "highlights": hi,
              "adsets": [{"id": aid, "name": a["name"], "status": a["effective_status"], "campaign": (a.get("campaign") or {}).get("name"), "spend7d": round(float((adset_ins["last_7d"].get(aid) or {}).get("spend", 0) or 0), 2), "spend30d": round(float((adset_ins["last_30d"].get(aid) or {}).get("spend", 0) or 0), 2), "roas30d": (lambda r: round(purch(r)[1] / float(r["spend"]), 2) if r and float(r.get("spend", 0) or 0) else 0)(adset_ins["last_30d"].get(aid))} for aid, a in adsets.items() if a["effective_status"] == "ACTIVE"],
              "generatedBy": "meta_weekly_report.py (regelbaseret)", "generatedAt": now.isoformat()}
    lines = [f"📊 *Meta Ads — Ugentlig rapport (uge fra {week_start})*", "",
             f"*Konto (Meta-attribueret):* 7d: {acct['7d']['spend']:.0f} kr → {acct['7d']['revenue']:.0f} kr, ROAS *{acct['7d']['roas']}×*, {acct['7d']['purchases']} køb · 30d: {acct['30d']['spend']:.0f} kr → {acct['30d']['revenue']:.0f} kr, ROAS *{acct['30d']['roas']}×*, {acct['30d']['purchases']} køb · trend {roas_trend}",
             f"*Førsteparts (egen tracking):* Meta 7d {fp_meta7['orders']} ordrer / {fp_meta7['revenue']} kr (ROAS {report['firstParty']['metaRoas7d']}×) · 30d {fp_meta30['orders']} ordrer / {fp_meta30['revenue']} kr (ROAS {report['firstParty']['metaRoas30d']}×)",
             f"*Break-even ROAS:* " + " · ".join(f"{k} {v}×" for k, v in report["breakEvenByVendor"].items() if k in ("vidaxl", "kayoom", "sollux", "ayyildiz", "eurofirany")), ""]
    if hi: lines += ["*Top:* " + ", ".join(hi[:4])]
    if sugg: lines += [f"*Forslag ({len(sugg)}):*"] + [f"{'🚀' if s['type']=='scale' else '⏸️' if s['type']=='pause' else '😴' if s['type']=='fatigue' else '👀'} {s['ad'][:60]} — {s['reason']}" for s in sugg[:8]]
    lines += ["", "_Fuld rapport i hubben → /ads → Ugentlig Rapport_"]
    slack = "\n".join(lines); print(slack)
    row = sb_post("meta_weekly_reports", {"week_start": week_start, "report_data": report, "ai_analysis": None, "slack_summary": slack, "usage_tokens": 0, "shop": "boligretning"})
    log(f"gemt i meta_weekly_reports: {row[0]['id'] if isinstance(row, list) and row else row}")
    if SLACK:
        try: urllib.request.urlopen(urllib.request.Request(SLACK, data=json.dumps({"text": slack}).encode(), headers={"Content-Type": "application/json"}), timeout=30); log("Slack sendt")
        except Exception as e: log(f"Slack fejl: {e}")

if __name__ == "__main__":
    try: sys.stdout.reconfigure(encoding="utf-8")
    except Exception: pass
    main()
