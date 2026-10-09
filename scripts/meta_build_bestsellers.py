"""
Meta bestseller-produktsæt – ugentlig genopbygning
===================================================
Opdaterer filteret på de to katalog-produktsæt "Hub-bestsellere – top 300/500" i Meta,
så de altid afspejler hubbens aktuelle rangering (product_performance_ranked.power_score,
kun produkter der er tilgængelige og på lager).

Kører mandag 05:00 UTC via .github/workflows/meta-bestseller-sets.yml (før ugerapporten 06:00)
eller manuelt: python scripts/meta_build_bestsellers.py [--dry-run]

Env: META_ACCESS_TOKEN, SUPABASE_URL, SUPABASE_SERVICE_KEY, (valgfri) SLACK_WEBHOOK_URL,
     META_SET_TOP300 / META_SET_TOP500 (override af produktsæt-id'er).
"""
import os, sys, json, time, datetime, urllib.request, urllib.parse

DRY = "--dry-run" in sys.argv
TOK = os.environ["META_ACCESS_TOKEN"]; V = "v21.0"
SB = os.environ["SUPABASE_URL"].rstrip("/"); SK = os.environ["SUPABASE_SERVICE_KEY"]
SLACK = os.environ.get("SLACK_WEBHOOK_URL")
SETS = {300: os.environ.get("META_SET_TOP300", "1398771251959650"),
        500: os.environ.get("META_SET_TOP500", "28984729487817206")}

def log(*a): print(datetime.datetime.now().strftime("%H:%M:%S"), *a, flush=True)

def fb(path, method="GET", **p):
    p["access_token"] = TOK
    if method == "GET":
        req = urllib.request.Request(f"https://graph.facebook.com/{V}/{path}?" + urllib.parse.urlencode(p))
    else:
        req = urllib.request.Request(f"https://graph.facebook.com/{V}/{path}", data=urllib.parse.urlencode(p).encode(), method="POST")
    for a in range(4):
        try: return json.load(urllib.request.urlopen(req, timeout=180))
        except urllib.error.HTTPError as e:
            b = e.read()[:500].decode("utf-8", "replace")
            if e.code in (429, 500, 503) or "request limit" in b: log("transient", e.code, "– venter"); time.sleep(60 * (a + 1)); continue
            raise RuntimeError(f"Meta {e.code} {path}: {b}")
    raise RuntimeError(f"Meta gav op: {path}")

def sb_get(path):
    req = urllib.request.Request(f"{SB}/rest/v1/{path}", headers={"apikey": SK, "Authorization": f"Bearer {SK}"})
    return json.load(urllib.request.urlopen(req, timeout=180))

# 1) Hub-rangering: tilgængelige produkter på lager, sorteret på power_score
rows = sb_get("product_performance_ranked?select=product_id,vendor,product_type,power_score,order_revenue,order_profit,score_updated_at"
              "&is_available=eq.true&stock_quantity=gt.0&order=power_score.desc.nullslast&limit=500")
ids = [str(r["product_id"]) for r in rows if r.get("product_id")]
if len(ids) < 100:
    raise SystemExit(f"For få produkter i rangeringen ({len(ids)}) – afbryder uden at røre sættene")
fresh = max((r.get("score_updated_at") or "" for r in rows), default="")
if fresh and (datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.datetime.fromisoformat(fresh[:19])).days > 7:
    raise SystemExit(f"Rangeringen er forældet (score_updated_at={fresh}) – afbryder uden at røre sættene")
log(f"hub-rangering: {len(ids)} produkter, score_updated_at={fresh}")

summary = []
for n, set_id in SETS.items():
    sub = ids[:n]
    before = fb(set_id, fields="name,product_count,filter")
    flt = before.get("filter") or {}
    if isinstance(flt, str):
        try: flt = json.loads(flt)
        except Exception: flt = {}
    old = set(str(x) for x in (flt.get("retailer_product_group_id") or {}).get("is_any", []))
    added, removed = len(set(sub) - old), len(old - set(sub))
    log(f"top {n}: {before.get('name')} – før {before.get('product_count')} varianter, {added} nye / {removed} ud")
    if DRY:
        summary.append(f"top {n}: +{added}/−{removed} (dry-run)"); continue
    fb(set_id, "POST", filter=json.dumps({"retailer_product_group_id": {"is_any": sub}}))
    time.sleep(5)
    after = fb(set_id, fields="product_count")
    log(f"top {n}: efter {after.get('product_count')} varianter")
    summary.append(f"top {n}: {after.get('product_count')} varianter (+{added}/−{removed})")

# Fordeling pr. leverandør i top 300 (til Slack/overblik)
vend = {}
for r in rows[:300]:
    vend[r.get("vendor") or "?"] = vend.get(r.get("vendor") or "?", 0) + 1
dist = ", ".join(f"{k} {v}" for k, v in sorted(vend.items(), key=lambda x: -x[1])[:6])
log("top 300 pr. leverandør:", dist)

if SLACK and not DRY:
    text = "📦 *Meta bestseller-sæt opdateret* – " + " · ".join(summary) + f"\nTop 300 pr. leverandør: {dist}"
    try: urllib.request.urlopen(urllib.request.Request(SLACK, data=json.dumps({"text": text}).encode(), headers={"Content-Type": "application/json"}), timeout=30)
    except Exception as e: log("Slack fejl", e)
log("FÆRDIG", summary)
