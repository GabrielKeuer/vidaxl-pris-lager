#!/usr/bin/env python3
"""
Ayyildiz Price Sync (ugentlig – Omar 9/10: 'Måske bare en ugentlig tjek')
Henter Ayyildiz' offentlige artikelliste (2026-Artikelliste-DE.xlsx; kolonne 'Stückpreis Netto' = netto EUR, 'EAN-Nr.' = barcode) og
sammenligner med Shopify-varianter (vendor Ayyildiz). Ændres kost (netto+12,50 EUR fragt)×7,46 med > 1 kr, eller er prisen under bund,
regnes pris om med importens regel: max(299, ceil9(2,5 × (netto + 12,50) × 7,46)); cost opdateres, førpris følger samme rabat-%.
- Spring > 25 % rettes IKKE – samles i ét GitHub-issue (label ayyildiz-pris). Værn: > 20 % af varianterne på én gang = afbryd.
- Rører ALDRIG lager (ejes af ayyildiz-inventory-sync). DRY_RUN=true: kun rapport.
"""
import os, sys, io, time
from datetime import datetime
import requests, openpyxl

SHOPIFY_STORE_URL = os.environ.get('SHOPIFY_STORE_URL', '').replace('https://', '').rstrip('/')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
XLSX_URL = os.environ.get('AYYILDIZ_XLSX_URL', 'https://ayyildizhali.de/katalog/2026-Artikelliste-DE.xlsx')
DRY_RUN = os.environ.get('DRY_RUN', '').lower() in ('1', 'true', 'yes')
GRAPHQL_URL = f"https://{SHOPIFY_STORE_URL}/admin/api/2025-07/graphql.json"
GH_TOKEN = os.environ.get('GITHUB_TOKEN'); GH_REPO = os.environ.get('GITHUB_REPOSITORY')
KURS, FRAGT_EUR, MARKUP, BUND = 7.46, 12.5, 2.5, 299
MAX_PRICE_JUMP, MAX_PRICE_SHARE, MIN_ROWS = 0.25, 0.20, 3000
stats = {'rows': 0, 'variants': 0, 'ikke_i_liste': 0, 'cost_drift': 0, 'changes': 0, 'flagged': 0, 'updated': 0, 'errors': 0}


def log(m): print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {m}", flush=True)


def ceil9(x):
    p = int(-(-x // 1))
    while p % 10 != 9: p += 1
    return p


def ny_pris(netto): return max(BUND, ceil9(MARKUP * (netto + FRAGT_EUR) * KURS))


def gql(q, v=None):
    r = requests.post(GRAPHQL_URL, json={'query': q, 'variables': v or {}}, headers={'Content-Type': 'application/json', 'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN}, timeout=60)
    r.raise_for_status(); d = r.json()
    if 'errors' in d: raise RuntimeError(d['errors'])
    return d['data']


def hent_liste():
    r = requests.get(XLSX_URL, timeout=180); r.raise_for_status()
    log(f"📥 Artikelliste hentet ({len(r.content)} bytes, Last-Modified: {r.headers.get('Last-Modified')})")
    ws = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True, data_only=True).worksheets[0]
    rows = ws.iter_rows(values_only=True); head = [str(h or '').strip() for h in next(rows)]
    i_ean, i_net = head.index('EAN-Nr.'), head.index('Stückpreis Netto')
    netto = {}
    for row in rows:
        ean = str(row[i_ean] or '').strip().split('.')[0]
        try: n = float(row[i_net])
        except (TypeError, ValueError): continue
        if ean and n > 0: netto[ean] = n
    stats['rows'] = len(netto); log(f"📋 {len(netto)} EAN med nettopris")
    if len(netto) < MIN_ROWS: log("❌ For få rækker – ligner en ødelagt fil. Afbryder."); sys.exit(1)
    return netto


def hent_varianter():
    q = """query($a:String){ productVariants(first:250, query:"vendor:Ayyildiz", after:$a){ nodes{ id sku barcode price compareAtPrice product{ id handle } inventoryItem{ unitCost{ amount } } } pageInfo{ hasNextPage endCursor } } }"""
    out, a = [], None
    while True:
        d = gql(q, {'a': a}); out += d['productVariants']['nodes']; pi = d['productVariants']['pageInfo']
        if not pi['hasNextPage']: break
        a = pi['endCursor']
    stats['variants'] = len(out); log(f"🔍 {len(out)} Ayyildiz-varianter i Shopify"); return out


def main():
    log("🚀 Ayyildiz pris-tjek" + (" (DRY RUN)" if DRY_RUN else ""))
    if not SHOPIFY_STORE_URL or not SHOPIFY_ACCESS_TOKEN: log("❌ Mangler Shopify-miljø"); sys.exit(1)
    netto, vs = hent_liste(), hent_varianter()
    changes, flagged = [], []
    for v in vs:
        ean = (v.get('barcode') or '').strip(); n = netto.get(ean); gl = float(v['price'])
        uc = (v['inventoryItem'].get('unitCost') or {}).get('amount'); gk = float(uc) if uc is not None else None
        cap = float(v['compareAtPrice']) if v.get('compareAtPrice') else None
        if n is None:
            stats['ikke_i_liste'] += 1
            if gl < BUND: changes.append({'productId': v['product']['id'], 'id': v['id'], 'sku': v['sku'], 'handle': v['product']['handle'], 'fra': gl, 'til': BUND, 'cap': ceil9(BUND / (gl / cap)) if cap and cap > gl else None, 'kost_fra': gk, 'kost': gk})
            continue
        kost = round((n + FRAGT_EUR) * KURS, 2); drift = gk is None or abs(gk - kost) > 1.0
        if drift: stats['cost_drift'] += 1
        if not drift and gl >= BUND: continue
        np_ = ny_pris(n)
        ch = {'productId': v['product']['id'], 'id': v['id'], 'sku': v['sku'], 'handle': v['product']['handle'], 'fra': gl, 'til': np_,
              'cap': ceil9(np_ / (gl / cap)) if cap and cap > gl else None, 'kost_fra': gk, 'kost': kost}
        if gl and abs(np_ - gl) / gl > MAX_PRICE_JUMP and not (gl < BUND and np_ == BUND): flagged.append(ch)
        else: changes.append(ch)
    stats['changes'], stats['flagged'] = len(changes), len(flagged)
    log(f"💶 {len(vs)} varianter · {stats['ikke_i_liste']} ikke i listen · {stats['cost_drift']} kost-afvigelser · {len(changes)} prisændringer · {len(flagged)} flaget (> 25 %)")
    for c in changes[:30]: log(f"   💶 {c['handle']} {c['sku']}: {c['fra']:.0f} → {c['til']} kr (kost {c['kost_fra']} → {c['kost']}, før {c['cap']})")
    for c in flagged[:30]: log(f"   🚩 FLAGET {c['handle']} {c['sku']}: {c['fra']:.0f} → {c['til']} kr (kost {c['kost_fra']} → {c['kost']})")
    if DRY_RUN: log("✅ DRY RUN – intet skrevet"); return
    if flagged: issue(flagged)
    if len(changes) > MAX_PRICE_SHARE * len(vs) and len(changes) > 50: log(f"❌ {len(changes)} ændringer > 20 % – springer over."); return
    pr = {}
    for c in changes: pr.setdefault(c['productId'], []).append(c)
    m = """mutation($p:ID!,$v:[ProductVariantsBulkInput!]!){ productVariantsBulkUpdate(productId:$p, variants:$v){ userErrors{ field message } } }"""
    for pid, cs in pr.items():
        var = []
        for c in cs:
            x = {'id': c['id'], 'price': f"{c['til']:.2f}"}
            if c['kost'] is not None: x['inventoryItem'] = {'cost': f"{c['kost']:.2f}"}
            if c['cap']: x['compareAtPrice'] = f"{c['cap']:.2f}"
            var.append(x)
        try:
            e = gql(m, {'p': pid, 'v': var})['productVariantsBulkUpdate']['userErrors']
            if e: stats['errors'] += len(var); log(f"   ❌ {cs[0]['handle']}: {e[:2]}")
            else: stats['updated'] += len(var)
        except Exception as ex: stats['errors'] += len(var); log(f"   ❌ {cs[0]['handle']}: {ex}")
        time.sleep(0.3)
    log(f"✅ Færdig: {stats['updated']} priser opdateret · {stats['errors']} fejl")
    if stats['errors']: sys.exit(1)


def issue(flagged):
    if not GH_TOKEN or not GH_REPO: return
    t = '🚩 Ayyildiz: prisspring over 25 % kræver stillingtagen'
    b = 'Ny nettopris i artikellisten giver > 25 % prisspring. Rettes IKKE automatisk.\n\n| Produkt | SKU | Pris nu | Ny pris | Kost før → nu |\n|---|---|---|---|---|\n' + '\n'.join(f"| {c['handle']} | {c['sku']} | {c['fra']:.0f} | {c['til']} | {c['kost_fra']} → {c['kost']} |" for c in flagged[:200]) + f"\n\nOpdateret {datetime.now().isoformat(timespec='minutes')}"
    h = {'Authorization': f'Bearer {GH_TOKEN}', 'Accept': 'application/vnd.github+json'}
    try:
        eks = [i for i in requests.get(f'https://api.github.com/repos/{GH_REPO}/issues', params={'state': 'open', 'labels': 'ayyildiz-pris'}, headers=h, timeout=30).json() if i.get('title') == t]
        if eks: requests.patch(eks[0]['url'], json={'body': b}, headers=h, timeout=30)
        else: requests.post(f'https://api.github.com/repos/{GH_REPO}/issues', json={'title': t, 'body': b, 'labels': ['ayyildiz-pris']}, headers=h, timeout=30)
        log(f"   🚩 {len(flagged)} flagede priser skrevet til GitHub-issue")
    except Exception as ex: log(f"   ⚠️ issue fejlede: {ex}")


if __name__ == '__main__': main()
