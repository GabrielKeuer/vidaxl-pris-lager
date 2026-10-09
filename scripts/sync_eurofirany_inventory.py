#!/usr/bin/env python3
"""
Eurofirany Inventory Sync til Shopify (BoligRetning)
Henter Eurofiranys integrationsfeed fullSpecification.csv (nøgle i EUROFIRANY_FEED_KEY) og sætter Shopify-lager for ALLE varianter
med vendor "Eurofirany" ud fra kolonnen "stock", matchet på EAN (variant.barcode = feedets barcode).
- Kun varianter hvis lager AFVIGER opdateres. EAN der ikke findes i feedet sættes til 0.
- Sikkerhedsværn: feedet skal have >= 5.000 rækker, og højst 50 % af varianterne må gå til 0 i én kørsel — ellers afbrydes uden ændringer.
- LAGERGRÆNSE (Omar 9/10): feed-lager < MIN_STOCK (5) sættes til 0 i Shopify – vi bestiller kun hos Eurofirany 1 gang dagligt/hver 2. dag.
- PRISOPDATERING (Omar 9/10): når feedets kost (price_wholesale×7,46+15) afviger fra variantens cost, eller prisen bryder bundprisen,
  regnes salgsprisen om med importens regler: Gardiner = max(149, ceil9(3,33×kost)); Tæpper/Bademåtter = max(299, ceil9(trappe×kost)),
  trappe 3,5× <10 EUR · 3,0× 10-20 · 2,7× 20-40 · 2,5× 40+. Førprisen følger med (samme rabat-%). Cost opdateres samtidig.
  Spring > MAX_PRICE_JUMP (25 %) rettes IKKE – de samles i et GitHub-issue til manuel stillingtagen. Værn: > 20 % af varianterne på én gang = afbryd prisdelen.
- DRY_RUN=true: rapporterer kun. Ingen hemmeligheder i koden (GitHub Secrets).
"""
import os, sys, csv, io, time
from datetime import datetime
import requests

SHOPIFY_STORE_URL = os.environ.get('SHOPIFY_STORE_URL', '').replace('https://', '').rstrip('/')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
FEED_KEY = os.environ.get('EUROFIRANY_FEED_KEY', '')
FEED_URL = f"https://pk.eurofirany.com.pl/api/integrations/fullSpecification/csv?authorizationKey={FEED_KEY}&size=1"
DRY_RUN = os.environ.get('DRY_RUN', '').lower() in ('1', 'true', 'yes')
GRAPHQL_URL = f"https://{SHOPIFY_STORE_URL}/admin/api/2025-07/graphql.json"
MIN_ROWS = 5000
MAX_TO_ZERO_SHARE = 0.5
EUR = 7.46
INDFRAGT = 15.0
MIN_STOCK = int(os.environ.get('EF_MIN_STOCK', '5'))
MAX_PRICE_JUMP = 0.25
MAX_PRICE_SHARE = 0.20
BUND = {'Gardiner': 149, 'Tæpper': 299, 'Bademåtter': 299}
GH_TOKEN = os.environ.get('GITHUB_TOKEN'); GH_REPO = os.environ.get('GITHUB_REPOSITORY')


def ceil9(x):
    p = int(-(-x // 1))
    while p % 10 != 9:
        p += 1
    return p


def faktor(ptype, netto):
    if ptype == 'Gardiner':
        return 3.33
    return 3.5 if netto < 10 else 3.0 if netto < 20 else 2.7 if netto < 40 else 2.5


def ny_pris(ptype, netto):
    if ptype not in BUND:
        return None
    return max(BUND[ptype], ceil9(faktor(ptype, netto) * (netto * EUR + INDFRAGT)))

stats = {'csv_rows': 0, 'variants': 0, 'changed': 0, 'to_zero': 0, 'missing_in_csv': 0, 'updated': 0, 'errors': 0, 'cost_drift': 0, 'buffer_zero': 0, 'price_changes': 0, 'price_flagged': 0, 'price_updated': 0}


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def download_csv():
    last = None
    for attempt in range(1, 4):
        try:
            r = requests.get(FEED_URL, timeout=120)
            r.raise_for_status()
            text = r.content.decode('utf-8-sig')
            log(f"📥 Hentet Eurofirany-feed ({len(text)} bytes, forsøg {attempt}/3)")
            return text
        except Exception as e:
            last = e
            if attempt < 3:
                log(f"⚠️  Forsøg {attempt}/3 fejlede ({type(e).__name__}) – prøver igen om {60*attempt}s")
                time.sleep(60 * attempt)
    log(f"❌ Kunne ikke hente feedet: {last}")
    sys.exit(1)


def parse_csv(text):
    reader = csv.DictReader(io.StringIO(text), delimiter=';')
    stock, cost = {}, {}
    for row in reader:
        ean = (row.get('barcode') or '').strip()
        if not ean:
            continue
        try:
            qty = int(float((row.get('stock') or '0').strip() or 0))
        except ValueError:
            qty = 0
        stock[ean] = max(0, qty)
        try:
            cost[ean] = float((row.get('price_wholesale') or '0').replace(',', '.'))
        except ValueError:
            pass
    stats['csv_rows'] = len(stock)
    log(f"📋 {len(stock)} EAN i feedet")
    if len(stock) < MIN_ROWS:
        log(f"❌ Feedet har kun {len(stock)} rækker (< {MIN_ROWS}) – ligner en ødelagt fil. Afbryder uden ændringer.")
        sys.exit(1)
    return stock, cost


def gql(query, variables=None):
    r = requests.post(GRAPHQL_URL, json={'query': query, 'variables': variables or {}},
                      headers={'Content-Type': 'application/json', 'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN}, timeout=60)
    r.raise_for_status()
    d = r.json()
    if 'errors' in d:
        raise RuntimeError(f"GraphQL errors: {d['errors']}")
    return d['data']


def fetch_variants():
    q = """query($a:String){ productVariants(first:250, query:"vendor:Eurofirany", after:$a){
      nodes{ id sku barcode price compareAtPrice product{ id handle productType } inventoryQuantity inventoryItem{ id unitCost{ amount } inventoryLevels(first:1){ nodes{ location{ id } } } } }
      pageInfo{ hasNextPage endCursor } } }"""
    out, after = [], None
    while True:
        d = gql(q, {'a': after})
        out.extend(d['productVariants']['nodes'])
        pi = d['productVariants']['pageInfo']
        if not pi['hasNextPage']:
            break
        after = pi['endCursor']
    stats['variants'] = len(out)
    log(f"🔍 {len(out)} Eurofirany-varianter i Shopify")
    return out


def main():
    log("🚀 Eurofirany → Shopify lager-sync" + (" (DRY RUN)" if DRY_RUN else ""))
    if not SHOPIFY_STORE_URL or not SHOPIFY_ACCESS_TOKEN or not FEED_KEY:
        log("❌ Mangler SHOPIFY_STORE_URL / SHOPIFY_ACCESS_TOKEN / EUROFIRANY_FEED_KEY i miljøet")
        sys.exit(1)
    stock, cost = parse_csv(download_csv())
    variants = fetch_variants()
    if not variants:
        log("ℹ️  Ingen Eurofirany-varianter – intet at gøre")
        return
    updates, location_id = [], None
    price_changes, flagged = [], []
    for v in variants:
        levels = v['inventoryItem']['inventoryLevels']['nodes']
        if levels and not location_id:
            location_id = levels[0]['location']['id']
        ean = (v.get('barcode') or '').strip()
        ny = stock.get(ean)
        if ny is None:
            stats['missing_in_csv'] += 1
            ny = 0
        if 0 < ny < MIN_STOCK:
            stats['buffer_zero'] += 1
            ny = 0
        if ny != (v['inventoryQuantity'] or 0):
            updates.append({'inventoryItemId': v['inventoryItem']['id'], 'quantity': ny, 'sku': v['sku'], 'fra': v['inventoryQuantity']})
            if ny == 0:
                stats['to_zero'] += 1
        # --- pris ---
        c = cost.get(ean)
        uc = v['inventoryItem'].get('unitCost') or {}
        ptype = (v.get('product') or {}).get('productType')
        if ptype not in BUND:
            continue
        if c is None or c <= 0:
            # ikke i feedet: kun bundprisen håndhæves (kost urørt)
            gl_pris = float(v['price'])
            if gl_pris < BUND[ptype]:
                cap = float(v['compareAtPrice']) if v.get('compareAtPrice') else None
                gk = float(uc['amount']) if uc.get('amount') is not None else 0
                price_changes.append({'productId': v['product']['id'], 'id': v['id'], 'sku': v['sku'], 'handle': v['product']['handle'], 'type': ptype,
                                      'fra': gl_pris, 'til': BUND[ptype], 'cap': ceil9(BUND[ptype] / (gl_pris / cap)) if cap and cap > gl_pris else None,
                                      'kost_fra': gk, 'kost': gk})
            continue
        kost = round(c * EUR + INDFRAGT, 2)
        gl_kost = float(uc['amount']) if uc.get('amount') is not None else None
        gl_pris = float(v['price'])
        drift = gl_kost is None or abs(gl_kost - kost) > 1.0
        if drift:
            stats['cost_drift'] += 1
        np_ = ny_pris(ptype, c)
        if not drift and gl_pris >= BUND[ptype]:
            continue
        if np_ == gl_pris and not drift:
            continue
        cap = float(v['compareAtPrice']) if v.get('compareAtPrice') else None
        ny_cap = ceil9(np_ / (gl_pris / cap)) if cap and cap > gl_pris else None
        ch = {'productId': v['product']['id'], 'id': v['id'], 'sku': v['sku'], 'handle': v['product']['handle'], 'type': ptype,
              'fra': gl_pris, 'til': np_, 'cap': ny_cap, 'kost_fra': gl_kost, 'kost': kost}
        spring = abs(np_ - gl_pris) / gl_pris if gl_pris else 1
        bundloeft = gl_pris < BUND[ptype] and np_ == BUND[ptype]
        if spring > MAX_PRICE_JUMP and not bundloeft:
            stats['price_flagged'] += 1
            flagged.append(ch)
        elif np_ != gl_pris or drift:
            price_changes.append(ch)
    stats['changed'] = len(updates)
    stats['price_changes'] = len(price_changes)
    log(f"📊 {len(variants)} varianter · {len(updates)} lager afviger · {stats['to_zero']} går til 0 (heraf {stats['buffer_zero']} pga. lager < {MIN_STOCK}) · {stats['missing_in_csv']} EAN ikke i feedet")
    log(f"💶 {stats['cost_drift']} kost-afvigelser · {len(price_changes)} prisændringer · {len(flagged)} flaget (> {int(MAX_PRICE_JUMP*100)} %)")
    for ch in price_changes[:25]:
        log(f"   💶 {ch['type']} {ch['handle']} {ch['sku']}: {ch['fra']:.0f} → {ch['til']} kr (kost {ch['kost_fra']} → {ch['kost']}, før {ch['cap']})")
    for ch in flagged:
        log(f"   🚩 FLAGET {ch['type']} {ch['handle']} {ch['sku']}: {ch['fra']:.0f} → {ch['til']} kr (kost {ch['kost_fra']} → {ch['kost']}) – rettes ikke automatisk")
    if variants and stats['to_zero'] / len(variants) > MAX_TO_ZERO_SHARE:
        log(f"❌ {stats['to_zero']} af {len(variants)} varianter ville gå til 0 (> {int(MAX_TO_ZERO_SHARE*100)} %). Afbryder uden ændringer.")
        sys.exit(1)
    if DRY_RUN:
        log("✅ DRY RUN – ingen ændringer skrevet")
        return
    opdater_priser(price_changes, flagged, len(variants))
    if not updates:
        log("✅ Lager: ingen ændringer")
        return
    if not location_id:
        log("❌ Kunne ikke finde lokation via inventoryLevels")
        sys.exit(1)
    m = """mutation($input: InventorySetQuantitiesInput!){ inventorySetQuantities(input:$input){ userErrors{ field message } } }"""
    for i in range(0, len(updates), 100):
        chunk = updates[i:i+100]
        try:
            d = gql(m, {'input': {'name': 'available', 'reason': 'correction', 'ignoreCompareQuantity': True,
                                  'quantities': [{'inventoryItemId': u['inventoryItemId'], 'locationId': location_id, 'quantity': u['quantity']} for u in chunk]}})
            errs = d['inventorySetQuantities']['userErrors']
            if errs:
                stats['errors'] += len(chunk)
                log(f"   ❌ userErrors: {errs[:3]}")
            else:
                stats['updated'] += len(chunk)
        except Exception as e:
            stats['errors'] += len(chunk)
            log(f"   ❌ {e}")
        time.sleep(0.5)
    log(f"✅ Lager færdig: {stats['updated']} opdateret · {stats['errors']} fejl")
    if stats['errors']:
        sys.exit(1)


def opdater_priser(price_changes, flagged, n):
    if flagged:
        rapporter_flaget(flagged)
    if not price_changes:
        return
    if len(price_changes) > MAX_PRICE_SHARE * n and len(price_changes) > 50:
        log(f"❌ {len(price_changes)} prisændringer (> {int(MAX_PRICE_SHARE*100)} % af {n}) – ligner fejl i feedet. Springer prisdelen over.")
        return
    pr = {}
    for ch in price_changes:
        pr.setdefault(ch['productId'], []).append(ch)
    m = """mutation($p:ID!,$v:[ProductVariantsBulkInput!]!){ productVariantsBulkUpdate(productId:$p, variants:$v){ userErrors{ field message } } }"""
    for pid, chs in pr.items():
        vs = []
        for ch in chs:
            x = {'id': ch['id'], 'price': f"{ch['til']:.2f}", 'inventoryItem': {'cost': f"{ch['kost']:.2f}"}}
            if ch['cap']:
                x['compareAtPrice'] = f"{ch['cap']:.2f}"
            vs.append(x)
        try:
            d = gql(m, {'p': pid, 'v': vs})
            errs = d['productVariantsBulkUpdate']['userErrors']
            if errs:
                stats['errors'] += len(vs)
                log(f"   ❌ pris userErrors {chs[0]['handle']}: {errs[:2]}")
            else:
                stats['price_updated'] += len(vs)
        except Exception as e:
            stats['errors'] += len(vs)
            log(f"   ❌ pris {chs[0]['handle']}: {e}")
        time.sleep(0.3)
    log(f"✅ Priser: {stats['price_updated']} varianter opdateret")


def rapporter_flaget(flagged):
    if not GH_TOKEN or not GH_REPO:
        return
    titel = '🚩 Eurofirany: prisspring over 25 % kræver stillingtagen'
    krop = 'Kost i feedet er ændret så meget, at ny salgspris ville springe > 25 %. Rettes IKKE automatisk.\n\n| Type | Produkt | SKU | Pris nu | Ny pris | Kost før → nu |\n|---|---|---|---|---|---|\n' + '\n'.join(
        f"| {c['type']} | {c['handle']} | {c['sku']} | {c['fra']:.0f} | {c['til']} | {c['kost_fra']} → {c['kost']} |" for c in flagged) + f"\n\nOpdateret {datetime.now().isoformat(timespec='minutes')}"
    h = {'Authorization': f'Bearer {GH_TOKEN}', 'Accept': 'application/vnd.github+json'}
    try:
        r = requests.get(f'https://api.github.com/repos/{GH_REPO}/issues', params={'state': 'open', 'labels': 'eurofirany-pris'}, headers=h, timeout=30)
        eks = [i for i in r.json() if i.get('title') == titel]
        if eks:
            requests.patch(eks[0]['url'], json={'body': krop}, headers=h, timeout=30)
        else:
            requests.post(f'https://api.github.com/repos/{GH_REPO}/issues', json={'title': titel, 'body': krop, 'labels': ['eurofirany-pris']}, headers=h, timeout=30)
        log(f"   🚩 {len(flagged)} flagede priser skrevet til GitHub-issue")
    except Exception as e:
        log(f"   ⚠️ kunne ikke skrive issue: {e}")


if __name__ == '__main__':
    main()

