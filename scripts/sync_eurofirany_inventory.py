#!/usr/bin/env python3
"""
Eurofirany Inventory Sync til Shopify (BoligRetning)
Henter Eurofiranys integrationsfeed fullSpecification.csv (nøgle i EUROFIRANY_FEED_KEY) og sætter Shopify-lager for ALLE varianter
med vendor "Eurofirany" ud fra kolonnen "stock", matchet på EAN (variant.barcode = feedets barcode).
- Kun varianter hvis lager AFVIGER opdateres. EAN der ikke findes i feedet sættes til 0.
- Sikkerhedsværn: feedet skal have >= 5.000 rækker, og højst 50 % af varianterne må gå til 0 i én kørsel — ellers afbrydes uden ændringer.
- Rapporterer desuden KOSTPRIS-afvigelser (feedets price_wholesale×7,46+15 vs. variantens cost) uden at ændre dem (prisregel håndteres i hubben).
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

stats = {'csv_rows': 0, 'variants': 0, 'changed': 0, 'to_zero': 0, 'missing_in_csv': 0, 'updated': 0, 'errors': 0, 'cost_drift': 0}


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
      nodes{ id sku barcode inventoryQuantity inventoryItem{ id unitCost{ amount } inventoryLevels(first:1){ nodes{ location{ id } } } } }
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
    for v in variants:
        levels = v['inventoryItem']['inventoryLevels']['nodes']
        if levels and not location_id:
            location_id = levels[0]['location']['id']
        ean = (v.get('barcode') or '').strip()
        ny = stock.get(ean)
        if ny is None:
            stats['missing_in_csv'] += 1
            ny = 0
        if ny != (v['inventoryQuantity'] or 0):
            updates.append({'inventoryItemId': v['inventoryItem']['id'], 'quantity': ny, 'sku': v['sku'], 'fra': v['inventoryQuantity']})
            if ny == 0:
                stats['to_zero'] += 1
        c = cost.get(ean)
        uc = v['inventoryItem'].get('unitCost') or {}
        if c is not None and uc.get('amount') is not None:
            forventet = round(c * EUR + INDFRAGT, 2)
            if abs(float(uc['amount']) - forventet) > 1.0:
                stats['cost_drift'] += 1
                if stats['cost_drift'] <= 10:
                    log(f"   💶 kost afviger {v['sku']}: Shopify {uc['amount']} vs feed {forventet}")
    stats['changed'] = len(updates)
    log(f"📊 {len(variants)} varianter · {len(updates)} afviger · {stats['to_zero']} går til 0 · {stats['missing_in_csv']} EAN ikke i feedet · {stats['cost_drift']} kost-afvigelser")
    if variants and stats['to_zero'] / len(variants) > MAX_TO_ZERO_SHARE:
        log(f"❌ {stats['to_zero']} af {len(variants)} varianter ville gå til 0 (> {int(MAX_TO_ZERO_SHARE*100)} %). Afbryder uden ændringer.")
        sys.exit(1)
    if DRY_RUN or not updates:
        log("✅ Færdig (ingen ændringer)")
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
    log(f"✅ Færdig: {stats['updated']} opdateret · {stats['errors']} fejl")
    if stats['errors']:
        sys.exit(1)


if __name__ == '__main__':
    main()
