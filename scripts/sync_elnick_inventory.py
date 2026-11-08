#!/usr/bin/env python3
"""
Elnick Inventory Sync til Shopify (BoligRetning)
Henter aktuelt lager fra Marlotte (GET /api/b2b/v1/stock, nøgle i secret ELNICK_API_KEY) og sætter Shopify-lager for ALLE varianter
med vendor "Elnick", matchet på SKU. Opdaterer kun lager og kun varianter, hvis lager afviger. Rører ikke produktstatus eller priser.
- LAGERGRÆNSE (Omar 8/11-2026): Marlotte-lager under MIN_STOCK (3) sættes til 0 i Shopify.
- SKU der ikke findes hos Marlotte sættes til 0.
- Sikkerhedsværn: svaret skal have mindst 100 SKU'er, og højst 50 % af varianterne må gå til 0 i én kørsel. Ellers afbrydes uden ændringer.
- DRY_RUN=true: rapporterer kun. Ingen hemmeligheder i koden.
"""
import os, sys, time
from datetime import datetime
import requests

SHOPIFY_STORE_URL = os.environ.get('SHOPIFY_STORE_URL', '').replace('https://', '').rstrip('/')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
ELNICK_API_KEY = os.environ.get('ELNICK_API_KEY', '')
STOCK_URL = 'https://mes.marlotte.pl/api/b2b/v1/stock'
GRAPHQL_URL = f"https://{SHOPIFY_STORE_URL}/admin/api/2025-07/graphql.json"
DRY_RUN = os.environ.get('DRY_RUN', '').lower() in ('1', 'true', 'yes')
MIN_STOCK = int(os.environ.get('ELNICK_MIN_STOCK', '3'))
MIN_ROWS = 100
MAX_TO_ZERO_SHARE = 0.5
BATCH = 100


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def hent_lager():
    last = None
    for forsog in range(1, 4):
        try:
            r = requests.get(STOCK_URL, headers={'Authorization': f'Bearer {ELNICK_API_KEY}'}, timeout=90)
            r.raise_for_status()
            d = r.json()
            items = d.get('items') or []
            log(f"📥 Hentet Marlotte-lager: {len(items)} SKU'er, tid {d.get('generated_at_iso')}")
            if len(items) < MIN_ROWS:
                log(f"❌ Kun {len(items)} SKU'er (< {MIN_ROWS}). Afbryder uden ændringer.")
                sys.exit(1)
            return {i['sku']: max(0, int(i.get('qty') or 0)) for i in items if i.get('sku')}
        except (requests.RequestException, ValueError) as e:
            last = e
            if forsog < 3:
                log(f"⚠️  Forsøg {forsog}/3 fejlede ({type(e).__name__}), prøver igen om {60 * forsog}s")
                time.sleep(60 * forsog)
    log(f"❌ Kunne ikke hente lager: {type(last).__name__}")
    sys.exit(1)


def gql(query, variables=None):
    for forsog in range(8):
        r = requests.post(GRAPHQL_URL, json={'query': query, 'variables': variables or {}},
                          headers={'Content-Type': 'application/json', 'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN}, timeout=60)
        r.raise_for_status()
        d = r.json()
        fejl = d.get('errors')
        if fejl and any((e.get('extensions') or {}).get('code') == 'THROTTLED' for e in fejl):
            time.sleep(2.5)
            continue
        if fejl:
            raise RuntimeError(f"GraphQL errors: {fejl}")
        return d['data']
    raise RuntimeError('throttled')


def hent_varianter():
    q = """query($a:String){ productVariants(first:250, query:"vendor:Elnick", after:$a){
      nodes{ sku inventoryQuantity product{ title vendor status } inventoryItem{ id inventoryLevels(first:2){ nodes{ location{ id } } } } }
      pageInfo{ hasNextPage endCursor } } }"""
    ud, efter = [], None
    while True:
        d = gql(q, {'a': efter})['productVariants']
        ud.extend(v for v in d['nodes'] if v['product']['vendor'] == 'Elnick')
        if not d['pageInfo']['hasNextPage']:
            break
        efter = d['pageInfo']['endCursor']
    log(f"🔍 {len(ud)} Elnick-varianter i Shopify")
    return ud


def main():
    if not (SHOPIFY_STORE_URL and SHOPIFY_ACCESS_TOKEN and ELNICK_API_KEY):
        log('❌ Mangler SHOPIFY_STORE_URL, SHOPIFY_ACCESS_TOKEN eller ELNICK_API_KEY.')
        sys.exit(1)
    lager = hent_lager()
    varianter = hent_varianter()
    if not varianter:
        log('Ingen varianter. Afbryder.')
        return
    aendr, mangler = [], 0
    for v in varianter:
        q = lager.get(v['sku'])
        if q is None:
            mangler += 1
        ny = q if q is not None and q >= MIN_STOCK else 0
        if ny != v['inventoryQuantity']:
            niv = v['inventoryItem']['inventoryLevels']['nodes']
            if len(niv) != 1:
                log(f"⚠️  {v['sku']}: {len(niv)} lokationer, springes over")
                continue
            aendr.append({'sku': v['sku'], 'titel': v['product']['title'], 'status': v['product']['status'], 'foer': v['inventoryQuantity'], 'ny': ny, 'item': v['inventoryItem']['id'], 'loc': niv[0]['location']['id']})
    nul = sum(1 for a in aendr if a['ny'] == 0)
    log(f"📊 {len(aendr)} ændres, {nul} går til 0, {mangler} SKU'er findes ikke hos Marlotte")
    for a in aendr[:200]:
        if a['ny'] == 0:
            log(f"   UDSOLGT {a['titel']} ({a['sku']}, {a['status']}): {a['foer']} → 0")
    if nul > len(varianter) * MAX_TO_ZERO_SHARE:
        log(f"❌ {nul} af {len(varianter)} ville gå til 0 (> {int(MAX_TO_ZERO_SHARE * 100)} %). Afbryder uden ændringer.")
        sys.exit(1)
    if DRY_RUN:
        log('🧪 DRY_RUN: intet skrevet.')
        return
    m = """mutation($input:InventorySetQuantitiesInput!){ inventorySetQuantities(input:$input){ userErrors{ field message } } }"""
    for i in range(0, len(aendr), BATCH):
        del_ = aendr[i:i + BATCH]
        d = gql(m, {'input': {'name': 'available', 'reason': 'correction', 'ignoreCompareQuantity': True,
                              'quantities': [{'inventoryItemId': a['item'], 'locationId': a['loc'], 'quantity': a['ny']} for a in del_]}})
        fejl = d['inventorySetQuantities']['userErrors']
        if fejl:
            log(f"❌ Shopify-fejl: {fejl}")
            sys.exit(1)
        time.sleep(0.5)
    log(f"✅ {len(aendr)} varianter opdateret")


if __name__ == '__main__':
    main()
