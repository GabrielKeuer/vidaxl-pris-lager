#!/usr/bin/env python3
"""
Ayyildiz Inventory Sync til Shopify (BoligRetning)
Henter den daglige lagerfil https://www.ayyildizhali.de/bestand/Bestand_aktuell.csv (offentlig URL, semikolon,
kolonner item_no;item_no_long;EAN1;...;stock;reserved;available;...) og sætter Shopify-lager for ALLE varianter
med vendor "Ayyildiz" ud fra kolonnen "available" (= stock - reserved), matchet på EAN (variant.barcode).
- Kun varianter hvis lager AFVIGER opdateres (sparer API-kald).
- EAN der ikke findes i filen sættes til 0 (udgået/ikke på lager).
- Sikkerhedsværn: filen skal have >= 5.000 rækker, og højst 50 % af varianterne må gå til 0 i én kørsel — ellers afbrydes uden ændringer.
- DRY_RUN=true: rapporterer kun.
Ingen hemmeligheder i koden: SHOPIFY_STORE_URL / SHOPIFY_ACCESS_TOKEN kommer fra miljøet (GitHub Secrets).
"""
import os, sys, csv, io, time
from datetime import datetime
import requests

SHOPIFY_STORE_URL = os.environ.get('SHOPIFY_STORE_URL', '').replace('https://', '').rstrip('/')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
CSV_URL = os.environ.get('AYYILDIZ_BESTAND_URL', 'https://www.ayyildizhali.de/bestand/Bestand_aktuell.csv')
DRY_RUN = os.environ.get('DRY_RUN', '').lower() in ('1', 'true', 'yes')
GRAPHQL_URL = f"https://{SHOPIFY_STORE_URL}/admin/api/2025-07/graphql.json"
MIN_ROWS = 5000
MAX_TO_ZERO_SHARE = 0.5

stats = {'csv_rows': 0, 'variants': 0, 'changed': 0, 'to_zero': 0, 'missing_in_csv': 0, 'updated': 0, 'errors': 0}


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def download_csv():
    last = None
    for attempt in range(1, 4):
        try:
            r = requests.get(CSV_URL, timeout=60)
            r.raise_for_status()
            text = r.content.decode('utf-8-sig')
            log(f"📥 Hentet {CSV_URL} ({len(text)} bytes, forsøg {attempt}/3)")
            return text
        except Exception as e:
            last = e
            if attempt < 3:
                log(f"⚠️  Forsøg {attempt}/3 fejlede ({type(e).__name__}) – prøver igen om {60*attempt}s")
                time.sleep(60 * attempt)
    log(f"❌ Kunne ikke hente lagerfilen: {last}")
    sys.exit(1)


def parse_csv(text):
    reader = csv.DictReader(io.StringIO(text), delimiter=';')
    stock = {}
    for row in reader:
        ean = (row.get('EAN1') or '').strip()
        if not ean:
            continue
        try:
            qty = int((row.get('available') or '0').strip())
        except ValueError:
            qty = 0
        stock[ean] = max(0, qty)
    stats['csv_rows'] = len(stock)
    log(f"📋 {len(stock)} EAN med lager i filen")
    if len(stock) < MIN_ROWS:
        log(f"❌ Filen har kun {len(stock)} rækker (< {MIN_ROWS}) – ligner en ødelagt fil. Afbryder uden ændringer.")
        sys.exit(1)
    return stock


def gql(query, variables=None):
    r = requests.post(GRAPHQL_URL, json={'query': query, 'variables': variables or {}},
                      headers={'Content-Type': 'application/json', 'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN}, timeout=60)
    r.raise_for_status()
    d = r.json()
    if 'errors' in d:
        raise RuntimeError(f"GraphQL errors: {d['errors']}")
    return d['data']


def fetch_variants():
    """Alle varianter med vendor Ayyildiz: barcode, nuværende lager, inventoryItem + location (fra inventoryLevels, kræver ikke read_locations)."""
    q = """query($a:String){ productVariants(first:250, query:"vendor:Ayyildiz", after:$a){
      nodes{ id sku barcode inventoryQuantity inventoryItem{ id inventoryLevels(first:1){ nodes{ location{ id } } } } }
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
    log(f"🔍 {len(out)} Ayyildiz-varianter i Shopify")
    return out


def main():
    log("🚀 Ayyildiz → Shopify lager-sync" + (" (DRY RUN)" if DRY_RUN else ""))
    if not SHOPIFY_STORE_URL or not SHOPIFY_ACCESS_TOKEN:
        log("❌ Mangler SHOPIFY_STORE_URL / SHOPIFY_ACCESS_TOKEN i miljøet")
        sys.exit(1)
    stock = parse_csv(download_csv())
    variants = fetch_variants()
    if not variants:
        log("ℹ️  Ingen Ayyildiz-varianter – intet at gøre")
        return
    updates, location_id = [], None
    for v in variants:
        levels = v['inventoryItem']['inventoryLevels']['nodes']
        if levels and not location_id:
            location_id = levels[0]['location']['id']
        ny = stock.get((v.get('barcode') or '').strip())
        if ny is None:
            stats['missing_in_csv'] += 1
            ny = 0
        if ny != (v['inventoryQuantity'] or 0):
            updates.append({'inventoryItemId': v['inventoryItem']['id'], 'locationId': None, 'quantity': ny, 'sku': v['sku'], 'fra': v['inventoryQuantity']})
            if ny == 0:
                stats['to_zero'] += 1
    stats['changed'] = len(updates)
    log(f"📊 Afvigelser: {len(updates)} af {len(variants)} (til 0: {stats['to_zero']}, EAN ikke i fil: {stats['missing_in_csv']})")
    for u in updates[:8]:
        log(f"   {u['sku']}: {u['fra']} → {u['quantity']}")
    if not updates:
        log("✅ Alt lager er allerede ajour")
        return
    if not location_id:
        log("❌ Kunne ikke finde location-id på varianterne")
        sys.exit(1)
    if stats['to_zero'] > MAX_TO_ZERO_SHARE * len(variants):
        log(f"❌ VÆRN: {stats['to_zero']} varianter ville gå til 0 (> {int(MAX_TO_ZERO_SHARE*100)} %). Afbryder uden ændringer.")
        sys.exit(1)
    if DRY_RUN:
        log("🟡 DRY RUN – ingen ændringer skrevet")
        return
    mutation = """mutation($input: InventorySetOnHandQuantitiesInput!){ inventorySetOnHandQuantities(input:$input){ userErrors{ field message } } }"""
    for i in range(0, len(updates), 100):
        batch = updates[i:i+100]
        try:
            d = gql(mutation, {'input': {'reason': 'correction', 'setQuantities': [
                {'inventoryItemId': u['inventoryItemId'], 'locationId': location_id, 'quantity': u['quantity']} for u in batch]}})
            errs = d['inventorySetOnHandQuantities']['userErrors']
            if errs:
                raise RuntimeError(errs)
            stats['updated'] += len(batch)
            log(f"📦 Batch {i//100+1}: {len(batch)} sat")
        except Exception as e:
            stats['errors'] += len(batch)
            log(f"❌ Batch {i//100+1} fejlede: {e}")
    log("=" * 50)
    log(f"EAN i fil {stats['csv_rows']} · varianter {stats['variants']} · ændret {stats['changed']} · opdateret {stats['updated']} · fejl {stats['errors']} · til 0 {stats['to_zero']} · mangler i fil {stats['missing_in_csv']}")
    if stats['errors']:
        sys.exit(1)
    log("✅ Ayyildiz lager-sync færdig")


if __name__ == '__main__':
    main()
