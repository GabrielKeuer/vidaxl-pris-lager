#!/usr/bin/env python3
"""
Tæppetorvet: lager-sync + lagermærker (taeppetorvet.dk, egen Shopify-butik).
1) Henter adgangstoken med butikkens app-nøgler (client credentials; token lever 24 t, hentes ved hver kørsel).
2) Kører PRÆCIS samme lager-sync som BoligRetning (scripts/sync_ayyildiz_inventory.py: Bestand_aktuell.csv → lager pr. EAN,
   værn mod ødelagt fil og massenul) – blot mod Tæppetorvets butik. BoligRetnings job og script er urørt.
3) Opdaterer lagermærkerne tt-form-*, tt-str-*, tt-farve-* pr. produkt ud fra hvilke varianter der er på lager.
   Kollektionerne Runde, Løbere, størrelser og farver bygger på mærkerne, så et tæppe forsvinder fra fx "160x230",
   når størrelsen er udsolgt i alle farver (Omar 18/11-2026: "vigtigt mærkerne bliver opdateret med lageropdateringen").
Miljø: TT_SHOPIFY_SHOP, TT_SHOPIFY_CLIENT_ID, TT_SHOPIFY_CLIENT_SECRET (GitHub Secrets), DRY_RUN=true for kun at rapportere.
"""
import os, sys, re, time
import requests

SHOP = os.environ.get('TT_SHOPIFY_SHOP', '').replace('https://', '').rstrip('/')
DRY_RUN = os.environ.get('DRY_RUN', '').lower() in ('1', 'true', 'yes')
FARVER = {'beige': ['Beige', 'Natur', 'Beige/Sort'], 'creme': ['Creme', 'Hvid'], 'graa': ['Grå', 'Lysegrå', 'Sølv', 'Antracit'],
          'brun': ['Brun', 'Mokka', 'Taupe', 'Kobber', 'Terracotta'], 'groen': ['Grøn'], 'sort': ['Sort'],
          'rosa': ['Rosa', 'Pink'], 'blaa': ['Blå', 'Marineblå', 'Turkis']}
FORM = {'Rund': 'rund', 'Løber': 'loeber', 'Rektangulær': 'rektangulaer'}


def hent_token():
    cid, sec = os.environ.get('TT_SHOPIFY_CLIENT_ID'), os.environ.get('TT_SHOPIFY_CLIENT_SECRET')
    if not (SHOP and cid and sec):
        print('❌ Mangler TT_SHOPIFY_SHOP / TT_SHOPIFY_CLIENT_ID / TT_SHOPIFY_CLIENT_SECRET', flush=True)
        sys.exit(1)
    r = requests.post(f'https://{SHOP}/admin/oauth/access_token', data={'grant_type': 'client_credentials', 'client_id': cid, 'client_secret': sec}, timeout=60)
    r.raise_for_status()
    return r.json()['access_token']


def gql(token, query, variables=None):
    for forsoeg in range(5):
        r = requests.post(f'https://{SHOP}/admin/api/2025-07/graphql.json', json={'query': query, 'variables': variables or {}},
                          headers={'Content-Type': 'application/json', 'X-Shopify-Access-Token': token}, timeout=60)
        d = r.json() if r.status_code < 500 else {}
        if r.status_code >= 500 or any((e.get('extensions') or {}).get('code') == 'THROTTLED' for e in d.get('errors', [])):
            time.sleep(3 * (forsoeg + 1))
            continue
        if 'errors' in d:
            raise RuntimeError(d['errors'])
        return d['data']
    raise RuntimeError('Shopify svarede ikke efter 5 forsøg')


def maerker(token):
    q = """query($c:String){ products(first:40, after:$c){ pageInfo{ hasNextPage endCursor }
      nodes{ id handle tags variants(first:250){ nodes{ availableForSale selectedOptions{ name value } } } } } }"""
    cur, n, aendret, ud = None, 0, 0, 0
    while True:
        d = gql(token, q, {'c': cur})['products']
        for p in d['nodes']:
            n += 1
            ny = set()
            for v in p['variants']['nodes']:
                if not v['availableForSale']:
                    continue
                o = {x['name']: x['value'] for x in v['selectedOptions']}
                if o.get('Form') in FORM:
                    ny.add('tt-form-' + FORM[o['Form']])
                m = re.match(r'^(\d+x\d+)', o.get('Størrelse') or '')
                if m:
                    ny.add('tt-str-' + m.group(1))
                for k, l in FARVER.items():
                    if o.get('Farve') in l:
                        ny.add('tt-farve-' + k)
            gl = [t for t in p['tags'] if t.startswith('tt-')]
            til, fra = sorted(t for t in ny if t not in gl), sorted(t for t in gl if t not in ny)
            if not til and not fra:
                continue
            aendret += 1
            ud += len(fra)
            print(f"   {p['handle']}: +{','.join(til) or '-'}  −{','.join(fra) or '-'}", flush=True)
            if DRY_RUN:
                continue
            if til:
                gql(token, 'mutation($id:ID!,$t:[String!]!){ tagsAdd(id:$id,tags:$t){ userErrors{ message } } }', {'id': p['id'], 't': til})
            if fra:
                gql(token, 'mutation($id:ID!,$t:[String!]!){ tagsRemove(id:$id,tags:$t){ userErrors{ message } } }', {'id': p['id'], 't': fra})
        if not d['pageInfo']['hasNextPage']:
            break
        cur = d['pageInfo']['endCursor']
    print(f"🏷️  Lagermærker: {n} produkter, {aendret} ændret, {ud} mærker fjernet" + (' (DRY RUN)' if DRY_RUN else ''), flush=True)


def main():
    token = hent_token()
    os.environ['SHOPIFY_STORE_URL'] = SHOP
    os.environ['SHOPIFY_ACCESS_TOKEN'] = token
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import sync_ayyildiz_inventory as lager  # læser miljøet ved import – derfor først nu
    kode = 0
    try:
        lager.main()
    except SystemExit as e:
        kode = e.code or 0
    if not DRY_RUN and not kode:
        time.sleep(20)  # giv Shopify tid til at opdatere "kan købes" på varianterne
    maerker(token)
    sys.exit(kode)


if __name__ == '__main__':
    main()
