"""B: AKTIVÉR vidaXL inventory-items der IKKE er stocked på lager-lokationen (inventoryActivate) med deres
rigtige feed-lager. Self-contained (eget Shopify-token + feed-download) → kører i GitHub Actions med
write_inventory-secret. Scanner FRISKT: læser shop_skus.json (cache: sku→inventory_item_id + location),
finder items uden inventoryLevel på lokationen, aktiverer dem. Idempotent + re-runnable.
Default DRY-RUN; --live udfører. KRÆVER: SHOPIFY_STORE_URL, SHOPIFY_ACCESS_TOKEN (write_inventory), FEED_URL."""
import os, io, sys, json, csv, time, zipfile, argparse
import requests

sys.stdout.reconfigure(encoding="utf-8")
STORE = (os.environ.get("SHOPIFY_STORE_URL") or "b7916a-38.myshopify.com").replace("https://", "").rstrip("/")
TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN")
GRAPHQL = f"https://{STORE}/admin/api/2024-10/graphql.json"

def gql(query, variables=None):
    if not TOKEN:
        sys.exit("❌ SHOPIFY_ACCESS_TOKEN mangler")
    headers = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}
    for attempt in range(1, 7):
        r = requests.post(GRAPHQL, headers=headers, json={"query": query, "variables": variables or {}}, timeout=120)
        if r.status_code != 200:
            raise Exception(f"HTTP {r.status_code}: {r.text[:300]}")
        d = r.json()
        if "errors" in d:
            if any("Throttled" in str(e).upper() for e in d["errors"]) and attempt < 6:
                time.sleep(2 ** attempt); continue
            raise Exception(f"GraphQL errors: {d['errors']}")
        cost = d.get("extensions", {}).get("cost", {}).get("throttleStatus", {})
        if cost.get("currentlyAvailable", 1000) < 200:
            time.sleep(0.5)
        return d
    raise Exception("Max retries")

FEED_DEFAULT = ("https://feed.vidaxl.io/api/v1/feeds/download/"
                "f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping_offer.csv")

def load_feed_stock():
    url = os.environ.get("FEED_URL") or FEED_DEFAULT
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=300); r.raise_for_status()
    if url.endswith(".zip") or r.content[:2] == b"PK":
        z = zipfile.ZipFile(io.BytesIO(r.content))
        nm = [f for f in z.namelist() if f.endswith(".csv")][0]
        reader = csv.DictReader(io.TextIOWrapper(z.open(nm), encoding="utf-8"))
    else:
        reader = csv.DictReader(io.StringIO(r.content.decode("utf-8", "replace")))
    stock = {}
    for row in reader:
        s = str(row.get("SKU") or "").strip().replace(".0", "")
        if s:
            try: stock[s] = int(float(row.get("Stock") or row.get("stock") or 0))
            except (ValueError, TypeError): stock[s] = 0
    return stock

NODES = ('query($ids:[ID!]!,$loc:ID!){nodes(ids:$ids){... on InventoryItem{id '
         'variant{sku} inventoryLevel(locationId:$loc){id}}}}')
ACT = ('mutation($id:ID!,$loc:ID!,$avail:Int){inventoryActivate(inventoryItemId:$id,locationId:$loc,'
       'available:$avail){inventoryLevel{id} userErrors{field message}}}')

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    a = ap.parse_args()

    cache = json.load(open("output/shop_skus.json", encoding="utf-8"))
    inv = cache["inventory_items"]   # sku -> inventory_item_id
    loc = str(cache["location_id"]); locgid = f"gid://shopify/Location/{loc}"
    print(f"cache: {len(inv)} inventory-items | location {loc} | {'LIVE' if a.live else 'DRY-RUN'}", flush=True)

    stock = load_feed_stock()
    print(f"feed-lager: {len(stock)} SKU", flush=True)

    # FRISK scan: hvilke items har intet inventoryLevel på lokationen?
    items = [(s, str(i)) for s, i in inv.items()]
    notstocked = []; checked = 0
    for k in range(0, len(items), 200):
        chunk = items[k:k + 200]
        ids = [f"gid://shopify/InventoryItem/{i}" for _, i in chunk]
        try:
            d = gql(NODES, {"ids": ids, "loc": locgid})
        except Exception as e:
            print(f"  scan-fejl @ {k}: {str(e)[:100]}", flush=True); continue
        for n, (sku, iid) in zip((d.get("data") or {}).get("nodes") or [], chunk):
            checked += 1
            if n is not None and n.get("inventoryLevel") is None:
                vsku = (n.get("variant") or {}).get("sku") or sku
                notstocked.append((vsku, iid))
        if (k // 200) % 50 == 0:
            print(f"  …{checked} scannet, {len(notstocked)} not-stocked", flush=True)
    print(f"\nSCAN: {checked} tjekket → {len(notstocked)} not-stocked på lokationen", flush=True)

    if not a.live:
        for s, iid in notstocked[:15]:
            print(f"   SKU {s} inv {iid} → available={stock.get(s, 0)}")
        print("\n(dry-run — intet aktiveret. --live for at udføre.)")
        return

    ok = already = fail = 0
    for i, (s, iid) in enumerate(notstocked, 1):
        q = stock.get(s, 0)
        try:
            d = gql(ACT, {"id": f"gid://shopify/InventoryItem/{iid}", "loc": locgid, "avail": q})
            errs = ((d.get("data") or {}).get("inventoryActivate") or {}).get("userErrors") or []
            if errs:
                msg = str(errs[0].get("message", "")).lower()
                if "already" in msg or "stocked" in msg: already += 1
                else: fail += 1; print(f"   ✗ SKU {s}: {errs[0]}", flush=True)
            else: ok += 1
        except Exception as e:
            fail += 1; print(f"   ✗ SKU {s}: {str(e)[:120]}", flush=True)
        if i % 100 == 0:
            print(f"   … {i}/{len(notstocked)} (ok={ok}, already={already}, fail={fail})", flush=True)
    print(f"\n=== FÆRDIG: aktiveret={ok}, allerede-aktiv={already}, fejl={fail} ===", flush=True)
    if fail:
        sys.exit(1)

if __name__ == "__main__":
    main()
