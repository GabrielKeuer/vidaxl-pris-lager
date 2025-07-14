# VidaXL Pris & Lager Sync

Automatisk synkronisering af priser og lager fra VidaXL til Shopify via Matrixify.

## Oversigt

- **Shop Cache**: Opdateres 2x dagligt (kl. 02:00 og 14:00)
- **Pris Sync**: Kører dagligt kl. 10:00
- **Lager Sync**: Kører hver time (45 min over)

## Output Filer

Matrixify URLs:
- Priser: `https://raw.githubusercontent.com/gabrielkeuer/vidaxl-pris-lager/main/output/price_updates.csv`
- Lager: `https://raw.githubusercontent.com/gabrielkeuer/vidaxl-pris-lager/main/output/inventory_updates.csv`

## Setup

1. Opret repo secret: `SHOPIFY_ACCESS_TOKEN`
2. Kør "Update Shop Cache" workflow manuelt første gang
3. Opsæt Matrixify scheduled imports med ovenstående URLs

## Workflows

- `update_shop_cache.yml` - Henter alle SKUs fra Shopify
- `sync_prices.yml` - Finder prisændringer
- `sync_inventory.yml` - Finder lagerændringer

## Logs

Se GitHub Actions tab for kørselslogs og fejl.


## 📦 Lager Sync Scripts

### Fuld Webshop Sync
- **Script**: `fuld_lager_sync_shopify_vidaxl.py`
- **Formål**: Synkroniserer HELE webshoppens lager med VidaXL
- **Output**: Matrixify CSV klar til import
- **Kør**: Via GitHub Actions → "Fuld Webshop Lager Sync"

### Daglig Pris/Lager Sync
- **Script**: `sync_vidaxl_direct.py`
- **Formål**: Daglig sync af ændringer
- **Kør**: Automatisk hver nat kl. 3

### Cache SKUs
- **Script**: `cache_shop_skus.py`
- **Formål**: Henter alle SKUs fra Shopify
