# Matrixify → Direct Shopify API — Migrationsplan

**Started:** 2026-06-02
**Owner:** Gabriel + Claude
**Status:** Foundation-fixes deployed (Sollux/Benuta retry + concurrency). Migration starter Dag 1.

---

## Bagrund

BoligRetning's e-commerce har 12 aktive scheduled workflows på tværs af to repos
(`vidaxl-pris-lager` og `dropxl-product-automation`). Af disse bruger 6 Matrixify
som CSV → Shopify-broker. De øvrige 6 kalder allerede Shopify GraphQL direkte.

Matrixify er bestemt til at blive udfaset for at:
- Spare $60-200/mo subscription
- Eliminere queue-collisions (Matrixify queue forsinker hourly-jobs)
- Få direkte kontrol over alle Shopify-write-flows
- Forberede HUB UI + manual bulk-edit tool

---

## Delmål

### Delmål 1 — Migrér de 6 aktive Matrixify-flows (prioriteret nu)

Order (lav-til-høj kompleksitet/risiko):
1. `sync_inventory.py` (VidaXL hourly, 100-18k ændringer pr. kørsel)
2. `daily_delete` (daglig, BEHOLD approval-gate)
3. `daily_create` (daglig, 7-12 produkter)
4. `daily_create_large` (daglig, 1-2 store produkter)
5. `sync_prices.py` (daglig, drops "159k-CSV append"-arkitekturen)
6. `rotate_groups.py` (månedlig, 50k SKU-mutations — HØJESTE RISIKO, gøres SIDST)

**Resultat når delmål 1 er nået:** Matrixify-subscription afmeldes helt.
Forventet timeline: ~1-2 uger.

### Delmål 2 — HUB UI + manual bulk-edit tool

Når alle scripts kører på direct API:
- Pricing/sale-konfig flyttes til Supabase `hub_settings` (delvist gjort)
- HUB-pages til at læse/skrive config + se job-status timeline
- Manual bulk-edit-tool der erstatter Matrixify til ad-hoc ændringer på eksisterende produkter

Estimat: 6-10 uger efter Delmål 1.

### Delmål 3 — Kayoom-integration (SFTP-baseret)

Kayoom leverer lager + pris via SFTP-server, ikke HTTPS-CSV.
- Tilføj `lib/sftp.py` (paramiko + retry, samme mønster som Sollux-retry)
- Nye scripts: `kayoom_inventory_sync.py`, `kayoom_price_sync.py`
- Samme downstream-arkitektur som VidaXL — kun "fetch CSV"-laget skiftes

Estimat: 1-2 uger efter Delmål 2 (eller parallelt hvis tid).

---

## Migrations-mønster pr. flow

For HVERT flow i Delmål 1, samme proces:

### Trin 1: Byg nyt script med `--dry-run`-mode

```python
# Nyt script har EXAKT samme interface som gammelt
# I --dry-run mode: skriv til output/new_<flow>.csv i SAMME format som gammelt
# I --live mode: kald Shopify GraphQL direkte, INGEN CSV-emission
```

### Trin 2: Workflow kører BÅDE gammelt og nyt (parallel-step)

```yaml
- name: Old script (still active)
  run: python scripts/sync_inventory.py
- name: New script (dry-run validation)
  run: python scripts/sync_inventory_v2.py --dry-run
- name: Compare outputs
  run: python scripts/_compare_csvs.py output/inventory_updates.csv output/new_inventory.csv
```

### Trin 3: Verificér dry-run match

Diff-script normaliserer (sortér rækker, normalisér floats, ignorér Matrixify-
specifikke kolonner) og fejler workflow hvis CSV'erne adskiller sig.

Når én run er identisk → klar til flip.

### Trin 4: Cutover

I samme workflow-PR:
- Slet old-script step
- Skift new-script til `--live` mode
- Slet CSV-commit step (intet behov for at gemme CSV længere)
- Gå ind i Matrixify UI og slet/pause scheduled import for det flow

### Trin 5: Validér én live-kørsel

- Watch næste planlagte run i GitHub Actions
- Spotcheck 2-3 SKUs i Shopify Admin — bekræft pris/lager opdateret som forventet
- Hvis OK → gå videre til næste flow
- Hvis NOT OK → revert workflow, debug

---

## Forward-compatibility — pr. flow disciplin

Under hver migration følger vi 3 strukturelle valg så HUB UI og Kayoom kommer let:

### 1. Adskil "fetch / transform / push"-lagene

```python
# === FETCH (Kayoom kommer på SFTP — kun denne funktion skiftes senere) ===
def fetch_supplier_data() -> pd.DataFrame: ...

# === TRANSFORM (genbrugeligt på tværs af suppliers) ===
def compute_diff(supplier_df, shop_state) -> list[Operation]: ...

# === PUSH (Niveau 2 GraphQL) ===
def push_to_shopify(ops: list[Operation], dry_run: bool) -> Result: ...
```

### 2. Magic numbers → `CONFIG` dict øverst

```python
CONFIG = {
    "batch_size": 100,
    "rate_limit_points_per_sec": 50,
    "max_retries": 3,
    # Senere (HUB): load_from_supabase("hub_settings.product_automation_X")
}
```

### 3. Pre-write snapshot pr. write-operation

```python
def snapshot(operations, label) -> Path:
    """Dump current Shopify state for affected SKUs to snapshots/<label>-<ts>.json
    Bruges som rollback-grundlag hvis noget går galt mid-run."""
```

---

## Per-flow noter

### sync_inventory.py (Dag 1)
- **Ny dependency:** SKU → inventory_item_id mapping cache
  - Udvid `update_shop_cache.py` til ALSO at cache `inventory_item_id` per SKU
  - Kører 2× daglig allerede, lille ekstra GraphQL-query
- **API:** `inventoryAdjustQuantities` (op til 250 ændringer pr. kald, Niveau 2)
- **Idempotens:** Brug `available` setQty mode (ikke delta) så re-run er sikkert

### daily_delete (Dag 2)
- **API:** `productDelete` pr. SKU
- **KRITISK:** Behold workflow's `manual-approval`-step UÆNDRET — det er på workflow-niveau, ikke i scriptet
- **Volumen:** 30-200 produkter typisk, ~60 sek total

### daily_create + daily_create_large (Dag 3-4)
- **MEST KODE** — orchestrér 4-5 mutations pr. produkt:
  - `productCreate`
  - `productVariantsBulkCreate`
  - `productCreateMedia` (billeder)
  - `metafieldsSet`
  - `collectionAdd`
- **CSV-compare svært:** XLSX-format med flere felter. Spotcheck 3-5 produkter manuelt i Shopify efter flip i stedet for fuld byte-diff.
- **Volumen:** Lille (7-12 produkter daglig), så hver kan tage 30 sek og det er fint

### sync_prices.py (Dag 5)
- **Arkitektur-fix samtidig:** Drop "append-to-159k-CSV"-mønstret. Compute kun dagens delta (50-500 rækker), push direkte.
- **API:** `productVariantsBulkUpdate` (250 pr. kald)
- **Drift-risiko:** State-fil kan afvige fra Shopify. Mitigér med weekly reconciliation cron (læser alle priser fra Shopify, opdaterer state).

### rotate_groups.py (Dag 6, SIDSTE)
- **HØJESTE RISIKO:** 50.000 SKU-mutations på én bras
- **Failure-scenario:** Halvdelen af A-gruppe normal, halvdelen stadig sale → kunder ser inkonsistent pricing
- **Krav før migration:**
  - Pre-flight snapshot af alle 50k SKUs' status
  - Idempotent execution (re-run kan resume)
  - Staged batching med checkpoint hver 1.000 SKUs
  - Slack-alert ved hver checkpoint
- **Volumen:** ~50k variant-updates, ~5-10 min total kørsel

---

## Tidslinje

| Dag | Fokus | Output |
|---|---|---|
| 1 | Extend `update_shop_cache.py` + migrate `sync_inventory.py` | Inventory direct |
| 2 | Migrate `daily_delete.py` (approval-gate uændret) | Delete direct |
| 3 | Migrate `daily_create.py` | Create direct |
| 4 | Migrate `daily_create_large.py` | Large-create direct |
| 5 | Migrate `sync_prices.py` + arkitektur-cleanup | Prices direct, ren delta |
| 6 | Migrate `rotate_groups.py` med staged batching + snapshot | Rotation direct |
| 7 | Cancel Matrixify subscription, cleanup døde files | Delmål 1 done |

---

## Vigtige beslutninger (truffet 2026-06-02)

- **Validering = CSV-file compare** (ikke shadow-period mod Shopify). Hurtigt, simpelt, rigorøst nok.
- **Mixed Niveau 2/3:** Kun `update_shop_cache` (read 165k SKUs) + `fuld_lager_sync` bruger Niveau 3 Bulk Operations. Resten kører Niveau 2 (batched array-mutations).
- **Repos forbliver adskilte:** `vidaxl-pris-lager` og `dropxl-product-automation` migreres parallelt, ikke konsolideret.
- **rotate_groups som sidste step:** Trods at det er den højst-risiko mutation, prioriteres det sidst så vi har 5 succesfulde migrations som template først.
- **Kayoom HOLDES UDE indtil Delmål 1 er færdig.** Strukturen forbereder dog Kayoom-on-SFTP via fetch/transform/push-adskillelse.

---

## Rollback-plan pr. flow

Hvis noget går galt EFTER flip til `--live`:

1. **Inventory:** State-fil i git → revert til forrige commit, kør gammelt script én gang
2. **Prices:** Snapshot JSON pre-write → script til at reverse-apply snapshot
3. **Deletes:** Snapshot var produkt-data → re-create via API (kompliceret men muligt)
4. **Creates:** Identificér nye produkt-IDs fra log → bulk-delete dem
5. **Rotation:** Pre-flight snapshot → reverse rotation til forrige status

Hver rollback er manuel (ikke automatisk). Auto-rollback er bygget i Delmål 2 (HUB).

---

## Open questions

- [ ] Skal `rotate_groups`-migrationen vente til JULI (efter første rotation gennem Matrixify), eller migreres FØR næste rotation? Default: vent.
- [ ] Skal weekly reconciliation cron bygges som del af `sync_prices`-migration eller separat? Default: separat (efter Delmål 1).
- [ ] Skal Slack-summary pr. successful run bygges som del af hver migration, eller bulk i en separat fase? Default: pr. migration (lille marginal kost).
